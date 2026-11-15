package parallel

import (
	"circular/graph"
	rebalance2 "circular/rebalance"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/elementsproject/glightning/glightning"
)

// A probe-only run (probeonly=true) is circular-pull or circular-push with
// probes in place of payments: payments nobody can settle, so nothing is
// paid and nothing moves. It searches as a run does (the candidates in their
// order, splits at once, each with its attempts, the fail streak that ends a
// run), so its "nothing carried" is as good as a run moving nothing.
//
// A split succeeds when its probe arrives; its route is then sized (how much
// of what is left it carries, in whole splits). Nothing moved, so the same
// route would carry the same again: what a route carried counts once, and
// the run's later searches leave out its channels (not our own), so that
// whatever they find is on top of it. The candidate is fired again, as in a
// run, for another route, while it has room and the amount is not covered.
// The run ends once the amount is covered, as a run that moved it.

// errProbeOnlyRounds refuses probeonly with slow mode: a second round would
// find the same routes again, and count them again.
var errProbeOnlyRounds = errors.New("probeonly runs one round: it cannot go with retryevery or stepppm")

// setProbeOnly makes the run probe-only. Call it after setStepping and
// setMaxFee: there is no fee budget, as nothing is paid.
func (r *AbstractRebalance) setProbeOnly(on bool) error {
	r.probeOnly = on
	if !on {
		return nil
	}
	if r.retryEvery > 0 || r.stepPPM > 0 {
		return errProbeOnlyRounds
	}
	r.maxFeeMsat = 0
	return nil
}

// probeWant is how much a probe-only split over candidate sizes its route up
// to (msat): what is left of the amount, up to the room the candidate has
// left after what its routes already carried.
func (r *AbstractRebalance) probeWant(candidate *graph.Channel) uint64 {
	left := subSaturating(r.amount, r.AmountRebalanced)
	m, ok := r.RebalanceMethods.(interface {
		room(*glightning.PeerChannel) uint64
	})
	if !ok {
		return left
	}
	if peerChannel, err := r.Node.GetPeerChannelFromGraphChannel(candidate); err == nil {
		if room := subSaturating(m.room(peerChannel), r.probeCarried[candidateId(candidate)]); room < left {
			left = room
		}
	}
	return left
}

// probeExclude is what a probe-only split's searches leave out: the channels
// of the routes the run already counted.
func (r *AbstractRebalance) probeExclude() []string {
	ids := make([]string, 0, len(r.probeProven))
	for id := range r.probeProven {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// probeSucceeded takes a probe-only split's success, and says whether what
// its route carried counts: not when the amount is already covered, nor
// when the route shares a channel with one already counted (two splits at
// once may find the same way: nothing moved, so it would be counted twice).
// What counts is trimmed to what is left of the amount, its channels are
// left out of the run's later searches, and the first route of a pair of
// channels is kept for the next run between them (Node.RememberRoute). The
// candidate is fired again for another route while the amount is not covered
// and it has room for a split.
func (r *AbstractRebalance) probeSucceeded(result *rebalance2.Result) bool {
	left := subSaturating(r.amount, r.AmountRebalanced)
	var middle []string // the route's channels, without our own two
	if n := len(result.Channels); n > 2 {
		middle = result.Channels[1 : n-1]
	}
	shared := false
	for _, id := range middle {
		shared = shared || r.probeProven[id]
	}
	candidate, known := r.launched[result.Candidate]
	if left == 0 || shared || result.AmountMsat == 0 {
		result.AmountMsat, result.Amount, result.Fee = 0, 0, 0
		if shared && left > 0 {
			r.requeue(result.Candidate, true) // its next search leaves those channels out
		}
		return false
	}
	if result.AmountMsat > left {
		// the fee of the part that counts: fee * left / carried
		hi, lo := bits.Mul64(result.Fee, left)
		result.Fee, _ = bits.Div64(hi, lo, result.AmountMsat) // left < AmountMsat: no overflow
		result.AmountMsat, result.Amount = left, left/1000
	}
	if r.probeProven == nil {
		r.probeProven, r.probeCarried = map[string]bool{}, map[string]uint64{}
	}
	for _, id := range middle {
		r.probeProven[id] = true
	}
	first := r.probeCarried[result.Candidate] == 0
	r.probeCarried[result.Candidate] += result.AmountMsat
	if known && r.TargetChannel != nil {
		if first {
			out, in := r.pair(candidate)
			r.Node.RememberRoute(out, in, result.Channels)
		}
		// (a route with no channel but our own two would be found again)
		if len(middle) > 0 && left > result.AmountMsat && r.probeWant(candidate) >= r.splitAmount {
			r.requeue(result.Candidate, true)
		}
	}
	return true
}

// probeRanOut is ranOut for a probe-only run: the candidates whose routes
// carried something are not among those that failed, though their last
// search found no further route.
func (r *AbstractRebalance) probeRanOut() string {
	carried := 0
	for id, msat := range r.probeCarried {
		if msat > 0 {
			carried++
			delete(r.gone, id)
		}
	}
	rest := r.ranOut()
	if carried == 0 {
		return rest
	}
	out := fmt.Sprintf("every candidate was probed: %d carried what their routes can", carried)
	if len(r.gone) > 0 {
		out += "; " + strings.TrimPrefix(rest, "no candidates left: ")
	}
	return out
}

// probeResult turns the run's tallies into a probe-only result: nothing
// moved and nothing was paid, so the fields that say what a run moved and
// paid are zero, and what the probes found has names of its own.
func (r *AbstractRebalance) probeResult() {
	res := r.Result
	res.Carried, res.Successes = res.Successes, map[string]*Success{}
	res.CarriedAmount, res.RebalancedAmount = res.RebalancedAmount, 0
	res.CarriedFeeMsat, res.FeesPaidMsat = res.FeesPaidMsat, 0
	res.CarriedAvgPPM, res.AvgPPM = res.AvgPPM, 0
}

// pair is the out and in channel (short channel ids) of a split over
// candidate: the candidate and the target, in the order the payment takes
// them.
func (r *AbstractRebalance) pair(candidate *graph.Channel) (string, string) {
	if candidate.Source == r.Node.Id {
		return candidate.ShortChannelId, r.TargetChannel.ShortChannelId
	}
	return r.TargetChannel.ShortChannelId, candidate.ShortChannelId
}

// provenFirst puts the candidates with a route a probe-only run found to
// the target (Node.ProvenRoute) before the others, each group in its order.
func (r *AbstractRebalance) provenFirst(ids []string, found map[string]*graph.Channel) []string {
	if r.TargetChannel == nil {
		return ids
	}
	proven := func(id string) bool {
		out, in := r.pair(found[id])
		return r.Node.ProvenRoute(out, in, false) != nil
	}
	sort.SliceStable(ids, func(i, j int) bool { return proven(ids[i]) && !proven(ids[j]) })
	return ids
}
