package parallel

import (
	"circular/graph"
	rebalance2 "circular/rebalance"
	"errors"
	"math/bits"
	"sort"

	"github.com/elementsproject/glightning/glightning"
)

// A probe-only run (probeonly=true) is circular-pull or circular-push with
// probes in place of payments: payments nobody can settle, so nothing is
// paid and nothing moves. It searches as a run does (the candidates in their
// order, splits at once, each with its attempts, the fail streak that ends a
// run), so its "nothing carried" is as good as a run moving nothing. A split
// succeeds when its probe arrives; its route is then sized (how much of what
// is left it carries, in whole splits), that counts toward the amount, and
// the candidate is not fired again: nothing moved, so the same route would
// carry the same again. The run ends once the amount is covered, as a run
// that moved it.

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
// to (msat): what is left of the amount, up to the candidate's room.
func (r *AbstractRebalance) probeWant(candidate *graph.Channel) uint64 {
	left := subSaturating(r.amount, r.AmountRebalanced)
	m, ok := r.RebalanceMethods.(interface {
		room(*glightning.PeerChannel) uint64
	})
	if !ok {
		return left
	}
	if peerChannel, err := r.Node.GetPeerChannelFromGraphChannel(candidate); err == nil {
		if room := m.room(peerChannel); room < left {
			left = room
		}
	}
	return left
}

// probeSucceeded takes a probe-only split's success: what it carried counts
// up to what is left of the amount (two splits at once may each have sized
// their route to all of it), and its route is kept for the next run between
// the two channels (Node.RememberRoute).
func (r *AbstractRebalance) probeSucceeded(result *rebalance2.Result) {
	if left := subSaturating(r.amount, r.AmountRebalanced); result.AmountMsat > left {
		// the fee of the part that counts: fee * left / carried
		hi, lo := bits.Mul64(result.Fee, left)
		result.Fee, _ = bits.Div64(hi, lo, result.AmountMsat) // left < AmountMsat: no overflow
		result.AmountMsat, result.Amount = left, left/1000
	}
	if candidate, ok := r.launched[result.Candidate]; ok && r.TargetChannel != nil && len(result.Channels) > 0 {
		out, in := r.pair(candidate)
		r.Node.RememberRoute(out, in, result.Channels)
	}
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
