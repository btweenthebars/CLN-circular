package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/elementsproject/glightning/glightning"
)

// MAX_PPM_LIMIT bounds what a fee ratio can come to (a channel that earned
// at an absurd rate)
const MAX_PPM_LIMIT = 1000000000

// notWorthIt is why a candidate is skipped when, at the run's feeratio, what
// the filled channel earns does not cover our fee on the drained one.
const notWorthIt = "not worth it at feeratio"

// earning is what a channel's liquidity earns, in ppm, and on what basis.
type earning struct {
	ppm   uint64
	basis string // "earned": its forwards; "current fee": too few forwards
}

// economics is what a split over a candidate may pay: maxppm, or with a fee
// ratio at most ratio × what the filled channel earns − our fee on the
// drained one.
type economics struct {
	limit uint64 // ppm; 0: not worth it
	earn  earning
	drain uint64 // our fee on the drained channel, ppm
	// most is, with a fee ratio, the most a split over the candidate could
	// pay at any maxppm (ratio × earned − drain; limit is it, capped at
	// maxppm); 0 without a ratio, or when not worth it
	most uint64
}

// why says how a fee ratio came to the limit, for a split's messages:
// "feeratio 0.9 × 1300 ppm earned by the filled channel − 0 ppm our fee on
// the drained one".
func (e economics) why(ratio float64) string {
	earned := fmt.Sprintf("%d ppm earned by the filled channel", e.earn.ppm)
	if e.earn.basis != "earned" {
		earned = fmt.Sprintf("%d ppm, our fee on the filled channel (too few forwards to go by),", e.earn.ppm)
	}
	return fmt.Sprintf("feeratio %g × %s − %d ppm our fee on the drained one", ratio, earned, e.drain)
}

func (e economics) String() string {
	return fmt.Sprintf("the filled channel earns %d ppm (%s), the drained one charges %d ppm: limit %d ppm",
		e.earn.ppm, e.earn.basis, e.drain, e.limit)
}

// setFeeRatio sets the run's fee ratio: 0, the node's default; below 0, off.
func (r *AbstractRebalance) setFeeRatio(ratio float64) {
	defaults := r.Node.GetDefaults()
	if ratio == 0 {
		ratio = defaults.FeeRatio
	}
	r.feeRatio = 0
	if ratio > 0 && !math.IsInf(ratio, 0) {
		r.feeRatio = ratio
	}
	days := defaults.EarnDays
	if days <= 0 {
		days = node.DEFAULT_EARN_DAYS
	}
	r.earnSince = time.Now().Add(-time.Duration(days) * 24 * time.Hour)
}

// ourFeePPM is the fee we charge on our channel scid to peer, in ppm of a
// split; false if the graph does not know it.
func (r *AbstractRebalance) ourFeePPM(scid, peer string) (uint64, bool) {
	ours, err := r.Node.Graph.GetChannel(scid + "/" + util.GetDirection(r.Node.Id, peer))
	if err != nil {
		return 0, false
	}
	return ours.ComputeFeePPM(r.splitAmount), true
}

// earns is what our liquidity in the channel scid with peer earns: the rate
// its forwards out paid since earnSince, or, if they carried less than
// node.EARN_MIN_VOLUME_MSAT, our current fee on it. Read once per run.
func (r *AbstractRebalance) earns(scid, peer string) earning {
	if e, ok := r.earnings[scid]; ok {
		return e
	}
	ppm, volume, err := r.readForwards()(scid, r.earnSince)
	return r.keepEarning(scid, peer, ppm, volume, err)
}

// readForwards reads what a channel earned: Node.Earned, or a test's fake.
func (r *AbstractRebalance) readForwards() func(scid string, since time.Time) (uint64, uint64, error) {
	if r.forwards != nil {
		return r.forwards
	}
	return r.Node.Earned
}

// keepEarning keeps, for the run, what the channel scid with peer earns
// from what reading its forwards gave.
func (r *AbstractRebalance) keepEarning(scid, peer string, ppm, volume uint64, err error) earning {
	e := earning{basis: "current fee"}
	switch {
	case err != nil:
		r.Node.Logln(glightning.Unusual, "unable to read the forwards of ", scid, ": ", err)
	case volume >= node.EARN_MIN_VOLUME_MSAT:
		e = earning{ppm: ppm, basis: "earned"}
	}
	if e.basis != "earned" {
		e.ppm, _ = r.ourFeePPM(scid, peer)
	}
	if r.earnings == nil {
		r.earnings = map[string]earning{}
	}
	r.earnings[scid] = e
	return e
}

// splitEconomics is what a split over candidate may pay. In circular-pull
// the candidate is drained and the target filled; in circular-push the
// other way round.
func (r *AbstractRebalance) splitEconomics(candidate *graph.Channel) economics {
	if r.feeRatio <= 0 {
		return economics{limit: r.maxPPM}
	}
	filled, drained := r.TargetChannel, candidate
	if candidate.Source != r.Node.Id {
		filled, drained = candidate, r.TargetChannel
	}
	// filled is a channel to us, drained one from us
	e := economics{earn: r.earns(filled.ShortChannelId, filled.Source)}
	e.drain, _ = r.ourFeePPM(drained.ShortChannelId, drained.Destination)
	limit := r.feeRatio*float64(e.earn.ppm) - float64(e.drain)
	switch {
	case !(limit >= 1): // NaN too
		return e
	case limit > MAX_PPM_LIMIT:
		e.most = MAX_PPM_LIMIT
	default:
		e.most = uint64(limit)
	}
	e.limit = e.most
	if e.limit > r.maxPPM {
		e.limit = r.maxPPM
	}
	return e
}

// notWorth says why a split over candidate is not worth it at the run's fee
// ratio, or "" if it is: its limit comes to nothing, or the peer sending to
// us charges more than that limit on its own (when the limit is below the
// run's maxppm; at maxppm that is the channel refusing, as without a ratio).
func (r *AbstractRebalance) notWorth(candidate *graph.Channel) (economics, string) {
	e := r.splitEconomics(candidate)
	if r.feeRatio <= 0 {
		return e, ""
	}
	if e.limit == 0 {
		return e, e.String()
	}
	if e.limit < r.maxPPM {
		in, out := r.TargetChannel, candidate
		if candidate.Source != r.Node.Id {
			in, out = candidate, r.TargetChannel
		}
		inFee := r.Node.Graph.LeastForwardFee(in, out, r.splitAmount)
		// above the run's maxppm too, it is the channel refusing, as without a
		// ratio: the split finds out, and the run says so
		if inFee > graph.MaxFeeForPPM(r.splitAmount, e.limit) && inFee <= graph.MaxFeeForPPM(r.splitAmount, r.maxPPM) {
			return e, fmt.Sprintf("%s charges at least %d ppm to send to us, more than this candidate's limit: %s",
				r.Node.Graph.GetAlias(in.Source), avgPPM(inFee, r.splitAmount), e)
		}
	}
	return e, ""
}

// FORWARD_READS is how many channels' forwards prefetchEarnings reads at
// once.
const FORWARD_READS = 8

// prefetchEarnings reads, before the run fires anything, what each channel
// it would fill earns: the target in circular-pull, every candidate in
// circular-push. Reading them as splits are fired held up the run.
func (r *AbstractRebalance) prefetchEarnings() {
	if r.feeRatio <= 0 || r.TargetChannel == nil {
		return
	}
	type read struct {
		scid, peer  string
		ppm, volume uint64
		err         error
	}
	var reads []*read
	if r.TargetChannel.Destination == r.Node.Id {
		reads = append(reads, &read{scid: r.TargetChannel.ShortChannelId, peer: r.TargetChannel.Source})
	} else {
		r.QueueLock.Lock()
		for i := 0; i < r.Candidates.Len(); i++ {
			c := r.Candidates.At(i)
			reads = append(reads, &read{scid: c.ShortChannelId, peer: c.Source})
		}
		r.QueueLock.Unlock()
	}
	forwards, since := r.readForwards(), r.earnSince
	slots := make(chan struct{}, FORWARD_READS)
	var wg sync.WaitGroup
	for _, rd := range reads {
		wg.Add(1)
		go func(rd *read) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			rd.ppm, rd.volume, rd.err = forwards(rd.scid, since)
		}(rd)
	}
	wg.Wait()
	for _, rd := range reads {
		r.keepEarning(rd.scid, rd.peer, rd.ppm, rd.volume, rd.err)
	}
}
