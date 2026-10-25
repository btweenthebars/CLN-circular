package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"fmt"
	"math"
	"time"

	"github.com/elementsproject/glightning/glightning"
)

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
	if ratio > 0 {
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
	read := r.forwards
	if read == nil {
		read = r.Node.Earned
	}
	e := earning{basis: "current fee"}
	ppm, volume, err := read(scid, r.earnSince)
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
	case limit < 1:
		e.limit = 0
	case limit >= float64(r.maxPPM):
		e.limit = r.maxPPM
	case limit >= math.MaxUint64:
		e.limit = math.MaxUint64
	default:
		e.limit = uint64(limit)
	}
	return e
}
