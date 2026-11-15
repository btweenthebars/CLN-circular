package rebalance

import (
	"circular/graph"
	"circular/util"
	"errors"
	"fmt"

	"github.com/elementsproject/glightning/glightning"
)

// errMeasureStopped ends a measurement before its next probe.
var errMeasureStopped = errors.New("measuring stopped: its probe budget or time ran out, or circular-stop")

// Probe sends route a probe (node.SendProbe): a payment that cannot be
// settled, so nothing is paid. It says whether the probe arrived: every
// channel carried its hop's amount, so each holds at least that much, which
// the graph learns. If not, it learns from the failure as from a payment's
// (learnFromFailure, which adds what failed to exclude) and returns why.
func (r *Rebalance) Probe(route *graph.Route, exclude map[string]bool) (bool, error) {
	if err := r.checkFirstHop(route); err != nil {
		return false, err
	}
	return r.probe(route, exclude)
}

// probe is Probe once the first hop is checked.
func (r *Rebalance) probe(route *graph.Route, exclude map[string]bool) (bool, error) {
	arrived, failure, err := r.sendProbeOver(route)
	if arrived {
		return true, nil
	}
	if failure != nil {
		return false, r.learnFromFailure(route, failure, exclude)
	}
	return false, err
}

// sendProbeOver sends route a probe. -> whether it arrived (the graph then
// learns that each channel carried its hop's amount); if not, the failure
// lightningd reported, for the caller to learn from, or else the error.
func (r *Rebalance) sendProbeOver(route *graph.Route) (bool, *glightning.PaymentErrorData, error) {
	send := r.sendProbe
	if send == nil {
		send = r.Node.SendProbe
	}
	err := send(route)
	var paymentError *glightning.PaymentError
	if errors.As(err, &paymentError) && paymentError.Data != nil {
		if paymentError.Data.ErringNode == r.Node.Id && paymentError.Data.ErringIndex > 0 {
			// our node refused it, as it refuses any payment it has no
			// invoice for: it got here
			r.learnFromProbe(route)
			return true, nil, nil
		}
		return false, paymentError.Data, nil
	}
	if err == nil {
		err = errors.New("a probe was not refused")
	}
	return false, nil, err
}

// learnBounds takes from a sizing probe's failure only what it shows of the
// channels: those before the one that failed carried the amount, and that
// one, short of liquidity, holds less. The node is not marked: it had just
// forwarded a split over that channel, and failed only an amount nobody
// asked it to carry. Nothing is excluded or avoided, and nothing counted.
func (r *Rebalance) learnBounds(route *graph.Route, failure *glightning.PaymentErrorData) {
	if failure.FailCode != failTemporaryChannelFailure {
		return
	}
	for k, hop := range route.Hops { // (our own node is the source of hop 0)
		if hop.Source != failure.ErringNode {
			continue
		}
		if k < 1 || k == len(route.Hops)-1 {
			return // one of our own channels: read from lightningd instead
		}
		for i := 1; i < k; i++ {
			r.Node.Graph.LearnLowerBound(channelId(route.Hops[i].Channel), route.Hops[i].MilliSatoshi)
		}
		r.Node.Graph.LearnUpperBound(channelId(hop.Channel), hop.MilliSatoshi)
		return
	}
}

// learnFromProbe records that a probe went all the way: each channel along the
// route, other than our own (kept up to date from listpeerchannels), holds at
// least its hop's amount, and moved nothing.
func (r *Rebalance) learnFromProbe(route *graph.Route) {
	for i := 1; i < len(route.Hops)-1; i++ {
		r.Node.Graph.LearnLowerBound(channelId(route.Hops[i].Channel), route.Hops[i].MilliSatoshi)
	}
	for i := 1; i < len(route.Hops); i++ {
		r.Node.Graph.NodeForwarded(route.Hops[i].Source)
	}
}

// Measurement is what probes found r's routes can carry now.
type Measurement struct {
	// Carried is the most one route carried, in whole splits of r.Amount
	// (msat); 0 when none carried even one
	Carried uint64
	// Route is that route, priced for one split
	Route *graph.Route
	// Probes counts the probes sent
	Probes int
	// Err is why the last search or probe failed, when nothing carried a
	// split, or why measuring stopped
	Err error
}

// Measure probes how much of want (msat) r's routes carry now, in whole
// splits of r.Amount: it tries up to tries routes, each search avoiding what
// the probes before found short, until one carries a split; then up to steps
// more probes over that route find how much more it carries: want first, then
// halfway (a binary search). Nothing is paid. stop, if it returns true, ends
// it before its next probe.
func (r *Rebalance) Measure(want uint64, tries, steps int, stop func() bool) Measurement {
	var m Measurement
	split := r.Amount
	exclude := r.startExclude()
	for t := 0; t < tries && m.Route == nil; t++ {
		if stop != nil && stop() {
			m.Err = errMeasureStopped
			return m
		}
		route, err := r.getRoute(exclude)
		if err != nil {
			m.Err = err
			return m
		}
		m.Probes++
		ok, err := r.Probe(route, exclude)
		if !ok {
			m.Err = err
			if !errors.Is(err, util.ErrTemporaryFailure) {
				return m // another route would not help: our channels, the in-peer
			}
			continue
		}
		m.Carried, m.Route, m.Err = split, route, nil
	}
	if m.Route == nil {
		return m
	}
	carried, probes := r.sizeRoute(m.Route, want, steps, stop)
	m.Carried, m.Probes = carried, m.Probes+probes
	return m
}

// PROBE_SIZE_FIRST_SPLITS bounds the first sizing probe: one HTLC of at most
// this many splits. Each one that arrives lets the next be twice as large:
// a probe that got stuck would hold its whole amount on our channel until it
// failed, so the large ones go only over a route that just carried half as
// much without delay.
const PROBE_SIZE_FIRST_SPLITS = 10

// sizeRoute finds how much of want (msat) route carries, in whole splits of
// its amount, which a probe just carried: up to steps more probes over the
// same channels. They grow while they arrive: want, or
// PROBE_SIZE_FIRST_SPLITS splits if that is less, then twice what arrived,
// up to want. Once one fails, the rest search between what arrived and what
// failed (halfway each time). An amount the channels' own limits or our out
// channel refuse counts as failed, and costs no probe. From a probe that
// fails, only the channels' bounds are learned (learnBounds). stop, if it
// returns true, ends it before its next probe. -> the most carried (at least
// the route's amount; a floor when the steps ran out first), and the probes
// sent.
func (r *Rebalance) sizeRoute(route *graph.Route, want uint64, steps int, stop func() bool) (uint64, int) {
	split := route.Amount
	ids := route.ChannelIds()
	top := want / split * split
	lo, hi, probes := split, top, 0
	next, growing := top, true
	if first := split * PROBE_SIZE_FIRST_SPLITS; next > first {
		next = first
	}
	for s := 0; s < steps && hi > lo; s++ {
		if stop != nil && stop() {
			break
		}
		amount := next
		if !growing {
			if amount = lo + (hi-lo)/2/split*split; amount <= lo {
				amount = lo + split
			}
		}
		arrived := false
		if bigger, err := r.Node.Graph.GetRouteOverChannels(ids, amount); err == nil && r.checkFirstHop(bigger) == nil {
			probes++
			var failure *glightning.PaymentErrorData
			if arrived, failure, _ = r.sendProbeOver(bigger); failure != nil {
				r.learnBounds(bigger, failure)
			}
		}
		if !arrived {
			hi, growing = amount-split, false
			continue
		}
		lo = amount
		if next = 2 * lo; next > top {
			next = top
		}
	}
	return lo, probes
}

// PROBE_SIZE_STEPS is how many more probes size a route that carried a
// probe-only split (Rebalance.ProbeWant).
const PROBE_SIZE_STEPS = 7

// probed is the result of a probe-only attempt whose probe arrived over
// route: the route is sized up to ProbeWant, and nothing was paid. Its
// AmountMsat is what the route carried, its Fee what that many splits would
// pay over it.
func (r *Rebalance) probed(route *graph.Route, pretty *graph.PrettyRoute) *Result {
	carried, probes := r.sizeRoute(route, r.ProbeWant, PROBE_SIZE_STEPS, func() bool {
		return r.Node.Stopped.Load() || (r.Halt != nil && r.Halt.Load())
	})
	r.payments += probes
	if r.Sent != nil {
		r.Sent.Add(int64(probes))
	}
	result := NewResult("success", carried/1000, r.OutChannel.Destination, r.InChannel.Source)
	result.AmountMsat = carried
	result.Channels = route.ChannelIds()
	result.Fee = pretty.Fee * (carried / route.Amount)
	result.PPM = pretty.FeePPM
	result.Route = pretty
	result.Message = fmt.Sprintf("a probe carried %d sats from %s to %s at %d ppm (nothing paid): paying it would cost %.3f sats",
		result.Amount, r.Node.Graph.GetAlias(r.OutChannel.Destination), r.Node.Graph.GetAlias(r.InChannel.Source),
		result.PPM, float64(result.Fee)/1000)
	return result
}
