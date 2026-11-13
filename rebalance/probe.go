package rebalance

import (
	"circular/graph"
	"circular/util"
	"errors"

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
			return true, nil
		}
		return false, r.learnFromFailure(route, paymentError.Data, exclude)
	}
	if err == nil {
		err = errors.New("a probe was not refused")
	}
	return false, err
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
	want = want / split * split // below a split: the split, which the first probe finds
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
	ids := m.Route.ChannelIds()
	lo, hi := split, want
	for s := 0; s < steps && hi > lo; s++ {
		if stop != nil && stop() {
			break
		}
		amount := hi
		if s > 0 {
			if amount = lo + (hi-lo)/2/split*split; amount <= lo {
				amount = lo + split
			}
		}
		route, err := r.Node.Graph.GetRouteOverChannels(ids, amount)
		if err != nil {
			hi = amount - split
			continue
		}
		m.Probes++
		if ok, _ := r.Probe(route, exclude); ok {
			lo, m.Carried = amount, amount
		} else {
			hi = amount - split
		}
	}
	return m
}
