package parallel

import (
	"circular/node"
	rebalance2 "circular/rebalance"
	"circular/util"
	"fmt"
	"sort"
	"time"
)

const (
	MEASURE_TRIES      = 3  // routes a candidate's measure searches until one carries a split
	MEASURE_STEPS      = 3  // more probes over that route, to find how much more it carries
	MEASURE_MAX_PROBES = 40 // probes per circular-quote
)

// MEASURE_MAX_TIME bounds a circular-quote's measuring: each probe can take
// up to node.PROBE_TIMEOUT
var MEASURE_MAX_TIME = 3 * time.Minute

// measureRoutes is Rebalance.Measure with the quote's tries and steps; tests
// replace it
var measureRoutes = func(r *rebalance2.Rebalance, want uint64, stop func() bool) rebalance2.Measurement {
	return r.Measure(want, MEASURE_TRIES, MEASURE_STEPS, stop)
}

// measure probes the routed candidates, cheapest first, for what their routes
// carry now (Rebalance.Measure), until amount (msat; 0: one payment each, as
// in a pair) is covered, MEASURE_MAX_PROBES probes were sent, MEASURE_MAX_TIME
// passed, or circular-stop. A candidate's want is the rest of the amount, up
// to its room. With an amount, Whole becomes what the probes found.
func (q *Quote) measure(amount uint64, n *node.Node) {
	order := []int{}
	for i, c := range q.Candidates {
		if c.Skipped == "" && c.Error == "" && c.PPM != nil && i < len(q.rebalances) && q.rebalances[i] != nil {
			order = append(order, i)
		}
	}
	sort.SliceStable(order, func(a, b int) bool { return *q.Candidates[order[a]].PPM < *q.Candidates[order[b]].PPM })
	used, deadline := 0, time.Now().Add(MEASURE_MAX_TIME)
	stopped := func() bool { return n.Stopped.Load() || used >= MEASURE_MAX_PROBES || time.Now().After(deadline) }
	stop := func() bool { // called before each probe
		if stopped() {
			return true
		}
		used++
		return false
	}
	left := amount
	for _, i := range order {
		if (amount > 0 && left == 0) || stopped() {
			break
		}
		c, r := q.Candidates[i], q.rebalances[i]
		want := left
		if amount == 0 {
			want = r.Amount
		} else if c.RoomSat > 0 && c.RoomSat*1000 < want {
			want = c.RoomSat * 1000
		}
		m := measureRoutes(r, want, stop)
		mr := &MeasuredRoute{CarriedSat: m.Carried / 1000, Probes: m.Probes}
		if m.Route != nil {
			ppm := m.Route.FeePPM()
			mr.PPM, mr.Hops = &ppm, len(m.Route.Hops)
			for _, hop := range m.Route.Hops[1:] {
				mr.Path = append(mr.Path, n.Graph.GetAlias(hop.Source))
			}
		}
		if m.Err != nil {
			mr.Error = m.Err.Error()
		}
		c.Measured = mr
		if amount > 0 {
			left -= util.Min(left, m.Carried)
		}
	}
	if amount > 0 {
		q.Whole = measuredWhole(amount, q.Candidates, used, stopped())
	}
}

// measuredWhole is wholeQuote from what the probes found: the measured
// candidates, cheapest measured route first, each up to what its route
// carried. probes counts them; cut says measuring stopped before the amount
// was covered (its budget, its time, or circular-stop).
func measuredWhole(amount uint64, candidates []*QuoteCandidate, probes int, cut bool) *WholeQuote {
	carried := []*QuoteCandidate{}
	var firstErr string
	for _, c := range candidates {
		if c.Measured == nil {
			continue
		}
		if c.Measured.CarriedSat > 0 && c.Measured.PPM != nil {
			carried = append(carried, c)
		} else if firstErr == "" && c.Measured.Error != "" {
			firstErr = c.Measured.Error
		}
	}
	sort.SliceStable(carried, func(i, j int) bool { return *carried[i].Measured.PPM < *carried[j].Measured.PPM })
	w := &WholeQuote{Amount: amount / 1000, Measured: true, Probes: probes}
	left, feePPMMsat := amount, uint64(0)
	for _, c := range carried {
		if left == 0 {
			break
		}
		ppm := *c.Measured.PPM
		take := util.Min(left, c.Measured.CarriedSat*1000)
		left -= take
		feePPMMsat += take * ppm
		if w.Candidates == 0 {
			w.MinPPM = ppm
		}
		w.Candidates++
		if ppm > w.TopPPM {
			w.TopPPM = ppm
		}
	}
	moved := amount - left
	w.Covered, w.FeeSat = moved/1000, (feePPMMsat+500_000_000)/1_000_000_000
	if moved > 0 {
		w.AvgPPM = (feePPMMsat + moved/2) / moved
	}
	probed := fmt.Sprintf("%d probes, nothing paid", probes)
	if cut && left > 0 {
		probed += "; measuring stopped there (its probe budget or time, or circular-stop)"
	}
	switch {
	case moved == 0:
		w.Verdict = fmt.Sprintf("measured: no route carried a split (%s)", probed)
		if firstErr != "" {
			w.Verdict += ": " + firstErr
		}
	case left == 0:
		w.Verdict = fmt.Sprintf("measured: routes carry the whole %d sat now: %d candidates, routes %d-%d ppm (avg %d), "+
			"about %d sat in fees (%s)", w.Amount, w.Candidates, w.MinPPM, w.TopPPM, w.AvgPPM, w.FeeSat, probed)
	default:
		w.Verdict = fmt.Sprintf("measured: routes carry only %d of %d sat now (%d candidates, routes %d-%d ppm; %s)",
			w.Covered, w.Amount, w.Candidates, w.MinPPM, w.TopPPM, probed)
	}
	return w
}
