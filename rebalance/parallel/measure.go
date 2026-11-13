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

// MEASURE_MAX_TIME bounds a measured circular-quote, from the call's start (a
// probe started before it can take node.PROBE_TIMEOUT more): with what the
// Bridge does around it, the Mac's 3-minute wait still gets the answer
var MEASURE_MAX_TIME = 100 * time.Second

// measureRoutes is Rebalance.Measure with the quote's tries and steps; tests
// replace it
var measureRoutes = func(r *rebalance2.Rebalance, want uint64, stop func() bool) rebalance2.Measurement {
	return r.Measure(want, MEASURE_TRIES, MEASURE_STEPS, stop)
}

// runningRun is a circular-pull or circular-push that is running, if any.
func runningRun() *Run {
	runs.Lock()
	defer runs.Unlock()
	for _, r := range runs.list {
		if r.Status == "running" {
			return r
		}
	}
	return nil
}

// measure probes the routed candidates, cheapest first, for what their routes
// carry now (Rebalance.Measure), until amount (msat; 0: one payment each, as
// in a pair) is covered, its probes were sent (q.maxProbes, else
// MEASURE_MAX_PROBES), its time passed since started (the quote call's
// start; q.maxTime, else MEASURE_MAX_TIME), or circular-stop. A
// candidate's want is the rest of the amount, up to its room. With an amount,
// Whole becomes what the probes found. Not while a run is going: its splits
// use the same channels, and probes holding their liquidity would make them
// fail (NotMeasured says so).
func (q *Quote) measure(amount uint64, n *node.Node, started time.Time) {
	if r := runningRun(); r != nil {
		q.NotMeasured = fmt.Sprintf("not measured: a circular-%s on %s is running, and probes would hold liquidity "+
			"on the channels its splits use; quote again once it ended", r.Kind, r.Target)
		return
	}
	order := []int{}
	for i, c := range q.Candidates {
		if c.Skipped == "" && c.Error == "" && c.PPM != nil && i < len(q.rebalances) && q.rebalances[i] != nil {
			order = append(order, i)
		}
	}
	sort.SliceStable(order, func(a, b int) bool { return *q.Candidates[order[a]].PPM < *q.Candidates[order[b]].PPM })
	maxProbes, maxTime := MEASURE_MAX_PROBES, MEASURE_MAX_TIME
	if q.maxProbes > 0 {
		maxProbes = q.maxProbes
	}
	if q.maxTime > 0 {
		maxTime = q.maxTime
	}
	used, deadline := 0, started.Add(maxTime)
	// why measuring stops before the next probe, if it does: a run that started meanwhile too
	why := func() string {
		switch {
		case n.Stopped.Load():
			return "circular-stop"
		case runningRun() != nil:
			return "a circular run started"
		case used >= maxProbes:
			return fmt.Sprintf("its %d probes", maxProbes)
		case time.Now().After(deadline):
			return fmt.Sprintf("its %v from the call's start", maxTime)
		}
		return ""
	}
	stopped := func() bool { return why() != "" }
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
	probes := 0
	for _, c := range q.Candidates {
		if c.Measured != nil {
			probes += c.Measured.Probes
		}
	}
	if probes == 0 { // nothing was probed: the quote as it was, and why
		for _, c := range q.Candidates {
			c.Measured = nil
		}
		reason := why()
		if reason == "" {
			reason = "no candidate had a route to probe"
		}
		q.NotMeasured = "not measured: " + reason
		return
	}
	if amount > 0 {
		q.Whole = measuredWhole(amount, q.Candidates, probes, stopped())
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
		probed += "; measuring stopped there (its probe budget or time, circular-stop, or a run that started)"
	}
	switch {
	case moved == 0:
		w.Verdict = fmt.Sprintf("measured: no route carried a split (%s)", probed)
		if firstErr != "" {
			w.Verdict += ": " + firstErr
		}
	case left == 0:
		w.Verdict = fmt.Sprintf("measured: routes carry the whole %d sat now: %d candidates, routes %d-%d ppm (avg %d), "+
			"about %d sat in fees (%s; each route measured alone: routes sharing channels may carry less together)",
			w.Amount, w.Candidates, w.MinPPM, w.TopPPM, w.AvgPPM, w.FeeSat, probed)
	default:
		w.Verdict = fmt.Sprintf("measured: routes carry only %d of %d sat now (%d candidates, routes %d-%d ppm; %s; "+
			"one route per candidate: a run that spreads its splits may find more)",
			w.Covered, w.Amount, w.Candidates, w.MinPPM, w.TopPPM, probed)
	}
	return w
}
