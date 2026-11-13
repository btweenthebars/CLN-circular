package parallel

import (
	"circular/rebalance"
	"circular/util"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// measuring stands in for Rebalance.Measure: each candidate's route (out
// scid -> a middle channel -> the target) carries carried[its scid] msat,
// found with probes probes; it records what each was asked for.
type measuring struct {
	t       *testing.T
	carried map[string]uint64
	middle  map[string][]string // the route's channel ids, per candidate
	probes  int
	wasted  int // stop() calls before a search that then sent no probe
	asked   []string
	wants   map[string]uint64
}

func (m *measuring) measure(r *rebalance.Rebalance, want uint64, stop func() bool) rebalance.Measurement {
	scid := r.OutChannel.ShortChannelId
	m.asked = append(m.asked, scid)
	m.wants[scid] = want
	out := rebalance.Measurement{}
	for i := 0; i < m.wasted; i++ {
		stop()
	}
	for i := 0; i < m.probes; i++ {
		if stop() {
			out.Err = errors.New("measuring stopped")
			return out
		}
		out.Probes++
	}
	c := m.carried[scid]
	if c == 0 {
		out.Err = errors.New("a channel on the route could not carry the amount")
		return out
	}
	route, err := r.Node.Graph.GetRouteOverChannels(m.middle[scid], r.Amount)
	assert.NoError(m.t, err)
	out.Carried, out.Route = util.Min(c, want/r.Amount*r.Amount), route
	return out
}

func dir(scid, from, to string) string { return scid + "/" + util.GetDirection(from, to) }

// three candidates into 9x1x1: 1x1x1 over 02p1 (100 ppm), 2x1x1 over 02p2
// (300 ppm), 3x1x1 over 02p3 (200 ppm); maxppm 1000, 100k splits.
func measuredRun(t *testing.T, carried map[string]uint64, probes int) (*AbstractRebalance, *measuring) {
	n, add, peer := quoteNetwork()
	target := add("9x1x1", "02in", "02self", 0)
	peer("02in", "9x1x1")
	m := &measuring{t: t, carried: carried, probes: probes, middle: map[string][]string{}, wants: map[string]uint64{}}
	cands := []string{}
	for i, base := range []uint64{10000, 30000, 20000} { // per 100k sats: 100, 300, 200 ppm
		scid, p, mid := []string{"1x1x1", "2x1x1", "3x1x1"}[i], []string{"02p1", "02p2", "02p3"}[i],
			[]string{"4x1x1", "5x1x1", "6x1x1"}[i]
		add(scid, "02self", p, 0)
		add(mid, p, "02in", base)
		peer(p, scid)
		cands = append(cands, scid)
		m.middle[scid] = []string{dir(scid, "02self", p), dir(mid, p, "02in"), dir("9x1x1", "02in", "02self")}
	}
	g := n.Graph
	c1, _ := g.GetChannel(dir("1x1x1", "02self", "02p1"))
	c2, _ := g.GetChannel(dir("2x1x1", "02self", "02p2"))
	c3, _ := g.GetChannel(dir("3x1x1", "02self", "02p3"))
	a := quoteRun(n, target, "", c1, c2, c3)
	a.maxPPM = 1000
	a.RebalanceMethods = &withRoom{a.RebalanceMethods.(*depletedOne)} // 300k each
	saved := measureRoutes
	measureRoutes = m.measure
	t.Cleanup(func() { measureRoutes = saved })
	return a, m
}


func TestMeasuredWhole(t *testing.T) {
	// 500k wanted: 1x1x1 (100 ppm) carries 200k, 3x1x1 (200 ppm) its room, 300k: 2x1x1 not needed
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 200000000, "2x1x1": 300000000, "3x1x1": 900000000}, 2)
	a.amount = 500000000
	q := a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, []string{"1x1x1", "3x1x1"}, m.asked, "cheapest first, until it is covered")
	assert.Equal(t, map[string]uint64{"1x1x1": 300000000, "3x1x1": 300000000}, m.wants, "the rest, up to its room")
	w := q.Whole
	if assert.NotNil(t, w) {
		assert.True(t, w.Measured)
		assert.Equal(t, 4, w.Probes)
		assert.Equal(t, uint64(500000), w.Covered)
		assert.Equal(t, 2, w.Candidates)
		assert.Equal(t, uint64(100), w.MinPPM)
		assert.Equal(t, uint64(200), w.TopPPM)
		assert.Equal(t, uint64(80), w.FeeSat, "200k at 100 + 300k at 200")
		assert.Equal(t, "measured: routes carry the whole 500000 sat now: 2 candidates, routes 100-200 ppm (avg 160), "+
			"about 80 sat in fees (4 probes, nothing paid; each route measured alone: routes sharing channels "+
			"may carry less together)", w.Verdict)
	}
	c := q.Candidates[0]
	if assert.NotNil(t, c.Measured) {
		assert.Equal(t, uint64(200000), c.Measured.CarriedSat)
		assert.Equal(t, uint64(100), *c.Measured.PPM)
		assert.Equal(t, 3, c.Measured.Hops)
		assert.Equal(t, []string{"02p1", "02in"}, c.Measured.Path)
		assert.Equal(t, 2, c.Measured.Probes)
	}
	assert.Nil(t, q.Candidates[1].Measured, "not needed")

	// only part of it, and one route that carries nothing
	a, _ = measuredRun(t, map[string]uint64{"1x1x1": 100000000, "3x1x1": 0, "2x1x1": 100000000}, 1)
	a.amount = 500000000
	q = a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, uint64(200000), q.Whole.Covered)
	assert.Equal(t, "measured: routes carry only 200000 of 500000 sat now (2 candidates, routes 100-300 ppm; "+
		"3 probes, nothing paid; one route per candidate: a run that spreads its splits may find more)", q.Whole.Verdict)
	assert.Equal(t, "a channel on the route could not carry the amount", q.Candidates[2].Measured.Error)
	assert.Equal(t, uint64(0), q.Candidates[2].Measured.CarriedSat)

	// none carries a split
	a, _ = measuredRun(t, map[string]uint64{}, 1)
	a.amount = 500000000
	q = a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, "measured: no route carried a split (3 probes, nothing paid): a channel on the route could "+
		"not carry the amount", q.Whole.Verdict)
	assert.Equal(t, uint64(0), q.Whole.Covered)
}

func TestMeasuringStops(t *testing.T) {
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 100000000, "2x1x1": 100000000, "3x1x1": 100000000}, 15)
	a.amount = 900000000
	q := a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, []string{"1x1x1", "3x1x1", "2x1x1"}, m.asked)
	assert.Equal(t, 40, q.Whole.Probes, "the budget: 15 + 15 + 10")
	assert.Equal(t, "measuring stopped", q.Candidates[1].Measured.Error, "2x1x1 ran out of probes")
	assert.Contains(t, q.Whole.Verdict, "measuring stopped there")
	assert.Equal(t, uint64(200000), q.Whole.Covered)

	// circular-stop: nothing more
	a, m = measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	a.amount = 500000000
	a.Node.Stopped.Store(true)
	q = a.quote("pull", true)
	before := *q.Whole
	q.measure(a.amount, a.Node, time.Now())
	assert.Empty(t, m.asked)
	assert.Equal(t, "not measured: circular-stop", q.NotMeasured)
	assert.Equal(t, before, *q.Whole, "nothing probed: the quote as it was, not \"no route carried\"")
	a.Node.Stopped.Store(false)

	// its time
	saved := MEASURE_MAX_TIME
	MEASURE_MAX_TIME = -time.Second
	defer func() { MEASURE_MAX_TIME = saved }()
	a, m = measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	a.amount = 500000000
	q = a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Empty(t, m.asked)
	assert.Equal(t, "not measured: its -1s from the call's start", q.NotMeasured)
	assert.False(t, q.Whole.Measured)
	MEASURE_MAX_TIME = saved

	// stopped before its first probe: its candidate's measurement, of nothing, is not shown
	a, _ = measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	a.amount = 500000000
	q = a.quote("pull", true)
	used := 0
	saved2 := measureRoutes
	measureRoutes = func(r *rebalance.Rebalance, want uint64, stop func() bool) rebalance.Measurement {
		used++
		a.Node.Stopped.Store(true)
		stop()
		return rebalance.Measurement{Err: errors.New("measuring stopped")}
	}
	q.measure(a.amount, a.Node, time.Now())
	measureRoutes = saved2
	a.Node.Stopped.Store(false)
	assert.Equal(t, 1, used)
	assert.Equal(t, "not measured: circular-stop", q.NotMeasured)
	for _, c := range q.Candidates {
		assert.Nil(t, c.Measured)
	}

	// no candidate with a route
	a, _ = measuredRun(t, map[string]uint64{}, 1)
	a.amount = 500000000
	q = a.quote("pull", true)
	for _, c := range q.Candidates {
		c.Error = "no route"
	}
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, "not measured: no candidate had a route to probe", q.NotMeasured)
}

// A run that starts while it measures stops it before the next probe.
func TestARunStartsMidMeasure(t *testing.T) {
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 100000000, "2x1x1": 100000000, "3x1x1": 100000000}, 1)
	a.amount = 900000000
	q := a.quote("pull", true)
	inner := m.measure
	var r *Run
	saved := measureRoutes
	measureRoutes = func(rb *rebalance.Rebalance, want uint64, stop func() bool) rebalance.Measurement {
		out := inner(rb, want, stop)
		if r == nil {
			r = startRun("pull", "7x1x1", 1000000, 500) // starts after the first candidate
		}
		return out
	}
	defer func() { measureRoutes = saved }()
	q.measure(a.amount, a.Node, time.Now())
	endRun(r, nil, nil)
	assert.Equal(t, []string{"1x1x1"}, m.asked)
	assert.Contains(t, q.Whole.Verdict, "measuring stopped there (its probe budget or time, circular-stop, or a run "+
		"that started)")
}

// Not while a run is going: probes would hold liquidity its splits need.
func TestNotMeasuredDuringARun(t *testing.T) {
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	a.amount = 500000000
	q := a.quote("pull", true)
	before := q.Whole
	r := startRun("pull", "7x1x1", 1000000, 500)
	q.measure(a.amount, a.Node, time.Now())
	endRun(r, nil, nil)
	assert.Empty(t, m.asked)
	assert.Equal(t, "not measured: a circular-pull on 7x1x1 is running, and probes would hold liquidity on the "+
		"channels its splits use; quote again once it ended", q.NotMeasured)
	assert.Equal(t, before, q.Whole, "the quote as it was")
	endRun(startRun("push", "8x1x1", 1000000, 500), nil, errors.New("failed"))
	q.measure(a.amount, a.Node, time.Now())
	assert.NotEmpty(t, m.asked, "once it ended (one that failed has ended too)")
}

// The time counts from the quote call's start; the probes counted are those sent.
func TestMeasureTimeAndProbes(t *testing.T) {
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	a.amount = 500000000
	q := a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now().Add(-MEASURE_MAX_TIME))
	assert.Empty(t, m.asked, "the quote itself took all of the time")

	a, m = measuredRun(t, map[string]uint64{"1x1x1": 100000000, "2x1x1": 100000000, "3x1x1": 100000000}, 1)
	m.wasted = 2
	a.amount = 900000000
	q = a.quote("pull", true)
	q.measure(a.amount, a.Node, time.Now())
	assert.Equal(t, 3, q.Whole.Probes, "3 sent, not the 9 checks")
}

// A pair: one payment, measured.
func TestMeasuredPair(t *testing.T) {
	a, m := measuredRun(t, map[string]uint64{"1x1x1": 100000000}, 1)
	q := a.quote("pull", true)
	q.Whole = nil
	q.measure(0, a.Node, time.Now())
	assert.Equal(t, uint64(100000000), m.wants["1x1x1"], "one split")
	assert.Nil(t, q.Whole, "no whole for one payment")
	assert.Equal(t, uint64(100000), q.Candidates[0].Measured.CarriedSat)
	assert.Equal(t, 3, len(m.asked), "each candidate once")
}

func TestMeasureIsAQuoteParameter(t *testing.T) {
	assert.True(t, (&RebalanceQuote{Measure: true}).Measure)
}

func measured(carried uint64, ppm *uint64, err string) *MeasuredRoute {
	return &MeasuredRoute{CarriedSat: carried, PPM: ppm, Error: err}
}

// The whole from what was measured: cheapest measured route first, each up to
// what it carried; the first error said when none carried.
func TestMeasuredWholeFigures(t *testing.T) {
	cs := []*QuoteCandidate{
		{Scid: "a", PPM: ppm(100), Measured: measured(300000, ppm(400), "")}, // its route was dearer when measured
		{Scid: "b", PPM: ppm(200), Measured: measured(500000, ppm(150), "")},
		{Scid: "c", PPM: ppm(300), Measured: measured(0, ppm(50), "")},       // nothing carried: not counted
		{Scid: "d", PPM: ppm(400)},                                           // not measured
		{Scid: "e", PPM: ppm(500), Measured: measured(0, nil, "first error")},
	}
	w := measuredWhole(600000000, cs, 9, true)
	assert.Equal(t, uint64(600000), w.Covered)
	assert.Equal(t, 2, w.Candidates, "b 500k at 150, then 100k of a at 400")
	assert.Equal(t, uint64(150), w.MinPPM)
	assert.Equal(t, uint64(400), w.TopPPM)
	assert.Equal(t, uint64(75+40), w.FeeSat)
	assert.NotContains(t, w.Verdict, "stopped", "covered: it did not matter")
	assert.Contains(t, w.Verdict, "9 probes, nothing paid")

	none := []*QuoteCandidate{
		{Scid: "x", Measured: measured(0, nil, "first error")},
		{Scid: "y", Measured: measured(0, nil, "second error")},
	}
	assert.Equal(t, "measured: no route carried a split (4 probes, nothing paid): first error",
		measuredWhole(100000000, none, 4, false).Verdict)
}
