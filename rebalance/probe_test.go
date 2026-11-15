package rebalance

import (
	"circular/graph"
	"errors"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

// probe is network.pay for a payment nobody can settle: it fails where a
// channel holds too little, else at our own node, which has no invoice for
// it; no liquidity moves either way.
func (n *network) probe(route *graph.Route) error {
	n.paid = append(n.paid, route.ChannelIds())
	ids := route.ChannelIds()
	for i, hop := range route.Hops {
		if real, ok := n.liquidity[ids[i]]; ok && real < hop.MilliSatoshi {
			dir := int(ids[i][len(ids[i])-1] - '0')
			return &glightning.PaymentError{Data: &glightning.PaymentErrorData{
				ErringNode: hop.Source, ErringChannel: hop.ShortChannelId, ErringDirection: dir,
				ErringIndex: uint64(i), FailCode: failTemporaryChannelFailure, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE",
			}}
		}
	}
	return &glightning.PaymentError{Data: &glightning.PaymentErrorData{
		ErringNode: self, ErringIndex: uint64(len(route.Hops)), FailCode: 0x400f,
		FailCodeName: "WIRE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS",
	}}
}

func probing(n *network, out, in *graph.Channel, amount uint64) *Rebalance {
	r := rebalanceOn(n, out, in, amount)
	r.pay = func(*graph.Route) (*graph.PrettyRoute, error) { panic("a measure never pays") }
	r.sendProbe = n.probe
	return r
}

func TestAProbeThatArrives(t *testing.T) {
	n, out, in := shortRoute()
	r := probing(n, out, in, 50000000)
	route, err := r.getRoute(r.startExclude())
	assert.NoError(t, err)
	c, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	c.Liquidity = 1000 // believed nearly empty
	n.SetNodeMemory(24 * time.Hour)
	n.NodeFailed("02a")
	n.NodeFailed("02a")
	ok, err := r.Probe(route, map[string]bool{})
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, uint64(60000000), n.liquidity[id("3x1x1", "02a", "02b")], "nothing moved")
	assert.Equal(t, uint64(50000000), c.Liquidity, "it holds at least what it carried")
	assert.False(t, c.Upper)
	assert.InDelta(t, 1.0, n.NodeScores()["02a"].Score, 0.01, "it forwarded: its failures halve")
	assert.Equal(t, 1, len(n.paid))
}

func TestAProbeThatFails(t *testing.T) {
	n, out, in := shortRoute()
	r := probing(n, out, in, 100000000)
	route, _ := r.getRoute(r.startExclude())
	exclude := map[string]bool{}
	ok, err := r.Probe(route, exclude)
	assert.False(t, ok)
	assert.True(t, errors.Is(err, errShortOfLiquidity), err)
	assert.True(t, exclude[id("3x1x1", "02a", "02b")], "the next search avoids it")
	c, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	assert.Less(t, c.Liquidity, uint64(100000000))
	assert.True(t, c.Upper)

	// our node refusing it at the first hop is not "it arrived"
	r.sendProbe = func(*graph.Route) error {
		return &glightning.PaymentError{Data: &glightning.PaymentErrorData{ErringNode: self, ErringIndex: 0,
			FailCode: failTemporaryChannelFailure, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}}
	}
	ok, err = r.Probe(route, map[string]bool{})
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "our channel 1x1x1")

	r.sendProbe = func(*graph.Route) error { return nil }
	ok, err = r.Probe(route, map[string]bool{})
	assert.False(t, ok)
	assert.EqualError(t, err, "a probe was not refused")

	r.sendProbe = func(*graph.Route) error { return errors.New("rpc down") }
	ok, err = r.Probe(route, map[string]bool{})
	assert.False(t, ok)
	assert.EqualError(t, err, "rpc down")
}

func TestNoProbeOverOurDepletedChannel(t *testing.T) {
	n, out, in := shortRoute()
	r := probing(n, out, in, 50000000)
	route, _ := r.getRoute(r.startExclude())
	r.Node.Peers[outP].Channels[0].SpendableMsat = glightning.AmountFromMSat(1000)
	ok, err := r.Probe(route, map[string]bool{})
	assert.False(t, ok)
	assert.Error(t, err)
	assert.Empty(t, n.paid, "nothing sent")
}

// shortRoute holds 60k sats: splits of 20k, 100k wanted. One probe finds a
// route, then 100k fails, 40k and 60k go: 60k, in 4 probes.
func TestMeasureFindsWhatARouteCarries(t *testing.T) {
	n, out, in := shortRoute()
	r := probing(n, out, in, 20000000)
	m := r.Measure(100000000, 3, 3, nil)
	assert.NoError(t, m.Err)
	assert.Equal(t, uint64(60000000), m.Carried)
	assert.Equal(t, 4, m.Probes)
	if assert.Equal(t, 4, len(n.paid)) {
		assert.Equal(t, n.paid[0], n.paid[3], "the same route all along")
	}
	assert.Equal(t, uint64(60000000), n.liquidity[id("3x1x1", "02a", "02b")], "nothing moved")
	assert.Equal(t, uint64(20000000), r.Amount, "the split asked for is kept")
	if assert.NotNil(t, m.Route) {
		assert.Equal(t, uint64(20000000), m.Route.Amount, "the route, priced for a split")
	}

	// fewer steps: 100k fails, 40k goes
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	m = r.Measure(100000000, 3, 2, nil)
	assert.Equal(t, uint64(40000000), m.Carried)
	assert.Equal(t, 3, m.Probes)

	// what it wants first: all of it
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	m = r.Measure(50000000, 3, 3, nil) // 40k in whole splits
	assert.Equal(t, uint64(40000000), m.Carried)
	assert.Equal(t, 2, m.Probes, "a split, then the 40k wanted")

	// no more than a split wanted: one probe
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	m = r.Measure(5000000, 3, 3, nil)
	assert.Equal(t, uint64(20000000), m.Carried)
	assert.Equal(t, 1, m.Probes)
}

// A route short of a split: the next search goes around what the probe
// found short.
func TestMeasureTriesAnotherRoute(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "6x1x1", outP, "02c", 0) // a longer way around 02a -> 02b
	addChannel(n.Graph, "7x1x1", "02c", "02d", 0)
	addChannel(n.Graph, "8x1x1", "02d", "02b", 0)
	r := probing(n, out, in, 80000000)
	m := r.Measure(80000000, 3, 3, nil)
	assert.NoError(t, m.Err)
	assert.Equal(t, uint64(80000000), m.Carried)
	assert.Equal(t, 2, m.Probes)
	if assert.NotNil(t, m.Route) {
		assert.Contains(t, m.Route.ChannelIds(), id("7x1x1", "02c", "02d"))
	}

	// with one try, the short route is all it gets
	n, out, in = shortRoute()
	addChannel(n.Graph, "6x1x1", outP, "02c", 0)
	addChannel(n.Graph, "7x1x1", "02c", "02d", 0)
	addChannel(n.Graph, "8x1x1", "02d", "02b", 0)
	r = probing(n, out, in, 80000000)
	m = r.Measure(80000000, 1, 3, nil)
	assert.Equal(t, uint64(0), m.Carried)
	assert.Nil(t, m.Route)
	assert.True(t, errors.Is(m.Err, errShortOfLiquidity))
}

func TestMeasureStops(t *testing.T) {
	n, out, in := shortRoute()
	r := probing(n, out, in, 20000000)
	m := r.Measure(100000000, 3, 3, func() bool { return true })
	assert.Equal(t, errMeasureStopped, m.Err)
	assert.Equal(t, 0, m.Probes)
	assert.Empty(t, n.paid)

	calls := 0
	m = r.Measure(100000000, 3, 3, func() bool { calls++; return calls > 2 }) // a split, then 100k
	assert.Equal(t, 2, m.Probes)
	assert.Equal(t, uint64(20000000), m.Carried)

	// the in-peer refusing: another route would not help
	n, out, in = shortRoute()
	n.liquidity[id("5x1x1", inP, self)] = 0
	r = probing(n, out, in, 20000000)
	m = r.Measure(100000000, 3, 3, nil)
	assert.Equal(t, 1, m.Probes)
	assert.Equal(t, uint64(0), m.Carried)
	assert.Contains(t, m.Err.Error(), "cannot send")
}

// A probe-only rebalance is Run with probes for payments: the same search
// and attempts, nothing paid; it succeeds when a probe arrives, and its
// amount is what that route carried.
func TestAProbeOnlyRebalance(t *testing.T) {
	// the way through 02a -> 02b is short of 80k: the second attempt goes around it
	n, out, in := shortRoute()
	addChannel(n.Graph, "6x1x1", outP, "02c", 0)
	addChannel(n.Graph, "7x1x1", "02c", "02d", 0)
	addChannel(n.Graph, "8x1x1", "02d", "02b", 0)
	r := probing(n, out, in, 80000000)
	r.ProbeOnly, r.Attempts = true, 5
	sent := &atomic.Int64{}
	r.Sent = sent
	res := r.Run()
	assert.Equal(t, "success", res.Status, res.Message)
	assert.Equal(t, uint64(80000000), res.AmountMsat)
	assert.Equal(t, uint64(80000), res.Amount)
	assert.Equal(t, uint64(2), res.Attempts, "the probe that failed and the one that arrived")
	assert.Equal(t, int64(2), sent.Load())
	assert.Contains(t, res.Channels, id("7x1x1", "02c", "02d"))
	assert.Equal(t, res.Channels, n.paid[1])
	assert.Equal(t, "a probe carried 80000 sats from "+outP+" to "+inP+" at 0 ppm (nothing paid): paying it would "+
		"cost 0.000 sats", res.Message)
	assert.Equal(t, uint64(60000000), n.liquidity[id("3x1x1", "02a", "02b")], "nothing moved")
	short, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	assert.True(t, short.Upper, "the failure is learned as a payment's")
	assert.Less(t, short.Liquidity, uint64(80000000))
	assert.Equal(t, map[string]int{"02a": 1}, res.FailedNodes)

	// out of attempts: a failure like a payment's
	n, out, in = shortRoute()
	addChannel(n.Graph, "6x1x1", outP, "02c", 0)
	addChannel(n.Graph, "7x1x1", "02c", "02d", 0)
	addChannel(n.Graph, "8x1x1", "02d", "02b", 0)
	r = probing(n, out, in, 80000000)
	r.ProbeOnly = true // one attempt
	res = r.Run()
	assert.Equal(t, "failure", res.Status)
	assert.Equal(t, uint64(1), res.Attempts)
	assert.Contains(t, res.Message, "rebalance failed after 1 payments.")
	assert.Equal(t, SideRoute, res.FailedAt)

	// not probe-only: it pays (the fake's panic says so)
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	assert.Panics(t, func() { r.Run() })
}

// Once a probe arrived, the route is sized up to ProbeWant: shortRoute holds
// 60k, splits of 20k, 100k wanted: 100k fails, 40k and 60k go, 80k fails.
func TestAProbeOnlyRebalanceSizesItsRoute(t *testing.T) {
	n, out, in := shortRoute()
	c, _ := n.GetChannel(id("2x1x1", outP, "02a"))
	c.FeePerMillionth = 500
	r := probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 100000000
	sent := &atomic.Int64{}
	r.Sent = sent
	res := r.Run()
	assert.Equal(t, int64(5), sent.Load())
	assert.Equal(t, "success", res.Status, res.Message)
	assert.Equal(t, uint64(60000000), res.AmountMsat)
	assert.Equal(t, uint64(60000), res.Amount)
	assert.Equal(t, uint64(5), res.Attempts, "the split and four sizing probes")
	assert.Equal(t, 5, len(n.paid))
	assert.Equal(t, uint64(500), res.PPM, "the route's rate for a split")
	assert.Equal(t, uint64(30000), res.Fee, "three splits of 20k at 500 ppm")
	assert.Contains(t, res.Message, "a probe carried 60000 sats")
	assert.Contains(t, res.Message, "paying it would cost 30.000 sats")
	assert.Equal(t, uint64(10000), res.Route.Fee, "the route is priced for one split")
	assert.Equal(t, uint64(60000000), n.liquidity[id("3x1x1", "02a", "02b")], "nothing moved")

	// nothing more wanted than the split: no sizing
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 20000000
	res = r.Run()
	assert.Equal(t, uint64(20000000), res.AmountMsat)
	assert.Equal(t, uint64(1), res.Attempts)

	// the run stopped meanwhile: no sizing probes
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 100000000
	halt := &atomic.Bool{}
	r.Halt = halt
	inner := r.sendProbe
	r.sendProbe = func(route *graph.Route) error { halt.Store(true); return inner(route) }
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, uint64(20000000), res.AmountMsat)
	assert.Equal(t, 1, len(n.paid))

	// circular-stop meanwhile: none either
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 100000000
	inner = r.sendProbe
	r.sendProbe = func(route *graph.Route) error { r.Node.Stopped.Store(true); return inner(route) }
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, 1, len(n.paid))

	// what is wanted counts in whole splits: 50k wanted is 40k
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 50000000
	res = r.Run()
	assert.Equal(t, uint64(40000000), res.AmountMsat)
	assert.Equal(t, 2, len(n.paid), "a split, then the 40k")
}

// Sizing probes that fail teach the channels' bounds and nothing else: the
// node that just carried the split is not marked for an amount nobody asked
// it to carry.
func TestSizingLearnsOnlyBounds(t *testing.T) {
	n, out, in := shortRoute()
	n.SetNodeMemory(24 * time.Hour)
	r := probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 100000000
	res := r.Run()
	assert.Equal(t, uint64(60000000), res.AmountMsat)
	assert.Empty(t, n.NodeScores(), "no node is remembered as failing")
	assert.Empty(t, res.FailedNodes)
	assert.Equal(t, 0, res.MiddleFailures)
	c, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	assert.True(t, c.Upper, "the channel holds less than the 80k that failed")
	assert.Less(t, c.Liquidity, uint64(80000000))
	assert.GreaterOrEqual(t, c.Liquidity, uint64(60000000))
	before, _ := n.GetChannel(id("2x1x1", outP, "02a"))
	assert.GreaterOrEqual(t, before.Liquidity, uint64(100000000), "the channel before it carried the 100k")
	assert.Empty(t, n.Avoided(time.Now().Unix()), "nothing is avoided")

	// failures that say nothing of a channel's liquidity teach nothing
	n, out, in = shortRoute()
	r = probing(n, out, in, 20000000)
	route, _ := r.getRoute(r.startExclude())
	hop := route.Hops[2]
	for _, f := range []*glightning.PaymentErrorData{
		{ErringNode: hop.Source, FailCode: failFeeInsufficient},
		{ErringNode: self, FailCode: failTemporaryChannelFailure},
		{ErringNode: "02nowhere", FailCode: failTemporaryChannelFailure},
		{ErringNode: route.Hops[len(route.Hops)-1].Source, FailCode: failTemporaryChannelFailure}, // our in channel
	} {
		r.learnBounds(route, f)
	}
	for _, c := range n.Channels {
		assert.False(t, c.Upper, c.ShortChannelId)
	}
}

// The first sizing probe is at most PROBE_SIZE_FIRST_SPLITS splits; each one
// that arrives lets the next be twice as large; and an amount our out
// channel cannot send costs no probe.
func TestSizingProbesGrow(t *testing.T) {
	sized := func(holds, want uint64) (*Result, []uint64) {
		n, out, in := shortRoute()
		n.liquidity[id("3x1x1", "02a", "02b")] = holds
		r := probing(n, out, in, 20000000)
		r.ProbeOnly, r.ProbeWant = true, want
		var sent []uint64
		inner := r.sendProbe
		r.sendProbe = func(route *graph.Route) error { sent = append(sent, route.Amount/1000000); return inner(route) }
		return r.Run(), sent
	}
	// all of the 45 splits wanted arrive: 10, 20, 40, then the 45
	res, sent := sized(5000000000, 900000000)
	assert.Equal(t, uint64(900000000), res.AmountMsat)
	assert.Equal(t, []uint64{20, 200, 400, 800, 900}, sent)
	// the route holds 500k: 200 and 400 arrive, 800 fails; then between them: 580 fails, 480 arrives,
	// 520 fails, 500 arrives, with the last of the seven steps
	res, sent = sized(500000000, 900000000)
	assert.Equal(t, []uint64{20, 200, 400, 800, 580, 480, 520, 500}, sent)
	assert.Equal(t, uint64(500000000), res.AmountMsat)
	assert.Equal(t, uint64(8), res.Attempts)
	// the first one fails: never more than ten splits in one HTLC
	res, sent = sized(100000000, 900000000)
	assert.Equal(t, []uint64{20, 200, 100, 140, 120}, sent)
	assert.Equal(t, uint64(100000000), res.AmountMsat)
	// no more than ten splits wanted: that first, as before
	res, sent = sized(5000000000, 160000000)
	assert.Equal(t, []uint64{20, 160}, sent)
	assert.Equal(t, uint64(160000000), res.AmountMsat)

	n, out, in := shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 5000000000
	r := probing(n, out, in, 20000000)
	r.ProbeOnly, r.ProbeWant = true, 100000000
	r.Node.Peers[outP].Channels[0].SpendableMsat = glightning.AmountFromMSat(70000000)
	res = r.Run()
	assert.Equal(t, uint64(60000000), res.AmountMsat, "what our channel can send in one HTLC")
	assert.Equal(t, uint64(3), res.Attempts, "the split, 40k, 60k: 100k and 80k were never sent")
	assert.Equal(t, 3, len(n.paid))
}

// Exclude keeps its searches off the channels given.
func TestExcludedChannels(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "6x1x1", outP, "02c", 0)
	addChannel(n.Graph, "7x1x1", "02c", "02d", 0)
	addChannel(n.Graph, "8x1x1", "02d", "02b", 0)
	r := probing(n, out, in, 20000000)
	route, err := r.getRoute(r.startExclude())
	assert.NoError(t, err)
	assert.Contains(t, route.ChannelIds(), id("3x1x1", "02a", "02b"), "the short way")
	r.Exclude = []string{id("3x1x1", "02a", "02b")}
	route, err = r.getRoute(r.startExclude())
	assert.NoError(t, err)
	assert.NotContains(t, route.ChannelIds(), id("3x1x1", "02a", "02b"))
	assert.Contains(t, route.ChannelIds(), id("7x1x1", "02c", "02d"))
	r.ProbeOnly = true
	res := r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Contains(t, res.Channels, id("7x1x1", "02c", "02d"), "the run's own searches too")
}
