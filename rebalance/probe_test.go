package rebalance

import (
	"circular/graph"
	"errors"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
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
