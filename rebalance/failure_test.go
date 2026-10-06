package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

const (
	self = "02self"
	outP = "02outpeer"
	inP  = "02inpeer"
)

func addChannel(g *graph.Graph, scid, from, to string, baseFee uint64) *graph.Channel {
	c := graph.NewChannel(&glightning.Channel{
		Source:                   from,
		Destination:              to,
		ShortChannelId:           scid,
		IsActive:                 true,
		AmountMsat:               glightning.AmountFromMSat(10000000000),
		BaseFeeMillisatoshi:      baseFee,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000000),
	}, 5000000000, 0)
	g.AddChannel(c)
	g.Channels[scid+"/"+util.GetDirection(from, to)] = c
	return c
}

// self -> outpeer -> 02a -> 02b -> inpeer -> self, every node charging 1000 msat
func testRoute(t *testing.T) (*Rebalance, *graph.Route) {
	g := graph.NewGraph()
	out := addChannel(g, "1x1x1", self, outP, 0)
	addChannel(g, "2x1x1", outP, "02a", 1000)
	addChannel(g, "3x1x1", "02a", "02b", 1000)
	addChannel(g, "4x1x1", "02b", inP, 1000)
	in := addChannel(g, "5x1x1", inP, self, 1000)
	// the reverse directions, to check what is learned about them
	addChannel(g, "3x1x1", "02b", "02a", 0)
	addChannel(g, "4x1x1", inP, "02b", 0)

	route, err := g.GetCircularRoute(out, in, 100000000, nil, 8, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(route.Hops))

	r := &Rebalance{OutChannel: out, InChannel: in, Amount: 100000000, Node: &node.Node{Graph: g, Id: self}}
	return r, route
}

func failureAt(erringNode, scid string, direction, failcode int) *glightning.PaymentErrorData {
	return &glightning.PaymentErrorData{
		ErringNode:      erringNode,
		ErringChannel:   scid,
		ErringDirection: direction,
		FailCode:        failcode,
		FailCodeName:    "test failure",
	}
}

func liquidity(r *Rebalance, scid, from, to string) uint64 {
	c, _ := r.Node.Graph.GetChannel(scid + "/" + util.GetDirection(from, to))
	return c.Liquidity
}

func TestTemporaryChannelFailureLearnsBounds(t *testing.T) {
	r, route := testRoute(t)
	// 02a's channel to 02b looked empty, but the payment got through it
	c, _ := r.Node.Graph.GetChannel("3x1x1/" + util.GetDirection("02a", "02b"))
	c.Liquidity = 0

	id := "4x1x1/" + util.GetDirection("02b", inP)
	exclude := map[string]bool{}
	err := r.learnFromFailure(route, failureAt("02b", "4x1x1", int(id[len(id)-1]-'0'), failTemporaryChannelFailure), exclude)

	assert.Equal(t, util.ErrTemporaryFailure, err)
	assert.True(t, exclude[id])
	// 02b could not forward what hop 3 carried: 100,000,000 + the in-peer's 1000
	assert.Equal(t, route.Hops[3].MilliSatoshi-1, liquidity(r, "4x1x1", "02b", inP))
	assert.Equal(t, uint64(10000000000)-route.Hops[3].MilliSatoshi, liquidity(r, "4x1x1", inP, "02b"))
	// the hops before it carried the payment
	assert.Equal(t, route.Hops[2].MilliSatoshi, liquidity(r, "3x1x1", "02a", "02b"))
	// ... which says nothing new about the reverse direction (5,000,000,000 < capacity - amount)
	assert.Equal(t, uint64(5000000000), liquidity(r, "3x1x1", "02b", "02a"))
}

func TestNodeFailureExcludesTheNode(t *testing.T) {
	r, route := testRoute(t)
	exclude := map[string]bool{}
	err := r.learnFromFailure(route, failureAt("02a", "", 0, 0x2002), exclude)

	assert.Equal(t, util.ErrTemporaryFailure, err)
	assert.True(t, exclude["02a"])
}

func TestDisabledChannelIsMarkedUnusable(t *testing.T) {
	r, route := testRoute(t)
	exclude := map[string]bool{}
	err := r.learnFromFailure(route, failureAt("02a", "3x1x1", 0, failChannelDisabled), exclude)

	assert.Equal(t, util.ErrTemporaryFailure, err)
	assert.Equal(t, uint64(0), liquidity(r, "3x1x1", "02a", "02b"))
	assert.Equal(t, uint64(5000000000), liquidity(r, "3x1x1", "02b", "02a"))
	assert.True(t, exclude["3x1x1/"+util.GetDirection("02a", "02b")])

	// the next search avoids it
	_, err = r.Node.Graph.GetCircularRoute(r.OutChannel, r.InChannel, r.Amount, exclude, 8, math.MaxUint64)
	assert.Equal(t, util.ErrNoRoute, err)
}

func TestFailuresThatStopTheRun(t *testing.T) {
	r, route := testRoute(t)

	// our own node refused the payment
	err := r.learnFromFailure(route, failureAt(self, "5x1x1", 0, 0x400f), map[string]bool{})
	assert.Error(t, err)
	assert.NotEqual(t, util.ErrTemporaryFailure, err)

	// the in-peer cannot send over its channel to us: nothing to learn about the network
	before := liquidity(r, "4x1x1", "02b", inP)
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	assert.Error(t, err)
	assert.NotEqual(t, util.ErrTemporaryFailure, err)
	assert.Equal(t, before, liquidity(r, "4x1x1", "02b", inP))

	// the in-peer changed its fee on our channel
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failFeeInsufficient), map[string]bool{})
	assert.Equal(t, util.ErrWireFeeInsufficient, err)
}

// When lightningd refuses at once to send over our out channel, the error
// names our node at index 0. It used to be reported as "our node failed the
// payment", as if the final hop had rejected it.
func TestFailureAtOurFirstHopNamesTheChannel(t *testing.T) {
	r, route := testRoute(t)
	failure := failureAt(self, "1x1x1", 0, failUnknownNextPeer)
	failure.FailCodeName = "WIRE_UNKNOWN_NEXT_PEER"
	err := r.learnFromFailure(route, failure, map[string]bool{})
	assert.EqualError(t, err, "our channel 1x1x1 to "+outP+" could not send the payment: WIRE_UNKNOWN_NEXT_PEER")

	// our node rejecting the payment as its final hop
	failure.ErringIndex = uint64(len(route.Hops))
	err = r.learnFromFailure(route, failure, map[string]bool{})
	assert.EqualError(t, err, "our node failed the payment: WIRE_UNKNOWN_NEXT_PEER")
}
