package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// network is a test graph whose channels really hold liquidity[id] msat.
type network struct {
	*graph.Graph
	liquidity map[string]uint64
	paid      [][]string // the channels of each payment sent
}

func id(scid, from, to string) string { return scid + "/" + util.GetDirection(from, to) }

// pay fails like lightningd when a channel along the route holds too little.
func (n *network) pay(route *graph.Route) (*graph.PrettyRoute, error) {
	n.paid = append(n.paid, route.ChannelIds())
	for i, hop := range route.Hops {
		if real, ok := n.liquidity[route.ChannelIds()[i]]; ok && real < hop.MilliSatoshi {
			dir := int(route.ChannelIds()[i][len(route.ChannelIds()[i])-1] - '0')
			return nil, &glightning.PaymentError{Data: &glightning.PaymentErrorData{
				ErringNode: hop.Source, ErringChannel: hop.ShortChannelId, ErringDirection: dir,
				ErringIndex: uint64(i), FailCode: failTemporaryChannelFailure, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE",
			}}
		}
	}
	for i, hop := range route.Hops {
		if _, ok := n.liquidity[route.ChannelIds()[i]]; ok {
			n.liquidity[route.ChannelIds()[i]] -= hop.MilliSatoshi
		}
	}
	return graph.NewPrettyRoute(route, ""), nil
}

// rebalanceOn returns a rebalance from our channel to outP to inP's channel to
// us, with plenty of balance on both, paying through n.
func rebalanceOn(n *network, out, in *graph.Channel, amount uint64) *Rebalance {
	peer := func(id, scid string) *glightning.Peer {
		return &glightning.Peer{Id: id, Connected: true, Channels: []*glightning.PeerChannel{{
			PeerId: id, ShortChannelId: scid, State: NORMAL, PeerConnected: true,
			TotalMsat: glightning.AmountFromMSat(10000000000), ToUsMsat: glightning.AmountFromMSat(5000000000),
			SpendableMsat: glightning.AmountFromMSat(5000000000), ReceivableMsat: glightning.AmountFromMSat(5000000000),
		}}}
	}
	nd := &node.Node{Graph: n.Graph, Id: self, PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{outP: peer(outP, out.ShortChannelId), inP: peer(inP, in.ShortChannelId)}}
	return &Rebalance{OutChannel: out, InChannel: in, Amount: amount, MaxPPM: 1000, Attempts: 1, MaxHops: 8,
		Node: nd, pay: n.pay}
}

// self -> outpeer -> 02a -> 02b -> inpeer -> self, where 02a -> 02b holds 60k sats
func shortRoute() (*network, *graph.Channel, *graph.Channel) {
	n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
	out := addChannel(n.Graph, "1x1x1", self, outP, 0)
	addChannel(n.Graph, "2x1x1", outP, "02a", 0)
	addChannel(n.Graph, "3x1x1", "02a", "02b", 0)
	addChannel(n.Graph, "4x1x1", "02b", inP, 0)
	in := addChannel(n.Graph, "5x1x1", inP, self, 0)
	n.liquidity[id("3x1x1", "02a", "02b")] = 60000000
	return n, out, in
}

// A split that failed for lack of liquidity used to drop its route and
// search again, though the route could carry part of the amount.
func TestSplitShortOfLiquidityRetriesItsRouteWithLess(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000

	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(50000), result.Amount)
	assert.Equal(t, uint64(50000000), result.AmountMsat)
	assert.Equal(t, uint64(2), result.Attempts, "the failed payment and the retry")
	assert.Equal(t, 2, len(n.paid))
	assert.Equal(t, n.paid[0], n.paid[1], "the same route, with half the amount")
	assert.Equal(t, n.paid[1], result.Channels)
	assert.Equal(t, uint64(100000000), r.Amount, "the rebalance keeps the amount asked for")

	// the graph learns that 02a -> 02b held less than 100k, then forwarded 50k
	c, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	assert.Less(t, c.Liquidity, uint64(50000000))

	// without a minimum, a rebalance delivers all of its amount or nothing
	n, out, in = shortRoute()
	r = rebalanceOn(n, out, in, 100000000)
	assert.Equal(t, "failure", r.Run().Status)
	assert.Equal(t, 1, len(n.paid))
}

func TestSplitStopsShrinkingAtTheMinimum(t *testing.T) {
	n, out, in := shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 20000000 // less than any allowed amount
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000

	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 3, len(n.paid), "100k, 50k, then 25k: the last allowed")

	n, out, in = shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 30000000
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	result = r.Run()
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, uint64(25000), result.Amount)
	assert.Equal(t, 3, len(n.paid))
}

// A route that worked is tried again before any search.
func TestKnownRouteIsTriedFirst(t *testing.T) {
	n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
	out := addChannel(n.Graph, "1x1x1", self, outP, 0)
	in := addChannel(n.Graph, "5x1x1", inP, self, 0)
	addChannel(n.Graph, "2x1x1", outP, "02a", 0)
	addChannel(n.Graph, "3x1x1", "02a", inP, 0) // free
	addChannel(n.Graph, "6x1x1", outP, "02b", 0)
	addChannel(n.Graph, "7x1x1", "02b", inP, 50000) // 500 ppm on 100k sats
	known := []string{id("1x1x1", self, outP), id("6x1x1", outP, "02b"), id("7x1x1", "02b", inP), id("5x1x1", inP, self)}

	r := rebalanceOn(n, out, in, 100000000)
	r.Route = known
	result := r.Run()
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, [][]string{known}, n.paid, "sent over the known route, no search")
	assert.Equal(t, uint64(500), result.PPM)

	// a known route that now costs more than maxppm is not used
	n.paid = nil
	r = rebalanceOn(n, out, in, 100000000)
	r.Route = known
	r.MaxPPM = 100
	result = r.Run()
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, id("2x1x1", outP, "02a"), n.paid[0][1], "the search found the free route")

	// nor one with a channel that is gone
	n.paid = nil
	r = rebalanceOn(n, out, in, 100000000)
	r.Route = []string{known[0], "9x9x9/0", known[3]}
	assert.Equal(t, "success", r.Run().Status)
	assert.Equal(t, 1, len(n.paid))
	assert.Equal(t, id("2x1x1", outP, "02a"), n.paid[0][1])
}

// A node that failed a payment itself was avoided for that rebalance only;
// every later split ran into it again.
func TestNodeFailuresAreAvoidedByLaterRebalances(t *testing.T) {
	n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
	out := addChannel(n.Graph, "1x1x1", self, outP, 0)
	in := addChannel(n.Graph, "5x1x1", inP, self, 0)
	addChannel(n.Graph, "2x1x1", outP, "02bad", 0)
	addChannel(n.Graph, "3x1x1", "02bad", inP, 0)
	addChannel(n.Graph, "6x1x1", outP, "02good", 0)
	addChannel(n.Graph, "7x1x1", "02good", inP, 1000)

	r := rebalanceOn(n, out, in, 100000000)
	r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
		n.paid = append(n.paid, route.ChannelIds())
		if route.Hops[2].Source == "02bad" {
			return nil, &glightning.PaymentError{Data: &glightning.PaymentErrorData{
				ErringNode: "02bad", ErringChannel: "3x1x1", ErringIndex: 2,
				FailCode: failNodeBit | 2, FailCodeName: "WIRE_TEMPORARY_NODE_FAILURE"}}
		}
		return graph.NewPrettyRoute(route, ""), nil
	}
	assert.Equal(t, "failure", r.Run().Status, "one attempt, spent on 02bad")

	n.paid = nil
	second := rebalanceOn(n, out, in, 100000000)
	second.pay = r.pay
	assert.Equal(t, "success", second.Run().Status)
	assert.Equal(t, 1, len(n.paid))
	assert.Equal(t, id("6x1x1", outP, "02good"), n.paid[0][1], "the next rebalance went around 02bad")
}

// A route reused after a split had used it up was sent again in full, and
// with attempts=1 no search ran.
func TestKnownRouteIsCheckedAgainstWhatTheGraphLearned(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "8x1x1", outP, "02c", 1) // a second way, 1 msat dearer: outpeer -> 02c -> 02b
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	first := r.Run()
	if first.Status != "success" {
		t.Fatal(first.Message)
	}
	assert.Equal(t, uint64(50000), first.Amount, "through 02a, shrunk")
	assert.Contains(t, first.Channels, id("3x1x1", "02a", "02b"))

	// 02a -> 02b is now believed nearly empty: the next split searches
	n.paid = nil
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Route = first.Channels
	second := r.Run()
	assert.Equal(t, "success", second.Status, second.Message)
	assert.Equal(t, uint64(100000), second.Amount)
	for _, paid := range n.paid {
		assert.NotContains(t, paid, id("3x1x1", "02a", "02b"))
	}
}

// When a reused route fails anyway, the same attempt searches for another.
func TestFailedKnownRouteDoesNotUseUpTheAttempt(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "8x1x1", outP, "02c", 0)
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	n.liquidity[id("3x1x1", "02a", "02b")] = 0 // the graph does not know yet
	r := rebalanceOn(n, out, in, 100000000)
	r.Route = []string{id("1x1x1", self, outP), id("2x1x1", outP, "02a"), id("3x1x1", "02a", "02b"),
		id("4x1x1", "02b", inP), id("5x1x1", inP, self)}
	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, 2, len(n.paid), "the known route, then a searched one")
	assert.Equal(t, id("8x1x1", outP, "02c"), n.paid[1][1])
	assert.Equal(t, uint64(2), result.Attempts)
}

// A reused route went straight through nodes other splits had learned to avoid.
func TestKnownRouteRespectsTheAvoidList(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "8x1x1", outP, "02c", 0)
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	n.liquidity = map[string]uint64{}
	n.Graph.Avoid("02a", 1<<40)
	r := rebalanceOn(n, out, in, 100000000)
	r.Route = []string{id("1x1x1", self, outP), id("2x1x1", outP, "02a"), id("3x1x1", "02a", "02b"),
		id("4x1x1", "02b", inP), id("5x1x1", inP, self)}
	assert.Equal(t, "success", r.Run().Status)
	assert.Equal(t, 1, len(n.paid))
	assert.Equal(t, id("8x1x1", outP, "02c"), n.paid[0][1])
}

// A node failure from our own out-peer put it on the avoid list, which
// blocked every route through our channel with it for 30 minutes.
func TestOurPeersAreNotAvoided(t *testing.T) {
	r, route := testRoute(t)
	exclude := map[string]bool{}
	_ = r.learnFromFailure(route, failureAt(outP, "2x1x1", 0, failNodeBit|2), exclude)
	assert.True(t, exclude[outP], "excluded for this run, as before")
	assert.Empty(t, r.Node.Graph.Avoided(0))

	_ = r.learnFromFailure(route, failureAt("02a", "3x1x1", 0, failNodeBit|2), exclude)
	assert.Equal(t, map[string]bool{"02a": true}, r.Node.Graph.Avoided(0))
}

// A peer of ours avoided as the middle hop of another rebalance blocked
// every rebalance through our channel with it.
func TestAvoidedPeersStillEndOurRoutes(t *testing.T) {
	n, out, in := shortRoute()
	n.liquidity = map[string]uint64{}
	n.Graph.Avoid(outP, 1<<40)
	n.Graph.Avoid(inP, 1<<40)
	r := rebalanceOn(n, out, in, 100000000)
	assert.Equal(t, "success", r.Run().Status)

	// the known route is checked the same way
	n.paid = nil
	r = rebalanceOn(n, out, in, 100000000)
	r.Route = []string{id("1x1x1", self, outP), id("2x1x1", outP, "02a"), id("3x1x1", "02a", "02b"),
		id("4x1x1", "02b", inP), id("5x1x1", inP, self)}
	assert.Equal(t, "success", r.Run().Status)
	assert.Equal(t, 1, len(n.paid))
}
