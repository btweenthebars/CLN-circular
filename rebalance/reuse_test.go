package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
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

// A route that failed for lack of liquidity was retried at once with half
// the amount, sending up to three payments in one attempt, though another
// route could have carried all of it. Now other routes are searched first,
// and the short route gets less only once none is left, as one of the
// attempts.
func TestShortRouteGetsLessOnlyWhenNoOtherRouteIsLeft(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 2

	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(50000), result.Amount)
	assert.Equal(t, uint64(50000000), result.AmountMsat)
	assert.Equal(t, uint64(2), result.Attempts, "the failed payment and the one with less")
	if assert.Equal(t, 2, len(n.paid)) {
		assert.Equal(t, n.paid[0], n.paid[1], "the same route, with half the amount")
		assert.Equal(t, n.paid[1], result.Channels)
	}
	assert.Equal(t, uint64(100000000), r.Amount, "the rebalance keeps the amount asked for")

	// the graph learns that 02a -> 02b held less than 100k, then forwarded 50k
	c, _ := n.GetChannel(id("3x1x1", "02a", "02b"))
	assert.Less(t, c.Liquidity, uint64(50000000))

	// sending less is an attempt: with one, the rebalance stops after the first payment
	n, out, in = shortRoute()
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	result = r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 1, len(n.paid))
	assert.Equal(t, uint64(1), result.Attempts)
	assert.Equal(t, uint64(100000), result.Amount, "a failure reports the amount asked for")

	// without a minimum, a rebalance delivers all of its amount or nothing
	n, out, in = shortRoute()
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 3
	assert.Equal(t, "failure", r.Run().Status)
	assert.Equal(t, 1, len(n.paid), "no other route, and no shrinking")
}

// Another route that can carry the whole amount is tried before the short
// one gets less.
func TestOtherRoutesComeBeforeSendingLess(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "8x1x1", outP, "02c", 1) // a second way, 1 msat dearer: outpeer -> 02c -> 02b
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 2

	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(100000), result.Amount, "all of it, over the other route")
	if assert.Equal(t, 2, len(n.paid)) {
		assert.Equal(t, id("2x1x1", outP, "02a"), n.paid[0][1])
		assert.Equal(t, id("8x1x1", outP, "02c"), n.paid[1][1])
	}
}

// After the short route got less and was short again, the next attempt
// searches for the smaller amount, which a route too small for the whole
// amount may carry.
func TestLaterAttemptsSearchForTheSmallerAmount(t *testing.T) {
	n, out, in := shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 30000000
	addChannel(n.Graph, "8x1x1", outP, "02c", 1)
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	n.LearnUpperBound(id("9x1x1", "02c", "02b"), 60000000) // known to hold less than 60k
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 3

	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(50000), result.Amount)
	assert.Equal(t, uint64(3), result.Attempts)
	if assert.Equal(t, 3, len(n.paid)) {
		assert.Equal(t, n.paid[0], n.paid[1], "100k, then 50k over the short route")
		assert.Equal(t, id("8x1x1", outP, "02c"), n.paid[2][1], "then 50k over the route that holds it")
	}
}

func TestSplitStopsShrinkingAtTheMinimum(t *testing.T) {
	n, out, in := shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 20000000 // less than any allowed amount
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 10

	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 3, len(n.paid), "100k, 50k, then 25k: the last allowed")
	assert.Equal(t, uint64(3), result.Attempts, "payments: the search that found nothing is not one")

	n, out, in = shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 30000000
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 10
	result = r.Run()
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, uint64(25000), result.Amount)
	assert.Equal(t, 3, len(n.paid))
	assert.Equal(t, uint64(3), result.Attempts)

	// each payment with less is one of the attempts
	n, out, in = shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 30000000
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 2
	result = r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 2, len(n.paid), "100k, then 50k: no attempt left for 25k")
	assert.Equal(t, uint64(2), result.Attempts)
}

// A payment with less that fails for another reason does not get the short
// route tried again.
func TestShortRouteIsDroppedWhenItFailsOtherwise(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 10000000
	r.Attempts = 5
	r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
		if route.Amount < 100000000 {
			n.paid = append(n.paid, route.ChannelIds())
			return nil, &glightning.PaymentError{Data: &glightning.PaymentErrorData{
				ErringNode: "02a", ErringChannel: "3x1x1", ErringIndex: 2,
				FailCode: failNodeBit | 2, FailCodeName: "WIRE_TEMPORARY_NODE_FAILURE"}}
		}
		return n.pay(route)
	}
	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 2, len(n.paid), "100k short, 50k failed at 02a: no 25k")
	assert.Equal(t, uint64(2), result.Attempts, "payments only")
}

// The short route got less without checking what the searches and payments
// in between had learned: here 02a, on it, failed another payment itself.
func TestShortRouteRespectsWhatWasLearnedSince(t *testing.T) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "8x1x1", "02a", "02c", 1) // a second way: 02a -> 02c -> 02b
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 3
	r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
		if route.Hops[2].ShortChannelId == "8x1x1" {
			n.paid = append(n.paid, route.ChannelIds())
			return nil, &glightning.PaymentError{Data: &glightning.PaymentErrorData{
				ErringNode: "02a", ErringChannel: "8x1x1", ErringIndex: 2,
				FailCode: failNodeBit | 2, FailCodeName: "WIRE_TEMPORARY_NODE_FAILURE"}}
		}
		return n.pay(route)
	}
	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	if assert.Equal(t, 2, len(n.paid), "100k over 02a -> 02b, then 02a failed the other route: nothing more through 02a") {
		assert.Equal(t, id("3x1x1", "02a", "02b"), n.paid[0][2])
		assert.Equal(t, id("8x1x1", "02a", "02c"), n.paid[1][2])
	}

	// what a payment with less learned about the short channel itself still lets it shrink further
	n, out, in = shortRoute()
	n.liquidity[id("3x1x1", "02a", "02b")] = 30000000
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 3
	result = r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(25000), result.Amount)

	// a payment with less that runs short on another channel of the route
	// leaves both excluded for others, not for the route itself
	n, out, in = shortRoute()
	n.liquidity[id("4x1x1", "02b", inP)] = 40000000
	r = rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 3
	result = r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, uint64(25000), result.Amount)
	assert.Equal(t, 3, len(n.paid), "100k short at 02a, 50k short at 02b, then 25k")
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
	r := rebalanceOn(n, out, in, 100000000)
	r.MinAmount = 25000000
	r.Attempts = 2
	first := r.Run()
	if first.Status != "success" {
		t.Fatal(first.Message)
	}
	assert.Equal(t, uint64(50000), first.Amount, "through 02a, shrunk")
	assert.Contains(t, first.Channels, id("3x1x1", "02a", "02b"))
	addChannel(n.Graph, "8x1x1", outP, "02c", 1) // a second way, 1 msat dearer: outpeer -> 02c -> 02b
	addChannel(n.Graph, "9x1x1", "02c", "02b", 0)

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

// With attempts=100, a split whose out-peer had no outbound liquidity made
// about 100 payments, each trying another of the out-peer's channels.
func TestOutPeerThatCannotForwardEndsTheRebalance(t *testing.T) {
	// the out-peer has 8 ways to the in-peer, none with liquidity
	deadOutPeer := func() (*network, *graph.Channel, *graph.Channel) {
		n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
		out := addChannel(n.Graph, "1x1x1", self, outP, 0)
		in := addChannel(n.Graph, "5x1x1", inP, self, 0)
		for i := 0; i < 8; i++ {
			mid := fmt.Sprintf("02m%d", i)
			addChannel(n.Graph, fmt.Sprintf("2x1x%d", i), outP, mid, uint64(i)) // each dearer than the last
			addChannel(n.Graph, fmt.Sprintf("3x1x%d", i), mid, inP, 0)
			n.liquidity[id(fmt.Sprintf("2x1x%d", i), outP, mid)] = 0
		}
		return n, out, in
	}
	n, out, in := deadOutPeer()
	r := rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100

	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, DEFAULT_MAX_NODE_FAILS, len(n.paid))
	assert.Equal(t, SideOut, result.FailedAt, "the out channel's peer: a pull leaves the candidate out, a push stops")
	assert.Equal(t, RetryNo, result.Retry)
	assert.Contains(t, result.Message, "could not forward 5 payments")

	// maxnodefails sets the limit
	n, out, in = deadOutPeer()
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	r.MaxNodeFails = 2
	result = r.Run()
	assert.Equal(t, 2, len(n.paid))
	assert.Contains(t, result.Message, "could not forward 2 payments")

	n, out, in = deadOutPeer()
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	r.MaxNodeFails = 100
	assert.Equal(t, "failure", r.Run().Status)
	assert.Equal(t, 8, len(n.paid), "every channel of the out-peer, then no route")

	// a new rebalance counts from zero
	n, out, in = deadOutPeer()
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 2
	assert.Equal(t, "failure", r.Run().Status)
	assert.Equal(t, 2, len(n.paid))
}

// A node past the out-peer that cannot forward was tried once per channel:
// after maxnodefails failures the rest of the rebalance routes around it.
func TestADrainedNodeIsRoutedAround(t *testing.T) {
	drained := func(dead int) (*network, *graph.Channel, *graph.Channel) {
		n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
		out := addChannel(n.Graph, "1x1x1", self, outP, 0)
		in := addChannel(n.Graph, "5x1x1", inP, self, 0)
		addChannel(n.Graph, "2x1x1", outP, "02a", 0)
		for i := 0; i < 8; i++ {
			mid := fmt.Sprintf("02m%d", i)
			addChannel(n.Graph, fmt.Sprintf("3x1x%d", i), "02a", mid, uint64(i))
			addChannel(n.Graph, fmt.Sprintf("4x1x%d", i), mid, inP, 0)
			if i < dead {
				n.liquidity[id(fmt.Sprintf("3x1x%d", i), "02a", mid)] = 0
			}
		}
		// a longer way around 02a: routes with fewer hops are tried first
		addChannel(n.Graph, "6x1x1", outP, "02b", 0)
		addChannel(n.Graph, "7x1x1", "02b", "02c", 0)
		addChannel(n.Graph, "8x1x1", "02c", "02d", 0)
		addChannel(n.Graph, "9x1x1", "02d", inP, 0)
		return n, out, in
	}

	// 02a fails 5 times: the 6th payment goes around it
	n, out, in := drained(8)
	r := rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, DEFAULT_MAX_NODE_FAILS+1, len(n.paid))
	assert.Equal(t, id("6x1x1", outP, "02b"), n.paid[len(n.paid)-1][1])
	assert.Equal(t, uint64(DEFAULT_MAX_NODE_FAILS+1), result.Attempts)

	// fewer failures than the limit: 02a's next channel works
	n, out, in = drained(DEFAULT_MAX_NODE_FAILS - 1)
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	result = r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, DEFAULT_MAX_NODE_FAILS, len(n.paid))
	assert.Equal(t, id("2x1x1", outP, "02a"), n.paid[len(n.paid)-1][1])

	// maxnodefails sets the limit
	n, out, in = drained(8)
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	r.MaxNodeFails = 2
	assert.Equal(t, "success", r.Run().Status)
	assert.Equal(t, 3, len(n.paid))

	// only for this rebalance: 02a is not on the avoid list
	_, avoided := n.Avoided(0)["02a"]
	assert.False(t, avoided)
}

// When a run stopped firing splits, the splits in flight went on through
// all their attempts: about 100 more payments in a live run.
func TestHaltEndsASplitBeforeItsNextPayment(t *testing.T) {
	n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
	out := addChannel(n.Graph, "1x1x1", self, outP, 0)
	in := addChannel(n.Graph, "5x1x1", inP, self, 0)
	for i := 0; i < 8; i++ { // every way fails past the out-peer, at a different node
		mid := fmt.Sprintf("02m%d", i)
		addChannel(n.Graph, fmt.Sprintf("2x1x%d", i), outP, mid, uint64(i))
		addChannel(n.Graph, fmt.Sprintf("3x1x%d", i), mid, inP, 0)
		n.liquidity[id(fmt.Sprintf("3x1x%d", i), mid, inP)] = 0
	}
	var halt atomic.Bool
	r := rebalanceOn(n, out, in, 100000000)
	r.Attempts = 100
	r.Halt = &halt
	pay := r.pay
	r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
		if len(n.paid) == 1 {
			halt.Store(true) // the run stops while this split's second payment is out
		}
		return pay(route)
	}

	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 2, len(n.paid), "no payment after the run stopped")
	assert.Equal(t, uint64(2), result.Attempts)
	assert.Equal(t, SideNone, result.FailedAt, "neither of its channels is to blame")
	assert.Contains(t, result.Message, "the run stopped")
	assert.True(t, result.Halted)
}

// circular-risk-ppm was the only way to set the risk rate, for every run.
func TestRiskPPMPerRebalance(t *testing.T) {
	route := func(nodeRisk uint64, risk int) string {
		n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
		out := addChannel(n.Graph, "1x1x1", self, outP, 0)
		in := addChannel(n.Graph, "5x1x1", inP, self, 0)
		small := addChannel(n.Graph, "2x1x1", outP, inP, 0) // free, barely larger than the payment
		small.AmountMsat = glightning.AmountFromMSat(220000000)
		small.Liquidity = 110000000
		addChannel(n.Graph, "3x1x1", outP, "02a", 0) // 10M sats, 10 ppm
		addChannel(n.Graph, "4x1x1", "02a", inP, 1000)
		r := rebalanceOn(n, out, in, 100000000)
		r.Node.RiskPPM, r.RiskPPM = nodeRisk, risk
		r.Attempts = 1
		assert.Equal(t, "success", r.Run().Status)
		return n.paid[0][1]
	}
	likely, cheap := id("3x1x1", outP, "02a"), id("2x1x1", outP, inP)
	assert.Equal(t, cheap, route(0, 0), "the node's rate, 0: fees and hops")
	assert.Equal(t, likely, route(0, 200))
	assert.Equal(t, likely, route(200, 0), "the node's rate")
	assert.Equal(t, cheap, route(200, -1), "below 0: off for this rebalance")
}

// A run said which nodes failed its payments only in the log. Each split
// now counts them, by node, whatever its outcome; a failure on our own in
// channel is about our channel, not the node.
func TestFailuresAreCountedByNode(t *testing.T) {
	network := func() (*network, *graph.Channel, *graph.Channel) {
		n := &network{Graph: graph.NewGraph(), liquidity: map[string]uint64{}}
		out := addChannel(n.Graph, "1x1x1", self, outP, 0)
		in := addChannel(n.Graph, "9x1x1", inP, self, 0)
		addChannel(n.Graph, "2x1x1", outP, "02a", 0)
		addChannel(n.Graph, "3x1x1", "02a", inP, 0)
		addChannel(n.Graph, "4x1x1", outP, "02b", 1)
		addChannel(n.Graph, "5x1x1", "02b", inP, 0)
		addChannel(n.Graph, "6x1x1", outP, "02c", 2)
		addChannel(n.Graph, "7x1x1", "02c", inP, 0)
		return n, out, in
	}

	// 02a cannot forward, then the out-peer cannot send to 02b; 02c works
	n, out, in := network()
	n.liquidity[id("3x1x1", "02a", inP)] = 0
	n.liquidity[id("4x1x1", outP, "02b")] = 0
	r := rebalanceOn(n, out, in, 100000000)
	r.Attempts = 3
	r.Sent = &atomic.Int64{}
	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Equal(t, 3, len(n.paid))
	assert.Equal(t, map[string]int{"02a": 1, outP: 1}, result.FailedNodes)
	assert.Equal(t, int64(3), r.Sent.Load(), "each payment counted as it is sent")

	// the in-peer cannot send to us over our channel: not its failure
	n, out, in = network()
	n.liquidity[id("9x1x1", inP, self)] = 0
	r = rebalanceOn(n, out, in, 100000000)
	r.Attempts = 3
	result = r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, 1, len(n.paid))
	assert.Empty(t, result.FailedNodes)

	// our own node, or a node that is not on the route: not counted either
	for _, erring := range []*glightning.PaymentErrorData{
		{ErringNode: self, ErringChannel: "1x1x1", ErringIndex: 0, FailCode: failTemporaryChannelFailure},
		{ErringNode: "02zz", ErringChannel: "8x8x8", ErringIndex: 2, FailCode: failTemporaryChannelFailure},
	} {
		n, out, in = network()
		r = rebalanceOn(n, out, in, 100000000)
		data := erring
		r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
			n.paid = append(n.paid, route.ChannelIds())
			return nil, &glightning.PaymentError{Data: data}
		}
		result = r.Run()
		assert.Equal(t, "failure", result.Status)
		assert.Equal(t, 1, len(n.paid))
		assert.Empty(t, result.FailedNodes, erring.ErringNode)
	}
}
