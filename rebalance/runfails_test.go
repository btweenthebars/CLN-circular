package rebalance

import (
	"fmt"
	"sync"
	"testing"

	"circular/graph"
	"circular/node"

	"github.com/stretchr/testify/assert"
)

func TestRunFailures(t *testing.T) {
	f := &RunFailures{Limit: 3}
	assert.False(t, f.fail("02a"))
	assert.False(t, f.fail("02b"))
	assert.False(t, f.fail("02a"))
	assert.True(t, f.fail("02a"), "the third")
	assert.False(t, f.fail("02a"), "dropped once")
	assert.Equal(t, []string{"02a"}, f.Dropped())
	for i := 0; i < 2; i++ {
		f.fail("02b")
	}
	assert.Equal(t, []string{"02a", "02b"}, f.Dropped())
	f.fail("02b")
	assert.Equal(t, "02a", f.DroppedNodes()[0].NodeId, "most first, then in the order dropped")
	f.fail("02b")
	assert.Equal(t, DroppedNode{NodeId: "02b", Failures: 5}, f.DroppedNodes()[0])

	assert.Equal(t, DEFAULT_MAX_RUN_NODE_FAILS, (&RunFailures{}).limit())
	off := &RunFailures{Limit: -1}
	for i := 0; i < 50; i++ {
		assert.False(t, off.fail("02a"))
	}
	assert.Empty(t, off.Dropped())
	var none *RunFailures
	assert.Nil(t, none.Dropped())
	assert.Nil(t, none.DroppedNodes())

	// the splits' goroutines share it
	shared := &RunFailures{Limit: 100}
	var wg sync.WaitGroup
	drops := make(chan bool, 400)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				drops <- shared.fail("02a")
			}
		}()
	}
	wg.Wait()
	close(drops)
	n := 0
	for d := range drops {
		if d {
			n++
		}
	}
	assert.Equal(t, 1, n)
	assert.Equal(t, 400, shared.DroppedNodes()[0].Failures)
}

// A node short of liquidity in the middle of routes fails payments across
// a run's splits: maxnodefails starts over with each split, the run's count
// does not. At its limit the node is left out of every split's searches.
func TestADrainedNodeIsDroppedForTheRun(t *testing.T) {
	// out-peer -> 02hub over 4 ways, each short; out-peer -> 02ok -> 02b carries
	build := func() (*network, *Rebalance) {
		n, out, in := shortRoute()
		n.liquidity[id("3x1x1", "02a", "02b")] = 0
		for i := 1; i <= 4; i++ {
			c := addChannel(n.Graph, fmt.Sprintf("6x1x%d", i), "02a", "02b", 0)
			n.liquidity[channelId(c)] = 0
		}
		addChannel(n.Graph, "7x1x1", outP, "02ok", 5000)
		addChannel(n.Graph, "8x1x1", "02ok", "02b", 5000)
		r := rebalanceOn(n, out, in, 50000000)
		r.Attempts, r.MaxNodeFails = 3, 50
		return n, r
	}
	shared := &RunFailures{Limit: 4}
	_, r := build()
	r.RunFailures = shared
	res := r.Run()
	assert.Equal(t, "failure", res.Status, "three tries, all through 02a")
	assert.Empty(t, shared.Dropped())
	// another split of the same run: its first failure is the run's fourth
	n2, r2 := build()
	r2.Node.Graph = n2.Graph
	r2.RunFailures = shared
	r2.Attempts = 5
	res = r2.Run()
	assert.Equal(t, []string{"02a"}, shared.Dropped())
	assert.Equal(t, "success", res.Status, res.Message)
	assert.Equal(t, 2, len(n2.paid), "one more failure through 02a, then around it")
	assert.Contains(t, res.Channels, id("7x1x1", outP, "02ok"))
	// a split started after: around it from its first search
	n3, r3 := build()
	r3.RunFailures = shared
	res = r3.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, 1, len(n3.paid))
}

// Failures at our out-peer (it is a candidate's own peer) and on our in
// channel are not the run's to count; and our two peers are never left out,
// though dropped.
func TestRunFailuresSpareOurPeers(t *testing.T) {
	n, out, in := shortRoute()
	n.liquidity[id("2x1x1", outP, "02a")] = 0
	r := rebalanceOn(n, out, in, 50000000)
	r.RunFailures = &RunFailures{Limit: 1}
	r.Run()
	assert.Empty(t, r.RunFailures.Dropped(), "a failure at the out-peer (k 1) does not count")

	r.RunFailures.fail(outP)
	r.RunFailures.fail(inP)
	r.RunFailures.fail("02z")
	exclude := map[string]bool{}
	r.excludeDropped(exclude)
	assert.Equal(t, map[string]bool{"02z": true}, exclude)
}

// What every search starts without: the graph's avoid list, the saved
// avoid list (a short channel id as both directions), the call's own; never
// our node's peers at the ends nor our two channels.
func TestStartExclude(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 50000000)
	dir := t.TempDir()
	for _, k := range []string{"03" + fmt.Sprintf("%064x", 7), "3x1x1", "4x1x1/1", out.ShortChannelId, in.ShortChannelId} {
		_, err := r.Node.AvoidCall(dir, &node.AvoidCmd{Add: k})
		assert.NoError(t, err, k)
	}
	r.Exclude = []string{"9x9x9/0", "02zz", inP, outP}
	n.Avoid("02late", 1<<40)
	ex := r.startExclude()
	for _, k := range []string{"03" + fmt.Sprintf("%064x", 7), "3x1x1/0", "3x1x1/1", "4x1x1/1", "9x9x9/0", "02zz", "02late", self} {
		assert.True(t, ex[k], k)
	}
	for _, k := range []string{outP, inP, channelId(out), channelId(in), "4x1x1/0"} {
		assert.False(t, ex[k], k)
	}
	// the saved list keeps searches off it: the only way goes through 3x1x1
	_, err := r.getRoute(ex)
	assert.Error(t, err)
}

// A node the run's other splits dropped meanwhile is left out from this
// split's next attempt on, and a known route through it is not reused.
func TestDroppedMeanwhile(t *testing.T) {
	build := func() (*network, *Rebalance) {
		n, out, in := shortRoute()
		n.liquidity[id("3x1x1", "02a", "02b")] = 0
		c := addChannel(n.Graph, "6x1x1", "02a", "02b", 0)
		n.liquidity[channelId(c)] = 0
		addChannel(n.Graph, "7x1x1", outP, "02ok", 5000)
		addChannel(n.Graph, "8x1x1", "02ok", "02b", 5000)
		r := rebalanceOn(n, out, in, 50000000)
		r.Attempts, r.MaxNodeFails = 5, 50
		return n, r
	}
	n, r := build()
	shared := &RunFailures{Limit: 50}
	r.RunFailures = shared
	pay := r.pay
	r.pay = func(route *graph.Route) (*graph.PrettyRoute, error) {
		if len(n.paid) == 0 {
			for i := 0; i < 50; i++ { // another split drops 02a while this payment is out
				shared.fail("02a")
			}
		}
		return pay(route)
	}
	res := r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, 2, len(n.paid), "the first through 02a, then around it, though 6x1x1 was never tried")

	// a route that worked before, through a node dropped since: searched instead
	n, r = build()
	r.Route = []string{id("1x1x1", self, outP), id("2x1x1", outP, "02a"), id("3x1x1", "02a", "02b"),
		id("4x1x1", "02b", inP), id("5x1x1", inP, self)}
	n.liquidity[id("3x1x1", "02a", "02b")] = 1 << 40
	r.RunFailures = &RunFailures{Limit: 1}
	r.RunFailures.fail("02a")
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Contains(t, res.Channels, id("7x1x1", outP, "02ok"))
	assert.Equal(t, 1, len(n.paid))
}
