package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/util"
	"encoding/json"
	"sync"
	"testing"

	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
	"github.com/stretchr/testify/assert"
)

// depletedOne is a run whose candidate depleted is beyond its threshold.
type depletedOne struct {
	*fakeSplits
	depleted string
}

func (d *depletedOne) CanUseChannel(c *glightning.PeerChannel) error {
	if c.ShortChannelId == d.depleted {
		return util.ErrChannelDepleted
	}
	return nil
}

// quoteNetwork is a node 02self in an empty graph, with add to add a channel
// (10M sats, half on each side) and peer to add a connected peer with one
// channel.
func quoteNetwork() (*node.Node, func(scid, from, to string, baseFee uint64) *graph.Channel, func(id, scid string)) {
	g := graph.NewGraph()
	n := &node.Node{Id: "02self", Graph: g, PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{}}
	add := func(scid, from, to string, baseFee uint64) *graph.Channel {
		c := graph.NewChannel(&glightning.Channel{Source: from, Destination: to, ShortChannelId: scid, IsActive: true,
			AmountMsat: glightning.AmountFromMSat(10000000000), BaseFeeMillisatoshi: baseFee, Delay: 10,
			HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0), HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000000)},
			5000000000, 0)
		g.AddChannel(c)
		g.Channels[scid+"/"+util.GetDirection(from, to)] = c
		return c
	}
	peer := func(id, scid string) {
		n.Peers[id] = &glightning.Peer{Id: id, Connected: true, Channels: []*glightning.PeerChannel{{
			PeerId: id, ShortChannelId: scid, State: rebalance.NORMAL, PeerConnected: true,
			TotalMsat: glightning.AmountFromMSat(10000000000), ToUsMsat: glightning.AmountFromMSat(5000000000),
			SpendableMsat: glightning.AmountFromMSat(5000000000), ReceivableMsat: glightning.AmountFromMSat(5000000000)}}}
	}
	return n, add, peer
}

// quoteRun is a run over candidates into (pull) or out of (push) target,
// with maxppm 100 and 100k-sat splits.
func quoteRun(n *node.Node, target *graph.Channel, depleted string, candidates ...*graph.Channel) *AbstractRebalance {
	q := deque.New[*graph.Channel]()
	for _, c := range candidates {
		q.PushBack(c)
	}
	a := &AbstractRebalance{Node: n, Candidates: q, QueueLock: &sync.Mutex{}, TargetChannel: target,
		splitAmount: 100000000, maxPPM: 100, maxHops: 8, attempts: 1}
	a.RebalanceMethods = &depletedOne{fakeSplits: &fakeSplits{AbstractRebalance: a}, depleted: depleted}
	return a
}

// A quote of a pull: for each candidate, in the order a run tries them, the
// route its first payment would take, or why there would be none.
func TestQuoteOfAPull(t *testing.T) {
	n, add, peer := quoteNetwork()
	n.Graph.Aliases["02p0"], n.Graph.Aliases["02m"], n.Graph.Aliases["02in"] = "Zero", "Middle", "Target"
	cheap := add("1x1x1", "02self", "02p0", 0)
	dear := add("2x1x1", "02self", "02p1", 0)
	low := add("3x1x1", "02self", "02p2", 0)
	target := add("9x1x1", "02in", "02self", 0)
	add("4x1x1", "02p0", "02m", 0)
	add("5x1x1", "02m", "02in", 1000)    // 10 ppm on 100k sats
	add("6x1x1", "02p1", "02in", 100000) // 1000 ppm
	add("7x1x1", "02p2", "02in", 0)
	peer("02p0", "1x1x1")
	peer("02p1", "2x1x1")
	peer("02p2", "3x1x1")
	peer("02in", "9x1x1")
	a := quoteRun(n, target, "3x1x1", cheap, dear, low)

	quote := a.quote("pull", true)
	assert.Equal(t, "pull", quote.Mode)
	assert.Equal(t, "9x1x1", quote.Target)
	assert.Equal(t, "Target", quote.TargetPeer)
	assert.Equal(t, uint64(100000), quote.Amount)
	if assert.Equal(t, 3, len(quote.Candidates)) {
		c := quote.Candidates[0]
		assert.Equal(t, "1x1x1", c.Scid)
		assert.Equal(t, "Zero", c.Alias)
		assert.Equal(t, "02p0", c.NodeId)
		assert.Empty(t, c.Error)
		if assert.NotNil(t, c.PPM) {
			assert.Equal(t, uint64(10), *c.PPM)
			assert.Equal(t, uint64(1000), *c.FeeMsat)
		}
		assert.Equal(t, 4, c.Hops)
		assert.Equal(t, []string{"Zero", "Middle", "Target"}, c.Path)
		if assert.NotNil(t, c.Estimate) {
			assert.Equal(t, 0.98, *c.Estimate)
			assert.Equal(t, 2, *c.GuessedHops)
		}

		c = quote.Candidates[1]
		assert.Equal(t, "2x1x1", c.Scid)
		assert.Nil(t, c.PPM)
		assert.Contains(t, c.Error, "route too expensive")
		assert.Equal(t, uint64(1000), c.CheapestPPM)

		c = quote.Candidates[2]
		assert.Equal(t, "3x1x1", c.Scid)
		assert.Equal(t, util.ErrChannelDepleted.Error(), c.Skipped)
	}
	assert.Equal(t, "1 of 3 candidates have a route within 100 ppm (cheapest 10 ppm); "+
		"1 would fail (the cheapest route above maxppm is 1000 ppm); 1 skipped", quote.Summary)

	out, err := json.Marshal(quote.Candidates[1])
	assert.NoError(t, err)
	assert.NotContains(t, string(out), `"ppm"`, "no route, no ppm")
	assert.Contains(t, string(out), `"cheapest_ppm":1000`)
	assert.Equal(t, 3, a.Candidates.Len(), "the queue is left as it was")
}

// In a push the target is our out channel and the candidates are in channels.
func TestQuoteOfAPush(t *testing.T) {
	n, add, peer := quoteNetwork()
	n.Graph.Aliases["02t"], n.Graph.Aliases["02c"] = "Target", "Candidate"
	target := add("1x1x1", "02self", "02t", 0)
	candidate := add("9x1x1", "02c", "02self", 0)
	add("2x1x1", "02t", "02x", 0)
	add("3x1x1", "02x", "02c", 0)
	peer("02t", "1x1x1")
	peer("02c", "9x1x1")

	quote := quoteRun(n, target, "", candidate).quote("push", false)
	assert.Equal(t, "Target", quote.TargetPeer)
	if assert.Equal(t, 1, len(quote.Candidates)) {
		c := quote.Candidates[0]
		assert.Equal(t, "9x1x1", c.Scid)
		assert.Equal(t, "Candidate", c.Alias, "the candidate's peer: the one sending to us")
		assert.Empty(t, c.Error)
		assert.Equal(t, []string{"Target", "02x", "Candidate"}, c.Path)
	}
	assert.Equal(t, "1 of 1 candidates have a route within 100 ppm (cheapest 0 ppm)", quote.Summary)
}
