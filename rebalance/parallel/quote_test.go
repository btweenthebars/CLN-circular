package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/util"
	"encoding/json"
	"sync"
	"testing"
	"time"

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

// Draining one of our channels gives up the fee we charge on it; filling one
// gives it more to earn.
func TestOwnFeeCost(t *testing.T) {
	n, add, peer := quoteNetwork()
	out := add("1x1x1", "02self", "02a", 5000) // our fee: 50 ppm on 100k sats
	in := add("1x1x1", "02a", "02self", 0)
	peer("02a", "1x1x1")
	a := quoteRun(n, nil, "")

	assert.Equal(t, 50.0, a.ownFeeCost(out), "circular-pull: the candidate is our out channel")
	assert.Equal(t, -50.0, a.ownFeeCost(in), "circular-push: the candidate is our in channel")
	assert.Equal(t, 0.0, a.ownFeeCost(graph.NewChannel(&glightning.Channel{ShortChannelId: "9x9x9", Source: "02self", Destination: "02z"}, 0, 0)), "unknown")
}

// A quoted route lists the nodes on it remembered for failing payments
// (circular-node-memory), and what each adds to it in the search.
func TestQuoteShowsRememberedNodes(t *testing.T) {
	n, add, peer := quoteNetwork()
	n.Graph.Aliases["02m"] = "Middle"
	n.RiskPPM = 200
	cheap := add("1x1x1", "02self", "02p0", 0)
	target := add("9x1x1", "02in", "02self", 0)
	add("4x1x1", "02p0", "02m", 0)
	add("5x1x1", "02m", "02in", 1000) // the only way
	peer("02p0", "1x1x1")
	peer("02in", "9x1x1")

	quote := quoteRun(n, target, "", cheap).quote("pull", true)
	assert.Empty(t, quote.Candidates[0].Remembered, "the memory is off")

	n.Graph.SetNodeMemory(24 * time.Hour)
	for i := 0; i < 100; i++ {
		n.Graph.NodeFailed("02m")
	}
	n.Graph.NodeFailed("02in") // the in-peer: its channel on the route is ours, never charged
	quote = quoteRun(n, target, "", cheap).quote("pull", true)
	assert.Equal(t, []RememberedHop{{NodeId: "02m", Alias: "Middle", Score: 100, PenaltyPPM: 400}},
		quote.Candidates[0].Remembered, "100/50 x 200 ppm")
	assert.Contains(t, quote.Summary, "1 of 1 candidates have a route within 100 ppm (cheapest 10 ppm), "+
		"1 of them through nodes remembered for failing payments")

	a := quoteRun(n, target, "", cheap)
	a.riskPPM = -1 // fees only: the search charges nothing
	assert.Equal(t, uint64(0), a.quote("pull", true).Candidates[0].Remembered[0].PenaltyPPM)
	assert.Equal(t, uint64(1000), graph.NodePenaltyPPM(1e6, 200), "capped at NODE_SCORE_MAX")
}

// withRoom is a run whose candidates each have 300k sats of room.
type withRoom struct{ *depletedOne }

func (r *withRoom) room(*glightning.PeerChannel) uint64 { return 300000000 }

func ppm(v uint64) *uint64 { return &v }

// The whole amount: cheapest route first, each candidate up to its room.
func TestWholeQuote(t *testing.T) {
	cs := []*QuoteCandidate{
		{Scid: "a", PPM: ppm(500), RoomSat: 1000000},
		{Scid: "b", PPM: ppm(300), RoomSat: 600000},
		{Scid: "c", PPM: ppm(900), Error: "no route"},
		{Scid: "d", PPM: ppm(100), Skipped: "depleted", RoomSat: 9000000},
		{Scid: "e", PPM: ppm(700), RoomSat: 2000000},
		{Scid: "f", PPM: ppm(50), RoomSat: 0},
	}
	w := wholeQuote(2000000000, cs)
	assert.Equal(t, uint64(2000000), w.Amount)
	assert.Equal(t, uint64(2000000), w.Covered)
	assert.Equal(t, 3, w.Candidates, "b 600k, a 1M, e the last 400k")
	assert.Equal(t, uint64(300), w.MinPPM)
	assert.Equal(t, uint64(700), w.TopPPM)
	assert.Equal(t, uint64(180+500+280), w.FeeSat)
	assert.Equal(t, uint64(480), w.AvgPPM)

	// one route at 272 ppm for an amount that does not divide evenly: avg 272, not 271
	one := []*QuoteCandidate{{Scid: "1x1x1", PPM: ppm(272), RoomSat: 3000000}}
	odd := wholeQuote(2042397000, one)
	assert.Equal(t, uint64(272), odd.AvgPPM)
	assert.Equal(t, uint64(556), odd.FeeSat, "555.5 sat rounds to 556")
	assert.Contains(t, w.Verdict, "the whole 2000000 sat: 3 candidates, routes 300-700 ppm (avg 480)")

	w = wholeQuote(5000000000, cs)
	assert.Equal(t, uint64(3600000), w.Covered)
	assert.Contains(t, w.Verdict, "only 3600000 of 5000000 sat")

	w = wholeQuote(1000000000, cs[2:4])
	assert.Equal(t, uint64(0), w.Covered)
	assert.Equal(t, "no candidate has a route within maxppm and room to give", w.Verdict)
}

func TestQuoteHasRoomAndTheWhole(t *testing.T) {
	n, add, peer := quoteNetwork()
	cheap := add("1x1x1", "02self", "02p0", 0)
	target := add("9x1x1", "02in", "02self", 0)
	add("4x1x1", "02p0", "02in", 0)
	peer("02p0", "1x1x1")
	peer("02in", "9x1x1")
	a := quoteRun(n, target, "", cheap)
	a.amount = 500000000
	a.RebalanceMethods = &withRoom{a.RebalanceMethods.(*depletedOne)}
	quote := a.quote("pull", true)
	assert.Equal(t, uint64(300000), quote.Candidates[0].RoomSat)
	if assert.NotNil(t, quote.Whole) {
		assert.Equal(t, uint64(300000), quote.Whole.Covered, "500k asked, 300k of room")
	}
}

// What a run may take from or give to a channel: its balance above what the
// run keeps there.
func TestRoom(t *testing.T) {
	c := &glightning.PeerChannel{TotalMsat: glightning.AmountFromMSat(10000000000),
		ToUsMsat: glightning.AmountFromMSat(3000000000)}
	pull := &RebalancePull{DepleteUpToPercent: 0.2, DepleteUpToAmount: 1000000000}
	assert.Equal(t, uint64(2000000000), pull.room(c), "3M ours, 1M kept (the lower of 20% and 1M)")
	push := &RebalancePush{FillUpToPercent: 0.2, FillUpToAmount: 5000000000}
	assert.Equal(t, uint64(5000000000), push.room(c), "7M theirs, 2M kept")
	pull.DepleteUpToAmount = 4000000000
	assert.Equal(t, uint64(1000000000), pull.room(c))
	c.ToUsMsat = glightning.AmountFromMSat(1000000000)
	assert.Equal(t, uint64(0), pull.room(c), "under what it keeps")

	// whole splits only, as CanUseChannel sends them: a pull's each with its fee at maxppm
	c.ToUsMsat = glightning.AmountFromMSat(3000000000)
	pull.DepleteUpToAmount = 1000000000
	pull.splitAmount, pull.maxPPM = 300000000, 1000 // 300k + 300 sat fee each
	assert.Equal(t, uint64(1800000000), pull.room(c), "2M of room: 6 splits of 300k+300, not 2M")
	pull.splitAmount = 500000000
	assert.Equal(t, uint64(1500000000), pull.room(c), "500k+500 each: 3 fit in 2M")
	push.splitAmount = 1500000000
	assert.Equal(t, uint64(4500000000), push.room(c), "5M of room: 3 splits of 1.5M")
	assert.Equal(t, uint64(7), wholeSplits(7, 0, 0), "no split size: the room as it is")
	assert.Equal(t, uint64(0), wholeSplits(299, 100, 300))
}
