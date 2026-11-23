package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
)

var nodeBad = "03" + strings.Repeat("b", 64)

func TestSetAvoid(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	assert.NoError(t, p.setAvoid([]string{" " + strings.ToUpper(nodeBad), "5x5x5", "6x6x6/1"}, 3))
	assert.Equal(t, []string{nodeBad, "5x5x5", "6x6x6/1"}, p.callAvoid)
	assert.Equal(t, 3, p.runFailures.Limit)
	assert.Equal(t, []string{nodeBad, "5x5x5/0", "5x5x5/1", "6x6x6/1"}, p.splitExclude())
	err := p.setAvoid([]string{"CoinGate"}, 0)
	assert.Contains(t, err.Error(), "avoid: \"coingate\" is not a node id")

	// the splits get it, and share the run's failures
	assert.NoError(t, p.setAvoid([]string{"5x5x5"}, 0))
	out := p.Candidates.Front()
	split := p.newSplit(out, p.TargetChannel, out, 100000000)
	assert.Same(t, p.runFailures, split.RunFailures)
	assert.Contains(t, split.Exclude, "5x5x5/1")
	assert.Contains(t, split.Exclude, "5x5x5/0")
	p.probeProven = map[string]bool{"7x7x7/0": true}
	assert.Contains(t, p.newSplit(out, p.TargetChannel, out, 100000000).Exclude, "7x7x7/0", "a probe-only run's too")

	// the commands take it, as a list or as one string
	for _, v := range []interface{}{[]interface{}{nodeBad, "5x5x5"}, nodeBad + ",5x5x5", nodeBad + " 5x5x5"} {
		pull := &RebalancePull{}
		assert.NoError(t, jrpc2.ParseNamedParams(pull, map[string]interface{}{"inscid": "1x1x1", "avoid": v,
			"maxrunnodefails": "-1"}))
		assert.Equal(t, []string{nodeBad, "5x5x5"}, pull.Avoid.V)
		assert.Equal(t, -1, int(pull.MaxRunNodeFails))
		push := &RebalancePush{}
		assert.NoError(t, jrpc2.ParseNamedParams(push, map[string]interface{}{"outscid": "1x1x1", "avoid": v}))
		assert.Equal(t, []string{nodeBad, "5x5x5"}, push.Avoid.V)
		q := &RebalanceQuote{}
		assert.NoError(t, jrpc2.ParseNamedParams(q, map[string]interface{}{"inscid": "1x1x1", "avoid": v}))
		assert.Equal(t, []string{nodeBad, "5x5x5"}, q.pull().Avoid.V)
		assert.Equal(t, []string{nodeBad, "5x5x5"}, q.push().Avoid.V)
	}
	q := &RebalanceQuote{MaxRunNodeFails: 4}
	assert.Equal(t, 4, int(q.pull().MaxRunNodeFails))
	assert.Equal(t, 4, int(q.push().MaxRunNodeFails))
}

// A candidate on the avoid list, or whose peer is, is never fired: the
// saved list (circular-avoid) or the run's own.
func TestAvoidedCandidates(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400}, probeWay{from: "02o2", via: "02b", ppm: 400})
	c1, c2 := p.Candidates.At(0), p.Candidates.At(1)
	assert.Equal(t, "", p.avoided(c1))
	p.callAvoid = []string{"02o1"}
	assert.Equal(t, "avoided: 02o1 is on this run's avoid", p.avoided(c1))
	assert.Equal(t, "", p.avoided(c2))
	p.callAvoid = []string{"2x1x1"}
	assert.Equal(t, "avoided: 2x1x1 is on this run's avoid", p.avoided(c2))
	p.callAvoid = []string{candidateId(c2)}
	assert.Equal(t, "avoided: "+candidateId(c2)+" is on this run's avoid", p.avoided(c2))
	p.callAvoid = nil
	_, err := p.Node.AvoidCall(t.TempDir(), &node.AvoidCmd{Add: "1x1x1"})
	assert.NoError(t, err)
	assert.Equal(t, "avoided: 1x1x1 is on the avoid list (circular-avoid)", p.avoided(c1))
	p.callAvoid = []string{"1x1x1"}
	assert.Equal(t, "avoided: 1x1x1 is on the avoid list (circular-avoid)", p.avoided(c1), "the saved list named first")

	// a run: 1x1x1 skipped, 2x1x1 carries
	p.callAvoid = nil
	res := stepRun(t, p.realSplits)
	assert.Equal(t, uint64(300000), res.CarriedAmount)
	for _, probe := range p.probes {
		assert.False(t, strings.HasPrefix(probe, "1x1x1"), probe)
	}
	// nothing else: the run says why
	q := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	q.callAvoid = []string{"02o1", "2x1x1"}
	res = stepRun(t, q.realSplits)
	assert.Empty(t, q.probes)
	assert.Equal(t, "no candidates left: 2 skipped (avoided: 02o1 is on this run's avoid: 1, avoided: 2x1x1 is on "+
		"this run's avoid: 1)", res.Stopped)

	// the quote lists it as skipped
	r := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400}, probeWay{from: "02o2", via: "02b", ppm: 400})
	r.callAvoid = []string{"02o2"}
	quote := r.quote("pull", true)
	if assert.Equal(t, 2, len(quote.Candidates)) {
		var skipped *QuoteCandidate
		for _, c := range quote.Candidates {
			if c.Scid == "2x1x1" {
				skipped = c
			}
		}
		if assert.NotNil(t, skipped) {
			assert.Equal(t, "avoided: 02o2 is on this run's avoid", skipped.Skipped)
		}
	}
}

// A node that fails maxrunnodefails of the run's payments in the middle of
// their routes, across the splits, is routed around for the rest of it.
func TestARunDropsANodeThatKeepsFailing(t *testing.T) {
	p := probePull(t, 1,
		probeWay{from: "02o1", via: "02bad", ppm: 100, holds: 1},
		probeWay{from: "02o2", via: "02bad", ppm: 100, holds: 1},
		probeWay{from: "02o2", via: "02good", ppm: 600})
	g := p.Node.Graph
	for i := 4; i <= 6; i++ { // more short ways out of 02bad
		c := netChannel(g, fmt.Sprintf("%dx3x1", i), "02bad", "02in", 0)
		p.holds[candidateId(c)] = 1
	}
	p.maxNodeFails = 50
	p.runFailures = &rebalance.RunFailures{Limit: 3}
	p.amount = 100000000
	var routes [][]string
	inner := p.probe
	p.probe = func(route *graph.Route) error {
		routes = append(routes, route.ChannelIds())
		return inner(route)
	}
	res := stepRun(t, p.realSplits)
	assert.Equal(t, uint64(100000), res.CarriedAmount, "via 02good")
	if assert.Equal(t, 1, len(res.DroppedNodes)) {
		assert.Equal(t, "02bad", res.DroppedNodes[0].NodeId)
		assert.Equal(t, 3, res.DroppedNodes[0].Failures)
	}
	through := func(ids []string) bool {
		for _, id := range ids {
			if strings.Contains(id, "x3x1/") && !strings.HasPrefix(id, "3x3x1") {
				return true
			}
		}
		return false
	}
	bad := 0
	for _, r := range routes {
		if through(r) {
			bad++
		}
	}
	assert.Equal(t, 3, bad, "three failures through 02bad, then none")
	assert.False(t, through(routes[len(routes)-1]))

	// no limit: tried until each split's own runs out
	p = probePull(t, 1, probeWay{from: "02o1", via: "02bad", ppm: 100, holds: 1})
	p.runFailures = &rebalance.RunFailures{Limit: -1}
	res = stepRun(t, p.realSplits)
	assert.Empty(t, res.DroppedNodes)
}

// In slow mode the count starts over each round (the liquidity may be back);
// the result keeps every round's.
func TestDroppedNodesAcrossRounds(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	p.Result = NewResult(p.amount)
	p.maxRunNodeFails = 2
	p.runFailures = &rebalance.RunFailures{Limit: 2}
	p.Node.Graph.Aliases["02x"] = "Hub"
	shared := p.runFailures
	for i := 0; i < 2; i++ {
		rebalance.FailForTest(shared, "02x")
	}
	p.refind = func() error { return nil }
	p.newRound(p.maxPPM, false)
	assert.NotSame(t, shared, p.runFailures)
	assert.Equal(t, 2, p.runFailures.Limit)
	assert.Empty(t, p.runFailures.Dropped())
	rebalance.FailForTest(p.runFailures, "02y")
	rebalance.FailForTest(p.runFailures, "02y")
	assert.Equal(t, []rebalance.DroppedNode{{NodeId: "02x", Alias: "Hub", Failures: 2}, {NodeId: "02y", Failures: 2}},
		p.droppedNodes())
	// dropped again in another round: listed once, with its failures in both
	p.newRound(p.maxPPM, false)
	for i := 0; i < 3; i++ {
		rebalance.FailForTest(p.runFailures, "02x")
	}
	assert.Equal(t, []rebalance.DroppedNode{{NodeId: "02x", Alias: "Hub", Failures: 5}, {NodeId: "02y", Failures: 2}},
		p.droppedNodes())
	// most failures first over the whole run, not in the order they were dropped
	for i := 0; i < 4; i++ {
		rebalance.FailForTest(p.runFailures, "02y")
	}
	assert.Equal(t, []rebalance.DroppedNode{{NodeId: "02y", Failures: 6}, {NodeId: "02x", Alias: "Hub", Failures: 5}},
		p.droppedNodes())
}

// A run refuses to start while the saved list cannot be read: what it holds
// could not be avoided.
func TestNoRunWithAnUnreadableAvoidList(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, node.AVOID_FILE), []byte("{broken"), 0o600))
	assert.Error(t, p.Node.LoadAvoidList(dir))
	err := p.setAvoid(nil, 0)
	assert.Contains(t, err.Error(), "the avoid list cannot be read")
	assert.NoError(t, os.Remove(filepath.Join(dir, node.AVOID_FILE)))
	assert.NoError(t, p.Node.LoadAvoidList(dir))
	assert.NoError(t, p.setAvoid(nil, 0))
}

// The quote's searches leave the run's avoid out too.
func TestTheQuoteAvoids(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 100}, probeWay{from: "02o1", via: "02b", ppm: 700})
	quoted := func() []string {
		for _, c := range p.quote("pull", true).Candidates {
			if c.Scid == "1x1x1" {
				return c.Path
			}
		}
		return nil
	}
	assert.Equal(t, []string{"02o1", "02a", "02in"}, quoted())
	p.callAvoid = []string{"02a"}
	assert.Equal(t, []string{"02o1", "02b", "02in"}, quoted())
}

// The pair quote (inscid and outscid) checks and applies its avoid too.
func TestThePairQuoteAvoids(t *testing.T) {
	viaA, viaB := "02"+strings.Repeat("a", 64), "02"+strings.Repeat("b", 64)
	p := probePull(t, 1, probeWay{from: "02o1", via: viaA, ppm: 100}, probeWay{from: "02o1", via: viaB, ppm: 700})
	p.Node.IndexPeersForTest()
	q := &RebalanceQuote{InScid: "9x1x1", OutScid: "1x1x1", Amount: 100000, MaxPPM: 2000}
	path := func() []string {
		res, err := q.quotePair(p.Node)
		assert.NoError(t, err)
		return res.Candidates[0].Path
	}
	assert.Equal(t, []string{"02o1", viaA, "02in"}, path())
	q.Avoid.V = []string{strings.ToUpper(viaA)}
	assert.Equal(t, []string{"02o1", viaB, "02in"}, path())
	q.Avoid.V = []string{"CoinGate"}
	_, err := q.quotePair(p.Node)
	assert.Contains(t, err.Error(), "avoid: ")
	for _, k := range []string{"9x1x1/1", "1x1x1"} { // its own two channels
		q.Avoid.V = []string{k}
		_, err = q.quotePair(p.Node)
		assert.Contains(t, err.Error(), "cannot be avoided", k)
	}
	q.Avoid.V = nil
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, node.AVOID_FILE), []byte("{broken"), 0o600))
	assert.Error(t, p.Node.LoadAvoidList(dir))
	_, err = q.quotePair(p.Node)
	assert.Contains(t, err.Error(), "the avoid list cannot be read")
}

// A call's avoid that names the run's own channel or its peer is refused,
// not ignored: the run goes through them.
func TestAvoidsEnd(t *testing.T) {
	self := "02self"
	in := &graph.Channel{Channel: &glightning.Channel{ShortChannelId: "9x1x1", Source: "02in", Destination: self}}
	out := &graph.Channel{Channel: &glightning.Channel{ShortChannelId: "1x1x1", Source: self, Destination: "02o1"}}
	for _, k := range []string{"9x1x1", "9x1x1/0", "9x1x1/1", "02in"} {
		err := avoidsEnd([]string{"5x5x5", k}, self, in)
		assert.Contains(t, err.Error(), "avoid: "+k+" is this run's own channel 9x1x1 or its peer", k)
	}
	for _, k := range []string{"1x1x1/1", "02o1"} {
		assert.Error(t, avoidsEnd([]string{k}, self, in, out), k)
		assert.NoError(t, avoidsEnd([]string{k}, self, in), k)
	}
	assert.NoError(t, avoidsEnd([]string{"99x1x1", "9x1x11/0", "19x1x1/1", self + "x"}, self, in, out))
	assert.NoError(t, avoidsEnd(nil, self, in, out))
}

// A run's soft avoid: the call's keys (validated, scids both ways), then the
// nodes remembered at the score or more, most failing first; never our node.
func TestSoftAvoidOf(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 100})
	g := p.Node.Graph
	g.SetNodeMemory(24 * time.Hour)
	hub, other, self := "03"+strings.Repeat("c", 64), "02"+strings.Repeat("d", 64), p.Node.Id
	for i := 0; i < 12; i++ {
		g.NodeFailed(hub)
	}
	for i := 0; i < 6; i++ {
		g.NodeFailed(other)
		g.NodeFailed(self)
	}
	for i := 0; i < 2; i++ {
		g.NodeFailed("02e" + strings.Repeat("e", 63))
	}
	soft, scores, err := softAvoidOf(p.Node, []string{strings.ToUpper(other), "5x5x5"}, 5, 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{other, "5x5x5/0", "5x5x5/1", hub}, soft.Active(), "given first, then remembered; once each")
	assert.Equal(t, rebalance.DEFAULT_SOFT_AVOID_STREAK, soft.Limit)
	assert.InDelta(t, 12, scores[hub], 0.1)
	_, has := scores["02e"+strings.Repeat("e", 63)]
	assert.False(t, has, "under the score")

	soft, _, err = softAvoidOf(p.Node, nil, 0, 7)
	assert.NoError(t, err)
	assert.Nil(t, soft, "nothing given, no score: none")
	soft, _, _ = softAvoidOf(p.Node, nil, 3, 7)
	assert.Equal(t, []string{hub, other}, soft.Active(), "most failing first")
	assert.Equal(t, 7, soft.Limit)
	_, _, err = softAvoidOf(p.Node, []string{"CoinGate"}, 0, 0)
	assert.Contains(t, err.Error(), "softavoid: ")
}

// A run's splits share its soft avoid, and the result says what it was.
func TestARunSoftAvoids(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02soft", ppm: 100}, probeWay{from: "02o1", via: "02ok", ppm: 600})
	p.Node.Graph.Aliases["02soft"] = "Soft"
	p.softAvoid = rebalance.NewSoftAvoid([]string{"02soft"}, 0)
	p.softScores = map[string]float64{"02soft": 8}
	var routes [][]string
	inner := p.probe
	p.probe = func(route *graph.Route) error {
		routes = append(routes, route.ChannelIds())
		return inner(route)
	}
	res := stepRun(t, p.realSplits)
	assert.NotZero(t, res.CarriedAmount)
	for _, r := range routes {
		assert.NotContains(t, r, "1x2x1/"+util.GetDirection("02o1", "02soft"), "never through 02soft")
	}
	assert.Contains(t, routes[0], "2x2x1/"+util.GetDirection("02o1", "02ok"), "around 02soft")
	assert.Equal(t, &rebalance.SoftAvoidReport{Nodes: []rebalance.SoftAvoidNode{{Key: "02soft", Alias: "Soft", Score: 8}}},
		res.SoftAvoid)
}

// The quote shows the routes a run would take: around the soft-avoided
// nodes, or through them when there is no other, flagged.
func TestTheQuoteSoftAvoidsToo(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02soft", ppm: 100}, probeWay{from: "02o1", via: "02ok", ppm: 600})
	p.softAvoid = rebalance.NewSoftAvoid([]string{"02soft"}, 0)
	q := p.quote("pull", true)
	for _, c := range q.Candidates {
		if c.Scid == "1x1x1" {
			assert.Equal(t, []string{"02o1", "02ok", "02in"}, c.Path)
			assert.False(t, c.ThroughSoftAvoided)
		}
	}
	assert.Equal(t, 0, q.SoftAvoid.Fallbacks)
	p.softAvoid = rebalance.NewSoftAvoid([]string{"02soft", "02ok"}, 0)
	q = p.quote("pull", true)
	for _, c := range q.Candidates {
		if c.Scid == "1x1x1" {
			assert.Equal(t, []string{"02o1", "02soft", "02in"}, c.Path)
			assert.True(t, c.ThroughSoftAvoided)
		}
	}
	assert.Equal(t, 1, q.SoftAvoid.Fallbacks)
	assert.Equal(t, []rebalance.SoftAvoidNode{{Key: "02soft"}, {Key: "02ok"}}, q.SoftAvoid.Nodes,
		"no alias for a node the graph does not name (not its id)")
}
