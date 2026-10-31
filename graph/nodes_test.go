package graph

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNodeScores(t *testing.T) {
	g := NewGraph()
	g.NodeFailed("02x")
	assert.Empty(t, g.NodeScores(), "the memory is off until set")

	g.SetNodeMemory(24 * time.Hour)
	for i := 0; i < 3; i++ {
		g.NodeFailed("02x")
	}
	assert.InDelta(t, 3, g.NodeScores()["02x"].Score, 0.01)
	g.NodeForwarded("02x")
	assert.InDelta(t, 1.5, g.NodeScores()["02x"].Score, 0.01, "a payment it forwarded halves it")
	g.NodeForwarded("02x")
	g.NodeForwarded("02x")
	assert.NotContains(t, g.nodePenalties(), "02x", "below 0.5: forgotten, not in a search either")
	assert.NotContains(t, g.NodeScores(), "02x", "below 0.5: forgotten")
	g.NodeForwarded("02unknown")
	assert.NotContains(t, g.NodeScores(), "02unknown")

	// saved scores fade by half every half-life
	now := time.Now().Unix()
	assert.Equal(t, 2, g.ApplyNodeScores(map[string]NodeScore{
		"02y": {Score: 100, At: now - 24*3600},
		"02z": {Score: 1, At: now - 48*3600}, // 0.25 now
	}))
	scores := g.NodeScores()
	assert.InDelta(t, 50, scores["02y"].Score, 0.1)
	assert.NotContains(t, scores, "02z")
	assert.Equal(t, 0, g.ApplyNodeScores(map[string]NodeScore{"02y": {Score: 500, At: now - 72*3600}}),
		"older than what the graph knows")
	g.NodeFailed("02y")
	assert.InDelta(t, 51, g.NodeScores()["02y"].Score, 0.1, "a failure adds to the faded score")

	g.SetNodeMemory(0)
	g.SetNodeMemory(24 * time.Hour)
	assert.Empty(t, g.NodeScores(), "off, then on again: nothing from before")
	g.NodeFailed("02y")
	g.SetNodeMemory(0)
	assert.Empty(t, g.NodeScores(), "off: forgotten")
	g.NodeFailed("02y")
	assert.Empty(t, g.NodeScores())
	assert.Equal(t, 0, g.ApplyNodeScores(map[string]NodeScore{"02y": {Score: 5, At: now}}))
}

// A reader that changes nothing (a quote) sees the faded scores and
// forgets none.
func TestNodeScoresNowForgetsNothing(t *testing.T) {
	g := NewGraph()
	g.SetNodeMemory(24 * time.Hour)
	now := time.Now().Unix()
	g.ApplyNodeScores(map[string]NodeScore{"02old": {Score: 1, At: now - 72*3600}, "02new": {Score: 10, At: now}})
	got := g.NodeScoresNow()
	assert.NotContains(t, got, "02old", "faded below 0.5: not shown")
	assert.InDelta(t, 10, got["02new"].Score, 0.01)
	assert.Len(t, g.nodeScores, 2, "nothing forgotten")
	g.NodeScores()
	assert.Len(t, g.nodeScores, 1, "NodeScores, for saving, does forget")
}

func TestNodePenalties(t *testing.T) {
	g := NewGraph()
	assert.Nil(t, g.nodePenalties(), "nothing remembered")
	g.SetNodeMemory(time.Hour)
	g.ApplyNodeScores(map[string]NodeScore{"02a": {Score: 25, At: time.Now().Unix()},
		"02b": {Score: 1000, At: time.Now().Unix()}})
	p := g.nodePenalties()
	assert.InDelta(t, 0.5, p["02a"], 0.01)
	assert.InDelta(t, 5, p["02b"], 0.01, "capped at NODE_SCORE_MAX")
	// 5 units at riskppm 200 on 100k sats: 1000 ppm, 100 sats
	assert.Equal(t, uint64(100000), nodeCost(5, 100000000, 200))
	assert.Zero(t, nodeCost(5, 100000000, 0), "fees only: no cost")
	assert.Zero(t, nodeCost(0, 100000000, 200))
}

// A node that failed many payments in earlier runs is routed around while a
// slightly dearer way exists; one that failed a few is not.
func TestRouteAvoidsARememberedNode(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, 0) // free
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 2000, 0) // 2 sats more
	amount := uint64(100000000)
	via := func(risk uint64) []string {
		route, err := g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 500), risk)
		assert.NoError(t, err)
		return scids(route)
	}
	viaA := []string{"1x1x1", "3x1x1", "4x1x1", "2x1x1"}
	viaB := []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}
	assert.Equal(t, viaA, via(200))

	g.SetNodeMemory(24 * time.Hour)
	g.NodeFailed("02a")
	g.NodeFailed("02a") // 2 failures: 2/50 x 200 ppm of 100k sats = 0.8 sats < 2 sats
	assert.Equal(t, viaA, via(200), "a few failures barely move it")
	for i := 0; i < 100; i++ {
		g.NodeFailed("02a")
	}
	assert.Equal(t, viaB, via(200), "102 failures: about 41 sats more than via 02b")
	assert.Equal(t, viaA, via(0), "fees only: the memory is not used")

	route, err := g.GetCheapestCircularRoute(out, in, amount, nil, 8)
	assert.NoError(t, err)
	assert.Equal(t, viaA, scids(route), "the cheapest route is still the cheapest")
}
