package graph

import (
	"circular/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOppositeChannelId(t *testing.T) {
	assert.Equal(t, "1x2x3/1", OppositeChannelId("1x2x3/0"))
	assert.Equal(t, "1x2x3/0", OppositeChannelId("1x2x3/1"))
}

func TestLearnUpperAndLowerBounds(t *testing.T) {
	g := newTestGraph()
	g.channel("1x1x1", "02a", "02b", 0, 0) // capacity 10,000,000,000 msat
	back := g.channel("1x1x1", "02b", "02a", 0, 0)
	forward := g.Channels["1x1x1/"+util.GetDirection("02a", "02b")]
	id := "1x1x1/" + util.GetDirection("02a", "02b")

	// it could not forward 2,000,000,000 msat
	g.LearnUpperBound(id, 2000000000)
	assert.Equal(t, uint64(1999999999), forward.Liquidity)
	assert.Equal(t, uint64(8000000000), back.Liquidity)

	// a larger failure tells nothing new
	g.LearnUpperBound(id, 3000000000)
	assert.Equal(t, uint64(1999999999), forward.Liquidity)

	// it forwarded 500,000,000 msat: the belief stays below the failure
	g.LearnLowerBound(id, 500000000)
	assert.Equal(t, uint64(1999999999), forward.Liquidity)
	assert.Equal(t, uint64(8000000000), back.Liquidity)

	// a lower bound above the belief raises it, and caps the other direction
	forward.Liquidity = 0
	g.LearnLowerBound(id, 500000000)
	assert.Equal(t, uint64(500000000), forward.Liquidity)
	assert.Equal(t, uint64(8000000000), back.Liquidity)
	g.LearnLowerBound(id, 9000000000)
	assert.Equal(t, uint64(1000000000), back.Liquidity)
}

func TestMarkUnusableLeavesTheOtherDirection(t *testing.T) {
	g := newTestGraph()
	forward := g.channel("1x1x1", "02a", "02b", 0, 0)
	back := g.channel("1x1x1", "02b", "02a", 0, 0)

	g.MarkUnusable("1x1x1/" + util.GetDirection("02a", "02b"))
	assert.Equal(t, uint64(0), forward.Liquidity)
	assert.Equal(t, uint64(5000000000), back.Liquidity)
	assert.False(t, forward.CanForward(1))
}

// refreshPeers adds our channels again every 30 seconds; the adjacency list
// used to grow by one entry per channel each time.
func TestAddChannelListsAChannelOnce(t *testing.T) {
	g := newTestGraph()
	c := g.channel("1x1x1", "02a", "02b", 0, 0)
	for i := 0; i < 100; i++ {
		g.AddChannel(c)
	}
	g.channel("2x1x1", "02a", "02b", 0, 0)
	assert.Equal(t, Edge{"1x1x1", "2x1x1"}, g.Inbound["02b"]["02a"])

	g.DeleteChannel(c)
	assert.Equal(t, Edge{"2x1x1"}, g.Inbound["02b"]["02a"])
}
