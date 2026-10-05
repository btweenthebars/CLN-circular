package graph

import (
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

// Closed channels used to stay routable until their last update was 14 days
// old.
func TestSyncChannelsRemovesChannelsMissingFromTheSnapshot(t *testing.T) {
	g := newTestGraph()
	open := g.channel("1x1x1", "02a", "02b", 0, 0)
	g.channel("1x1x1", "02b", "02a", 0, 0)
	g.channel("2x2x2", "02b", "02c", 0, 0) // closed
	g.channel("3x3x3", "02a", "02c", 0, 0) // one of ours, unannounced
	open.Liquidity = 42

	ours := map[string]bool{"3x3x3/" + util.GetDirection("02a", "02c"): true}
	listed := []*glightning.Channel{
		g.Channels["1x1x1/"+util.GetDirection("02a", "02b")].Channel,
		g.Channels["1x1x1/"+util.GetDirection("02b", "02a")].Channel,
	}
	now := uint(time.Now().Unix())
	assert.Equal(t, 1, g.SyncChannels(listed, ours, now))

	assert.Len(t, g.Channels, 3)
	_, err := g.GetChannel("2x2x2/" + util.GetDirection("02b", "02c"))
	assert.Equal(t, util.ErrNoChannel, err)
	assert.Empty(t, g.Inbound["02c"]["02b"], "removed from the adjacency list")
	c, err := g.GetChannel("1x1x1/" + util.GetDirection("02a", "02b"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), c.Liquidity, "listed channels keep their liquidity belief")
	_, err = g.GetChannel("3x3x3/" + util.GetDirection("02a", "02c"))
	assert.NoError(t, err)

	// a snapshot with less than half of the graph is not trusted for removals
	assert.Equal(t, -1, g.SyncChannels(nil, ours, now))
	assert.Len(t, g.Channels, 3)

	// one of ours added after the keep set was read is updated after the snapshot
	added := g.channel("4x4x4", "02c", "02a", 0, 0)
	added.LastUpdate = now
	assert.Equal(t, 0, g.SyncChannels(listed, ours, now))
	assert.Len(t, g.Channels, 4)
}
