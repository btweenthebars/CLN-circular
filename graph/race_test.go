package graph

import (
	"circular/util"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// Searches run while gossip updates, failure learning, refreshes and the
// graph file writer change the graph. Run with -race: channels used to be
// changed in place while other goroutines read them without a lock.
func TestConcurrentGraphUse(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	var ids []string
	var snapshot []*glightning.Channel
	for i := 0; i < 10; i++ {
		mid := fmt.Sprintf("02m%d", i)
		for _, c := range []*Channel{
			g.channel(fmt.Sprintf("%dx2x1", i+10), outP, mid, 0, uint64(i)),
			g.channel(fmt.Sprintf("%dx3x1", i+10), mid, inP, 1, 0),
		} {
			id := c.ShortChannelId + "/" + util.GetDirection(c.Source, c.Destination)
			ids = append(ids, id)
			copied := *c.Channel
			snapshot = append(snapshot, &copied)
		}
	}

	const rounds = 200
	var wg sync.WaitGroup
	run := func(f func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				f(i)
			}
		}()
	}

	run(func(int) {
		if route, err := g.GetCircularRoute(out, in, 1000000, nil, 8, 1000000); err == nil {
			_ = NewPrettyRoute(route, "").String()
		}
	})
	run(func(int) { _, _ = g.GetCheapestCircularRoute(out, in, 1000000, map[string]bool{ids[0]: true}, 8) })
	run(func(i int) {
		g.ApplyChannelUpdate(&ChannelUpdate{ChannelId: ids[i%len(ids)], Timestamp: uint(i + 1),
			MessageFlags: 1, FeePPM: uint32(i), HtlcMaximumMsat: 10000000000})
	})
	run(func(i int) {
		id := ids[i%len(ids)]
		g.LearnUpperBound(id, 4000000000)
		g.LearnLowerBound(id, 1000000)
		if i%7 == 0 {
			g.MarkUnusable(id)
		}
	})
	run(func(i int) {
		g.RefreshChannels(snapshot[:2])
		if i%10 == 0 {
			g.SyncChannels(snapshot, map[string]bool{"1x1x1/" + util.GetDirection(self, outP): true,
				"2x1x1/" + util.GetDirection(inP, self): true}, 0)
			g.PruneChannels()
			g.RefreshLiquidity(0)
		}
	})
	run(func(i int) {
		if i%2 == 0 {
			g.SetInboundFee(ids[i%len(ids)], -1, -1)
		} else {
			g.DeleteInboundFee(ids[(i-1)%len(ids)])
		}
	})
	run(func(int) {
		_ = g.GetStats()
		g.Lock()
		_, err := json.Marshal(g)
		g.Unlock()
		assert.NoError(t, err)
	})
	wg.Wait()
}
