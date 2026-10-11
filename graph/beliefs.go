package graph

// Belief is what the graph has learned about one direction of a channel: the
// liquidity it is believed to hold, and when that was learned (Unix time).
type Belief struct {
	Liquidity uint64 `json:"liquidity"`
	Timestamp int64  `json:"timestamp"`
}

// learned reports whether c's belief came from a payment, not from the
// default of half the capacity.
func (c *Channel) learned() bool {
	return c.Timestamp != 0 && c.Liquidity != c.AmountMsat.MSat()/2
}

// Beliefs returns what the graph has learned, by channel id. It is small
// next to the graph: only channels that payments told something about.
func (g *Graph) Beliefs() map[string]Belief {
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	beliefs := make(map[string]Belief)
	for id, c := range g.Channels {
		if c.learned() {
			beliefs[id] = Belief{Liquidity: c.Liquidity, Timestamp: c.Timestamp}
		}
	}
	return beliefs
}

// ApplyBeliefs gives the channels in the graph the beliefs saved by an
// earlier run, unless they are older than since (Unix time) or no longer fit
// the channel's capacity. It returns how many it applied.
func (g *Graph) ApplyBeliefs(beliefs map[string]Belief, since int64) int {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	applied := 0
	for id, b := range beliefs {
		c, ok := g.Channels[id]
		if !ok || b.Timestamp < since || b.Liquidity > c.AmountMsat.MSat() {
			continue
		}
		c.Liquidity = b.Liquidity
		c.Timestamp = b.Timestamp
		applied++
	}
	return applied
}
