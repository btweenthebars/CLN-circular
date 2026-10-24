package graph

import (
	"math"
	"time"
)

// Belief is what the graph has learned about one direction of a channel: the
// liquidity it is believed to hold, and when that was learned (Unix time).
type Belief struct {
	Liquidity uint64 `json:"liquidity"`
	Timestamp int64  `json:"timestamp"`
	Upper     bool   `json:"upper,omitempty"` // Liquidity is a bound from a failure
}

// learned reports whether c's belief came from a payment, not from the
// default of half the capacity.
func (c *Channel) learned() bool {
	return c.Timestamp != 0 && (c.Upper || c.Liquidity != c.AmountMsat.MSat()/2)
}

// Beliefs returns what the graph has learned, by channel id. It is small
// next to the graph: only channels that payments told something about.
func (g *Graph) Beliefs() map[string]Belief {
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	beliefs := make(map[string]Belief)
	for id, c := range g.Channels {
		if c.learned() {
			beliefs[id] = Belief{Liquidity: c.Liquidity, Timestamp: c.Timestamp, Upper: c.Upper}
		}
	}
	return beliefs
}

// ApplyBeliefs gives the channels in the graph the beliefs saved by an
// earlier run, unless they are older than since (Unix time) or than what the
// graph knows (our channels, read from listpeerchannels), or no longer fit
// the channel's capacity. It returns how many it applied.
func (g *Graph) ApplyBeliefs(beliefs map[string]Belief, since int64) int {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	applied := 0
	for id, b := range beliefs {
		c, ok := g.Channels[id]
		if !ok || b.Timestamp < since || b.Timestamp <= c.Timestamp || b.Liquidity > c.AmountMsat.MSat() {
			continue
		}
		c.Liquidity = b.Liquidity
		c.Timestamp = b.Timestamp
		c.Upper = b.Upper
		applied++
	}
	return applied
}

// DecayLiquidity moves each learned belief toward the default, half the
// capacity, halving its distance from it every halfLife. A bound from a
// failure (Channel.Upper) loosens toward the capacity instead, since "at
// most the capacity" says nothing, and becomes the default once it gets
// there. Beliefs of 0 are left alone. What a payment
// showed fades as balances change, instead of holding in full until
// circular-liquidity-refresh and then vanishing at once. It returns how many
// beliefs it moved. A halfLife of 0 turns it off.
func (g *Graph) DecayLiquidity(halfLife time.Duration) int {
	if halfLife <= 0 {
		return 0
	}
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	now := time.Now().Unix()
	last := g.lastDecay
	g.lastDecay = now
	moved := 0
	for _, c := range g.Channels {
		if c.Timestamp == 0 {
			continue
		}
		// decay only for the time since the belief was learned, or since the
		// last decay, whichever is later
		from := c.Timestamp
		if last > from {
			from = last
		}
		elapsed := now - from
		def := c.AmountMsat.MSat() / 2
		// a channel found unusable (disabled, closed, unknown to its node)
		// stays so until the hard reset or a newer policy
		if elapsed <= 0 || (c.Liquidity == def && !c.Upper) || c.Liquidity == 0 {
			continue
		}
		keep := math.Pow(0.5, float64(elapsed)/halfLife.Seconds())
		if c.Upper {
			capacity := c.AmountMsat.MSat()
			if c.Liquidity < capacity {
				c.Liquidity = capacity - uint64(float64(capacity-c.Liquidity)*keep)
			}
			if c.Liquidity >= capacity || capacity-c.Liquidity <= capacity/1024 {
				// as good as no bound: back to the default
				c.Liquidity, c.Upper = def, false
			}
		} else if c.Liquidity > def {
			c.Liquidity = def + uint64(float64(c.Liquidity-def)*keep)
		} else {
			c.Liquidity = def - uint64(float64(def-c.Liquidity)*keep)
		}
		moved++
	}
	return moved
}

// Avoid keeps key, a node id or a channel id ("scid/direction"), out of
// route searches until until (Unix time), across rebalances: for failures
// that liquidity beliefs cannot express, such as a node that failed the
// payment itself.
func (g *Graph) Avoid(key string, until int64) {
	g.avoidLock.Lock()
	defer g.avoidLock.Unlock()

	if until > g.avoid[key] {
		g.avoid[key] = until
	}
}

// Unavoid takes key off the avoid list, for example when a newer
// channel_update arrives for that channel.
func (g *Graph) Unavoid(key string) {
	g.avoidLock.Lock()
	defer g.avoidLock.Unlock()

	delete(g.avoid, key)
}

// Avoided returns the keys still avoided at now (Unix time), as a new
// exclusion set to start a search with, and forgets the expired ones.
func (g *Graph) Avoided(now int64) map[string]bool {
	g.avoidLock.Lock()
	defer g.avoidLock.Unlock()

	exclude := make(map[string]bool, len(g.avoid))
	for key, until := range g.avoid {
		if until <= now {
			delete(g.avoid, key)
			continue
		}
		exclude[key] = true
	}
	return exclude
}

// AvoidList returns the avoid list, with when each entry expires, to save.
func (g *Graph) AvoidList() map[string]int64 {
	g.avoidLock.Lock()
	defer g.avoidLock.Unlock()

	list := make(map[string]int64, len(g.avoid))
	for key, until := range g.avoid {
		list[key] = until
	}
	return list
}
