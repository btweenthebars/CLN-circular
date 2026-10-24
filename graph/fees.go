package graph

import "sort"

const (
	// DearFactor: a channel whose fee rate is at least this many times its
	// node's median is marked Dear.
	DearFactor = 2
	// DearShare is the share of its capacity a Dear channel that no payment
	// has told us about is believed to hold, instead of half. Among one
	// node's 140 channels with known balances, those priced at twice or more
	// their peer's median rate held 27-36% on the peer's side on average
	// (median about 20%), the others 39-47%.
	DearShare = 0.3
	// dearMinChannels: below this many channels, a node's median says too
	// little
	dearMinChannels = 3
	// dearMinGap: a Dear channel's rate is also at least this many ppm above
	// the median, so that 1 ppm is not dear next to a median of 0
	dearMinGap = 50
)

// MarkDearChannels marks the channels whose fee rate is at least DearFactor
// times, and dearMinGap ppm above, their node's median rate over its active
// channels, for nodes with at least dearMinChannels of them, and returns how many it marked. Automatic
// fee managers raise a channel's fee as its outbound side drains, so a
// channel priced well above its node's others likely holds little. It keeps
// the medians for ApplyChannelUpdate.
func (g *Graph) MarkDearChannels() int {
	g.channelsLock.Lock()
	defer g.channelsLock.Unlock()

	rates := make(map[string][]uint64)
	for _, c := range g.Channels {
		if c.IsActive {
			rates[c.Source] = append(rates[c.Source], c.FeePerMillionth)
		}
	}
	medians := make(map[string]uint64, len(rates))
	for node, r := range rates {
		if len(r) < dearMinChannels {
			continue
		}
		sort.Slice(r, func(i, j int) bool { return r[i] < r[j] })
		medians[node] = r[len(r)/2]
	}
	g.feeMedians = medians

	dear := 0
	for _, c := range g.Channels {
		c.Dear = g.isDear(c)
		if c.Dear {
			dear++
		}
	}
	return dear
}

// isDear tells whether c's rate is at least DearFactor times its node's
// median, from the medians MarkDearChannels kept. Call with channelsLock held.
func (g *Graph) isDear(c *Channel) bool {
	median, ok := g.feeMedians[c.Source]
	return ok && c.FeePerMillionth+1 >= DearFactor*(median+1) && c.FeePerMillionth >= median+dearMinGap
}
