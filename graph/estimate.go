package graph

// Estimate is a rough chance that route can carry its amount, and how many of
// its hops it had to guess. Our own two channels, at its ends, are known. For
// each hop between our peers whose liquidity no payment has told us about,
// it takes the chance that the hop holds what it must forward if any
// liquidity up to the channel's capacity were as likely as any other. A hop
// whose liquidity was learned counts as able to carry it: the search only
// takes it if it can.
func (g *Graph) Estimate(route *Route) (float64, int) {
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	chance, guessed := 1.0, 0
	for i := 1; i < len(route.Hops)-1; i++ {
		hop := route.Hops[i]
		if hop.Channel == nil || hop.Channel.learned() {
			continue
		}
		guessed++
		capacity := hop.Channel.AmountMsat.MSat()
		if capacity <= hop.MilliSatoshi {
			chance = 0
			continue
		}
		chance *= float64(capacity-hop.MilliSatoshi) / float64(capacity)
	}
	return chance, guessed
}
