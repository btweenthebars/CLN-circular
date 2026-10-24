package graph

// Estimate is a rough chance that route can carry its amount, and how many of
// its hops it had to guess. Our own two channels, at its ends, are known. For
// each hop between our peers it takes the hop's Channel.SuccessChance, the
// model the route search weighs routes with; a hop whose liquidity no payment
// has told us about is guessed: any balance up to its capacity is taken as
// equally likely.
func (g *Graph) Estimate(route *Route) (float64, int) {
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	chance, guessed := 1.0, 0
	for i := 1; i < len(route.Hops)-1; i++ {
		hop := route.Hops[i]
		if hop.Channel == nil {
			continue
		}
		if !hop.Channel.learned() {
			guessed++
		}
		chance *= hop.Channel.SuccessChance(hop.MilliSatoshi)
	}
	return chance, guessed
}
