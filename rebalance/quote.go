package rebalance

import (
	"circular/graph"
	"circular/node"
	"time"
)

// startExclude is what a rebalance leaves out of its searches at first: what
// recent rebalances learned to avoid, and our own node.
func (r *Rebalance) startExclude() map[string]bool {
	exclude := r.Node.Graph.Avoided(time.Now().Unix())
	for _, id := range node.ExcludeKeys(r.Node.AvoidedKeys()) {
		exclude[id] = true // the saved avoid list (circular-avoid)
	}
	for _, id := range r.Exclude {
		exclude[id] = true
	}
	exclude[r.Node.Id] = true
	// Our own peers may be avoided as middle hops of other routes, but here
	// they are the ends of ours: excluding them would leave no route.
	delete(exclude, r.OutChannel.Destination)
	delete(exclude, r.InChannel.Source)
	// so are our two channels (a candidate on the avoid list is never fired)
	delete(exclude, channelId(r.OutChannel))
	delete(exclude, channelId(r.InChannel))
	return exclude
}

// Quote returns the route the first payment of r would be sent over, without
// paying anything: after the same checks of our two channels, with the same
// search and what recent rebalances learned to avoid. r.Amount is in msat.
// When a route is found but our out channel cannot send it, Quote returns
// both.
func (r *Rebalance) Quote() (*graph.Route, error) {
	if err := r.validateLiquidityParameters(r.OutChannel, r.InChannel); err != nil {
		return nil, err
	}
	route, err := r.getRoute(r.startExclude())
	if err != nil {
		return nil, err
	}
	return route, r.checkFirstHop(route)
}
