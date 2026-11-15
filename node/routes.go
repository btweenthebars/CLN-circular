package node

import (
	"sync"
	"time"
)

// ROUTE_MEMORY is how long a route that a probe-only run found to carry a
// split is kept for the next run between the same two of our channels.
const ROUTE_MEMORY = time.Hour

type provenRoute struct {
	channels []string
	at       time.Time
}

// provenRoutes holds, per pair of our channels ("out scid>in scid"), the
// channel ids of the last route a probe-only run found to carry a split. In
// memory only: a plugin restart forgets them.
type provenRoutes struct {
	sync.Mutex
	routes map[string]provenRoute
}

// RememberRoute keeps channels, the route from our channel out to our channel
// in that just carried a probe, for the next run between the two.
func (n *Node) RememberRoute(out, in string, channels []string) {
	n.proven.Lock()
	defer n.proven.Unlock()
	if n.proven.routes == nil {
		n.proven.routes = map[string]provenRoute{}
	}
	n.proven.routes[out+">"+in] = provenRoute{channels: append([]string(nil), channels...), at: time.Now()}
}

// ProvenRoute is the route RememberRoute kept for out and in, if it is not
// older than ROUTE_MEMORY; take forgets it, so that one split tries it.
func (n *Node) ProvenRoute(out, in string, take bool) []string {
	n.proven.Lock()
	defer n.proven.Unlock()
	key := out + ">" + in
	route, ok := n.proven.routes[key]
	if !ok {
		return nil
	}
	if time.Since(route.at) > ROUTE_MEMORY {
		delete(n.proven.routes, key)
		return nil
	}
	if take {
		delete(n.proven.routes, key)
	}
	return route.channels
}
