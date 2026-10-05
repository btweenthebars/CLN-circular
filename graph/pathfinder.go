package graph

import (
	"circular/util"
	"container/heap"
	"math"
	"math/bits"
)

// The route search runs backwards, from the node that receives the payment to
// the node that sends it, because every node's fee depends on the amount it
// forwards, which is only known once everything after it has been priced.
//
// Each search state (a label) is a channel plus the number of channels from it
// to the end of the route. Keeping one label per (channel, hop count) instead
// of one per channel means that a cheap but long path to a channel cannot hide
// a slightly more expensive but shorter one that would still fit within the
// hop limit. A label is dropped only when another label on the same channel is
// at least as cheap and at most as long.

// label is one way of reaching the end of the route from `channel`.
type label struct {
	channel *Channel
	id      string // "scid/direction", empty for our own channels
	hops    int    // channels from this one to the end of the route, inclusive
	fee     uint64 // fees charged by the nodes after this channel
	amount  uint64 // amount carried by this channel
	next    *label // the following channel, towards the receiver
	final   bool   // the route is complete
}

type searchRequest struct {
	src, dst string   // first and last node of the path that is searched
	firstHop *Channel // our channel into src; nil when src is the sender
	lastHop  *Channel // channel from dst to us; nil when dst is the receiver
	amount   uint64   // amount delivered to the receiver
	maxHops  int      // channels in the whole route, firstHop and lastHop included
	maxFee   uint64   // total fees allowed
	exclude  map[string]bool
}

// GetRoute finds the cheapest route from src, which pays, to dst, which
// receives amount, using at most maxHops channels.
func (g *Graph) GetRoute(src, dst string, amount uint64, exclude map[string]bool, maxHops int) (*Route, error) {
	return g.searchRoute(searchRequest{
		src:     src,
		dst:     dst,
		amount:  amount,
		maxHops: maxHops,
		maxFee:  math.MaxUint64,
		exclude: exclude,
	})
}

// GetCircularRoute finds the route of a rebalance: our channel `out` to the
// out-peer, a path through the network, then the in-peer's channel `in` back
// to us. Fees are counted for every node that charges one, the out-peer and
// the in-peer included, together with their inbound fees. Shorter routes fail
// less often, so it returns the route with the fewest channels (at most
// maxHops, ours included) whose fees stay within maxFee, and the cheapest of
// those.
func (g *Graph) GetCircularRoute(out, in *Channel, amount uint64, exclude map[string]bool, maxHops int, maxFee uint64) (*Route, error) {
	// The first hop limit with a route within maxFee gives the answer: the
	// cheapest route of at most that many channels has exactly that many.
	for hops := 2; hops <= maxHops; hops++ {
		route, err := g.searchRoute(searchRequest{
			src:      out.Destination,
			dst:      in.Source,
			firstHop: out,
			lastHop:  in,
			amount:   amount,
			maxHops:  hops,
			maxFee:   maxFee,
			exclude:  exclude,
		})
		if err != util.ErrNoRoute {
			return route, err
		}
	}
	return nil, util.ErrNoRoute
}

// GetCheapestCircularRoute is GetCircularRoute without a fee limit, returning
// the cheapest route of at most maxHops channels.
func (g *Graph) GetCheapestCircularRoute(out, in *Channel, amount uint64, exclude map[string]bool, maxHops int) (*Route, error) {
	return g.searchRoute(searchRequest{
		src:      out.Destination,
		dst:      in.Source,
		firstHop: out,
		lastHop:  in,
		amount:   amount,
		maxHops:  maxHops,
		maxFee:   math.MaxUint64,
		exclude:  exclude,
	})
}

// MaxFeeForPPM returns the largest fee, in msat, whose rate on amount does not
// exceed ppm once rounded down the way Route.FeePPM rounds it.
func MaxFeeForPPM(amount, ppm uint64) uint64 {
	if amount == 0 || ppm == math.MaxUint64 {
		return math.MaxUint64
	}
	hi, lo := bits.Mul64(ppm+1, amount)
	if hi != 0 {
		return math.MaxUint64
	}
	return (lo - 1) / 1000000
}

func (g *Graph) searchRoute(req searchRequest) (*Route, error) {
	channels, err := g.search(req)
	if err != nil {
		return nil, err
	}
	return newRouteFromChannels(req.src, req.dst, req.amount, channels, g), nil
}

func (g *Graph) search(req searchRequest) ([]*Channel, error) {
	g.channelsLock.RLock()
	g.adjacencyListLock.RLock()
	g.inboundFeesLock.RLock()
	defer g.channelsLock.RUnlock()
	defer g.adjacencyListLock.RUnlock()
	defer g.inboundFeesLock.RUnlock()

	if _, ok := g.Inbound[req.dst]; !ok && req.dst != req.src {
		return nil, util.ErrNoSuchNode
	}
	if _, ok := g.Inbound[req.src]; !ok && req.dst != req.src {
		return nil, util.ErrNoSuchNode
	}

	// our own node, when the route starts and ends with our channels
	self, firstHopId := "", ""
	if req.firstHop != nil {
		self = req.firstHop.Source
		firstHopId = req.firstHop.ShortChannelId + "/" + util.GetDirection(self, req.src)
	}

	// Channels between src and dst may use every hop not taken by our channels.
	pathHops := req.maxHops
	if req.firstHop != nil {
		pathHops--
	}

	// best[id][h] is the lowest fee seen for channel id with h hops to the end.
	best := make(map[string][]uint64)
	dominated := func(id string, hops int, fee uint64, popped bool) bool {
		costs, ok := best[id]
		if !ok {
			return false
		}
		for h := 1; h < hops; h++ {
			if costs[h] <= fee {
				return true
			}
		}
		if popped {
			return costs[hops] < fee
		}
		return costs[hops] <= fee
	}
	record := func(id string, hops int, fee uint64) {
		costs, ok := best[id]
		if !ok {
			costs = make([]uint64, req.maxHops+1)
			for i := range costs {
				costs[i] = math.MaxUint64
			}
			best[id] = costs
		}
		costs[hops] = fee
	}

	// expanded[u] holds the labels already expanded at node u, as (hops, fee,
	// fee plus u's outbound fee). A label at u that has no fewer hops, no lower
	// fee and no lower fee-plus-outbound-fee than one of them cannot price any
	// channel into u lower, so it is not expanded again. (A negative inbound fee
	// rate can make that off by rate x fee difference, a few msat at most.)
	// Without this, each channel out of a large node rescans all of the node's
	// channels.
	expanded := make(map[string][]expansion)

	pq := &labelQueue{}

	if req.lastHop != nil {
		heap.Push(pq, &label{channel: req.lastHop, hops: 1, amount: req.amount})
	} else {
		// dst receives the payment and charges no fee
		for v, edge := range g.Inbound[req.dst] {
			if req.exclude[v] {
				continue
			}
			for _, scid := range edge {
				id := scid + "/" + util.GetDirection(v, req.dst)
				channel, ok := g.Channels[id]
				if !ok || req.exclude[id] || !channel.CanForward(req.amount) {
					continue
				}
				if pathHops < 1 || dominated(id, 1, 0, false) {
					continue
				}
				record(id, 1, 0)
				heap.Push(pq, &label{channel: channel, id: id, hops: 1, amount: req.amount})
			}
		}
	}

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*label)

		if current.final {
			return current.path(), nil
		}
		if current.id != "" && dominated(current.id, current.hops, current.fee, true) {
			continue
		}

		// u forwards the payment over current.channel
		u := current.channel.Source
		outboundFee := current.channel.ComputeFee(current.amount)

		if u == req.src {
			if req.firstHop == nil {
				// src is the sender: it pays itself no fee
				return current.path(), nil
			}
			// the out-peer charges for forwarding from our channel
			fee := nodeFee(outboundFee, g.inboundFee(firstHopId, current.amount))
			if current.hops+1 <= req.maxHops && current.fee+fee <= req.maxFee && current.fee+fee >= current.fee {
				heap.Push(pq, &label{
					channel: req.firstHop,
					hops:    current.hops + 1,
					fee:     current.fee + fee,
					amount:  current.amount + fee,
					next:    current,
					final:   true,
				})
			}
			// a route never passes through src twice
			continue
		}

		if current.hops+1 > pathHops {
			continue
		}
		if !addExpansion(expanded, u, expansion{current.hops, current.fee, current.fee + outboundFee}) {
			continue
		}

		for w, edge := range g.Inbound[u] {
			// never route through our own node, and never come back to dst
			if req.exclude[w] || w == self || w == req.dst {
				continue
			}
			for _, scid := range edge {
				id := scid + "/" + util.GetDirection(w, u)
				channel, ok := g.Channels[id]
				if !ok || req.exclude[id] {
					continue
				}

				// what u charges for this channel and the next one, inbound fee included
				fee := nodeFee(outboundFee, g.inboundFee(id, current.amount))
				totalFee := current.fee + fee
				if totalFee < current.fee || totalFee > req.maxFee {
					continue
				}
				amount := current.amount + fee
				if !channel.CanForward(amount) {
					continue
				}

				hops := current.hops + 1
				if dominated(id, hops, totalFee, false) {
					continue
				}
				record(id, hops, totalFee)
				heap.Push(pq, &label{
					channel: channel,
					id:      id,
					hops:    hops,
					fee:     totalFee,
					amount:  amount,
					next:    current,
				})
			}
		}
	}

	return nil, util.ErrNoRoute
}

type expansion struct {
	hops         int
	fee          uint64
	withOutbound uint64
}

// addExpansion records e at node u, unless an earlier expansion there makes it
// pointless, in which case it returns false.
func addExpansion(expanded map[string][]expansion, u string, e expansion) bool {
	list := expanded[u]
	for _, x := range list {
		if x.hops <= e.hops && x.fee <= e.fee && x.withOutbound <= e.withOutbound {
			return false
		}
	}
	// drop the expansions that e makes pointless
	kept := list[:0]
	for _, x := range list {
		if !(e.hops <= x.hops && e.fee <= x.fee && e.withOutbound <= x.withOutbound) {
			kept = append(kept, x)
		}
	}
	expanded[u] = append(kept, e)
	return true
}

// nodeFee is what a node charges: its outbound fee plus its inbound fee, which
// can be negative, never below zero.
func nodeFee(outboundFee uint64, inboundFee int64) uint64 {
	if inboundFee >= 0 {
		return outboundFee + uint64(inboundFee)
	}
	discount := uint64(-inboundFee)
	if discount >= outboundFee {
		return 0
	}
	return outboundFee - discount
}

// path lists the channels from l to the end of the route, in payment order.
func (l *label) path() []*Channel {
	channels := make([]*Channel, 0, l.hops)
	for current := l; current != nil; current = current.next {
		channels = append(channels, current.channel)
	}
	return channels
}

// newRouteFromChannels prices a route that delivers amount over channels.
func newRouteFromChannels(src, dst string, amount uint64, channels []*Channel, g *Graph) *Route {
	hops := make([]RouteHop, len(channels))
	for i, channel := range channels {
		hops[i] = RouteHop{Channel: channel}
	}
	route := NewRoute(src, dst, amount, hops, g)
	last := len(route.Hops) - 1
	route.Hops[last].MilliSatoshi = amount
	route.Hops[last].Delay = INITIAL_DELAY
	route.recomputeFeeAndDelay()
	return route
}
