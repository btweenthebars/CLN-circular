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
//
// With a risk rate (riskPPM), a label also carries a cost: its fees plus, for
// each channel between our peers, -ln(the chance it can carry its amount)
// times riskPPM ppm of that amount (see Channel.SuccessChance). The search
// returns the route of least cost whose fees stay within the limit, so a
// slightly dearer route through channels likely to hold the amount wins over
// a cheaper one through channels unlikely to. Without one, the cost is the
// fees.
//
// Peers refuse an HTLC that expires more than 2016 blocks ahead, so labels
// whose CLTV delay passes MAX_ROUTE_DELAY are dropped: every route returned is
// within it. Labels are not compared on delay, which keeps the search fast,
// so a cheap label with a long delay can hide a dearer one with a short delay
// and then run past the limit. A search comparing delays as well would find
// that route, but on mainnet data it took seconds per search and found no
// route the fast search missed.

// label is one way of reaching the end of the route from `channel`.
type label struct {
	channel *Channel
	id      string // "scid/direction", empty for our own channels
	hops    int    // channels from this one to the end of the route, inclusive
	fee     uint64 // fees charged by the nodes after this channel
	cost    uint64 // fee plus the uncertainty cost of the channels from this one on
	amount  uint64 // amount carried by this channel
	delay   uint   // CLTV delay of the HTLC over this channel, in blocks
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
	riskPPM  uint64 // what each unit of -ln(chance) costs, in ppm of a channel's amount; 0: fees only
	// nodePenalty is, with riskPPM, each remembered node's extra -ln(chance)
	// units for the channels out of it (nodes.go)
	nodePenalty map[string]float64
}

// channelCost is the uncertainty cost of sending amount over channel: its
// own chance, and its node's remembered failures.
func (req *searchRequest) channelCost(channel *Channel, amount uint64) uint64 {
	cost := uncertaintyCost(channel, amount, req.riskPPM)
	if cost == math.MaxUint64 || req.nodePenalty == nil {
		return cost
	}
	return addSaturating(cost, nodeCost(req.nodePenalty[channel.Source], amount, req.riskPPM))
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

// GetLikelyCircularRoute is GetCircularRoute weighing each route's fees
// against its chance of success: it returns the route of least fees plus
// uncertainty cost (see searchRequest.riskPPM), at most maxHops channels,
// whose fees stay within maxFee. Routes through channels likely to hold the
// amount win over slightly cheaper ones that are not, and a long route pays
// for each channel it adds. If it finds none, or riskPPM is 0, it returns
// what GetCircularRoute does, so it never finds fewer routes.
func (g *Graph) GetLikelyCircularRoute(out, in *Channel, amount uint64, exclude map[string]bool, maxHops int, maxFee, riskPPM uint64) (*Route, error) {
	if riskPPM > 0 {
		route, err := g.searchRoute(searchRequest{
			src:      out.Destination,
			dst:      in.Source,
			firstHop: out,
			lastHop:  in,
			amount:   amount,
			maxHops:  maxHops,
			maxFee:   maxFee,
			exclude:  exclude,
			riskPPM:  riskPPM,
			// the nodes that failed earlier runs' payments cost more
			nodePenalty: g.nodePenalties(),
		})
		if err != util.ErrNoRoute {
			return route, err
		}
		// the least costly labels may have hidden one within maxFee
	}
	return g.GetCircularRoute(out, in, amount, exclude, maxHops, maxFee)
}

// uncertaintyCost is what the search adds for sending amount over channel:
// -ln(the chance it can) times riskPPM ppm of amount, in msat.
func uncertaintyCost(channel *Channel, amount, riskPPM uint64) uint64 {
	if riskPPM == 0 {
		return 0
	}
	chance := channel.SuccessChance(amount)
	if chance >= 1 {
		return 0
	}
	if chance <= 0 {
		return math.MaxUint64
	}
	cost := -math.Log(chance) * float64(riskPPM) * float64(amount) / 1e6
	if cost >= math.MaxUint64/2 {
		return math.MaxUint64
	}
	return uint64(math.Ceil(cost))
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

// LeastForwardFee returns the least that in's source, the in-peer of a
// rebalance over out and in, charges to forward amount over in to us, as the
// route search counts it: its fee for in, with the inbound fee of whichever
// channel into it makes that lowest. Inbound fees can be negative, so this
// can be less than its fee for in. As in the search, our own channels into
// the in-peer are left out, but for out when the out-peer is the in-peer.
func (g *Graph) LeastForwardFee(in, out *Channel, amount uint64) uint64 {
	g.channelsLock.RLock()
	g.adjacencyListLock.RLock()
	g.inboundFeesLock.RLock()
	defer g.channelsLock.RUnlock()
	defer g.adjacencyListLock.RUnlock()
	defer g.inboundFeesLock.RUnlock()

	self, peer := in.Destination, in.Source
	firstHopId := ""
	if out != nil && out.Source == self && out.Destination == peer {
		firstHopId = out.ShortChannelId + "/" + util.GetDirection(self, peer)
	}
	outboundFee := in.ComputeFee(amount)
	forwarded := addSaturating(amount, outboundFee)
	least, found := uint64(math.MaxUint64), false
	for w, edge := range g.Inbound[peer] {
		for _, scid := range edge {
			id := scid + "/" + util.GetDirection(w, peer)
			if _, ok := g.Channels[id]; !ok || (w == self && id != firstHopId) {
				continue
			}
			found = true
			if fee := nodeFee(outboundFee, g.inboundFee(id, forwarded)); fee < least {
				least = fee
			}
		}
	}
	if !found {
		return outboundFee
	}
	return least
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

	// best[id] holds, for channel id, the labels pushed so far that no other
	// beats: one is beaten by another with no more hops, no higher fee and no
	// higher cost. Comparing the fee as well as the cost keeps a label within
	// maxFee that a cheaper-cost one, later over maxFee, would otherwise hide.
	// Without a risk rate, cost is fee and this is the lowest fee per hop count.
	best := make(map[string][]mark)
	dominated := func(id string, hops int, fee, cost uint64, popped bool) bool {
		for _, m := range best[id] {
			if m.hops <= hops && m.fee <= fee && m.cost <= cost {
				// a popped label finds its own mark: only another one beats it
				if popped && m.hops == hops && m.fee == fee && m.cost == cost {
					continue
				}
				return true
			}
		}
		return false
	}
	record := func(id string, hops int, fee, cost uint64) {
		kept := best[id][:0]
		for _, m := range best[id] {
			if !(hops <= m.hops && fee <= m.fee && cost <= m.cost) {
				kept = append(kept, m)
			}
		}
		best[id] = append(kept, mark{hops, fee, cost})
	}

	// expanded[u] holds the labels already expanded at node u, as (hops, fee,
	// fee plus u's outbound fee). A label at u that has no fewer hops, no lower
	// fee and no lower fee-plus-outbound-fee than one of them cannot price any
	// channel into u lower, so it is not expanded again: the inbound fee is
	// computed on the amount plus the outbound fee, so a channel's price
	// depends on the label only through fee plus outbound fee (exactly, up to
	// rounding, for inbound rates down to -100%). Without this, each channel
	// out of a large node rescans all of the node's channels.
	expanded := make(map[string][]expansion)

	pq := &labelQueue{}

	if req.lastHop != nil {
		heap.Push(pq, &label{channel: req.lastHop, hops: 1, amount: req.amount, delay: INITIAL_DELAY})
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
				cost := req.channelCost(channel, req.amount)
				if pathHops < 1 || cost == math.MaxUint64 || dominated(id, 1, 0, cost, false) {
					continue
				}
				record(id, 1, 0, cost)
				heap.Push(pq, &label{channel: channel, id: id, hops: 1, cost: cost, amount: req.amount, delay: INITIAL_DELAY})
			}
		}
	}

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*label)

		if current.final {
			return current.path(), nil
		}
		if current.id != "" && dominated(current.id, current.hops, current.fee, current.cost, true) {
			continue
		}

		// u forwards the payment over current.channel
		u := current.channel.Source
		outboundFee := current.channel.ComputeFee(current.amount)
		// and adds its CLTV delta for that channel: past the limit, no route
		// can be built on this label
		delay := current.delay + current.channel.Delay

		if u == req.src {
			if req.firstHop == nil {
				// src is the sender: it pays itself no fee
				return current.path(), nil
			}
			// the out-peer charges for forwarding from our channel
			fee := nodeFee(outboundFee, g.inboundFee(firstHopId, addSaturating(current.amount, outboundFee)))
			totalFee := addSaturating(current.fee, fee)
			if current.hops+1 <= req.maxHops && totalFee <= req.maxFee && totalFee < math.MaxUint64 &&
				addSaturating(current.amount, fee) < math.MaxUint64 && delay <= MAX_ROUTE_DELAY {
				heap.Push(pq, &label{
					channel: req.firstHop,
					hops:    current.hops + 1,
					fee:     totalFee,
					cost:    addSaturating(current.cost, fee),
					amount:  current.amount + fee,
					delay:   delay,
					next:    current,
					final:   true,
				})
			}
			// a route never passes through src twice
			continue
		}

		if current.hops+1 > pathHops || delay > MAX_ROUTE_DELAY {
			continue
		}
		if !addExpansion(expanded, u, expansion{current.hops, current.fee, addSaturating(current.fee, outboundFee), current.cost - current.fee}) {
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

				// what u charges for this channel and the next one: as in LND, the
				// inbound fee applies to the amount plus the outbound fee
				fee := nodeFee(outboundFee, g.inboundFee(id, addSaturating(current.amount, outboundFee)))
				totalFee := addSaturating(current.fee, fee)
				if totalFee > req.maxFee || totalFee == math.MaxUint64 {
					continue
				}
				amount := addSaturating(current.amount, fee)
				if amount == math.MaxUint64 || !channel.CanForward(amount) {
					continue
				}

				uncertainty := req.channelCost(channel, amount)
				cost := addSaturating(addSaturating(current.cost, fee), uncertainty)
				if cost == math.MaxUint64 {
					continue
				}
				hops := current.hops + 1
				if dominated(id, hops, totalFee, cost, false) {
					continue
				}
				record(id, hops, totalFee, cost)
				heap.Push(pq, &label{
					channel: channel,
					id:      id,
					hops:    hops,
					fee:     totalFee,
					cost:    cost,
					amount:  amount,
					delay:   delay,
					next:    current,
				})
			}
		}
	}

	return nil, util.ErrNoRoute
}

// mark is a label's place in best: its hops, fee and cost.
type mark struct {
	hops      int
	fee, cost uint64
}

type expansion struct {
	hops         int
	fee          uint64
	withOutbound uint64
	uncertainty  uint64 // cost - fee: what a later channel's cost adds to cannot change it
}

// addExpansion records e at node u, unless an earlier expansion there makes it
// pointless, in which case it returns false.
func addExpansion(expanded map[string][]expansion, u string, e expansion) bool {
	list := expanded[u]
	for _, x := range list {
		if x.hops <= e.hops && x.fee <= e.fee && x.withOutbound <= e.withOutbound && x.uncertainty <= e.uncertainty {
			return false
		}
	}
	// drop the expansions that e makes pointless
	kept := list[:0]
	for _, x := range list {
		if !(e.hops <= x.hops && e.fee <= x.fee && e.withOutbound <= x.withOutbound && e.uncertainty <= x.uncertainty) {
			kept = append(kept, x)
		}
	}
	expanded[u] = append(kept, e)
	return true
}

// nodeFee is what a node charges: its outbound fee plus its inbound fee, which
// can be negative, never below zero.
func nodeFee(outboundFee uint64, inboundFee int64) uint64 {
	if outboundFee == math.MaxUint64 {
		return outboundFee // saturated: no discount makes it cheap
	}
	if inboundFee >= 0 {
		return addSaturating(outboundFee, uint64(inboundFee))
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

// GetRouteOverChannels prices a route that delivers amount over the channels
// with the given ids ("scid/direction", in payment order), with their current
// policies. Rebalances use it to send again over a route that worked, or to
// retry one with less. It returns util.ErrNoRoute if a channel is gone or
// disabled, cannot carry its hop's amount within its HTLC limits, or the
// route's delay passes MAX_ROUTE_DELAY. Liquidity beliefs are not checked:
// the route has just shown what it can carry.
func (g *Graph) GetRouteOverChannels(ids []string, amount uint64) (*Route, error) {
	if len(ids) == 0 {
		return nil, util.ErrNoRoute
	}
	channels := make([]*Channel, len(ids))
	g.channelsLock.RLock()
	for i, id := range ids {
		channels[i] = g.Channels[id]
	}
	g.channelsLock.RUnlock()
	for i, c := range channels {
		if c == nil || !c.IsActive || (i > 0 && channels[i-1].Destination != c.Source) {
			return nil, util.ErrNoRoute
		}
	}

	route := newRouteFromChannels(channels[0].Destination, channels[len(channels)-1].Source, amount, channels, g)
	for _, hop := range route.Hops {
		if hop.MilliSatoshi == math.MaxUint64 || hop.MilliSatoshi < hop.minHtlcMsat || hop.MilliSatoshi > hop.maxHtlcMsat {
			return nil, util.ErrNoRoute
		}
	}
	if route.Hops[0].Delay > MAX_ROUTE_DELAY {
		return nil, util.ErrNoRoute
	}
	return route, nil
}

// CanCarry reports whether the graph believes every channel of route, other
// than the first and last (our own), holds its hop's amount, and whether none
// of its nodes or channels is in exclude.
func (g *Graph) CanCarry(route *Route, exclude map[string]bool) bool {
	ids := route.ChannelIds()
	g.channelsLock.RLock()
	defer g.channelsLock.RUnlock()

	for i := 1; i < len(route.Hops)-1; i++ {
		hop := route.Hops[i]
		if exclude[ids[i]] || exclude[hop.Source] {
			return false
		}
		c, ok := g.Channels[ids[i]]
		if !ok || !c.CanForward(hop.MilliSatoshi) {
			return false
		}
	}
	return true
}

// ChannelIds returns the ids ("scid/direction") of the route's channels, in
// payment order.
func (r *Route) ChannelIds() []string {
	ids := make([]string, len(r.Hops))
	for i, hop := range r.Hops {
		ids[i] = hop.ShortChannelId + "/" + util.GetDirection(hop.Source, hop.Destination)
	}
	return ids
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
