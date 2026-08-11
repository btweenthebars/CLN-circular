package graph

import (
	"circular/util"
	"container/heap"
	"log"
	"strings"
)

func (g *Graph) GetRoute(src, dst string, amount uint64, exclude map[string]bool, maxHops int) (*Route, error) {
	hops, err := g.dijkstra(src, dst, amount, exclude, maxHops-2) // -2 because we already know the source and destination
	if err != nil {
		return nil, err
	}

	route := NewRoute(src, dst, amount, hops, g)
	return route, nil
}

func (g *Graph) dijkstra(src, dst string, amount uint64, exclude map[string]bool, maxHops int) ([]RouteHop, error) {
	g.channelsLock.RLock()
	g.adjacencyListLock.RLock()
	defer g.channelsLock.RUnlock()
	defer g.adjacencyListLock.RUnlock()

	if _, ok := g.Inbound[dst]; !ok {
		return nil, util.ErrNoSuchNode
	}
	if _, ok := g.Inbound[src]; !ok {
		return nil, util.ErrNoSuchNode
	}

	distance := make(map[string]int)
	hop := make(map[string]RouteHop)
	nextEdge := make(map[string]string)
	maxDistance := 1 << 31

	getDistance := func(key string) int {
		if d, ok := distance[key]; ok {
			return d
		}
		return maxDistance
	}

	pq := make(PriorityQueue, 0, 16)
	heap.Init(&pq)

	for v, edge := range g.Inbound[dst] {
		if exclude[v] {
			continue
		}
		for _, scid := range edge {
			var sb strings.Builder
			sb.WriteString(scid)
			sb.WriteString("/")
			sb.WriteString(util.GetDirection(v, dst))
			channelId := sb.String()

			if channel, ok := g.Channels[channelId]; ok {
				if !channel.CanForward(amount) {
					continue
				}
				distance[channelId] = 0
				hop[channelId] = RouteHop{
					Channel:      channel,
					MilliSatoshi: amount,
					Delay:        channel.Delay,
				}
				heap.Push(&pq, &Item{value: &PqItem{
					Node:   v,
					Edge:   channelId,
					Amount: amount,
					Delay:  channel.Delay,
					Hops:   1,
				}, priority: 0})
			}
		}
	}

	var bestSrcEdge string
	bestDistance := maxDistance

	for pq.Len() > 0 {
		pqItem := heap.Pop(&pq).(*Item)
		u := pqItem.value.Node
		edgeU := pqItem.value.Edge
		currentAmount := pqItem.value.Amount
		delay := pqItem.value.Delay
		hops := pqItem.value.Hops
		priority := pqItem.priority

		if priority > getDistance(edgeU) {
			continue
		}

		if u == src {
			if priority < bestDistance {
				bestDistance = priority
				bestSrcEdge = edgeU
			}
			break
		}

		if hops >= maxHops {
			continue
		}

		channelU := g.Channels[edgeU]
		outboundFeeU := channelU.ComputeFee(currentAmount)

		for w, edge := range g.Inbound[u] {
			if exclude[w] {
				continue
			}

			for _, scid := range edge {
				var sb strings.Builder
				sb.WriteString(scid)
				sb.WriteString("/")
				sb.WriteString(util.GetDirection(w, u))
				edgeW := sb.String()

				if _, ok := g.Channels[edgeW]; !ok {
					log.Println("channel not found:", edgeW)
					continue
				}
				channelW := g.Channels[edgeW]

				if !channelW.CanForward(currentAmount) {
					continue
				}

				inboundFeeU := g.GetInboundFee(channelW, currentAmount)
				netFeeU := int64(outboundFeeU) + inboundFeeU
				if netFeeU < 0 {
					netFeeU = 0
				}

				newDistance := priority + int(netFeeU)
				if newDistance < getDistance(edgeW) {
					distance[edgeW] = newDistance

					newAmount := currentAmount + uint64(netFeeU)
					newDelay := delay + channelW.Delay

					hop[edgeW] = RouteHop{
						Channel:      channelW,
						MilliSatoshi: newAmount,
						Delay:        newDelay,
					}
					nextEdge[edgeW] = edgeU

					heap.Push(&pq, &Item{value: &PqItem{
						Node:   w,
						Edge:   edgeW,
						Amount: newAmount,
						Delay:  newDelay,
						Hops:   hops + 1,
					}, priority: newDistance})
				}
			}
		}
	}

	if bestDistance == maxDistance {
		return nil, util.ErrNoRoute
	}

	finalHops := make([]RouteHop, 0, 10)
	currEdge := bestSrcEdge
	for currEdge != "" {
		finalHops = append(finalHops, hop[currEdge])
		currEdge = nextEdge[currEdge]
	}
	return finalHops, nil
}
