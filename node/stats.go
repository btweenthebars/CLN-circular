package node

import (
	"circular/graph"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"strconv"
	"time"
)

type Stats struct {
	GraphStats     *graph.Stats                `json:"graph_stats"`
	Successes      []glightning.SendPaySuccess `json:"successes"`
	Failures       []glightning.SendPayFailure `json:"failures"`
	Routes         []graph.PrettyRoute         `json:"routes"`
	ActivePayments []*ActivePayment            `json:"active_payments"`
}

func (s *Stats) Name() string {
	return "circular-stats"
}

func (s *Stats) New() interface{} {
	return &Stats{}
}

func (s *Stats) Call() (jrpc2.Result, error) {
	return GetNode().GetStats(), nil
}

func (n *Node) GetStats() *Stats {
	defer util.TimeTrack(time.Now(), "node.GetStats", n.Logf)

	successes, err := n.DB.ListSuccesses()
	if err != nil {
		n.Logln(glightning.Unusual, err)
	}

	failures, err := n.DB.ListFailures()
	if err != nil {
		n.Logln(glightning.Unusual, err)
	}

	routes, err := n.DB.ListRoutes()
	if err != nil {
		n.Logln(glightning.Unusual, err)
	}

	n.activePaymentsLock.RLock()
	activePayments := make([]*ActivePayment, 0, len(n.ActivePayments))
	for _, p := range n.ActivePayments {
		activePayments = append(activePayments, p)
	}
	n.activePaymentsLock.RUnlock()

	return &Stats{
		GraphStats:     n.Graph.GetStats(),
		Successes:      successes,
		Failures:       failures,
		Routes:         routes,
		ActivePayments: activePayments,
	}
}

func (s *Stats) String() string {
	var result string
	result += "Node stats:" + "\n"
	result += s.GraphStats.String() + "\n"
	result += "successes: " + strconv.Itoa(len(s.Successes)) + "\n"
	result += "failures: " + strconv.Itoa(len(s.Failures)) + "\n"
	result += "routes: " + strconv.Itoa(len(s.Routes)) + "\n"
	result += "active payments: " + strconv.Itoa(len(s.ActivePayments)) + "\n"

	if len(s.ActivePayments) > 0 {
		result += "\nActive Payments Details:\n"
		for _, p := range s.ActivePayments {
			elapsed := time.Since(p.CreatedAt).Round(time.Second)
			result += fmt.Sprintf("- Hash: %s\n  Amount: %d sats\n  Elapsed: %s\n", p.PaymentHash, p.AmountMsat/1000, elapsed)
			if p.Route != nil && len(p.Route.Hops) > 0 {
				result += "  Route: "
				for idx, hop := range p.Route.Hops {
					if idx > 0 {
						result += " -> "
					}
					result += hop.Alias
				}
				result += "\n"
			}
		}
		result += "\n"
	}

	var totalMoved uint64 = 0
	for _, success := range s.Successes {
		totalMoved += success.MilliSatoshi
	}
	result += "Total amount of BTC rebalanced: " + strconv.FormatUint(totalMoved/1000, 10) + "sats"

	return result
}
