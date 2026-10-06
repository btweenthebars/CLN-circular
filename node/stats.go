package node

import (
	"circular/graph"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"strconv"
	"strings"
	"time"
)

type Stats struct {
	GraphStats *graph.Stats `json:"graph_stats"`
	// RebalancedMsat is the total the successes below delivered
	RebalancedMsat uint64                      `json:"rebalanced_msat"`
	Successes      []glightning.SendPaySuccess `json:"successes"`
	Failures       []glightning.SendPayFailure `json:"failures"`
	Routes         []graph.PrettyRoute         `json:"routes"`
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

	var rebalanced uint64
	for _, success := range successes {
		rebalanced += successAmount(success)
	}

	return &Stats{
		GraphStats:     n.Graph.GetStats(),
		RebalancedMsat: rebalanced,
		Successes:      successes,
		Failures:       failures,
		Routes:         routes,
	}
}

// successAmount returns what a successful rebalance delivered, in msat. CLN
// 23.05 removed the msatoshi field the total used to add up, so it was always
// 0. The amount is in amount_msat, which glightning keeps as text: "123msat"
// from older CLN, or the number lightningd sends now, printed like "1.23e+08".
func successAmount(success glightning.SendPaySuccess) uint64 {
	if success.MilliSatoshi > 0 {
		return success.MilliSatoshi
	}
	text := strings.TrimSuffix(strings.TrimSpace(success.AmountMilliSatoshi), "msat")
	if amount, err := strconv.ParseUint(text, 10, 64); err == nil {
		return amount
	}
	// exact for whole msat amounts below 2^53, about 90,000 BTC
	if amount, err := strconv.ParseFloat(text, 64); err == nil && amount >= 0 && amount < 1<<63 {
		return uint64(amount)
	}
	return 0
}

func (s *Stats) String() string {
	var result string
	result += "Node stats:" + "\n"
	result += s.GraphStats.String() + "\n"
	result += "successes: " + strconv.Itoa(len(s.Successes)) + "\n"
	result += "failures: " + strconv.Itoa(len(s.Failures)) + "\n"
	result += "routes: " + strconv.Itoa(len(s.Routes)) + "\n"

	result += "Total amount of BTC rebalanced: " + strconv.FormatUint(s.RebalancedMsat/1000, 10) + "sats"

	return result
}
