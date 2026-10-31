package node

import (
	"circular/graph"
	"math"
	"sort"

	"github.com/elementsproject/glightning/jrpc2"
)

// NodesCmd is circular-nodes: the nodes remembered for failing rebalances'
// payments, most first.
type NodesCmd struct {
	Limit int `json:"limit,omitempty"` // how many (default 20)
}

// RememberedNode is one of them: its failure score (failed payments,
// faded) and what it adds, at the node's risk rate, to a route through it,
// in ppm of the amount.
type RememberedNode struct {
	NodeId     string  `json:"node_id"`
	Alias      string  `json:"alias"`
	Score      float64 `json:"score"`
	PenaltyPPM uint64  `json:"penalty_ppm"`
}

type nodesResult struct {
	HalfLifeHours int              `json:"half_life_hours"` // 0: the memory is off
	RiskPPM       uint64           `json:"risk_ppm"`        // 0: the search ignores it
	Count         int              `json:"count"`
	Nodes         []RememberedNode `json:"nodes"`
}

func (c *NodesCmd) Name() string {
	return "circular-nodes"
}

func (c *NodesCmd) New() interface{} {
	return &NodesCmd{}
}

func (c *NodesCmd) Call() (jrpc2.Result, error) {
	return GetNode().rememberedNodes(c.Limit), nil
}

func (n *Node) rememberedNodes(limit int) *nodesResult {
	if limit <= 0 {
		limit = 20
	}
	scores := n.Graph.NodeScores()
	out := &nodesResult{HalfLifeHours: int(n.nodeMemory.Hours()), RiskPPM: n.RiskPPM, Count: len(scores),
		Nodes: make([]RememberedNode, 0, len(scores))}
	for id, s := range scores {
		out.Nodes = append(out.Nodes, RememberedNode{NodeId: id, Alias: n.Graph.GetAlias(id),
			Score: math.Round(s.Score*10) / 10, PenaltyPPM: graph.NodePenaltyPPM(s.Score, n.RiskPPM)})
	}
	sort.Slice(out.Nodes, func(i, j int) bool {
		if out.Nodes[i].Score != out.Nodes[j].Score {
			return out.Nodes[i].Score > out.Nodes[j].Score
		}
		return out.Nodes[i].NodeId < out.Nodes[j].NodeId
	})
	if len(out.Nodes) > limit {
		out.Nodes = out.Nodes[:limit]
	}
	return out
}
