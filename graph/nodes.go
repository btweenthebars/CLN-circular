package graph

import (
	"math"
	"time"
)

// Nodes that keep failing rebalances' payments are remembered across runs: a
// node's score counts the payments it failed in the middle of a route, fading
// by half every halfLife, and halved by each payment it forwarded. The route
// search with a risk rate (riskPPM) treats a node's score as a lower chance
// for every channel out of it: NODE_SCORE_SCALE failed payments cost as much
// as a channel with a chance of 1/e (riskPPM ppm of the amount), up to
// NODE_SCORE_MAX. A node that failed a hundred payments last night is then
// routed around while cheaper ways exist; one that failed a few barely moves.

const (
	// NODE_SCORE_SCALE is how many failed payments make one unit of
	// -ln(chance) in the search: riskPPM ppm of the amount each
	NODE_SCORE_SCALE = 50.0
	// NODE_SCORE_MAX caps a node's score in the search: at riskppm 200,
	// 1000 ppm
	NODE_SCORE_MAX = 250.0
	// NODE_SCORE_FORGET is the score below which a node is forgotten
	NODE_SCORE_FORGET = 0.5
)

// NodeScore is a node's failure score as of At (Unix time).
type NodeScore struct {
	Score float64 `json:"score"`
	At    int64   `json:"at"`
}

// SetNodeMemory sets how fast nodes' scores fade: by half every halfLife. 0
// turns the memory off: no score is kept or used.
func (g *Graph) SetNodeMemory(halfLife time.Duration) {
	g.nodeScoresLock.Lock()
	defer g.nodeScoresLock.Unlock()
	g.nodeHalfLife = halfLife
	if halfLife <= 0 {
		g.nodeScores = nil
	}
}

// fadedLocked is s faded to now. nodeScoresLock must be held.
func (g *Graph) fadedLocked(s NodeScore, now int64) float64 {
	if g.nodeHalfLife <= 0 {
		return 0
	}
	age := float64(now-s.At) / g.nodeHalfLife.Seconds()
	if age <= 0 {
		return s.Score
	}
	return s.Score * math.Pow(0.5, age)
}

// NodeFailed counts a payment that node id failed in the middle of a route.
func (g *Graph) NodeFailed(id string) {
	g.changeNode(id, func(score float64) float64 { return score + 1 })
}

// NodeForwarded halves node id's score: it forwarded a payment.
func (g *Graph) NodeForwarded(id string) {
	g.changeNode(id, func(score float64) float64 { return score / 2 })
}

func (g *Graph) changeNode(id string, change func(float64) float64) {
	g.nodeScoresLock.Lock()
	defer g.nodeScoresLock.Unlock()
	if g.nodeHalfLife <= 0 {
		return
	}
	now := time.Now().Unix()
	score := change(g.fadedLocked(g.nodeScores[id], now))
	if score < NODE_SCORE_FORGET {
		delete(g.nodeScores, id)
		return
	}
	if g.nodeScores == nil {
		g.nodeScores = map[string]NodeScore{}
	}
	g.nodeScores[id] = NodeScore{Score: score, At: now}
}

// NodeScores returns the nodes' scores faded to now, by node id, forgetting
// those that faded below NODE_SCORE_FORGET: what is saved across restarts.
func (g *Graph) NodeScores() map[string]NodeScore {
	g.nodeScoresLock.Lock()
	defer g.nodeScoresLock.Unlock()
	now := time.Now().Unix()
	out := make(map[string]NodeScore, len(g.nodeScores))
	for id, s := range g.nodeScores {
		score := g.fadedLocked(s, now)
		if score < NODE_SCORE_FORGET {
			delete(g.nodeScores, id)
			continue
		}
		out[id] = NodeScore{Score: score, At: now}
	}
	return out
}

// ApplyNodeScores keeps the scores saved by an earlier run, unless the graph
// knows a newer one for the node.
func (g *Graph) ApplyNodeScores(scores map[string]NodeScore) int {
	g.nodeScoresLock.Lock()
	defer g.nodeScoresLock.Unlock()
	if g.nodeHalfLife <= 0 {
		return 0
	}
	applied := 0
	for id, s := range scores {
		if old, ok := g.nodeScores[id]; ok && old.At >= s.At {
			continue
		}
		if g.nodeScores == nil {
			g.nodeScores = map[string]NodeScore{}
		}
		g.nodeScores[id] = s
		applied++
	}
	return applied
}

// nodePenalties is, for a search, each scored node's -ln(chance) units:
// its score faded to now over NODE_SCORE_SCALE, capped at NODE_SCORE_MAX.
func (g *Graph) nodePenalties() map[string]float64 {
	g.nodeScoresLock.Lock()
	defer g.nodeScoresLock.Unlock()
	if len(g.nodeScores) == 0 {
		return nil
	}
	now := time.Now().Unix()
	out := make(map[string]float64, len(g.nodeScores))
	for id, s := range g.nodeScores {
		score := g.fadedLocked(s, now)
		if score > NODE_SCORE_MAX {
			score = NODE_SCORE_MAX
		}
		out[id] = score / NODE_SCORE_SCALE
	}
	return out
}

// NodePenaltyPPM is what a node's score adds, in ppm of the amount, to a
// route through it in a search at riskPPM.
func NodePenaltyPPM(score float64, riskPPM uint64) uint64 {
	return uint64(math.Round(math.Min(score, NODE_SCORE_MAX) / NODE_SCORE_SCALE * float64(riskPPM)))
}

// nodeCost is what the search adds for sending amount out of a node with
// penalty units (nodePenalties) at riskPPM: penalty × riskPPM ppm of amount,
// in msat.
func nodeCost(penalty float64, amount, riskPPM uint64) uint64 {
	if penalty <= 0 {
		return 0
	}
	cost := penalty * float64(riskPPM) * float64(amount) / 1e6
	if cost >= math.MaxUint64/2 {
		return math.MaxUint64
	}
	return uint64(math.Ceil(cost))
}
