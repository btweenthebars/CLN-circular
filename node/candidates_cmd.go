package node

import (
	"circular/util"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/elementsproject/glightning/jrpc2"
)

// CandidatesCmd is circular-candidates: how each of our channels did as a
// circular-pull or circular-push candidate, in the order runs rank them.
type CandidatesCmd struct {
	Scid  string `json:"scid,omitempty"`  // only this channel
	Limit int    `json:"limit,omitempty"` // how many (default 50)
}

// CandidateView is one channel's record as circular-candidates shows it.
type CandidateView struct {
	Scid string `json:"scid"`
	// As is "pull" for our channel to the peer, which circular-pull drains,
	// "push" for the peer's channel to us, which circular-push fills; ""
	// when the channel is no longer known
	As          string  `json:"as,omitempty"`
	PeerId      string  `json:"peer_id,omitempty"`
	Alias       string  `json:"alias,omitempty"`
	Successes   uint64  `json:"successes"`
	Failures    uint64  `json:"failures"`
	SuccessRate float64 `json:"success_rate"` // what the ranking orders by: (successes+1)/(successes+failures+2)
	MovedSat    uint64  `json:"moved_sat"`    // what the successes counted moved
	PaidPPM     *uint64 `json:"paid_ppm,omitempty"`
	LastSuccess string  `json:"last_success,omitempty"` // UTC
	LastFailure string  `json:"last_failure,omitempty"`
	Failing     bool    `json:"failing,omitempty"` // its last split failed
	// RankedLastUntil: it failed within the last 30 minutes and has not
	// succeeded since, so runs try it after all the others until then
	RankedLastUntil string `json:"ranked_last_until,omitempty"`
}

type candidatesResult struct {
	Count      int             `json:"count"` // records shown before the limit
	Candidates []CandidateView `json:"candidates"`
}

func (c *CandidatesCmd) Name() string {
	return "circular-candidates"
}

func (c *CandidatesCmd) New() interface{} {
	return &CandidatesCmd{}
}

func (c *CandidatesCmd) Call() (jrpc2.Result, error) {
	return GetNode().candidateViews(c.Scid, c.Limit), nil
}

// candidateViews lists the candidate records, of channel scid only if
// given, in the order RankCandidates gives them (before what moving
// liquidity on each costs us, which depends on the run): at most limit
// (default 50).
func (n *Node) candidateViews(scid string, limit int) *candidatesResult {
	if limit <= 0 {
		limit = 50
	}
	records := n.candidateList()
	ids := make([]string, 0, len(records))
	for id := range records {
		if s, _, _ := strings.Cut(id, "/"); scid == "" || s == scid {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids) // a stable starting order: the records come from a map
	ids = n.RankCandidates(ids, nil)

	// whose channel each is: our peers' channels, or the graph's
	peers := map[string]string{}
	if n.PeersLock != nil {
		n.PeersLock.RLock()
		for id, p := range n.Peers {
			for _, c := range p.Channels {
				peers[c.ShortChannelId] = id
			}
		}
		n.PeersLock.RUnlock()
	}

	now := time.Now().Unix()
	out := &candidatesResult{Count: len(ids), Candidates: make([]CandidateView, 0, len(ids))}
	for _, id := range ids {
		if len(out.Candidates) == limit {
			break
		}
		r := records[id]
		s, dir, _ := strings.Cut(id, "/")
		v := CandidateView{Scid: s, Successes: r.Successes, Failures: r.Failures, Failing: r.Failing,
			SuccessRate: math.Round(r.successRate()*1000) / 1000, MovedSat: r.AmountMsat / 1000}
		v.PeerId = peers[s]
		if v.PeerId == "" && n.Graph != nil {
			if c, err := n.Graph.GetChannel(id); err == nil {
				switch n.Id {
				case c.Source:
					v.PeerId = c.Destination
				case c.Destination:
					v.PeerId = c.Source
				}
			}
		}
		if v.PeerId != "" {
			v.As = "push"
			if util.GetDirection(n.Id, v.PeerId) == dir {
				v.As = "pull"
			}
			if n.Graph != nil {
				if alias := n.Graph.GetAlias(v.PeerId); alias != v.PeerId { // the id when it has none
					v.Alias = alias
				}
			}
		}
		if r.AmountMsat > 0 {
			ppm := uint64(math.Round(float64(r.FeeMsat) * 1e6 / float64(r.AmountMsat)))
			v.PaidPPM = &ppm
		}
		v.LastSuccess, v.LastFailure = utcTime(r.LastSuccess), utcTime(r.LastFailure)
		if r.backingOff(now) {
			v.RankedLastUntil = utcTime(r.LastFailure + int64(candidateBackoff.Seconds()))
		}
		out.Candidates = append(out.Candidates, v)
	}
	return out
}

// utcTime is Unix time t as RFC 3339 in UTC; "" for 0.
func utcTime(t int64) string {
	if t == 0 {
		return ""
	}
	return time.Unix(t, 0).UTC().Format(time.RFC3339)
}
