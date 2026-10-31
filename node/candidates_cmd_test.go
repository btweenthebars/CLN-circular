package node

import (
	"circular/graph"
	"circular/util"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
)

// circular-candidates shows the records the ranking uses, in its order.
func TestCandidateViews(t *testing.T) {
	g := graph.NewGraph()
	g.Aliases = map[string]string{"02a": "Alpha", "02g": "Gamma"}
	// 3x1x1, our channel to 02g, is in the graph but not among our peers'
	// channels (not read yet)
	g.Channels["3x1x1/"+util.GetDirection("02self", "02g")] = graph.NewChannel(&glightning.Channel{
		ShortChannelId: "3x1x1", Source: "02self", Destination: "02g"}, 0, 0)
	n := &Node{Id: "02self", Graph: g, PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{
		"02a": {Id: "02a", Channels: []*glightning.PeerChannel{{ShortChannelId: "1x1x1"}}},
		"02z": {Id: "02z", Channels: []*glightning.PeerChannel{{ShortChannelId: "2x1x1"}}},
	}}
	pullA := "1x1x1/" + util.GetDirection("02self", "02a") // drained by circular-pull
	pushA := "1x1x1/" + util.GetDirection("02a", "02self") // filled by circular-push
	pullZ := "2x1x1/" + util.GetDirection("02self", "02z")
	pullG := "3x1x1/" + util.GetDirection("02self", "02g")
	for i := 0; i < 3; i++ {
		n.RecordCandidate(pullA, true, 1000000, 100) // 100 ppm
	}
	n.RecordCandidate(pushA, false, 0, 0) // just failed: last for 30 minutes
	n.RecordCandidate(pullZ, false, 0, 0)
	n.RecordCandidate(pullZ, true, 2000000, 1000) // 500 ppm, failing no more
	n.RecordCandidate("9x9x9/0", true, 1000000, 0)
	n.RecordCandidate(pullG, true, 1000000, 50)
	n.RecordCandidate(pullG, true, 1000000, 50)

	res := n.candidateViews("", 0)
	assert.Equal(t, 5, res.Count)
	var order []string
	for _, v := range res.Candidates {
		order = append(order, v.Scid+":"+v.As)
	}
	// rates 4/5, 3/4, 2/3, 2/4; the one backing off last
	assert.Equal(t, []string{"1x1x1:pull", "3x1x1:pull", "9x9x9:", "2x1x1:pull", "1x1x1:push"}, order)

	a := res.Candidates[0]
	assert.Equal(t, "02a", a.PeerId)
	assert.Equal(t, "Alpha", a.Alias)
	assert.Equal(t, uint64(3), a.Successes)
	assert.Equal(t, 0.8, a.SuccessRate)
	assert.Equal(t, uint64(3000), a.MovedSat)
	if assert.NotNil(t, a.PaidPPM) {
		assert.Equal(t, uint64(100), *a.PaidPPM)
	}
	assert.Equal(t, utcTime(n.CandidateRecord(pullA).LastSuccess), a.LastSuccess)
	assert.Empty(t, a.LastFailure)
	assert.Empty(t, a.RankedLastUntil)

	g3 := res.Candidates[1]
	assert.Equal(t, "02g", g3.PeerId, "from the graph")
	assert.Equal(t, "Gamma", g3.Alias)
	assert.Equal(t, 0.75, g3.SuccessRate)

	unknown := res.Candidates[2]
	assert.Empty(t, unknown.PeerId, "no longer known")
	assert.Empty(t, unknown.Alias)
	assert.Equal(t, 0.667, unknown.SuccessRate)
	if assert.NotNil(t, unknown.PaidPPM) {
		assert.Zero(t, *unknown.PaidPPM, "moved for free")
	}

	z := res.Candidates[3]
	assert.Empty(t, z.Alias, "no alias: not its id again")
	assert.False(t, z.Failing, "succeeded since")
	assert.NotEmpty(t, z.LastFailure)
	assert.Equal(t, uint64(500), *z.PaidPPM)
	assert.Empty(t, z.RankedLastUntil)

	push := res.Candidates[4]
	assert.Equal(t, "02a", push.PeerId)
	assert.True(t, push.Failing)
	assert.Nil(t, push.PaidPPM, "moved nothing")
	failed := n.CandidateRecord(pushA).LastFailure
	assert.Equal(t, utcTime(failed+int64(candidateBackoff.Seconds())), push.RankedLastUntil)
	at, err := time.Parse(time.RFC3339, push.RankedLastUntil)
	assert.NoError(t, err)
	assert.InDelta(t, time.Now().Add(30*time.Minute).Unix(), at.Unix(), 5)

	// one channel's, both ways; a limit
	res = n.candidateViews("1x1x1", 0)
	assert.Equal(t, 2, res.Count)
	assert.Equal(t, "pull", res.Candidates[0].As)
	assert.Equal(t, "push", res.Candidates[1].As)
	res = n.candidateViews("", 2)
	assert.Equal(t, 5, res.Count, "what matched, before the limit")
	assert.Len(t, res.Candidates, 2)
	assert.Empty(t, n.candidateViews("7x7x7", 0).Candidates)

	out, err := json.Marshal(n.candidateViews("1x1x1", 0))
	assert.NoError(t, err)
	assert.Contains(t, string(out), `"scid":"1x1x1","as":"pull","peer_id":"02a","alias":"Alpha","successes":3,"failures":0,"success_rate":0.8,"moved_sat":3000,"paid_ppm":100`)
	assert.NotContains(t, string(out), `"paid_ppm":null`)

	// nothing recorded yet
	empty := (&Node{Id: "02self"}).candidateViews("", 0)
	assert.Zero(t, empty.Count)
	assert.NotNil(t, empty.Candidates, "[] in JSON, not null")
}

func TestUTCTime(t *testing.T) {
	assert.Equal(t, "", utcTime(0))
	assert.Equal(t, "2026-10-31T11:00:00Z", utcTime(1793444400))
}
