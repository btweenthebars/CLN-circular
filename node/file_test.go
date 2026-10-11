package node

import (
	"circular/graph"
	"circular/util"
	"encoding/json"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// graphWithChannel returns a graph with direction 02a -> 02b of channel scid
// (capacity 1,000,000 msat), believed to hold liquidity.
func graphWithChannel(scid string, liquidity uint64, timestamp int64) *graph.Graph {
	g := graph.NewGraph()
	c := graph.NewChannel(&glightning.Channel{
		Source:                   "02a",
		Destination:              "02b",
		ShortChannelId:           scid,
		IsActive:                 true,
		AmountMsat:               glightning.AmountFromMSat(1000000),
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(1000000),
	}, liquidity, timestamp)
	g.AddChannel(c)
	g.Channels[scid+"/"+util.GetDirection("02a", "02b")] = c
	return g
}

func id(scid string) string { return scid + "/" + util.GetDirection("02a", "02b") }

// The full graph was rewritten every 10 minutes, tens of MB each time, though
// listchannels rebuilds it at every start. Only what payments taught is saved.
func TestLearnedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Unix()
	g := graphWithChannel("1x1x1", 123, now)
	for scid, liquidity := range map[string]uint64{"2x2x2": 500000, "3x3x3": 7} { // 500000: the default
		other := graphWithChannel(scid, liquidity, now)
		for k, c := range other.Channels {
			g.AddChannel(c)
			g.Channels[k] = c
		}
	}
	g.Channels[id("3x3x3")].Timestamp = 0 // never learned
	n := &Node{Graph: g}
	assert.NoError(t, n.SaveLearned(dir))

	data, err := os.ReadFile(filepath.Join(dir, LEARNED_FILE))
	assert.NoError(t, err)
	assert.JSONEq(t, `{"version":1,"liquidity":{"`+id("1x1x1")+`":{"liquidity":123,"timestamp":`+
		jsonNumber(now)+`}}}`, string(data))

	beliefs, legacy, err := n.LoadLearned(dir)
	assert.NoError(t, err)
	assert.False(t, legacy)

	fresh := graphWithChannel("1x1x1", 500000, 0)
	assert.Equal(t, 1, fresh.ApplyBeliefs(beliefs, now-60))
	assert.Equal(t, uint64(123), fresh.Channels[id("1x1x1")].Liquidity)
	// too old, or for a channel that is gone
	fresh = graphWithChannel("1x1x1", 500000, 0)
	assert.Equal(t, 0, fresh.ApplyBeliefs(beliefs, now+60))
	assert.Equal(t, 0, graph.NewGraph().ApplyBeliefs(beliefs, 0))
}

func jsonNumber(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// A truncated file, as a power cut can leave, falls back to the copy before.
func TestCorruptLearnedFileFallsBack(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Unix()
	n := &Node{Graph: graphWithChannel("1x1x1", 1, now)}
	assert.NoError(t, n.SaveLearned(dir))
	n.Graph = graphWithChannel("1x1x1", 2, now)
	assert.NoError(t, n.SaveLearned(dir)) // the first copy is now learned.json.old

	assert.NoError(t, os.WriteFile(filepath.Join(dir, LEARNED_FILE), []byte(`{"version":1,"liq`), 0644))
	beliefs, _, err := n.LoadLearned(dir)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), beliefs[id("1x1x1")].Liquidity, "loaded the previous copy")

	assert.NoError(t, os.WriteFile(filepath.Join(dir, LEARNED_FILE+".old"), []byte(`not json`), 0644))
	_, _, err = n.LoadLearned(dir)
	assert.Equal(t, util.ErrNoGraphToLoad, err)
	_, _, err = n.LoadLearned(t.TempDir())
	assert.Equal(t, util.ErrNoGraphToLoad, err)
}

// An earlier version's graph.json gives its beliefs once, then is removed.
func TestLegacyGraphIsMigrated(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Unix()
	legacy := graphWithChannel("1x1x1", 42, now)
	data, err := json.Marshal(legacy)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "graph.json"), data, 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "graph.json.old"), data, 0644))

	n := &Node{Graph: graphWithChannel("1x1x1", 500000, 0), dataDir: dir, liquidityRefresh: time.Hour}
	n.loadLearned()
	assert.True(t, n.migrateLegacy)
	n.applyLearned()
	assert.Equal(t, uint64(42), n.Graph.Channels[id("1x1x1")].Liquidity)
	for _, name := range []string{"graph.json", "graph.json.old"} {
		_, err := os.Stat(filepath.Join(dir, name))
		assert.True(t, os.IsNotExist(err), name+" is removed")
	}
	beliefs, legacyLoaded, err := n.LoadLearned(dir)
	assert.NoError(t, err)
	assert.False(t, legacyLoaded)
	assert.Equal(t, uint64(42), beliefs[id("1x1x1")].Liquidity)
}
