package node

import (
	"circular/graph"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func graphWithChannel(scid string) *graph.Graph {
	g := graph.NewGraph()
	c := graph.NewChannel(&glightning.Channel{
		Source:                   "02a",
		Destination:              "02b",
		ShortChannelId:           scid,
		IsActive:                 true,
		AmountMsat:               glightning.AmountFromMSat(1000000),
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(1000000),
	}, 500000, 0)
	g.AddChannel(c)
	g.Channels[scid+"/"+util.GetDirection("02a", "02b")] = c
	return g
}

func TestGraphFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	n := &Node{Graph: graphWithChannel("1x1x1")}
	assert.NoError(t, n.SaveGraphToFile(dir, "graph.json"))

	loaded := &Node{}
	assert.NoError(t, loaded.LoadGraphFromFile(dir, "graph.json"))
	assert.Len(t, loaded.Graph.Channels, 1)
	assert.Equal(t, graph.Edge{"1x1x1"}, loaded.Graph.Inbound["02b"]["02a"])

	_, err := os.Stat(dir + "/graph.json.tmp")
	assert.True(t, os.IsNotExist(err), "the temporary file is renamed into place")
}

// A truncated graph.json, as a power cut can leave, used to stop the plugin at
// every start until the file was deleted.
func TestCorruptGraphFileFallsBack(t *testing.T) {
	dir := t.TempDir()
	n := &Node{Graph: graphWithChannel("1x1x1")}
	assert.NoError(t, n.SaveGraphToFile(dir, "graph.json"))
	n.Graph = graphWithChannel("2x2x2")
	assert.NoError(t, n.SaveGraphToFile(dir, "graph.json")) // the first copy is now graph.json.old

	assert.NoError(t, os.WriteFile(dir+"/graph.json", []byte(`{"channels":{"2x2x2/0":{"chan`), 0644))
	loaded := &Node{}
	assert.NoError(t, loaded.LoadGraphFromFile(dir, "graph.json"))
	_, ok := loaded.Graph.Channels["1x1x1/"+util.GetDirection("02a", "02b")]
	assert.True(t, ok, "loaded the previous copy")

	assert.NoError(t, os.WriteFile(dir+"/graph.json.old", []byte(`not json`), 0644))
	assert.Equal(t, util.ErrNoGraphToLoad, (&Node{}).LoadGraphFromFile(dir, "graph.json"))

	assert.Equal(t, util.ErrNoGraphToLoad, (&Node{}).LoadGraphFromFile(t.TempDir(), "graph.json"))
}
