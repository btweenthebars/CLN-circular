package graph

import (
	"circular/util"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func LoadGraphFromFile(dir, filename string) (*Graph, error) {
	file, err := os.Open(dir + "/" + filename)
	if err != nil {
		if err != nil {
			return nil, util.ErrNoGraphToLoad
		}
	}
	defer file.Close()

	g := NewGraph()

	err = json.NewDecoder(file).Decode(g)
	if err != nil {
		return nil, err
	}

	for _, c := range g.Channels {
		g.AddChannel(c)
	}
	return g, nil
}

func TestPathfinderBasic(t *testing.T) {
	t.Log("graph/pathfinder_test.go")

	graph, err := LoadGraphFromFile("testdata", "graph.json")
	if err != nil {
		t.Fatal(err)
	}
	src := "02d41224b71a5346a656f8949c66d11495e39dac55ab8772f55c26ca515db910ea"
	dst := "03c731efa9935d869d87e57d4496de2b3badfb9ec7dbbd40051fb19351027336c5"
	amount := 200000000
	exclude := map[string]bool{
		"02a30b35b374b0bde273f2e36f1a6db9b1d9f4591d00416ffa541b6eb16e70921f": true,
	}
	maxHops := 10

	hops, err := graph.dijkstra(src, dst, uint64(amount), exclude, maxHops)
	if err != nil {
		t.Fatal(err)
	}
	assert.LessOrEqual(t, len(hops), maxHops)
	for i := 0; i < len(hops)-1; i++ {
		assert.Equal(t, hops[i].Destination, hops[i+1].Source)
		assert.GreaterOrEqual(t, hops[i].Liquidity, hops[i].MilliSatoshi)
		assert.GreaterOrEqual(t, hops[i].MilliSatoshi, hops[i+1].MilliSatoshi)
		assert.Greater(t, hops[i].Delay, hops[i+1].Delay)
	}
	assert.Equal(t, hops[len(hops)-1].Destination, dst)
	assert.Equal(t, hops[0].Source, src)
}

func BenchmarkGraph_GetRoute(b *testing.B) {
	graph, err := LoadGraphFromFile("testdata", "mainnet_graph.json")
	if err != nil {
		b.Fatal(err)
	}
	rand.Seed(69)

	// get a slice of the ids of all the nodes in the graph
	ids := make([]string, len(graph.Inbound))
	i := 0
	for k := range graph.Inbound {
		ids[i] = k
		i++
	}

	inputs := make([]int, 0)
	for i := 3; i <= 8; i++ {
		inputs = append(inputs, i)
	}

	for _, h := range inputs {
		b.Run(fmt.Sprintf("dijkstra_%d_maxhops", h), func(b *testing.B) {
			b.N = 1000
			for i := 0; i < b.N; i++ {
				// get random key from inbound map
				src := ids[rand.Intn(len(ids))]
				dst := ids[rand.Intn(len(ids))]
				amount := uint64(rand.Intn(1000000000))
				graph.GetRoute(src, dst, amount, nil, h)
			}
		})
	}
}

func TestPathfinderInboundFee(t *testing.T) {
	g := NewGraph()

	a := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b1 := "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb11"
	b2 := "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22"
	cNode := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	g.Inbound[a] = make(map[string]Edge)
	g.Inbound[b1] = make(map[string]Edge)
	g.Inbound[b2] = make(map[string]Edge)
	g.Inbound[cNode] = make(map[string]Edge)

	chAB1 := NewChannel(&glightning.Channel{
		Source:              a,
		Destination:         b1,
		ShortChannelId:      "1x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 100,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB1)
	g.Channels["1x1x1/"+util.GetDirection(a, b1)] = chAB1

	chB1C := NewChannel(&glightning.Channel{
		Source:              b1,
		Destination:         cNode,
		ShortChannelId:      "2x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 500,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chB1C)
	g.Channels["2x1x1/"+util.GetDirection(b1, cNode)] = chB1C

	chAB2 := NewChannel(&glightning.Channel{
		Source:              a,
		Destination:         b2,
		ShortChannelId:      "3x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 200,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB2)
	g.Channels["3x1x1/"+util.GetDirection(a, b2)] = chAB2

	chB2C := NewChannel(&glightning.Channel{
		Source:              b2,
		Destination:         cNode,
		ShortChannelId:      "4x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 500,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chB2C)
	g.Channels["4x1x1/"+util.GetDirection(b2, cNode)] = chB2C

	hops, err := g.dijkstra(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(hops))
	assert.Equal(t, "1x1x1", hops[0].ShortChannelId)
	assert.Equal(t, "2x1x1", hops[1].ShortChannelId)

	g.SetInboundFee("4x1x1/"+util.GetDirection(b2, cNode), -300, 0)

	hops, err = g.dijkstra(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(hops))
	assert.Equal(t, "3x1x1", hops[0].ShortChannelId)
	assert.Equal(t, "4x1x1", hops[1].ShortChannelId)

	route := NewRoute(a, cNode, 1000000, hops, g)
	assert.Equal(t, uint64(400), route.Fee())

	chB2C.BaseFeeMillisatoshi = 100
	hops, err = g.dijkstra(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	route = NewRoute(a, cNode, 1000000, hops, g)
	assert.Equal(t, uint64(200), route.Fee())
}

func TestPrettyRouteSavings(t *testing.T) {
	g := NewGraph()

	self := "02selfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	a := "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b := "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	c := "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	g.Inbound[self] = make(map[string]Edge)
	g.Inbound[a] = make(map[string]Edge)
	g.Inbound[b] = make(map[string]Edge)
	g.Inbound[c] = make(map[string]Edge)

	chOut := NewChannel(&glightning.Channel{
		Source:              self,
		Destination:         a,
		ShortChannelId:      "9x9x9",
		IsActive:            true,
		BaseFeeMillisatoshi: 0,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chOut)
	g.Channels["9x9x9/"+util.GetDirection(self, a)] = chOut

	chAB := NewChannel(&glightning.Channel{
		Source:              a,
		Destination:         b,
		ShortChannelId:      "1x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 100,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB)
	g.Channels["1x1x1/"+util.GetDirection(a, b)] = chAB

	chBC := NewChannel(&glightning.Channel{
		Source:              b,
		Destination:         c,
		ShortChannelId:      "2x1x1",
		IsActive:            true,
		BaseFeeMillisatoshi: 500,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chBC)
	g.Channels["2x1x1/"+util.GetDirection(b, c)] = chBC

	chIn := NewChannel(&glightning.Channel{
		Source:              c,
		Destination:         self,
		ShortChannelId:      "8x8x8",
		IsActive:            true,
		BaseFeeMillisatoshi: 0,
		FeePerMillionth:     0,
		Delay:               10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chIn)
	g.Channels["8x8x8/"+util.GetDirection(c, self)] = chIn

	// Test 1: Inbound fee is 0. No savings.
	hops, err := g.dijkstra(a, c, 1000000, nil, 10)
	assert.NoError(t, err)
	route := NewRoute(a, c, 1000000, hops, g)
	route.Prepend(chOut)
	route.Append(chIn)
	pr := NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(0), pr.InboundSavingsMSat)

	// Test 2: Set a negative inbound fee on chAB
	g.SetInboundFee("1x1x1/"+util.GetDirection(a, b), -200, 0)
	hops, err = g.dijkstra(a, c, 1000000, nil, 10)
	assert.NoError(t, err)
	route = NewRoute(a, c, 1000000, hops, g)
	route.Prepend(chOut)
	route.Append(chIn)
	pr = NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(200), pr.InboundSavingsMSat)

	// Test 3: Positive inbound fee (surcharge)
	g.SetInboundFee("1x1x1/"+util.GetDirection(a, b), 150, 0)
	hops, err = g.dijkstra(a, c, 1000000, nil, 10)
	assert.NoError(t, err)
	route = NewRoute(a, c, 1000000, hops, g)
	route.Prepend(chOut)
	route.Append(chIn)
	pr = NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(-150), pr.InboundSavingsMSat)
}
