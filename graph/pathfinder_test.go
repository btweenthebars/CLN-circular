package graph

import (
	"circular/util"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"math"
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

	route, err := graph.GetRoute(src, dst, uint64(amount), exclude, maxHops)
	if err != nil {
		t.Fatal(err)
	}
	hops := route.Hops
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
		Source:                   a,
		Destination:              b1,
		ShortChannelId:           "1x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      100,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB1)
	g.Channels["1x1x1/"+util.GetDirection(a, b1)] = chAB1

	chB1C := NewChannel(&glightning.Channel{
		Source:                   b1,
		Destination:              cNode,
		ShortChannelId:           "2x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      500,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chB1C)
	g.Channels["2x1x1/"+util.GetDirection(b1, cNode)] = chB1C

	chAB2 := NewChannel(&glightning.Channel{
		Source:                   a,
		Destination:              b2,
		ShortChannelId:           "3x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      200,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB2)
	g.Channels["3x1x1/"+util.GetDirection(a, b2)] = chAB2

	chB2C := NewChannel(&glightning.Channel{
		Source:                   b2,
		Destination:              cNode,
		ShortChannelId:           "4x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      500,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chB2C)
	g.Channels["4x1x1/"+util.GetDirection(b2, cNode)] = chB2C

	route, err := g.GetRoute(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(route.Hops))
	// Both paths cost 500. B2's inbound discount of -200 makes its path the cheaper one.

	g.SetInboundFee("3x1x1/"+util.GetDirection(a, b2), -200, 0)
	route, err = g.GetRoute(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(route.Hops))
	assert.Equal(t, "3x1x1", route.Hops[0].ShortChannelId)
	assert.Equal(t, "4x1x1", route.Hops[1].ShortChannelId)
	assert.Equal(t, uint64(300), route.Fee()) // B2.out (500) + B2.in (-200) = 300

	chB2C.BaseFeeMillisatoshi = 100
	route, err = g.GetRoute(a, cNode, 1000000, nil, 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), route.Fee()) // B2.out (100) + B2.in (-200) = 0
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
		Source:                   self,
		Destination:              a,
		ShortChannelId:           "9x9x9",
		IsActive:                 true,
		BaseFeeMillisatoshi:      0,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chOut)
	g.Channels["9x9x9/"+util.GetDirection(self, a)] = chOut

	chAB := NewChannel(&glightning.Channel{
		Source:                   a,
		Destination:              b,
		ShortChannelId:           "1x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      100,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chAB)
	g.Channels["1x1x1/"+util.GetDirection(a, b)] = chAB

	chBC := NewChannel(&glightning.Channel{
		Source:                   b,
		Destination:              c,
		ShortChannelId:           "2x1x1",
		IsActive:                 true,
		BaseFeeMillisatoshi:      500,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chBC)
	g.Channels["2x1x1/"+util.GetDirection(b, c)] = chBC

	chIn := NewChannel(&glightning.Channel{
		Source:                   c,
		Destination:              self,
		ShortChannelId:           "8x8x8",
		IsActive:                 true,
		BaseFeeMillisatoshi:      0,
		FeePerMillionth:          0,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000),
	}, 5000000, 0)
	g.AddChannel(chIn)
	g.Channels["8x8x8/"+util.GetDirection(c, self)] = chIn

	// Test 1: Inbound fee is 0. No savings.
	route, err := g.GetCircularRoute(chOut, chIn, 1000000, nil, 10, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(route.Hops))
	assert.Equal(t, uint64(600), route.Fee()) // a (100) + b (500)
	pr := NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(0), pr.InboundSavingsMSat)

	// Test 2: Set a negative inbound fee on chAB
	g.SetInboundFee("1x1x1/"+util.GetDirection(a, b), -200, 0)
	route, err = g.GetCircularRoute(chOut, chIn, 1000000, nil, 10, math.MaxUint64)
	assert.NoError(t, err)
	pr = NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(200), pr.InboundSavingsMSat)

	// Test 3: Positive inbound fee (surcharge)
	g.SetInboundFee("1x1x1/"+util.GetDirection(a, b), 150, 0)
	route, err = g.GetCircularRoute(chOut, chIn, 1000000, nil, 10, math.MaxUint64)
	assert.NoError(t, err)
	pr = NewPrettyRoute(route, "hash")
	assert.Equal(t, int64(-150), pr.InboundSavingsMSat)
}

// testGraph builds small graphs for the route search tests.
type testGraph struct {
	*Graph
}

func newTestGraph() *testGraph {
	return &testGraph{NewGraph()}
}

// channel adds the direction from -> to of channel scid, with plenty of liquidity.
func (g *testGraph) channel(scid, from, to string, baseFee, ppm uint64) *Channel {
	c := NewChannel(&glightning.Channel{
		Source:                   from,
		Destination:              to,
		ShortChannelId:           scid,
		IsActive:                 true,
		AmountMsat:               glightning.AmountFromMSat(10000000000),
		BaseFeeMillisatoshi:      baseFee,
		FeePerMillionth:          ppm,
		Delay:                    10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000000),
	}, 5000000000, 0)
	g.AddChannel(c)
	g.Channels[scid+"/"+util.GetDirection(from, to)] = c
	return c
}

func scids(route *Route) []string {
	result := make([]string, len(route.Hops))
	for i, hop := range route.Hops {
		result[i] = hop.ShortChannelId
	}
	return result
}

const (
	self = "02self"
	outP = "02outpeer"
	inP  = "02inpeer"
)

// The out-peer's fee on its first channel used to be left out of the search,
// so a route through an expensive first channel looked free.
func TestCircularRouteCountsOutPeerFee(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 2000) // the out-peer charges 2000 ppm here
	g.channel("4x1x1", "02a", inP, 0, 0)
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 0, 10)

	route, err := g.GetCheapestCircularRoute(out, in, 1000000, nil, 8)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
	assert.Equal(t, uint64(10), route.FeePPM())

	route, err = g.GetCircularRoute(out, in, 1000000, nil, 8, MaxFeeForPPM(1000000, 50))
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), route.FeePPM())
}

// The in-peer's inbound fee depends on the channel the payment arrives on,
// and used to be left out of the search.
func TestCircularRouteCountsInPeerInboundFee(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 1000, 0) // the in-peer charges 1000 msat to reach us
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, 0)
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 1, 0)
	// ... but waives it for payments arriving from 02b
	g.SetInboundFee("6x1x1/"+util.GetDirection("02b", inP), -1000, 0)

	route, err := g.GetCheapestCircularRoute(out, in, 1000000, nil, 8)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
	assert.Equal(t, uint64(1), route.Fee())
}

// A cheap long path to a channel used to hide a shorter, slightly more
// expensive one, so a route within maxhops came back as "no route".
func TestCircularRouteHopLimitKeepsShorterPaths(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02b", 0, 0)
	g.channel("4x1x1", "02b", "02a", 0, 0)
	g.channel("5x1x1", "02a", "02x", 0, 0)
	g.channel("6x1x1", "02x", inP, 100, 0) // short way, 100 msat
	g.channel("7x1x1", "02x", "02w", 0, 0) // long way, free
	g.channel("8x1x1", "02w", inP, 0, 0)

	// 6 channels in total: out, outpeer-b, b-a, a-x, x-inpeer, in
	route, err := g.GetCheapestCircularRoute(out, in, 1000000, nil, 6)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "4x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
	assert.Equal(t, uint64(100), route.Fee())

	route, err = g.GetCircularRoute(out, in, 1000000, nil, 6, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(route.Hops))

	// with one more hop allowed, the free route wins
	route, err = g.GetCheapestCircularRoute(out, in, 1000000, nil, 7)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), route.Fee())
	assert.Equal(t, 7, len(route.Hops))
}

// Failed channels are excluded by "scid/direction"; they used to be ignored.
func TestCircularRouteExcludesChannels(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, 0)
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 0, 100)

	exclude := map[string]bool{"4x1x1/" + util.GetDirection("02a", inP): true}
	route, err := g.GetCheapestCircularRoute(out, in, 1000000, exclude, 8)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))

	exclude["02b"] = true
	_, err = g.GetCheapestCircularRoute(out, in, 1000000, exclude, 8)
	assert.Equal(t, util.ErrNoRoute, err)
}

// Within maxppm the route with the fewest hops wins, as before.
func TestCircularRoutePrefersFewerHopsWithinBudget(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, inP, 0, 9) // 3 hops, 9 ppm
	g.channel("4x1x1", outP, "02a", 0, 1)
	g.channel("5x1x1", "02a", inP, 0, 0) // 4 hops, 1 ppm

	route, err := g.GetCircularRoute(out, in, 1000000, nil, 8, MaxFeeForPPM(1000000, 10))
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "2x1x1"}, scids(route))

	route, err = g.GetCircularRoute(out, in, 1000000, nil, 8, MaxFeeForPPM(1000000, 5))
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "4x1x1", "5x1x1", "2x1x1"}, scids(route))

	_, err = g.GetCircularRoute(out, in, 1000000, nil, 8, MaxFeeForPPM(1000000, 0))
	assert.Equal(t, util.ErrNoRoute, err)
}

func TestMaxFeeForPPM(t *testing.T) {
	assert.Equal(t, uint64(10), MaxFeeForPPM(1000000, 10))
	assert.Equal(t, uint64(2199), MaxFeeForPPM(200000000, 10))
	assert.Equal(t, uint64(math.MaxUint64), MaxFeeForPPM(200000000, math.MaxUint64))

	route := &Route{Amount: 200000000, Hops: []RouteHop{{MilliSatoshi: 200000000 + 2199}}}
	assert.Equal(t, uint64(10), route.FeePPM())
	route.Hops[0].MilliSatoshi++
	assert.Equal(t, uint64(11), route.FeePPM())
}

// LND computes a forwarding node's inbound fee on the amount it forwards plus
// its outbound fee. Computing it on the forwarded amount alone underpaid
// nodes with a positive inbound fee, which then failed the payment.
func TestInboundFeeAppliesToAmountPlusOutboundFee(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, 1000) // 02a charges 1000 ppm...
	// ... plus 1000 ppm inbound for payments arriving from the out-peer
	g.SetInboundFee("3x1x1/"+util.GetDirection(outP, "02a"), 0, 1000)

	const amount = 1000000000 // 1M sats
	// outbound: 1,000,000 msat; inbound: 1000 ppm of 1,001,000,000 msat
	const fee = 1000000 + 1001000

	route, err := g.GetCheapestCircularRoute(out, in, amount, nil, 8)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "4x1x1", "2x1x1"}, scids(route))
	assert.Equal(t, uint64(fee), route.Fee())
	assert.Equal(t, uint64(amount+fee), route.Hops[1].MilliSatoshi, "what the out-peer forwards to 02a")

	// the search prices the route the same way
	_, err = g.GetCircularRoute(out, in, amount, nil, 8, fee-1)
	assert.Equal(t, util.ErrNoRoute, err)
	_, err = g.GetCircularRoute(out, in, amount, nil, 8, fee)
	assert.NoError(t, err)

	pretty := NewPrettyRoute(route, "")
	assert.Equal(t, int64(1001000), pretty.Hops[2].InboundFee)
	assert.Equal(t, uint64(1000000), pretty.Hops[2].OutboundFee)
}

// As LND's InboundFee.CalcFee: the rate is capped at 10,000,000 ppm either
// way, and the proportional part rounds toward zero.
func TestInboundFeeMatchesLND(t *testing.T) {
	g := NewGraph()
	g.SetInboundFee("1x1x1/0", 5, math.MaxInt32)
	g.SetInboundFee("1x1x1/1", 0, -1)
	g.SetInboundFee("2x1x1/0", -10, math.MinInt32)

	assert.Equal(t, int64(5+10000000), g.inboundFee("1x1x1/0", 1000000))
	assert.Equal(t, int64(0), g.inboundFee("1x1x1/1", 999999))
	assert.Equal(t, int64(-1), g.inboundFee("1x1x1/1", 1999999))
	assert.Equal(t, int64(-10-10000000), g.inboundFee("2x1x1/0", 1000000))
	// 10 BTC at the capped rate: rate x amount does not fit in an int64
	assert.Equal(t, int64(5+10000000000000), g.inboundFee("1x1x1/0", 1000000000000))
	assert.Equal(t, int64(0), g.inboundFee("3x1x1/0", 1000000), "no inbound fee")
}

// A channel whose fee wrapped around looked cheap and was picked over a
// reasonable one.
func TestSearchAvoidsChannelsWithHugeFees(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, math.MaxUint32) // keeps payments off
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 0, 100)

	// 4.99M sats: 4x1x1's amount x ppm does not fit in 64 bits
	route, err := g.GetCheapestCircularRoute(out, in, 4990000000, nil, 8)
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
		assert.Equal(t, uint64(499000), route.Fee())
	}
}

// Peers refuse HTLCs that expire more than 2016 blocks ahead, and the search
// had no limit on a route's total CLTV delay.
func TestCircularRouteKeepsWithinTheDelayLimit(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0).Delay = 400
	g.channel("4x1x1", "02a", inP, 0, 0).Delay = 1500 // the cheap way, too slow
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 100, 0)

	for _, find := range []func() (*Route, error){
		func() (*Route, error) { return g.GetCircularRoute(out, in, 1000000, nil, 8, 1000) },
		func() (*Route, error) { return g.GetCheapestCircularRoute(out, in, 1000000, nil, 8) },
	} {
		route, err := find()
		if assert.NoError(t, err) {
			assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
			assert.Equal(t, uint(INITIAL_DELAY+10+10+10), route.Hops[0].Delay)
		}
	}

	// with no way within the limit, there is no route
	g.Channels["6x1x1/"+util.GetDirection("02b", inP)].Delay = 2000
	_, err := g.GetCheapestCircularRoute(out, in, 1000000, nil, 8)
	assert.Equal(t, util.ErrNoRoute, err)

	// a route right at the limit is fine
	g.Channels["4x1x1/"+util.GetDirection("02a", inP)].Delay = MAX_ROUTE_DELAY - INITIAL_DELAY - 10 - 400
	route, err := g.GetCheapestCircularRoute(out, in, 1000000, nil, 8)
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"1x1x1", "3x1x1", "4x1x1", "2x1x1"}, scids(route))
		assert.Equal(t, uint(MAX_ROUTE_DELAY), route.Hops[0].Delay)
	}

	// node to node, the sender adds no delta of its own (3x1x1's 400)
	g.Channels["4x1x1/"+util.GetDirection("02a", inP)].Delay = MAX_ROUTE_DELAY - INITIAL_DELAY
	route, err = g.GetRoute(outP, inP, 1000000, nil, 8)
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"3x1x1", "4x1x1"}, scids(route))
		assert.Equal(t, uint(MAX_ROUTE_DELAY), route.Hops[0].Delay)
	}
	g.Channels["4x1x1/"+util.GetDirection("02a", inP)].Delay++
	g.Channels["6x1x1/"+util.GetDirection("02b", inP)].Delay = 10
	route, err = g.GetRoute(outP, inP, 1000000, nil, 8)
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"5x1x1", "6x1x1"}, scids(route))
	}
}
