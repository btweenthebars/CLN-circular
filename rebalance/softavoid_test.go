package rebalance

import (
	"circular/graph"
	"circular/util"
	"errors"
	"testing"

	"github.com/elementsproject/glightning/glightning"

	"github.com/stretchr/testify/assert"
)

func TestSoftAvoid(t *testing.T) {
	assert.Nil(t, NewSoftAvoid(nil, 5), "none: no soft avoid")
	var none *SoftAvoid
	assert.Nil(t, none.Active())
	none.paid(false) // nil-safe
	none.fellBack()
	assert.Nil(t, none.Report(nil, nil))

	keys := []string{"02a", "02b"}
	s := NewSoftAvoid(keys, 0)
	keys[0] = "changed" // the caller's slice is its own
	assert.Equal(t, DEFAULT_SOFT_AVOID_STREAK, s.Limit, "0: the default")
	got := s.Active()
	got[0] = "x"
	assert.Equal(t, []string{"02a", "02b"}, s.Active(), "a copy")

	s = NewSoftAvoid([]string{"02a"}, 3)
	s.paid(false)
	s.paid(false)
	s.paid(true) // a payment went through: the streak starts over
	s.paid(false)
	s.paid(false)
	assert.Equal(t, []string{"02a"}, s.Active())
	s.paid(false)
	assert.Nil(t, s.Active(), "3 in a row: let back")
	s.paid(true)
	assert.Nil(t, s.Active(), "for the rest of the run")
	r := s.Report(nil, nil)
	assert.Equal(t, "after 3 failed payments in a row", r.Lifted)

	never := NewSoftAvoid([]string{"02a"}, -1)
	for i := 0; i < 500; i++ {
		never.paid(false)
	}
	assert.Equal(t, []string{"02a"}, never.Active(), "-1: never by a streak")

	s = NewSoftAvoid([]string{"02a", "02b", "3x1x1/0"}, 0)
	s.fellBack()
	s.fellBack()
	r = s.Report(func(k string) string { return map[string]string{"02b": "Hub"}[k] },
		func(k string) float64 { return map[string]float64{"02b": 9, "02a": 2}[k] })
	assert.Equal(t, &SoftAvoidReport{Fallbacks: 2, Nodes: []SoftAvoidNode{
		{Key: "02b", Alias: "Hub", Score: 9}, {Key: "02a", Score: 2}, {Key: "3x1x1/0"}}}, r, "most failing first")
}

// softNet is shortRoute with a second way, dearer, around 02a: out-peer →
// 02ok → 02b.
func softNet() (*network, *Rebalance) {
	n, out, in := shortRoute()
	addChannel(n.Graph, "7x1x1", outP, "02ok", 5000)
	addChannel(n.Graph, "8x1x1", "02ok", "02b", 5000)
	r := rebalanceOn(n, out, in, 50000000)
	r.Attempts, r.MaxNodeFails = 10, 50
	return n, r
}

// The soft-avoided nodes are left out while another route exists; with none
// left, the split goes through them at once; and its own peers never count.
func TestSoftAvoidedNodesAreRoutedAround(t *testing.T) {
	n, r := softNet()
	res := r.Run()
	assert.Contains(t, res.Channels, id("3x1x1", "02a", "02b"), "without soft avoid: the cheap way, via 02a")

	n, r = softNet()
	r.Soft = NewSoftAvoid([]string{"02a"}, 0)
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Contains(t, res.Channels, id("7x1x1", outP, "02ok"), "around 02a")
	assert.False(t, r.SoftFellBack)
	assert.Equal(t, 1, len(n.paid))

	// no way without them: the same search goes through them
	n, r = softNet()
	r.Soft = NewSoftAvoid([]string{"02a", "02ok"}, 0)
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Contains(t, res.Channels, id("3x1x1", "02a", "02b"))
	assert.True(t, r.SoftFellBack)
	assert.Equal(t, 1, r.Soft.Report(nil, nil).Fallbacks)
	assert.Equal(t, []string{"02a", "02ok"}, r.Soft.Active(), "a fallback lets them back for that search only")

	// a route above maxppm without them is no route either
	n, r = softNet()
	r.MaxPPM = 150 // 02ok's way costs 200 ppm
	r.Soft = NewSoftAvoid([]string{"02a"}, 0)
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Contains(t, res.Channels, id("3x1x1", "02a", "02b"))

	// the split's own peers are never left out
	n, r = softNet()
	r.Soft = NewSoftAvoid([]string{outP, inP, "02a"}, 0)
	with := r.softExclude(map[string]bool{"02z": true})
	assert.Equal(t, map[string]bool{"02z": true, "02a": true}, with, "the ends: never")
	r.Soft = NewSoftAvoid([]string{outP, inP}, 0)
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.False(t, r.SoftFellBack)
	assert.Equal(t, 0, r.Soft.Report(nil, nil).Fallbacks)
}

// Payments that keep failing on the other routes let the soft-avoided nodes
// back for the whole run, though routes without them are still found.
func TestAFailureStreakLetsThemBack(t *testing.T) {
	build := func() (*network, *Rebalance) {
		n, r := softNet()
		n.liquidity[id("8x1x1", "02ok", "02b")] = 0
		for _, s := range []string{"10", "11", "12"} { // three more ways around 02a, all drained
			via := "02w" + s
			addChannel(n.Graph, s+"x1x1", outP, via, 5000)
			c := addChannel(n.Graph, s+"x2x1", via, "02b", 5000)
			n.liquidity[channelId(c)] = 0
		}
		return n, r
	}
	n, r := build()
	r.Soft = NewSoftAvoid([]string{"02a"}, 2)
	res := r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, 3, len(n.paid), "two failures around 02a, then through it")
	assert.Contains(t, res.Channels, id("3x1x1", "02a", "02b"))
	rep := r.Soft.Report(nil, nil)
	assert.Equal(t, "after 2 failed payments in a row", rep.Lifted)
	assert.Equal(t, 0, rep.Fallbacks, "let back by the streak, not for want of a route")

	// probes count as payments do
	n, r = build()
	r.ProbeOnly = true
	r.sendProbe = n.probe
	r.Soft = NewSoftAvoid([]string{"02a"}, 2)
	res = r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, "after 2 failed payments in a row", r.Soft.Report(nil, nil).Lifted)
}

// A quote and a measure search as a run does.
func TestTheQuoteSoftAvoids(t *testing.T) {
	_, r := softNet()
	r.Soft = NewSoftAvoid([]string{"02a"}, 0)
	route, err := r.Quote()
	assert.NoError(t, err)
	assert.Contains(t, route.ChannelIds(), id("7x1x1", outP, "02ok"))
	_, r = softNet()
	r.Soft = NewSoftAvoid([]string{"02a", "02ok"}, 0)
	route, err = r.Quote()
	assert.NoError(t, err)
	assert.Contains(t, route.ChannelIds(), id("3x1x1", "02a", "02b"))
	assert.True(t, r.SoftFellBack)

	n, r := softNet()
	r.sendProbe = n.probe
	r.Soft = NewSoftAvoid([]string{"02a"}, 0)
	m := r.Measure(r.Amount, 3, 0, nil)
	if assert.NotNil(t, m.Route) {
		assert.Contains(t, m.Route.ChannelIds(), id("7x1x1", outP, "02ok"))
	}

	// what a measure's probes find failing lets no node back: a quote shows how a run starts
	n, r = softNet()
	n.liquidity[id("8x1x1", "02ok", "02b")] = 0
	r.sendProbe = n.probe
	r.Soft = NewSoftAvoid([]string{"02a"}, 1)
	r.Measure(r.Amount, 3, 0, nil)
	assert.Equal(t, 2, len(n.paid), "a failure in the middle, then through 02a (no other way left)")
	assert.Equal(t, []string{"02a"}, r.Soft.Active())
	assert.False(t, r.measuring)
	n2, r2 := softNet() // a split of a run, with the same soft avoid: its failures count
	n2.liquidity[id("8x1x1", "02ok", "02b")] = 0
	r2.Soft = r.Soft
	r2.Run()
	assert.Nil(t, r2.Soft.Active())
}

// Only failures in the middle of a route count towards letting them back:
// not our own channels or node, the in-peer's, nor errors before sending.
func TestOnlyRouteFailuresCount(t *testing.T) {
	fail := func(data *glightning.PaymentErrorData) func(*graph.Route) (*graph.PrettyRoute, error) {
		return func(route *graph.Route) (*graph.PrettyRoute, error) {
			if data == nil {
				return nil, errors.New("unable to create invoice")
			}
			return nil, &glightning.PaymentError{Data: data}
		}
	}
	dirOf := func(scid, from, to string) int {
		if util.GetDirection(from, to) == "1" {
			return 1
		}
		return 0
	}
	var r *Rebalance
	for name, pay := range map[string]func(*graph.Route) (*graph.PrettyRoute, error){
		"local error": fail(nil),
		"our out channel": fail(&glightning.PaymentErrorData{ErringNode: self, ErringIndex: 0,
			FailCode: failTemporaryChannelFailure, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}),
		"the out-peer": fail(&glightning.PaymentErrorData{ErringNode: outP, ErringIndex: 1, ErringChannel: "7x1x1",
			ErringDirection: dirOf("7x1x1", outP, "02ok"), FailCode: failTemporaryChannelFailure,
			FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}),
		"the in channel": fail(&glightning.PaymentErrorData{ErringNode: inP, ErringIndex: 4, ErringChannel: "5x1x1",
			ErringDirection: dirOf("5x1x1", inP, self), FailCode: failTemporaryChannelFailure,
			FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}),
	} {
		_, r = softNet()
		r.Attempts = 3
		r.Soft = NewSoftAvoid([]string{"02a"}, 1)
		r.pay = pay
		r.Run()
		assert.Equal(t, []string{"02a"}, r.Soft.Active(), name)
	}
	// one in the middle does
	n, r := softNet()
	n.liquidity[id("8x1x1", "02ok", "02b")] = 0
	r.Soft = NewSoftAvoid([]string{"02a"}, 1)
	r.Run()
	assert.Nil(t, r.Soft.Active())
}

// Once a split went through them for want of another route, its later
// searches do too, without first searching in vain without them.
func TestASplitThatFellBackStaysThrough(t *testing.T) {
	n, r := softNet()
	n.liquidity[id("3x1x1", "02a", "02b")] = 0
	r.Soft = NewSoftAvoid([]string{"02a", "02ok"}, -1)
	res := r.Run()
	assert.Equal(t, "success", res.Status)
	assert.Equal(t, 2, len(n.paid), "via 02a (short), then via 02ok")
	assert.Equal(t, 1, r.Soft.Report(nil, nil).Fallbacks, "one split, one fallback")
}

// A payment that goes through ends the streak, for every split of the run.
func TestASuccessEndsTheStreak(t *testing.T) {
	for _, probe := range []bool{false, true} {
		soft := NewSoftAvoid([]string{"02a"}, 2)
		for i := 0; i < 3; i++ { // each split: one failure in the middle, then through
			n, r := softNet()
			n.liquidity[id("8x1x1", "02ok", "02b")] = 0
			r.Soft = soft
			if probe {
				r.ProbeOnly, r.sendProbe = true, n.probe
			}
			res := r.Run()
			assert.Equal(t, "success", res.Status)
			assert.Equal(t, 2, len(n.paid))
		}
		assert.Equal(t, []string{"02a"}, soft.Active(), "never 2 in a row: probe-only %v", probe)
	}
}
