package node

import (
	"bufio"
	"circular/graph"
	"circular/util"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
)

// fakeLightningd answers lightningd's JSON-RPC on a unix socket: answer gives
// each method's result (or error) and records the calls.
type fakeLightningd struct {
	mu     sync.Mutex
	calls  []map[string]interface{}
	answer func(method string, params map[string]interface{}) (result interface{}, rpcErr map[string]interface{})
}

func (f *fakeLightningd) serve(t *testing.T, l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			dec := json.NewDecoder(bufio.NewReader(conn))
			for {
				var req map[string]interface{}
				if dec.Decode(&req) != nil {
					return
				}
				params, _ := req["params"].(map[string]interface{})
				f.mu.Lock()
				f.calls = append(f.calls, map[string]interface{}{"method": req["method"], "params": params})
				f.mu.Unlock()
				result, rpcErr := f.answer(req["method"].(string), params)
				resp := map[string]interface{}{"jsonrpc": "2.0", "id": req["id"]}
				if rpcErr != nil {
					resp["error"] = rpcErr
				} else {
					resp["result"] = result
				}
				b, _ := json.Marshal(resp)
				conn.Write(b)
			}
		}()
	}
}

func (f *fakeLightningd) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for _, c := range f.calls {
		out = append(out, c["method"].(string))
	}
	return out
}

func (f *fakeLightningd) params(method string) map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c["method"] == method {
			return c["params"].(map[string]interface{})
		}
	}
	return nil
}

func probeNode(t *testing.T, f *fakeLightningd) (*Node, *graph.Route) {
	dir, err := os.MkdirTemp("", "ln")
	assert.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	l, err := net.Listen("unix", filepath.Join(dir, "lightning-rpc"))
	assert.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	go f.serve(t, l)
	ln := glightning.NewLightning()
	assert.NoError(t, ln.StartUp("lightning-rpc", dir))
	n := &Node{lightning: ln, Id: "02self", ActivePayments: map[string]*ActivePayment{}, activePaymentsLock: &sync.RWMutex{}}

	g := graph.NewGraph()
	ch := func(scid, from, to string) *graph.Channel {
		c := graph.NewChannel(&glightning.Channel{Source: from, Destination: to, ShortChannelId: scid, IsActive: true,
			AmountMsat: glightning.AmountFromMSat(10000000000), Delay: 10,
			HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000000)}, 5000000000, 0)
		g.AddChannel(c)
		g.Channels[scid+"/"+util.GetDirection(from, to)] = c
		return c
	}
	ch("1x1x1", "02self", "02a")
	ch("2x1x1", "02a", "02b")
	ch("3x1x1", "02b", "02self")
	route, err := g.GetRouteOverChannels([]string{"1x1x1/" + util.GetDirection("02self", "02a"),
		"2x1x1/" + util.GetDirection("02a", "02b"), "3x1x1/" + util.GetDirection("02b", "02self")}, 100000000)
	assert.NoError(t, err)
	return n, route
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// A probe that arrives: our node fails it, knowing no such payment; its
// record is deleted.
func TestSendProbe(t *testing.T) {
	f := &fakeLightningd{answer: func(method string, p map[string]interface{}) (interface{}, map[string]interface{}) {
		if method == "waitsendpay" {
			return nil, map[string]interface{}{"code": 203, "message": "failed: WIRE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS",
				"data": map[string]interface{}{"erring_index": 3, "failcode": 16399, "erring_node": "02self",
					"failcodename": "WIRE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS"}}
		}
		return map[string]interface{}{}, nil
	}}
	n, route := probeNode(t, f)
	err := n.SendProbe(route)
	var paymentError *glightning.PaymentError
	if assert.True(t, errors.As(err, &paymentError), err) {
		assert.Equal(t, uint64(3), paymentError.Data.ErringIndex)
		assert.Equal(t, "02self", paymentError.Data.ErringNode)
	}
	assert.Equal(t, []string{"sendpay", "waitsendpay", "delpay"}, f.methods())
	sp := f.params("sendpay")
	hash := sp["payment_hash"].(string)
	assert.Regexp(t, hex64, hash)
	assert.Regexp(t, hex64, sp["payment_secret"])
	assert.NotEqual(t, hash, sp["payment_secret"])
	assert.Equal(t, PROBE_LABEL, sp["label"])
	assert.Equal(t, float64(100000000), sp["amount_msat"])
	assert.Equal(t, 3, len(sp["route"].([]interface{})))
	assert.Equal(t, map[string]interface{}{"payment_hash": hash, "status": "failed"}, f.params("delpay"))
	assert.Equal(t, hash, f.params("waitsendpay")["payment_hash"])
	assert.Equal(t, float64(PROBE_TIMEOUT), f.params("waitsendpay")["timeout"])
	assert.Empty(t, n.ActivePayments, "listed in circular-active only while in flight")

	// two probes: two hashes
	n.SendProbe(route)
	f.mu.Lock()
	second := f.calls[3]["params"].(map[string]interface{})["payment_hash"]
	f.mu.Unlock()
	assert.NotEqual(t, hash, second)
}

func TestSendProbeOutcomes(t *testing.T) {
	answer := map[string]interface{}{}
	var waitErr map[string]interface{}
	var sendErr map[string]interface{}
	f := &fakeLightningd{answer: func(method string, p map[string]interface{}) (interface{}, map[string]interface{}) {
		switch method {
		case "waitsendpay":
			if waitErr != nil {
				return nil, waitErr
			}
			return answer, nil
		case "sendpay":
			if sendErr != nil {
				return nil, sendErr
			}
		}
		return map[string]interface{}{}, nil
	}}
	n, route := probeNode(t, f)

	// still in flight after its time: its record cannot be deleted yet
	waitErr = map[string]interface{}{"code": 200, "message": "Timed out while waiting"}
	err := n.SendProbe(route)
	assert.Equal(t, util.ErrSendPayTimeout.Error(), err.Error())
	assert.Equal(t, []string{"sendpay", "waitsendpay"}, f.methods())

	// settled: cannot happen
	waitErr = nil
	assert.Equal(t, ErrProbeSettled, n.SendProbe(route))

	// refused at once, at our own channel
	f.calls = nil
	sendErr = map[string]interface{}{"code": 204, "message": "No connection to first peer found",
		"data": map[string]interface{}{"erring_index": 0, "failcode": 16394, "erring_node": "02self",
			"erring_channel": "1x1x1", "failcodename": "WIRE_UNKNOWN_NEXT_PEER"}}
	err = n.SendProbe(route)
	var paymentError *glightning.PaymentError
	assert.True(t, errors.As(err, &paymentError))
	assert.Equal(t, []string{"sendpay", "delpay"}, f.methods())
}

func TestDelpayRequest(t *testing.T) {
	params := jrpc2.GetNamedParams(&delpayRequest{PaymentHash: "h", Status: "failed"})
	assert.Equal(t, "delpay", (&delpayRequest{}).Name())
	assert.Equal(t, map[string]interface{}{"payment_hash": "h", "status": "failed"}, params)
}
