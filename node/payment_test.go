package node

import (
	"errors"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Every sendpay error used to be reported as "first peer not ready".
func TestSendPayError(t *testing.T) {
	// lightningd failed the HTLC at our own channel
	local := &jrpc2.RpcError{
		Code:    204,
		Message: "No connection to first peer found",
		Data: []byte(`{"erring_index":0,"failcode":16394,"failcodename":"WIRE_UNKNOWN_NEXT_PEER",` +
			`"erring_node":"02self","erring_channel":"1x1x1","erring_direction":0}`),
	}
	var paymentError *glightning.PaymentError
	if assert.True(t, errors.As(sendPayError(local), &paymentError)) {
		assert.Equal(t, 0x400a, paymentError.Data.FailCode)
		assert.Equal(t, "02self", paymentError.Data.ErringNode)
		assert.Equal(t, "1x1x1", paymentError.Data.ErringChannel)
	}

	// anything else keeps its own message
	invalid := &jrpc2.RpcError{Code: -32602, Message: "Invalid payment_secret"}
	err := sendPayError(invalid)
	assert.False(t, errors.As(err, &paymentError))
	assert.True(t, errors.Is(err, invalid))
	assert.Contains(t, err.Error(), "Invalid payment_secret")

	noData := &jrpc2.RpcError{Code: 204, Message: "Try another route"}
	assert.False(t, errors.As(sendPayError(noData), &paymentError))
	assert.Contains(t, sendPayError(errors.New("connection reset")).Error(), "connection reset")
}
