package node

import (
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestInvoiceRequestParams(t *testing.T) {
	params := jrpc2.GetNamedParams(&invoiceRequest{
		AmountMsat:  200000000,
		Label:       "circular-test",
		Description: INVOICE_DESCRIPTION,
		Expiry:      INVOICE_EXPIRY,
	})

	assert.Equal(t, "invoice", (&invoiceRequest{}).Name())
	assert.Len(t, params, 4)
	assert.Equal(t, uint64(200000000), params["amount_msat"])
	assert.Equal(t, "circular-test", params["label"])
	assert.Equal(t, INVOICE_DESCRIPTION, params["description"])
	assert.Equal(t, uint32(INVOICE_EXPIRY), params["expiry"])
}

func TestNewInvoiceLabelIsUnique(t *testing.T) {
	a, err := newInvoiceLabel()
	assert.NoError(t, err)
	b, err := newInvoiceLabel()
	assert.NoError(t, err)

	assert.True(t, strings.HasPrefix(a, INVOICE_LABEL_PREFIX))
	assert.Len(t, a, len(INVOICE_LABEL_PREFIX)+32)
	assert.NotEqual(t, a, b)
}

// The payment secret must reach lightningd, or the final hop rejects our own payment.
func TestSendPayRequestCarriesPaymentSecret(t *testing.T) {
	params := jrpc2.GetNamedParams(&glightning.SendPayRequest{
		PaymentHash:   "hash",
		Label:         "circular-test",
		MilliSatoshis: 200000000,
		PaymentSecret: "secret",
	})

	assert.Equal(t, "secret", params["payment_secret"])
	assert.Equal(t, uint64(200000000), params["amount_msat"])
	assert.Equal(t, "hash", params["payment_hash"])
}
