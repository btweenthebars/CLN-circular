package node

import (
	"circular/util"
	"crypto/rand"
	"encoding/hex"
	"github.com/elementsproject/glightning/glightning"
	"strings"
)

const (
	INVOICE_LABEL_PREFIX = "circular-"
	INVOICE_DESCRIPTION  = "circular rebalance"
	// The invoice only needs to outlive waitsendpay. An HTLC that reaches us
	// after it has expired or been deleted is rejected by lightningd.
	INVOICE_EXPIRY = SENDPAY_TIMEOUT + 60 // seconds
)

// RebalanceInvoice is the short-lived invoice that a rebalance pays to our own
// node. Because the payment completes against a real invoice, lightningd checks
// the incoming HTLC itself (payment secret, amount, CLTV, final hop) before it
// releases the preimage, and the preimage never leaves lightningd.
type RebalanceInvoice struct {
	Label         string
	PaymentHash   string
	PaymentSecret string
	AmountMsat    uint64
}

// invoiceRequest is sent with our own parameter struct because glightning's
// Invoice result has no payment_secret field.
type invoiceRequest struct {
	AmountMsat  uint64 `json:"amount_msat"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Expiry      uint32 `json:"expiry,omitempty"`
}

func (r *invoiceRequest) Name() string {
	return "invoice"
}

type invoiceResponse struct {
	PaymentHash   string `json:"payment_hash"`
	PaymentSecret string `json:"payment_secret"`
}

func newInvoiceLabel() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return INVOICE_LABEL_PREFIX + hex.EncodeToString(b), nil
}

// CreateRebalanceInvoice creates the invoice for one payment attempt that
// delivers amountMsat to our node, and records its payment hash so that the
// sendpay notifications can recognise the payment as ours.
func (n *Node) CreateRebalanceInvoice(amountMsat uint64) (*RebalanceInvoice, error) {
	label, err := newInvoiceLabel()
	if err != nil {
		return nil, err
	}

	var resp invoiceResponse
	err = n.lightning.Request(&invoiceRequest{
		AmountMsat:  amountMsat,
		Label:       label,
		Description: INVOICE_DESCRIPTION,
		Expiry:      INVOICE_EXPIRY,
	}, &resp)
	if err != nil {
		return nil, err
	}

	if resp.PaymentHash == "" || resp.PaymentSecret == "" {
		n.deleteInvoice(label, "unpaid")
		return nil, util.ErrNoPaymentSecret
	}

	if err := n.DB.Set(resp.PaymentHash, []byte(label)); err != nil {
		n.deleteInvoice(label, "unpaid")
		return nil, err
	}

	return &RebalanceInvoice{
		Label:         label,
		PaymentHash:   resp.PaymentHash,
		PaymentSecret: resp.PaymentSecret,
		AmountMsat:    amountMsat,
	}, nil
}

// DeleteRebalanceInvoice deletes the invoice if its status is still `status`.
// An invoice that expired in the meantime is deleted too.
func (n *Node) DeleteRebalanceInvoice(label, status string) error {
	_, err := n.lightning.DeleteInvoice(label, status)
	if err != nil && status != "expired" && strings.Contains(err.Error(), "status is expired") {
		_, err = n.lightning.DeleteInvoice(label, "expired")
	}
	return err
}

// deleteInvoice is DeleteRebalanceInvoice for callers that are done with the
// payment either way and only need the failure logged.
func (n *Node) deleteInvoice(label, status string) {
	if err := n.DeleteRebalanceInvoice(label, status); err != nil {
		n.Logf(glightning.Debug, "unable to delete invoice %s (%s): %v", label, status, err)
	}
}
