package node

import (
	"circular/graph"
	"circular/util"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/elementsproject/glightning/glightning"
)

const (
	// PROBE_TIMEOUT is how long a probe is waited for: one that has not failed
	// by then tells nothing (its HTLC still fails in the end: nobody can settle it)
	PROBE_TIMEOUT = 60 // seconds
	PROBE_LABEL   = "circular-probe"
)

// ErrProbeSettled would mean someone settled a probe, which needs a preimage
// that was never made: it cannot happen.
var ErrProbeSettled = errors.New("a probe was settled: this cannot happen")

func random32() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// SendProbe sends route a payment that nobody can settle: a random payment
// hash, with no invoice behind it and no preimage anywhere. It can only fail:
// at our own node, which knows no such payment, once every channel along the
// route carried its hop's amount; or where a channel could not. Nothing is paid
// either way. Its record is then deleted from lightningd's payments (a probe
// still pending after PROBE_TIMEOUT keeps it). -> the failure: a
// *glightning.PaymentError for the caller to read, util.ErrSendPayTimeout,
// or another error.
func (n *Node) SendProbe(route *graph.Route) error {
	defer util.TimeTrack(time.Now(), "node.SendProbe", n.Logf)
	hash, err := random32()
	if err != nil {
		return err
	}
	secret, err := random32()
	if err != nil {
		return err
	}
	n.AddActivePayment(hash, graph.NewPrettyRoute(route, hash), route.Amount)
	defer n.RemoveActivePayment(hash)

	if _, err := n.lightning.SendPay(route.ToLightningRoute(), hash, PROBE_LABEL, route.Amount, "", secret, 0); err != nil {
		n.deletePayment(hash)
		return sendPayError(err)
	}
	_, err = n.lightning.WaitSendPay(hash, PROBE_TIMEOUT)
	if err == nil {
		n.Logln(glightning.Unusual, "probe ", hash, " was settled")
		return ErrProbeSettled
	}
	if err.Error() != util.ErrSendPayTimeout.Error() {
		n.deletePayment(hash)
	}
	return err
}

// delpayRequest is lightningd's delpay, which glightning does not have.
type delpayRequest struct {
	PaymentHash string `json:"payment_hash"`
	Status      string `json:"status"`
}

func (r *delpayRequest) Name() string {
	return "delpay"
}

// deletePayment deletes a failed probe's record, so that probes do not fill
// listsendpays; a failure to is only logged.
func (n *Node) deletePayment(hash string) {
	var resp map[string]interface{}
	if err := n.lightning.Request(&delpayRequest{PaymentHash: hash, Status: "failed"}, &resp); err != nil {
		n.Logf(glightning.Debug, "unable to delete probe %s: %v", hash, err)
	}
}
