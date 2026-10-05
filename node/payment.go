package node

import (
	"circular/graph"
	"circular/util"
	"github.com/dgraph-io/badger/v4"
	"github.com/elementsproject/glightning/glightning"
	"strings"
	"time"
)

const (
	FAILURE_PREFIX  = "f_"
	SUCCESS_PREFIX  = "s_"
	ROUTE_PREFIX    = "r_"
	TIMEOUT_PREFIX  = "timeout_"
	SENDPAY_TIMEOUT = 120 // 2 minutes
)

type ActivePayment struct {
	PaymentHash string             `json:"payment_hash"`
	Route       *graph.PrettyRoute `json:"route"`
	AmountMsat  uint64             `json:"amount_msat"`
	CreatedAt   time.Time          `json:"created_at"`
}

func (n *Node) AddActivePayment(paymentHash string, route *graph.PrettyRoute, amount uint64) {
	n.activePaymentsLock.Lock()
	defer n.activePaymentsLock.Unlock()
	n.ActivePayments[paymentHash] = &ActivePayment{
		PaymentHash: paymentHash,
		Route:       route,
		AmountMsat:  amount,
		CreatedAt:   time.Now(),
	}
}

func (n *Node) RemoveActivePayment(paymentHash string) {
	n.activePaymentsLock.Lock()
	defer n.activePaymentsLock.Unlock()
	delete(n.ActivePayments, paymentHash)
}

// SendPay pays `invoice` along `route`. The invoice's payment secret travels in
// the final hop's onion, so lightningd only settles the HTLC that we sent, and
// only if it carries the full amount.
func (n *Node) SendPay(route *graph.Route, invoice *RebalanceInvoice) (*glightning.SendPayFields, error) {
	defer util.TimeTrack(time.Now(), "node.SendPay", n.Logf)
	finalRoute := route.ToLightningRoute()
	paymentHash := invoice.PaymentHash

	prettyRoute := graph.NewPrettyRoute(route, paymentHash)
	n.AddActivePayment(paymentHash, prettyRoute, route.Amount)
	defer n.RemoveActivePayment(paymentHash)

	n.Logln(glightning.Debug, "sending payment")
	if _, err := n.lightning.SendPay(finalRoute, paymentHash, invoice.Label, route.Amount, "", invoice.PaymentSecret, 0); err != nil {
		n.Logln(glightning.Unusual, err)
		n.deleteInvoice(invoice.Label, "unpaid")
		return nil, util.ErrFirstPeerNotReady
	}

	n.Logln(glightning.Debug, "waiting for payment to be confirmed")
	result, err := n.lightning.WaitSendPay(paymentHash, SENDPAY_TIMEOUT)

	if err != nil {
		n.Logf(glightning.Debug, "%+v", err)
		n.Logln(glightning.Debug, "err.Error(): ", err.Error())

		// in case of timeout, there's some work to do
		if err.Error() == util.ErrSendPayTimeout.Error() {
			return n.manageTimeout(invoice)
		}

		// the payment has failed for good, so the invoice must never be paid
		n.deleteInvoice(invoice.Label, "unpaid")

		// the caller learns from the failure (*glightning.PaymentError) before retrying
		return nil, err
	}

	n.deleteInvoice(invoice.Label, "paid")
	return result, nil
}

func (n *Node) manageTimeout(invoice *RebalanceInvoice) (*glightning.SendPayFields, error) {
	paymentHash := invoice.PaymentHash

	// delete the invoice. In this way lightningd fails the HTLC if it reaches us later
	n.Logln(glightning.Debug, "payment timed out, deleting invoice ", invoice.Label)
	if err := n.DeleteRebalanceInvoice(invoice.Label, "unpaid"); err != nil {
		if strings.Contains(err.Error(), "status is paid") {
			// the HTLC reached us and was settled before we could delete the invoice:
			// the payment is complete, only the settlement upstream was slow
			n.Logln(glightning.Info, "payment timed out but its invoice is paid, treating it as successful")
			n.deleteInvoice(invoice.Label, "paid")
			return nil, nil
		}
		n.Logln(glightning.Unusual, "unable to delete invoice ", invoice.Label, ": ", err)
	}

	// replace the payment hash entry with a timeout marker, so that the later
	// sendpay_failure notification is still recognised as ours
	if err := n.DB.Delete(paymentHash); err != nil {
		n.Logln(glightning.Unusual, err)
	}

	// save the failure in the DB. This will be used to update the liquidity
	n.Logln(glightning.Debug, "saving payment timeout to database")
	if err := n.DB.Set(TIMEOUT_PREFIX+paymentHash, []byte("timeout")); err != nil {
		n.Logln(glightning.Unusual, err)
	}

	return nil, util.ErrSendPayTimeout
}

func (n *Node) deleteIfOurs(paymentHash string) error {
	key := paymentHash
	_, err := n.DB.Get(key)

	// check if this payment was made by us
	if err == badger.ErrKeyNotFound {
		// check if the payment timed out
		key = TIMEOUT_PREFIX + paymentHash
		_, err = n.DB.Get(key)
		if err == badger.ErrKeyNotFound {
			return err // this payment was not made by us
		}
	}

	err = n.DB.Delete(key)
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return err
	}

	return nil
}

func (n *Node) OnPaymentFailure(sf *glightning.SendPayFailure) {
	if err := n.deleteIfOurs(sf.Data.PaymentHash); err != nil {
		return // this payment was not made by us
	}

	// save to db
	if err := n.SaveToDb(FAILURE_PREFIX+sf.Data.PaymentHash, sf); err != nil {
		n.Logln(glightning.Unusual, err)
	}

	// The graph learns from the failure synchronously, in the rebalance that sent
	// the payment, which knows the amount on every hop of the route.
	n.Logf(glightning.Debug, "code: %d, failcode: %d, failcodename: %s", sf.Code, sf.Data.FailCode, sf.Data.FailCodeName)
}

func (n *Node) OnPaymentSuccess(ss *glightning.SendPaySuccess) {
	if err := n.deleteIfOurs(ss.PaymentHash); err != nil {
		return // this payment was not made by us
	}

	// save to db
	if err := n.SaveToDb(SUCCESS_PREFIX+ss.PaymentHash, ss); err != nil {
		n.Logln(glightning.Unusual, err)
	}
}
