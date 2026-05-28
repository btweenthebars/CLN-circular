package node

import (
	"fmt"
	"github.com/elementsproject/glightning/jrpc2"
	"time"
)

type ActivePaymentsCmd struct {
	ActivePayments []*ActivePayment `json:"active_payments"`
}

func (a *ActivePaymentsCmd) Name() string {
	return "circular-active"
}

func (a *ActivePaymentsCmd) New() interface{} {
	return &ActivePaymentsCmd{}
}

func (a *ActivePaymentsCmd) Call() (jrpc2.Result, error) {
	n := GetNode()
	n.activePaymentsLock.RLock()
	activePayments := make([]*ActivePayment, 0, len(n.ActivePayments))
	for _, p := range n.ActivePayments {
		activePayments = append(activePayments, p)
	}
	n.activePaymentsLock.RUnlock()

	return &ActivePaymentsCmd{
		ActivePayments: activePayments,
	}, nil
}

func (a *ActivePaymentsCmd) String() string {
	var result string
	result += fmt.Sprintf("Active payments: %d\n", len(a.ActivePayments))
	if len(a.ActivePayments) > 0 {
		result += "\nActive Payments Details:\n"
		for _, p := range a.ActivePayments {
			elapsed := time.Since(p.CreatedAt).Round(time.Second)
			result += fmt.Sprintf("- Hash: %s\n  Amount: %d sats\n  Elapsed: %s\n", p.PaymentHash, p.AmountMsat/1000, elapsed)
			if p.Route != nil && len(p.Route.Hops) > 0 {
				result += "  Route: "
				for idx, hop := range p.Route.Hops {
					if idx > 0 {
						result += " -> "
					}
					result += hop.Alias
				}
				result += "\n"
			}
		}
	}
	return result
}
