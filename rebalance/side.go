package rebalance

import (
	"circular/util"
	"errors"
)

// Side tells where a rebalance failed, so that the parallel commands blame
// the right channel.
type Side int

const (
	// SideNone: it succeeded, or failed for a reason on neither of our
	// channels, such as circular-stop, a timeout or an RPC error.
	SideNone Side = iota
	// SideRoute: no route within the limits, or the routes between our two
	// peers failed.
	SideRoute
	// SideOut: our out channel, or the peer at its other end.
	SideOut
	// SideIn: our in channel, or the peer at its other end.
	SideIn
)

// Retry tells whether a failure on one of our channels may pass.
type Retry int

const (
	// RetryNo: it will fail the same way.
	RetryNo Retry = iota
	// RetryWhenFree: the channel was busy (HTLC slots, value or balance
	// taken by HTLCs in flight); a try may pass once some of them settle.
	RetryWhenFree
	// RetryNow: our copy of the channel's policy was out of date and has been
	// read again; another try may pass at once.
	RetryNow
)

// sideError is an error on one of our channels.
type sideError struct {
	side  Side
	retry Retry
	err   error
}

func (e *sideError) Error() string { return e.err.Error() }
func (e *sideError) Unwrap() error { return e.err }

func onOut(err error, retry Retry) error {
	return &sideError{side: SideOut, retry: retry, err: err}
}

func onIn(err error, retry Retry) error {
	return &sideError{side: SideIn, retry: retry, err: err}
}

// failureSide tells where err happened, and whether it may pass.
func failureSide(err error) (Side, Retry) {
	var se *sideError
	switch {
	case err == nil:
		return SideNone, RetryNo
	case errors.As(err, &se):
		return se.side, se.retry
	// spendable and receivable leave out HTLCs in flight: the balance may
	// come back once they settle
	case errors.Is(err, util.ErrOutgoingChannelDepleted):
		return SideOut, RetryWhenFree
	case errors.Is(err, util.ErrIncomingChannelDepleted):
		return SideIn, RetryWhenFree
	case errors.Is(err, util.ErrOutgoingChannelNotInNormalState), errors.Is(err, util.ErrOutgoingPeerDisconnected),
		errors.Is(err, util.ErrNoOutgoingChannel):
		return SideOut, RetryNo
	case errors.Is(err, util.ErrIncomingChannelNotInNormalState), errors.Is(err, util.ErrIncomingPeerDisconnected),
		errors.Is(err, util.ErrNoIncomingChannel):
		return SideIn, RetryNo
	case errors.Is(err, util.ErrNoRoute), errors.Is(err, util.ErrNoSuchNode), errors.Is(err, util.ErrTemporaryFailure),
		errors.As(err, &util.ErrRouteTooExpensive{}):
		return SideRoute, RetryNo
	}
	return SideNone, RetryNo
}

// isPrecheck reports whether err is our copy of one of our channels' balance
// being too low for the payment, found before paying, rather than a refusal
// by lightningd or a peer.
func isPrecheck(err error) bool {
	var se *sideError
	if err == nil || errors.As(err, &se) {
		return false
	}
	return errors.Is(err, util.ErrOutgoingChannelDepleted) || errors.Is(err, util.ErrIncomingChannelDepleted)
}
