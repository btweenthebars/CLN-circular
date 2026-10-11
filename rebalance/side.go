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

// sideError is an error on one of our channels.
type sideError struct {
	side Side
	// transient: the channel refused for the moment (HTLC slots, value in
	// flight, a policy about to be refreshed); a later try may work
	transient bool
	err       error
}

func (e *sideError) Error() string { return e.err.Error() }
func (e *sideError) Unwrap() error { return e.err }

func onOut(err error, transient bool) error {
	return &sideError{side: SideOut, transient: transient, err: err}
}

func onIn(err error, transient bool) error {
	return &sideError{side: SideIn, transient: transient, err: err}
}

// failureSide tells where err happened, and whether it may pass.
func failureSide(err error) (Side, bool) {
	var se *sideError
	switch {
	case err == nil:
		return SideNone, false
	case errors.As(err, &se):
		return se.side, se.transient
	case errors.Is(err, util.ErrOutgoingChannelDepleted), errors.Is(err, util.ErrOutgoingChannelNotInNormalState),
		errors.Is(err, util.ErrOutgoingPeerDisconnected), errors.Is(err, util.ErrNoOutgoingChannel):
		return SideOut, false
	case errors.Is(err, util.ErrIncomingChannelDepleted), errors.Is(err, util.ErrIncomingChannelNotInNormalState),
		errors.Is(err, util.ErrIncomingPeerDisconnected), errors.Is(err, util.ErrNoIncomingChannel):
		return SideIn, false
	case errors.Is(err, util.ErrNoRoute), errors.Is(err, util.ErrNoSuchNode), errors.Is(err, util.ErrTemporaryFailure),
		errors.As(err, &util.ErrRouteTooExpensive{}):
		return SideRoute, false
	}
	return SideNone, false
}
