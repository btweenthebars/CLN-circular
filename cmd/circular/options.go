package main

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/rebalance/parallel"
	"github.com/elementsproject/glightning/glightning"
	"log"
)

func registerOptions(p *glightning.Plugin) {
	if err := p.RegisterNewIntOption("circular-graph-refresh",
		"How often the gossip graph gets refreshed (minutes)",
		graph.DEFAULT_GRAPH_REFRESH_INTERVAL); err != nil {

		log.Fatalln("error registering option circular-graph-refresh:", err)
	}

	if err := p.RegisterNewIntOption("circular-peer-refresh",
		"How often the peer list gets refreshed (seconds)",
		node.DEFAULT_PEER_REFRESH_INTERVAL); err != nil {

		log.Fatalln("error registering option circular-peer-refresh:", err)
	}

	if err := p.RegisterNewIntOption("circular-liquidity-refresh",
		"The period of time after which the liquidity is reset (minutes)",
		node.DEFAULT_LIQUIDITY_RESET_INTERVAL); err != nil {

		log.Fatalln("error registering option circular-liquidity-reset:", err)
	}

	if err := p.RegisterNewIntOption("circular-liquidity-halflife",
		"How fast learned liquidity fades toward the default: half the way every this many minutes (0: no fading)",
		node.DEFAULT_LIQUIDITY_HALF_LIFE); err != nil {

		log.Fatalln("error registering option circular-liquidity-halflife:", err)
	}

	if err := p.RegisterNewIntOption("circular-node-memory",
		"How long nodes that failed rebalances' payments are remembered: their failure score halves every this many hours, and with circular-risk-ppm routes through them cost more (0: off)",
		node.DEFAULT_NODE_MEMORY); err != nil {

		log.Fatalln("error registering option circular-node-memory:", err)
	}

	if err := p.RegisterNewIntOption("circular-risk-ppm",
		"How much a route's chance of success weighs against its fees: each unit of -ln(chance) of a channel costs this many ppm of its amount (0: choose by fees and hops alone)",
		node.DEFAULT_RISK_PPM); err != nil {

		log.Fatalln("error registering option circular-risk-ppm:", err)
	}

	for _, o := range []struct {
		name, desc string
		value      int
	}{
		{"circular-attempts", "Payments a rebalance (or split) may make, when a call leaves attempts out", rebalance.DEFAULT_ATTEMPTS},
		{"circular-max-hops", "Channels a route may have, when a call leaves maxhops out", rebalance.DEFAULT_MAXHOPS},
		{"circular-max-node-fails", "Payments a node may fail for lack of liquidity before a rebalance routes around it, when a call leaves maxnodefails out", rebalance.DEFAULT_MAX_NODE_FAILS},
		{"circular-max-fail-streak", "Failed payments in a row after which circular-pull/push stop, when a call leaves maxfailstreak out (below 0: no limit)", parallel.DEFAULT_MAX_FAIL_STREAK},
		{"circular-max-time", "Seconds after which circular-pull/push stop, when a call leaves maxtime out (0: no limit)", 0},
		{"circular-max-fee", "Sats circular-pull/push may pay in fees per run, when a call leaves maxfee out (0: no budget)", 0},
		{"circular-earn-days", "Days of forwards that count for what a channel's liquidity earns, with feeratio", node.DEFAULT_EARN_DAYS},
	} {
		if err := p.RegisterNewIntOption(o.name, o.desc, o.value); err != nil {
			log.Fatalln("error registering option "+o.name+":", err)
		}
	}

	if err := p.RegisterNewOption("circular-fee-ratio",
		"When a call leaves feeratio out: circular-pull/push pay at most this share (e.g. 0.8) of what the filled channel earns, less our fee on the drained one (0: off)",
		"0"); err != nil {

		log.Fatalln("error registering option circular-fee-ratio:", err)
	}

	if err := p.RegisterNewBoolOption("circular-save-stats",
		"Whether circular should save stats in the database",
		true); err != nil {

		log.Fatalln("error registering option circular-liquidity-reset:", err)
	}
}
