package main

import (
	"circular/graph"
	"circular/node"
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

	if err := p.RegisterNewIntOption("circular-risk-ppm",
		"How much a route's chance of success weighs against its fees: each unit of -ln(chance) of a channel costs this many ppm of its amount (0: choose by fees and hops alone)",
		node.DEFAULT_RISK_PPM); err != nil {

		log.Fatalln("error registering option circular-risk-ppm:", err)
	}

	if err := p.RegisterNewBoolOption("circular-save-stats",
		"Whether circular should save stats in the database",
		true); err != nil {

		log.Fatalln("error registering option circular-liquidity-reset:", err)
	}
}
