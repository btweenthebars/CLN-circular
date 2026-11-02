package main

import (
	"circular/node"
	"circular/rebalance"
	"circular/rebalance/parallel"
	"github.com/elementsproject/glightning/glightning"
)

func registerMethods(p *glightning.Plugin) {
	rpcRebalanceByNode := glightning.NewRpcMethod(&rebalance.RebalanceByNode{}, "Rebalance by NodeID")
	rpcRebalanceByNode.LongDesc = "Rebalance the node `innode` from the node `outnode` for amount `amount` for at most `maxppm`"
	rpcRebalanceByNode.Category = "utility"
	p.RegisterMethod(rpcRebalanceByNode)

	rpcRebalanceByScid := glightning.NewRpcMethod(&rebalance.RebalanceByScid{}, "Rebalance by Scid")
	rpcRebalanceByScid.LongDesc = "Rebalance the channel `inchannel` from the channel `outchannel` for amount `amount` for at most `maxppm`"
	rpcRebalanceByScid.Category = "utility"
	p.RegisterMethod(rpcRebalanceByScid)

	rpcRebalancePull := glightning.NewRpcMethod(&parallel.RebalancePull{}, "Pull liquidity into a channel from many sources in parallel")
	rpcRebalancePull.LongDesc = "Rebalance the channel `inscid` from many channels concurrently"
	rpcRebalancePull.Category = "utility"
	p.RegisterMethod(rpcRebalancePull)

	rpcRebalancePush := glightning.NewRpcMethod(&parallel.RebalancePush{}, "Push liquidity out of a channel to many destinations in parallel")
	rpcRebalancePush.LongDesc = "Rebalance the channel `outscid` from many channels concurrently"
	rpcRebalancePush.Category = "utility"
	p.RegisterMethod(rpcRebalancePush)

	rpcQuote := glightning.NewRpcMethod(&parallel.RebalanceQuote{}, "Quote a rebalance without paying")
	rpcQuote.LongDesc = "With the parameters of circular-pull (`inscid`), circular-push (`outscid`) or circular (both), " +
		"show for each candidate the route a run would try first, its fee and a rough chance of success, or why it would not be tried. Nothing is paid. Give the parameters by name (-k or key=value)."
	rpcQuote.Category = "utility"
	p.RegisterMethod(rpcQuote)

	rpcStats := glightning.NewRpcMethod(&node.Stats{}, "Get stats")
	rpcStats.LongDesc = "Get the stats of the usage of circular"
	rpcStats.Category = "utility"
	p.RegisterMethod(rpcStats)

	rpcActive := glightning.NewRpcMethod(&node.ActivePaymentsCmd{}, "Get active in-flight rebalances")
	rpcActive.LongDesc = "Get the list of active in-flight rebalancing payments currently being waited on"
	rpcActive.Category = "utility"
	p.RegisterMethod(rpcActive)

	rpfDeleteStats := glightning.NewRpcMethod(&node.DeleteStats{}, "Delete Stats")
	rpfDeleteStats.LongDesc = "Delete the stats of the usage of circular"
	rpfDeleteStats.Category = "utility"
	p.RegisterMethod(rpfDeleteStats)

	rpcStop := glightning.NewRpcMethod(&node.Stop{}, "Stop circular")
	rpcStop.LongDesc = "Stop future htlcs from being fired"
	rpcStop.Category = "utility"
	p.RegisterMethod(rpcStop)

	rpcNodes := glightning.NewRpcMethod(&node.NodesCmd{}, "Nodes remembered for failing rebalances' payments")
	rpcNodes.LongDesc = "The nodes that failed rebalances' payments, most first: their failure score (failed payments, fading by half every circular-node-memory hours, halved by each payment they forwarded) and what it adds to routes through them in the route search"
	rpcNodes.Category = "utility"
	p.RegisterMethod(rpcNodes)

	rpcCandidates := glightning.NewRpcMethod(&node.CandidatesCmd{}, "How our channels did as rebalance candidates")
	rpcCandidates.LongDesc = "How each of our channels did as a circular-pull (drained) or circular-push (filled) candidate, in the order runs rank them: successes, failures, the success rate the ranking orders by, what the successes moved and paid, and whether a recent failure puts it last for now. scid: only that channel; limit: how many (default 50)"
	rpcCandidates.Category = "utility"
	p.RegisterMethod(rpcCandidates)

	rpcResults := glightning.NewRpcMethod(&parallel.ResultsCmd{}, "The last circular-pull / circular-push runs and their results")
	rpcResults.LongDesc = "The runs circular knows (the running ones and the last 20 finished, in memory since the plugin started), newest first: kind, target, amount, maxppm, started, ended, status and the result each call returned - for a caller that lost its connection meanwhile. target: only that channel's"
	rpcResults.Category = "utility"
	p.RegisterMethod(rpcResults)

	rpcResume := glightning.NewRpcMethod(&node.Resume{}, "Resume circular")
	rpcResume.LongDesc = "Resume activity after a stop command. Allows htlcs to be fired"
	rpcResume.Category = "utility"
	p.RegisterMethod(rpcResume)

}
