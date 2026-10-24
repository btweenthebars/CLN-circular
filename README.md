# circular [![Go](https://img.shields.io/badge/go-1.18%2B-blue.svg)](https://golang.org) ![License](https://img.shields.io/badge/license-MIT-green.svg)

`circular` is a high-performance Core Lightning plugin designed for routing nodes to rebalance channels efficiently and cost-effectively.

It features a custom edge-based backwards Dijkstra pathfinding algorithm that accounts for channel liquidity, bLIP-18 inbound fee policies (discounts and surcharges), and feedback from failed payment attempts.

## Features
* **bLIP-18 Inbound Fee Aware**: Parses gossip TLV type 55555 for inbound fees, computing exact net fees (`max(0, outbound + inbound)`) to discover routes with negative fee discounts.
* **Success-Aware Routes**: Weighs each route's fees against its chance of success, from what circular knows of each channel's liquidity, so it prefers routes likely to carry the payment over slightly cheaper ones that are not (`circular-risk-ppm`).
* **Failure-Informed Pathfinding**: Automatically excludes failing channels across retry attempts, bypassing bottlenecks without getting stuck on dead routes.
* **Parallel Multi-Channel Rebalancing**: Simultaneously pull into or push out of target channels across multiple peers (`circular-pull` and `circular-push`).
* **In-Flight Payment Tracking**: Real-time visibility into active parallel rebalancing operations via `circular-active`.
* **Fee-Aware Selection**: Prioritizes lowest-cost outgoing routes rather than blindly picking the largest balance channel.
* **Safe Self-Payments**: Each attempt pays a short-lived invoice (label `circular-…`) that circular deletes once the attempt ends. lightningd checks the incoming HTLC's payment secret, amount and CLTV before it settles, so circular needs no `htlc_accepted` hook.

## Security
Earlier versions settled rebalances from an `htlc_accepted` hook that released the preimage for any HTLC carrying a known payment hash, including forwarded HTLCs and HTLCs paying less than the rebalance amount. A node on a rebalance route could use that to obtain the preimage and keep the payment. If you run an older build, update before rebalancing again.

## Endpoints
* `circular-pull`: Pull liquidity into a channel using multiple peer channels as sources in parallel
* `circular-push`: Push liquidity out of a channel using multiple peer channels as destinations in parallel
* `circular`: Rebalance between two specific channels by Short Channel ID (`inscid` / `outscid`)
* `circular-node`: Rebalance between two specific peers by Node ID (`innode` / `outnode`)
* `circular-quote`: Show what a `circular-pull`, `circular-push` or `circular` run would try first, without paying anything
* `circular-active`: List currently active in-flight rebalancing payments
* `circular-stats`: Get usage statistics and historical performance data
* `circular-delete-stats`: Reset and delete saved rebalancing statistics
* `circular-stop`: Stop `circular` from firing new HTLCs (active ones will complete)
* `circular-resume`: Resume normal rebalancing activity after a `circular-stop`

Detailed explanation of the endpoints follows in the Usage section.

## Building
You need Go 1.18 or higher to build this plugin.

```bash
git clone https://github.com/btweenthebars/CLN-circular.git
cd CLN-circular
go build -o circular ./cmd/circular
chmod +x circular
```

Alternatively, you can run:
```bash
make build
```

## Running
This plugin is dynamic, meaning that you can start and stop it via the CLI. For general plugin installation instructions see [How to install a plugin](https://github.com/lightningd/plugins/blob/master/README.md#Installation).

The executable that you have just built is called `circular`.
The startup options are:
* `circular-graph-refresh` (**minutes**): How often the graph is refreshed. Default is 10. At each refresh and at shutdown, what rebalances have learned about channels' liquidity is saved to `circular/learned.json` (a few hundred KB), and loaded at the next start. Earlier versions saved the whole graph to `graph.json` instead; its beliefs are migrated at the first start, then the file is removed.
* `circular-peer-refresh` (**seconds**): How often the list of peers is refreshed . Default is 30.
* `circular-liquidity-refresh` (**minutes**): Period of time after which we consider a liquidity belief not valid anymore. Default is 300.
* `circular-liquidity-halflife` (**minutes**): How fast a liquidity belief fades back toward the default (half the capacity): half of the way every this many minutes. Default is 60; 0 turns fading off, so beliefs hold in full until `circular-liquidity-refresh`.

* `circular-risk-ppm` (**ppm**): How much a route's chance of success weighs against its fees. Default is 200; 0 (or less) chooses routes as before: the fewest hops within `maxppm`, then the cheapest. Each channel between your two peers gets a chance of carrying the payment: circular takes what it believes the channel holds (half its capacity until payments teach it otherwise) as the middle of an even spread of possible balances. The search then takes the route with the least fees plus, for each channel, -ln(its chance) times this many ppm of the amount: a channel with an even chance adds about 0.69 × 200 = 139 ppm, one with a 99% chance about 2 ppm. `maxppm` still bounds the fees themselves. If no route fits this way, the search falls back to the one used with 0, so it never finds fewer routes. Higher values favour likely routes more. With it on, the number of hops is no longer kept to the least: a route may use up to `maxhops` channels when the extra ones are likely to carry the payment, so set `maxhops` to the longest route you accept. A channel no payment has told circular about, whose proportional fee (ppm) is at least twice, and 50 ppm above, the median over its node's active channels (for nodes with at least 3 channels; recomputed at each graph refresh), is taken to hold about 30% of its capacity rather than half: automatic fee managers raise a channel's fee as it drains, and among one node's 140 channels with known balances, those priced that way held a median of about 20% on the peer's side, the others about 40%. This only lowers its chance in the search; it does not change what the channel may carry. When a payment fails on a channel, circular knows it holds less than that amount: from then on any balance below it is taken as equally likely, so a split of 90% of the failed amount gets about a 10% chance there, and later splits and runs go elsewhere. The bound is kept in `learned.json` and loosens toward the capacity with `circular-liquidity-halflife`, the chance it gives rising back to what an unknown channel gets; a payment larger than the bound ends it, and smaller ones it carries lower it by what they moved.
* `circular-save-stats` (**boolean**): Whether to save stats about the usage of the plugin. Default is true. Save this to false if you are not interested in stats, as this data can grow big if you are running a lot of rebalances. You can delete the stats with the method `circular-delete-stats`.

Some failures say nothing about liquidity, so circular avoids what caused them in every rebalance for the next 30 minutes, not only in the one that hit them: a node that failed the payment itself, an erring node that was not on the route, a channel whose fee or CLTV error persists after refreshing its policy, and channels failing with other codes. A newer channel_update, from gossip or listchannels, lifts a channel early. Your own peers are only avoided for the rebalance that hit them. The list is saved in `learned.json`. A channel found disabled or closed is not faded back by `circular-liquidity-halflife`; it waits for the hard reset or a newer policy.

You can also set a preferred logging level.
For example, with this startup command you would refresh the graph every 5 minutes, peers every 60 seconds, and reset liquidity on channels every 120 minutes. You would also *not* save stats and set the logging level to **DEBUG**.

⚠ This command is meant to be an example of how to use startup options. You should probably use a configuration file for CLN instead of starting it in this way.️
```bash
lightningd --plugin=/path/to/circularexecutable --circular-graph-refresh=5 --circular-peer-refresh=60 --circular-liquidity-refresh=120 --circular-save-stats=false --log-level=debug:plugin-circular
```

## Usage
### Rebalance one channel at a time
via Short Channel ID:
```bash
lightning-cli circular -k inscid=123456x1x1 outscid=345678x1x1 amount=200000 maxppm=10 attempts=1
```

via Node ID:
```bash
lightning-cli circular-node -k outnode=123abc innode=345def amount=200000 maxppm=10 attempts=1
```

Required parameters:
* `outnode` or `outscid`: the node/scid that you want to use to send the payment
* `innode` or `inscid`: the node/scid where you want to receive the payment

Optional parameters:
* `amount`(sats, default=200000) is the amount that you want to rebalance
* `maxppm`(default=10) is the maximum ppm that you are willing to pay
* `attempts`(default=1) is the number of payments that may be made once a path is found (one more in the parallel commands when a split's reused route fails). The `attempts` a rebalance reports are the payments it made; searches that found no route are not counted.
* `maxnodefails`(default=5) is how many of a rebalance's payments one node may fail to forward for lack of liquidity. Each failure only rules out the channel that failed, so a drained node with many channels would otherwise cost one payment per channel. After that many, the rest of the rebalance routes around the node; if it is the peer of the out channel, which every route goes through, the rebalance stops: `circular-pull` then leaves that channel out for the rest of the run, and `circular-push`, whose every split goes out through that peer, starts no more splits. Set it above `attempts` + 1 to turn it off.
* `riskppm` sets `circular-risk-ppm` for this call only: above 0, that rate; -1, routes chosen by fees and hops alone; left out, the startup option.
* `maxhops`(default=8) is the maximum number of hops that a path is allowed to have

### Pull liquidity into a channel from many sources in parallel
```bash
lightning-cli circular-pull -k inscid=123456x1x1 amount=500000 splits=5 splitamount=20000 maxppm=10 maxoutppm=50 attempts=1 maxhops=8 depleteuptopercent=0.5 depleteuptoamount=2000000
```

Required parameters:
* `inscid`: the Short Channel Id where you want to pull liquidity.

Optional parameters:
* `amount`(sats, default=400000) is the **total** amount that you want to rebalance
* `splits`(default=4) is the maximum number of rebalances that will happen in parallel
* `splitamount`(sats, default=100000) is the amount that each rebalance will carry
* `minsplit`(sats, default=`splitamount`, which turns it off) is the smallest amount a split may shrink to. By default a split always asks for its full amount. With a lower `minsplit`, a split whose route fails because a channel along it lacks liquidity still searches for another route first; only if none is left does it send half the amount over the route that was short, down to `minsplit`, and later searches look for the smaller amount. Each payment with less uses one of the `attempts`, so this also needs `attempts` of 2 or more.
* `maxoutppm`(default=50) is the maximum ppm of the outgoing channels that `circular` is allowed to use to rebalance `inscid`. Useful to avoid rebalancing a channel from channels where you can profit
* `maxppm`(default=10), `attempts`(default=1), `maxnodefails`(default=5), `riskppm` and `maxhops`(default=8) are the same as for the `circular` command
* `maxfailstreak`(default=300): once this many payments in a row have failed, across all splits, without a split succeeding in between, no more splits are started; splits already in flight stop before their next payment, and the result's `stopped` says why (it also says so when the target channel takes no more). A later success does not start firing again. It counts payments, so with `attempts=100` the default amounts to about 3 failed splits in a row. A run whose target nothing can reach within `maxppm` otherwise goes through every candidate's `attempts`. Set it to -1 to turn this off.
* `outlist` is a JSON array of node ids that you want to use as sources. If this is specified, `maxoutppm` is ignored. An example of how to use this parameter is the following:
```bash
cli circular-pull -k inscid=123456x1x1 outlist='["03700917a25f79a3e427fe86e49b5041b583c73dd223cfa9a87cd6be5076b7b7a5", "025614be3600e9899bc044d331ab58a9fe1ccf30e75ae35943cdd11218a0a55dba"]' amount=800000 splitamount=80000 splits=4 maxppm=5000
```

`depleteuptopercent` and `depleteuptoamount` are a bit special: 
* `depleteuptopercent`(default=0.2) is a threshold percentage for the amount to leave in the outgoing channels. This must be between 0 and 1.
* `depleteuptoamount`(sats, default=1000000) is a value in sats for the amount to leave in the outgoing channels.
The actual amount that is going to be left in the outgoing channels is the minimum of `depleteuptopercent` and `depleteuptoamount`.

Example: you have a 10M channel and you set `depleteuptopercent` to 0.2 (20%) and `depleteuptoamount` to 1000000. The actual amount that will be left in that channel will be the minimum of 0.2 and 1000000. So in this case, at least 1000000 sats will be left in that channel.

A channel is only used for a split if its balance, less the HTLCs in flight, stays above the threshold after sending that split and its fees.

The result of `circular-pull` (and `circular-push`) looks like this:
```json
{
  "rebalance_target": 2926336,
  "rebalanced_amount": 640136,
  "attempts": 2073,
  "time": "4877.893s",
  "successes": {
    "123456x1x0": {
      "alias": "BCash_Is_Trash",
      "node_id": "02...",
      "756": 640136,
      "involving": ["03...", "02..."]
    }
  }
}
```
Each entry in `successes` is a candidate channel: its peer, the sats moved at each fee rate (ppm), and under `involving` the other nodes its successful routes went through, each once, in the order first seen (without your node and that peer; the target channel's peer is in every entry). `attempts` is the total of payments the splits made. `last_error`, when present, is the last failed split's message, prefixed with that split's channel and peer (`last failed split, via 123456x1x0 (alias): ...`); its count of payments is that split's alone.

Candidates are tried best first, by how they did in earlier splits (kept across runs in `learned.json`): a channel that failed in the last 30 minutes, and hasn't succeeded since, goes last; the others by success rate; then those that have moved liquidity before those that have not; then by the fee rate their splits paid (for those that have not, your own fee alone), adjusted by your own fee on the channel: `circular-pull` adds it, since draining a channel gives up what it earns you, and `circular-push` subtracts it, since filling a channel gives it more to earn. Channels never tried count as an even chance.

A failed split counts against its candidate only if it failed on the candidate's channel or along the route. If the target channel refuses a split because the other splits hold its HTLC slots or balance, the run keeps no more splits in flight than the target took, and the refused candidate is tried again once one finishes. That limit grows back by one once as many splits succeed, in case the refusal had another cause; each time growing it is refused, the run waits twice as long before trying again, and half as long once a growth holds. A candidate the target refuses as busy more than 3 times, beyond what the target's limit explains, is tried after the others. If the target refuses with nothing else in flight because our copy of its balance is too low, its channel is read again from lightningd and the split tried once more, since that copy may have been read while other splits held it. If it refuses that candidate again, or lightningd or the peer refused it, the candidate is left out for the run, as the target may refuse it alone (for the fees its route adds); if it then refuses another candidate alone too, or refuses for good (disabled, disconnected, its own fee above `maxppm` even with its inbound discounts), no more splits are started. A stale-fee failure on a channel of ours gets one more try, only if reading the channel again shows a new fee.

Once a split succeeds, the next split through the same channel first tries the same route again, with current fees, and only searches if that route is no longer usable or now costs more than `maxppm`. Splits that shrank count for what they delivered, and the following splits ask for what is left.

`successes` is keyed by the short channel id of each channel that was used: the outgoing channel drained by `circular-pull`, or the incoming channel filled by `circular-push`. Each entry has the peer's alias (when known) and node id, then the sats rebalanced at each fee rate paid, in ppm.

### Push liquidity out of a channel to many destinations in parallel
**Symmetrical to `circular-pull`, but for pushing liquidity out of a channel.**
```bash
lightning-cli circular-push -k outscid=123456x1x1 amount=500000 splits=5 splitamount=20000 maxppm=10 minoutppm=50 attempts=1 maxhops=8 filluptopercent=0.5 filluptoamount=2000000
```

Required parameters:
* `outscid`: the Short Channel Id from which you want to push out liquidity.

Optional parameters:
* `amount`, `splits`, `splitamount`, `minsplit`, `maxppm`, `attempts`, `maxnodefails`, `maxfailstreak`, `riskppm` and `maxhops` are the same as for the `circular-pull` command.
* `minoutppm`(default=50) is the minimum ppm charged by your node that a channel has to charge to be selected by `circular-push`. Useful to avoid rebalancing a channel to channels where you can't profit from.
* `inlist` is a JSON array of node ids that you want to use as destinations. If this is specified, `minoutppm` is ignored. An example of how to use this parameter is the following:
```bash
cli circular-push -k outscid=123456x1x1 inlist='["03700917a25f79a3e427fe86e49b5041b583c73dd223cfa9a87cd6be5076b7b7a5", "025614be3600e9899bc044d331ab58a9fe1ccf30e75ae35943cdd11218a0a55dba"]' amount=800000 splitamount=80000 splits=4 maxppm=5000
```

`filluptopercent` and `filluptoamount` are a bit special:
* `filluptopercent`(default=0.8) is a threshold percentage for the minimum amount that is allowed to stay as remote liquidity in the incoming channels. This must be between 0 and 1.
* `filluptoamount`(sats, default=10000000) is a value in sats for the minimum amount that is allowed to stay as remote liquidity in the incoming channels.
  The actual amount that is going to be left in the incoming channels is the minimum of `filluptopercent` and `filluptoamount`.

Example: you have a 10M channel and you set `filluptopercent` to 0.2 (20%) and `filluptoamount` to 1000000. The minimum amount of remote liquidity that will be left in that channel will be the minimum of 0.2 and 1000000. So in this case, at least 1000000 sats will be left in that channel.

As for `circular-pull`, the threshold is checked after the split, against the remote balance less the HTLCs in flight.


### Quote a rebalance without paying
```bash
lightning-cli circular-quote -k inscid=123456x1x1 splitamount=20000 maxppm=500 maxoutppm=50 maxhops=6
```
`circular-quote` takes the parameters of `circular-pull` (with `inscid`), `circular-push` (with `outscid`) or `circular` (with both), so you can swap the command name in a call you already make with named parameters (`-k` or `key=value`). Name them: positional arguments fill its fields in an order of its own, not that of the command you copied. It picks the candidates the same way and runs the same route search as a run, and pays nothing. For each candidate, in the order a run would try them, it shows the route of its first payment: fee rate, fee, hops and the path from your out-peer to your in-peer. A candidate without one shows why (`error`), and when every route costs more than `maxppm`, the cheapest rate found (`cheapest_ppm`). A candidate a run would not use, because of `depleteuptopercent`/`depleteuptoamount` or `filluptopercent`/`filluptoamount`, shows why under `skipped`. Candidates a run leaves out before trying anything are not listed: those over `maxoutppm` (pull) or not over `minoutppm` (push), and in a push, peers whose fee to send to you is above `maxppm` even with their inbound discounts.

```json
{
  "mode": "pull",
  "target": "123456x1x1",
  "target_peer": "Target",
  "amount": 20000,
  "maxppm": 500,
  "summary": "1 of 2 candidates have a route within 500 ppm (cheapest 412 ppm); 1 would fail (the cheapest route above maxppm is 518 ppm)",
  "candidates": [
    {
      "scid": "234567x1x1",
      "alias": "Zero",
      "node_id": "02...",
      "ppm": 412,
      "fee_msat": 8240,
      "hops": 5,
      "path": ["Zero", "Hub", "Other", "Target"],
      "estimate": 0.83,
      "guessed_hops": 2
    },
    {
      "scid": "345678x1x1",
      "alias": "One",
      "node_id": "03...",
      "cheapest_ppm": 518,
      "error": "route too expensive. Cheapest route found was 518 ppm, but maxppm is 500"
    }
  ]
}
```
`estimate` is a rough chance that the route can carry the payment: the product of each channel's chance, as `circular-risk-ppm` describes. `guessed_hops` counts the channels no payment has told circular about, for which any balance up to the capacity is taken as equally likely. It is a guide for comparing routes, not a promise. A quote reflects what circular knows at that moment: what earlier payments taught it and the nodes it is avoiding.

### Get stats about the usage of the plugin
```bash
lightning-cli circular-stats > stats.json
```
This command will return the following stats:
* `graph_stats`: stats about the graph that `circular` has learned
* `rebalanced_msat`: the total amount the successful rebalances below moved
* `successes`: successful rebalances done by `circular`
* `failures`: failed rebalances done by `circular`
* `routes`: routes taken by `circular`

It's a good idea to pipe the output into a file, since it can be quite big.
⚠ To limit the size, `circular` will only keep the last 14 days of stats.

Panics and other output to stderr go to `circular/stderr.log` in the lightning directory. It is moved to `stderr.log.old` when it passes 10 MB.

### Get active in-flight payments
```bash
lightning-cli circular-active
```
This returns a JSON list of all currently active, in-flight rebalancing payments (useful to monitor what parallel commands like `circular-pull` or `circular-push` are waiting on):
* `active_payments`: JSON array containing the payment hash, amount (msat), creation time, and routing hops of each active payment.


## Benchmarks
Here is the performance of the pathfinding algorithm on the mainnet lightning network graph as of August 2022 (about 16000 nodes and 80000 channels). The benchmarks consist in finding a route between two random nodes and measuring the time it takes to find the route. Different values of `maxhops` are tested to show that shorter routes take less time to compute. Those routes are preferred by `circular`, since the longer the route, the most likely it is to fail.

On a laptop:
```bash
goos: linux
goarch: amd64
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz

30 runs sampled
name                                 time/op
Graph_GetRoute/dijkstra_3_maxhops-8  2.44ms ± 2%
Graph_GetRoute/dijkstra_4_maxhops-8  3.25ms ± 5%
Graph_GetRoute/dijkstra_5_maxhops-8  6.57ms ± 5%
Graph_GetRoute/dijkstra_6_maxhops-8  10.6ms ± 5%
Graph_GetRoute/dijkstra_7_maxhops-8  14.6ms ±10%
Graph_GetRoute/dijkstra_8_maxhops-8  17.6ms ± 7%
```

On a Raspberry Pi 4:
```bash
goos: linux
goarch: arm64

30 runs sampled
name                                 time/op
Graph_GetRoute/dijkstra_3_maxhops-4  20.4ms ± 1%
Graph_GetRoute/dijkstra_4_maxhops-4  23.3ms ± 2%
Graph_GetRoute/dijkstra_5_maxhops-4  36.0ms ± 3%
Graph_GetRoute/dijkstra_6_maxhops-4  52.3ms ± 6%
Graph_GetRoute/dijkstra_7_maxhops-4  67.3ms ± 7%
Graph_GetRoute/dijkstra_8_maxhops-4  78.0ms ± 8%
```

To replicate these benchmarks on your own machine, you can use the following command:
```bash
cd graph
go test -bench=. -timeout 0 -count 30 | tee bench.txt

benchstat bench.txt
```
(you need `benchstat` installed)

## Contributing
If you have any problems feel free to open an issue or join our [Telegram group](https://t.me/+u_R8kAfpSJBjMjI0). Pull requests are welcome as well.

Special thanks to devzorLN🐸 for helping me test the plugin.
