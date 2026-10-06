package main

import (
	"circular/node"
	"github.com/elementsproject/glightning/glightning"
	"os"
)

// TODO: listen for `channel_state_changed` and `channel_opened`
// 		so we don't have to refresh peer list every time

func registerSubscriptions(p *glightning.Plugin) {
	p.SubscribeSendPayFailure(OnSendPayFailure)
	p.SubscribeSendPaySuccess(OnSendPaySuccess)
	p.SubscribeConnect(OnConnect)
	p.SubscribeDisconnect(OnDisconnect)
	p.SubscribeShutdown(OnShutdown)
}

// OnShutdown runs when lightningd shuts down or `plugin stop` stops the
// plugin. lightningd expects the plugin to exit within 30 seconds.
func OnShutdown() {
	node.GetNode().Close()
	os.Exit(0)
}

func OnSendPayFailure(sf *glightning.SendPayFailure) {
	node.GetNode().OnPaymentFailure(sf)
}

func OnSendPaySuccess(ss *glightning.SendPaySuccess) {
	node.GetNode().OnPaymentSuccess(ss)
}

func OnConnect(c *glightning.ConnectEvent) {
	node.GetNode().OnConnect(c)
}

func OnDisconnect(d *glightning.DisconnectEvent) {
	node.GetNode().OnDisconnect(d)
}
