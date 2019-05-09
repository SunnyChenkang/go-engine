package main

import (
	"loggo"
	"rbuffergo"
	"socketgo"
)

func main() {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 7})

	loggo.Info("start")

	rbuffergo.New(1, true)

	c := socketgo.LuConfig{}
	socketgo.New(&c)
}
