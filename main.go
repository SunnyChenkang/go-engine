package main

import (
	"loggo"
	"rbuffergo"
	"socketgo"
	"texas"
)

func main() {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 7})

	loggo.Info("start")

	rbuffergo.New(1, true)

	c := socketgo.LuConfig{}
	socketgo.New(&c)

	texas.Load("aaa")
}
