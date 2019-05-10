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

	d := texas.StrToBytes("黑3,梅A,方10,鬼")
	loggo.Info("%v", texas.BytesToStr(d))

	texas.Load()

	max, trans := texas.GetMax("黑3,梅A,梅K,方10,方9,鬼,红2")
	loggo.Info("max %v, trans %v", max, texas.BytesToStr(trans))

	max, trans = texas.GetMax("黑3,梅Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, texas.BytesToStr(trans))

	max, trans = texas.GetMax("方2,方Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, texas.BytesToStr(trans))

	max, trans = texas.GetMax("方J,方Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, texas.BytesToStr(trans))
}
