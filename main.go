package main

import (
	"dht"
	"encoding/json"
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

	dht.Load()
	fd := dht.Find("a")
	dd, _ := json.Marshal(fd)
	loggo.Info("%v", string(dd))

	texas.Load()

	max, trans := texas.GetMax("黑3,梅A,梅K,方10,方9,鬼,红2")
	loggo.Info("max %v, trans %v", max, trans)

	max, trans = texas.GetMax("黑3,梅Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, trans)

	max, trans = texas.GetMax("方2,方Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, trans)

	max, trans = texas.GetMax("方J,方Q,梅K,方10,方9,方8,红J")
	loggo.Info("max %v, trans %v", max, trans)

	loggo.Info("%v", texas.GetWinType(max))
	loggo.Info("%v", texas.GetWinType("方J,方Q,梅K,方10,方9,方8,红J"))

	loggo.Info("%v", texas.Compare("方J,方Q,梅K,方10,方9,方8,红J", "方J,方Q,梅K,方10,方9,红8,红10"))
	loggo.Info("%v", texas.GetWinProbability("方J,方Q,梅K,方10,方7,红7,红J"))

	loggo.Info("%v", texas.GetHandProbability("方7,方10", "黑2,黑4,黑5,黑K"))

	loggo.Info("%v", texas.GetHandProbability("黑A,梅4", "红2,方A,黑A,红8,红3"))

}