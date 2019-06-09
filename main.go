package main

import (
	"github.com/esrrhs/go-engine/src/common"
	"github.com/esrrhs/go-engine/src/loggo"
	"github.com/esrrhs/go-engine/src/spider"
	"github.com/esrrhs/go-engine/src/texas"
	"github.com/go-sql-driver/mysql"
)

func main() {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 2})

	loggo.Info("start")

	dbconfig := mysql.NewConfig()
	dbconfig.User = "root"
	dbconfig.Passwd = "123123"
	dbconfig.Net = "tcp"
	dbconfig.Addr = "192.168.1.115:4406"

	dsn := dbconfig.FormatDSN()

	common.Ini()
	config := spider.Config{1, 100, 100, 1, true,
		"puppeteer", 60}
	entry := "http://bt.hliang.com/show-4a1fa9b7bb73346774d2603b63f3c12b2e6581d4.html"
	//entry := "http://www.csdn.net"
	//entry := "https://www.80ying.com"
	//entry := "http://www.esrrhs.xyz"
	db := spider.Load(dsn, 100)
	spider.Start(db, config, entry)

	texas.Load()

	max, trans := texas.GetMax("方8,红J,方6,红3,方5,红A,方4")
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
