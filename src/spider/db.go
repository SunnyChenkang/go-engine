package spider

import (
	"database/sql"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
)

var gdb *sql.DB
var gcrawl chan string
var gparse chan *PageInfo
var gsave chan *DBInfo

type DBInfo struct {
	Title string
	Name  string
	Url   string
}

type PageLinkInfo struct {
	Url  string
	Name string
}
type PageInfo struct {
	Url   string
	Title string
	Son   []PageLinkInfo
}

func Load(threadnum int, buffersize int) error {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "spider", 7})

	loggo.Info("sqlite3 Load start")

	db, err := sql.Open("sqlite3", "./spider.db")
	if err != nil {
		loggo.Error("open sqlite3 fail %v", err)
		return err
	}
	gdb = db

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_info](" +
		"[tile] TEXT NOT NULL," +
		"[name] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	num := GetSize()
	loggo.Info("sqlite3 size %v", num)

	gcrawl = make(chan string, buffersize)
	gparse = make(chan *PageInfo, buffersize)
	gsave = make(chan *DBInfo, buffersize)

	for i := 0; i < threadnum; i++ {
		go Crawler()
		go Parser()
		go Saver()
	}

	return nil
}

func Entry(url string) {
	gcrawl <- url
}

func Crawler() {
	for {
		job := <-gcrawl
		loggo.Info("receive crawl job %v", job)

		pg := simplecrawl(job)
		if pg != nil {
			loggo.Info("crawl job ok %v %v %v", job, pg.Title, len(pg.Son))
			gparse <- pg
		}
	}
}

func Parser() {

}

func Saver() {

}

func GetSize() int {

	rows, err := gdb.Query("select count(*) from link_info")
	if err != nil {
		loggo.Error("Query sqlite3 fail %v", err)
		return 0
	}
	defer rows.Close()

	rows.Next()

	var num int
	err = rows.Scan(&num)
	if err != nil {
		loggo.Error("Scan sqlite3 fail %v", err)
		return 0
	}

	return num
}
