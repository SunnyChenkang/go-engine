package spider

import (
	"database/sql"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"strconv"
	"strings"
)

var gdb *sql.DB

func Load() error {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "spider", 7})

	loggo.Info("sqlite3 Load start")

	db, err := sql.Open("sqlite3", "./spider.db")
	if err != nil {
		loggo.Error("open sqlite3 fail %v", err)
		return err
	}
	gdb = db

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_info](" +
		"[title] TEXT NOT NULL," +
		"[name] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	num := GetSize()
	loggo.Info("sqlite3 size %v", num)

	return nil
}

func InsertSpider(title string, name string, url string) {

	tx, err := gdb.Begin()
	if err != nil {
		loggo.Error("Begin sqlite3 fail %v", err)
		return
	}
	stmt, err := tx.Prepare("insert into link_info(title, name, url, time) values(?, ?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return
	}
	defer stmt.Close()
	_, err = stmt.Exec(title, name, url)
	if err != nil {
		loggo.Error("insert sqlite3 fail %v", err)
	}
	tx.Commit()

	gdb.Exec("delete from link_info where date('now', '-30 day') > date(time)")

	num := GetSize()

	loggo.Info("InsertSpider size %v %v %v %v", title, name, url, num)
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

type FindData struct {
	Title string
	Name  string
	URL   string
}

func Last(n int) []FindData {
	var ret []FindData

	retmap := make(map[string]string)

	rows, err := gdb.Query("select title,name,url from link_info order by time desc limit 0," + strconv.Itoa(n))
	if err != nil {
		loggo.Error("Query sqlite3 fail %v", err)
	}
	defer rows.Close()

	for rows.Next() {

		var title string
		var name string
		var url string
		err = rows.Scan(&title, &name, &url)
		if err != nil {
			loggo.Error("Scan sqlite3 fail %v", err)
		}

		_, ok := retmap[url]
		if ok {
			continue
		}
		retmap[url] = name

		ret = append(ret, FindData{title, name, url})
	}

	return ret
}

func Find(str string) []FindData {
	var ret []FindData

	retmap := make(map[string]string)

	strs := strings.Split(str, " ")

	for _, s := range strs {
		rows, err := gdb.Query("select title,name,url from link_info where name like '%" + s + "%' or title like '%" + s + "%'")
		if err != nil {
			loggo.Error("Query sqlite3 fail %v", err)
		}
		defer rows.Close()

		for rows.Next() {

			var title string
			var name string
			var url string
			err = rows.Scan(&title, &name, &url)
			if err != nil {
				loggo.Error("Scan sqlite3 fail %v", err)
			}

			_, ok := retmap[url]
			if ok {
				continue
			}
			retmap[url] = name

			ret = append(ret, FindData{title, name, url})
		}
	}

	return ret
}
