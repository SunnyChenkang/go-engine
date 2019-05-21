package spider

import (
	"database/sql"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

var gdb *sql.DB

var gInsertStmt *sql.Stmt
var gSizeStmt *sql.Stmt
var gLastStmt *sql.Stmt
var gFindStmt *sql.Stmt
var gDeleteStmt *sql.Stmt

var gInsertJobStmt *sql.Stmt
var gSizeJobStmt *sql.Stmt
var gPeekJobStmt *sql.Stmt
var gDeleteJobStmt *sql.Stmt
var gHasJobStmt *sql.Stmt

var gInsertDoneStmt *sql.Stmt
var gSizeDoneStmt *sql.Stmt

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

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_job_info](" +
		"[src] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[deps] INT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_done_info](" +
		"[src] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	////

	stmt, err := gdb.Prepare("insert into link_info(title, name, url, time) values(?, ?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gInsertStmt = stmt

	stmt, err = gdb.Prepare("select count(*) from link_info")
	if err != nil {
		loggo.Error("HasDone Prepare sqlite3 fail %v", err)
		return err
	}
	gSizeStmt = stmt

	stmt, err = gdb.Prepare("select title,name,url from link_info order by time desc limit 0, ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gLastStmt = stmt

	stmt, err = gdb.Prepare("select title,name,url from link_info where name like ? or title like ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gFindStmt = stmt

	stmt, err = gdb.Prepare("delete from link_info where date('now', '-30 day') > date(time)")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gDeleteStmt = stmt

	////

	stmt, err = gdb.Prepare("insert into link_job_info(src, url, deps, time) values(?, ?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gInsertJobStmt = stmt

	stmt, err = gdb.Prepare("select count(*) from link_job_info where src = ?")
	if err != nil {
		loggo.Error("HasDone Prepare sqlite3 fail %v", err)
		return err
	}
	gSizeJobStmt = stmt

	stmt, err = gdb.Prepare("delete from link_job_info where src = ? and url = ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gDeleteJobStmt = stmt

	stmt, err = gdb.Prepare("select url, deps from link_job_info where src = ? limit 0, ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gPeekJobStmt = stmt

	stmt, err = gdb.Prepare("select url from link_job_info where src = ? and url = ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gHasJobStmt = stmt

	////

	stmt, err = gdb.Prepare("insert into link_done_info(src, url, time) values(?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return err
	}
	gInsertDoneStmt = stmt

	stmt, err = gdb.Prepare("select count(*) from link_done_info")
	if err != nil {
		loggo.Error("HasDone Prepare sqlite3 fail %v", err)
		return err
	}
	gSizeDoneStmt = stmt

	////

	num := GetSize()
	loggo.Info("sqlite3 size %v", num)

	return nil
}

func PopSpiderJob(src string, n int) ([]string, []int) {

	var ret []string
	var retdeps []int

	rows, err := gPeekJobStmt.Query(src, n)
	if err != nil {
		loggo.Error("Query sqlite3 fail %v", err)
		return ret, retdeps
	}
	defer rows.Close()

	for rows.Next() {

		var url string
		var deps int
		err = rows.Scan(&url, &deps)
		if err != nil {
			loggo.Error("Scan sqlite3 fail %v", err)
		}

		ret = append(ret, url)
		retdeps = append(retdeps, deps)
	}

	for i, url := range ret {
		gDeleteJobStmt.Exec(src, url)
		loggo.Info("PopSpiderJob %v %v %v", src, url, retdeps[i])
	}

	return ret, retdeps
}

func InsertSpiderJob(src string, url string, deps int) {

	_, err := gInsertJobStmt.Exec(src, url, deps)
	if err != nil {
		loggo.Error("InsertSpiderJob insert sqlite3 fail %v", err)
	}

	num := GetJobSize(src)

	loggo.Info("InsertSpiderJob %v size %v", url, num)
}

func InsertSpider(title string, name string, url string) {

	_, err := gInsertStmt.Exec(title, name, url)
	if err != nil {
		loggo.Error("InsertSpider insert sqlite3 fail %v", err)
	}

	gDeleteStmt.Exec("delete from link_info where date('now', '-30 day') > date(time)")

	num := GetSize()

	loggo.Info("InsertSpider %v %v %v size %v", title, name, url, num)
}

func HasJob(src string, url string) bool {
	var surl string
	err := gHasJobStmt.QueryRow(src, url).Scan(&surl)
	if err != nil {
		return false
	}
	return true
}

func GetSize() int {

	var ret int
	err := gSizeStmt.QueryRow().Scan(&ret)
	if err != nil {
		loggo.Error("Scan sqlite3 fail %v", err)
		return 0
	}
	return ret
}

func GetJobSize(src string) int {

	var ret int
	err := gSizeJobStmt.QueryRow(src).Scan(&ret)
	if err != nil {
		loggo.Error("Scan sqlite3 fail %v", err)
		return 0
	}
	return ret
}

type FindData struct {
	Title string
	Name  string
	URL   string
}

func Last(n int) []FindData {
	var ret []FindData

	retmap := make(map[string]string)

	rows, err := gLastStmt.Query(n)
	if err != nil {
		loggo.Error("Query sqlite3 fail %v", err)
		return ret
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
		rows, err := gFindStmt.Query("%"+s+"%", "%"+s+"%")
		if err != nil {
			loggo.Error("Query sqlite3 fail %v", err)
			return ret
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
