package spider

import (
	"database/sql"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"net/url"
	"strings"
	"sync"
	"time"
)

type DB struct {
	gdb         *sql.DB
	lock        sync.Mutex
	gInsertStmt *sql.Stmt
	gSizeStmt   *sql.Stmt
	gLastStmt   *sql.Stmt
	gFindStmt   *sql.Stmt
	gDeleteStmt *sql.Stmt
}

type JobDB struct {
	gdb            *sql.DB
	src            string
	lock           sync.Mutex
	gInsertJobStmt *sql.Stmt
	gSizeJobStmt   *sql.Stmt
	gPeekJobStmt   *sql.Stmt
	gDeleteJobStmt *sql.Stmt
	gHasJobStmt    *sql.Stmt
}

type DoneDB struct {
	gdb             *sql.DB
	src             string
	lock            sync.Mutex
	gInsertDoneStmt *sql.Stmt
	gSizeDoneStmt   *sql.Stmt
	gDeleteDoneStmt *sql.Stmt
	gHasDoneStmt    *sql.Stmt
}

func Load(dsn string, conn int) *DB {

	loggo.Info("sqlite3 Load start")

	gdb, err := sql.Open("mysql", dsn)
	if err != nil {
		loggo.Error("open mysql fail %v", err)
		return nil
	}

	err = gdb.Ping()
	if err != nil {
		loggo.Error("open mysql fail %v", err)
		return nil
	}

	gdb.SetConnMaxLifetime(0)
	gdb.SetMaxIdleConns(conn)
	gdb.SetMaxOpenConns(conn)

	ret := new(DB)

	ret.gdb = gdb

	_, err = gdb.Exec("CREATE DATABASE IF NOT EXISTS spider")
	if err != nil {
		loggo.Error("CREATE DATABASE fail %v", err)
		return nil
	}

	_, err = gdb.Exec("CREATE TABLE  IF NOT EXISTS spider.link_info(" +
		"url VARCHAR(200)  NOT NULL ," +
		"title VARCHAR(200) NOT NULL," +
		"name VARCHAR(200) NOT NULL," +
		"time DATETIME NOT NULL," +
		"PRIMARY KEY(url)," +
		"INDEX `time`(`time`) USING BTREE" +
		");")
	if err != nil {
		loggo.Error("CREATE TABLE fail %v", err)
		return nil
	}

	stmt, err := gdb.Prepare("insert IGNORE  into spider.link_info(title, name, url, time) values(?, ?, ?, NOW())")
	if err != nil {
		loggo.Error("Prepare mysql fail %v", err)
		return nil
	}
	ret.gInsertStmt = stmt

	stmt, err = gdb.Prepare("select count(*) as ret from spider.link_info")
	if err != nil {
		loggo.Error("HasDone Prepare mysql fail %v", err)
		return nil
	}
	ret.gSizeStmt = stmt

	stmt, err = gdb.Prepare("select title,name,url from spider.link_info order by time desc limit 0, ?")
	if err != nil {
		loggo.Error("Prepare mysql fail %v", err)
		return nil
	}
	ret.gLastStmt = stmt

	stmt, err = gdb.Prepare("select title,name,url from spider.link_info where name like ? or title like ? limit 0,10000")
	if err != nil {
		loggo.Error("Prepare mysql fail %v", err)
		return nil
	}
	ret.gFindStmt = stmt

	stmt, err = gdb.Prepare("delete from spider.link_info where (TO_DAYS(NOW()) - TO_DAYS(time))>=30")
	if err != nil {
		loggo.Error("Prepare mysql fail %v", err)
		return nil
	}
	ret.gDeleteStmt = stmt

	////
	go DeleteOldSpider(ret)

	num := GetSize(ret)
	loggo.Info("mysql size %v", num)

	return ret
}

func LoadJob(src string) *JobDB {

	loggo.Info("sqlite3 Load Job start")

	dstURL, _ := url.Parse(src)
	host := dstURL.Host

	gdb, err := sql.Open("sqlite3", "./spider_job_"+host+".db")
	if err != nil {
		loggo.Error("open sqlite3 Job fail %v", err)
		return nil
	}

	ret := new(JobDB)

	ret.gdb = gdb
	ret.src = src

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_job_info](" +
		"[src] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[deps] INT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	stmt, err := gdb.Prepare("insert into link_job_info(src, url, deps, time) values(?, ?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare Job sqlite3 fail %v", err)
		return nil
	}
	ret.gInsertJobStmt = stmt

	stmt, err = gdb.Prepare("select count(*) from link_job_info where src = ?")
	if err != nil {
		loggo.Error("HasDone Job Prepare sqlite3 fail %v", err)
		return nil
	}
	ret.gSizeJobStmt = stmt

	stmt, err = gdb.Prepare("delete from link_job_info where src = ? and url = ?")
	if err != nil {
		loggo.Error("Prepare Job sqlite3 fail %v", err)
		return nil
	}
	ret.gDeleteJobStmt = stmt

	stmt, err = gdb.Prepare("select url, deps from link_job_info where src = ? limit 0, ?")
	if err != nil {
		loggo.Error("Prepare Job sqlite3 fail %v", err)
		return nil
	}
	ret.gPeekJobStmt = stmt

	stmt, err = gdb.Prepare("select url from link_job_info where src = ? and url = ?")
	if err != nil {
		loggo.Error("Prepare Job sqlite3 fail %v", err)
		return nil
	}
	ret.gHasJobStmt = stmt

	num := GetJobSize(ret)
	loggo.Info("sqlite3 Job size %v", num)

	return ret
}

func LoadDone(src string) *DoneDB {

	loggo.Info("sqlite3 Load Done start")

	dstURL, _ := url.Parse(src)
	host := dstURL.Host

	gdb, err := sql.Open("sqlite3", "./spider_done_"+host+".db")
	if err != nil {
		loggo.Error("open sqlite3 Done fail %v", err)
		return nil
	}

	ret := new(DoneDB)
	ret.gdb = gdb
	ret.src = src

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [link_done_info](" +
		"[src] TEXT NOT NULL," +
		"[url] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([url]) ON CONFLICT IGNORE);")

	////

	stmt, err := gdb.Prepare("insert into link_done_info(src, url, time) values(?, ?, DATETIME())")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return nil
	}
	ret.gInsertDoneStmt = stmt

	stmt, err = gdb.Prepare("select count(*) from link_done_info where src = ?")
	if err != nil {
		loggo.Error("HasDone Prepare sqlite3 fail %v", err)
		return nil
	}
	ret.gSizeDoneStmt = stmt

	stmt, err = gdb.Prepare("delete from link_done_info where src = ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return nil
	}
	ret.gDeleteDoneStmt = stmt

	stmt, err = gdb.Prepare("select url from link_done_info where src = ? and url = ?")
	if err != nil {
		loggo.Error("Prepare sqlite3 fail %v", err)
		return nil
	}
	ret.gHasDoneStmt = stmt

	////

	num := GetDoneSize(ret)
	loggo.Info("sqlite3 size %v", num)

	return ret
}

func PopSpiderJob(db *JobDB, n int) ([]string, []int) {

	var ret []string
	var retdeps []int

	db.lock.Lock()
	rows, err := db.gPeekJobStmt.Query(db.src, n)
	if err != nil {
		loggo.Error("PopSpiderJob Query sqlite3 fail %v %v", db.src, err)
		db.lock.Unlock()
		return ret, retdeps
	}
	defer rows.Close()
	db.lock.Unlock()

	for rows.Next() {

		var url string
		var deps int
		err = rows.Scan(&url, &deps)
		if err != nil {
			loggo.Error("PopSpiderJob Scan sqlite3 fail %v %v", db.src, err)
		}

		ret = append(ret, url)
		retdeps = append(retdeps, deps)
	}

	for i, url := range ret {
		db.lock.Lock()
		db.gDeleteJobStmt.Exec(db.src, url)
		db.lock.Unlock()
		loggo.Info("PopSpiderJob %v %v %v", db.src, url, retdeps[i])
	}

	return ret, retdeps
}

func DeleteSpiderDone(db *DoneDB) {
	db.lock.Lock()
	db.gDeleteDoneStmt.Exec(db.src)
	db.lock.Unlock()
}

func InsertSpiderJob(db *JobDB, url string, deps int) {

	db.lock.Lock()
	_, err := db.gInsertJobStmt.Exec(db.src, url, deps)
	if err != nil {
		loggo.Error("InsertSpiderJob insert sqlite3 fail %v %v", url, err)
	}
	db.lock.Unlock()

	num := GetJobSize(db)

	loggo.Info("InsertSpiderJob %v size %v", url, num)
}

func InsertSpiderDone(db *DoneDB, url string) {

	db.lock.Lock()
	_, err := db.gInsertDoneStmt.Exec(db.src, url)
	if err != nil {
		loggo.Error("InsertSpiderDone insert sqlite3 fail %v %v", url, err)
	}
	db.lock.Unlock()

	num := GetDoneSize(db)

	loggo.Info("InsertSpiderDone %v size %v", url, num)
}

func DeleteOldSpider(db *DB) {
	for {
		db.lock.Lock()
		db.gDeleteStmt.Exec()
		db.lock.Unlock()

		loggo.Info("DeleteOldSpider %v", GetSize(db))

		time.Sleep(time.Hour)
	}
}

func InsertSpider(db *DB, title string, name string, url string) {

	db.lock.Lock()
	_, err := db.gInsertStmt.Exec(title, name, url)
	if err != nil {
		loggo.Error("InsertSpider insert sqlite3 fail %v %v", url, err)
	}
	db.lock.Unlock()

	loggo.Info("InsertSpider %v %v %v", title, name, url)
}

func HasJob(db *JobDB, url string) bool {
	db.lock.Lock()
	var surl string
	err := db.gHasJobStmt.QueryRow(db.src, url).Scan(&surl)
	if err != nil {
		db.lock.Unlock()
		return false
	}
	db.lock.Unlock()
	return true
}

func HasDone(db *DoneDB, url string) bool {
	db.lock.Lock()
	var surl string
	err := db.gHasDoneStmt.QueryRow(db.src, url).Scan(&surl)
	if err != nil {
		db.lock.Unlock()
		return false
	}
	db.lock.Unlock()
	return true
}

func GetSize(db *DB) int {
	db.lock.Lock()
	var ret int
	err := db.gSizeStmt.QueryRow().Scan(&ret)
	if err != nil {
		loggo.Error("GetSize fail %v", err)
	}
	db.lock.Unlock()
	return ret
}

func GetJobSize(db *JobDB) int {
	db.lock.Lock()
	var ret int
	err := db.gSizeJobStmt.QueryRow(db.src).Scan(&ret)
	if err != nil {
		loggo.Error("GetJobSize fail %v %v", db.src, err)
	}
	db.lock.Unlock()
	return ret
}

func GetDoneSize(db *DoneDB) int {
	db.lock.Lock()
	var ret int
	err := db.gSizeDoneStmt.QueryRow(db.src).Scan(&ret)
	if err != nil {
		loggo.Error("GetDoneSize fail %v %v", db.src, err)
	}
	db.lock.Unlock()
	return ret
}

type FindData struct {
	Title string
	Name  string
	URL   string
}

func Last(db *DB, n int) []FindData {

	var ret []FindData

	retmap := make(map[string]string)

	db.lock.Lock()

	rows, err := db.gLastStmt.Query(n)
	if err != nil {
		loggo.Error("Last Query sqlite3 fail %v", err)
		db.lock.Unlock()
		return ret
	}
	defer rows.Close()

	for rows.Next() {

		var title string
		var name string
		var url string
		err := rows.Scan(&title, &name, &url)
		if err != nil {
			loggo.Error("Last Scan sqlite3 fail %v", err)
		}

		_, ok := retmap[url]
		if ok {
			continue
		}
		retmap[url] = name

		ret = append(ret, FindData{title, name, url})
	}

	db.lock.Unlock()

	return ret
}

func Find(db *DB, str string) []FindData {

	var ret []FindData

	retmap := make(map[string]string)

	strs := strings.Split(str, " ")

	for _, s := range strs {
		db.lock.Lock()

		rows, err := db.gFindStmt.Query("%"+s+"%", "%"+s+"%")
		if err != nil {
			loggo.Error("Find Query sqlite3 fail %v", err)
			db.lock.Unlock()
			return ret
		}
		defer rows.Close()

		for rows.Next() {

			var title string
			var name string
			var url string
			err = rows.Scan(&title, &name, &url)
			if err != nil {
				loggo.Error("Find Scan sqlite3 fail %v", err)
			}

			_, ok := retmap[url]
			if ok {
				continue
			}
			retmap[url] = name

			ret = append(ret, FindData{title, name, url})
		}

		db.lock.Unlock()
	}

	return ret
}
