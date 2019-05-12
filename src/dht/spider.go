package dht

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shiyanhui/dht"
	_ "github.com/shiyanhui/dht"
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

	gdb.Exec("CREATE TABLE  IF NOT EXISTS [meta_info](" +
		"[infohash] CHAR(40) NOT NULL," +
		"[name] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([name], [infohash]) ON CONFLICT IGNORE);")

	num := GetSize()
	loggo.Info("sqlite3 size %v", num)

	go Crawl()

	return nil
}

type file struct {
	Path   []interface{} `json:"path"`
	Length int           `json:"length"`
}

type bitTorrent struct {
	InfoHash string `json:"infohash"`
	Name     string `json:"name"`
	Files    []file `json:"files,omitempty"`
	Length   int    `json:"length,omitempty"`
}

func OnCrawl(w *dht.Wire) {
	for resp := range w.Response() {
		loggo.Info("OnCrawl resp bytes %v", len(resp.MetadataInfo))

		metadata, err := dht.Decode(resp.MetadataInfo)
		if err != nil {
			continue
		}
		info := metadata.(map[string]interface{})

		if _, ok := info["name"]; !ok {
			continue
		}

		bt := bitTorrent{
			InfoHash: hex.EncodeToString(resp.InfoHash),
			Name:     info["name"].(string),
		}

		if v, ok := info["files"]; ok {
			files := v.([]interface{})
			bt.Files = make([]file, len(files))

			for i, item := range files {
				f := item.(map[string]interface{})
				bt.Files[i] = file{
					Path:   f["path"].([]interface{}),
					Length: f["length"].(int),
				}
			}
		} else if _, ok := info["length"]; ok {
			bt.Length = info["length"].(int)
		}

		data, err := json.Marshal(bt)
		if err == nil {
			loggo.Info("Crawl %s", data)

			InsertSpider(bt.InfoHash, bt.Name)
		}
	}
}

func InsertSpider(infohash string, name string) {
	_, err := gdb.Exec("insert into meta_info(infohash, name, time) values('" + infohash + "', '" + name + "', DATETIME())")
	if err != nil {
		loggo.Error("insert sqlite3 fail %v", err)
	}

	gdb.Exec("delete from meta_info where date('now', '-30 day') > date(time)")

	num := GetSize()

	loggo.Info("InsertSpider size %v %v %v", infohash, name, num)
}

func GetSize() int {

	rows, err := gdb.Query("select count(*) from meta_info")
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

func Crawl() {
	w := dht.NewWire(65536, 1024, 256)
	go OnCrawl(w)
	go w.Run()

	config := dht.NewCrawlConfig()
	config.OnAnnouncePeer = func(infoHash, ip string, port int) {
		w.Request([]byte(infoHash), ip, port)
	}
	d := dht.New(config)

	go d.Run()
}

type FindData struct {
	Infohash string
	Name     string
}

func Find(str string) []FindData {
	var ret []FindData

	retmap := make(map[string]string)

	strs := strings.Split(str, " ")

	for _, s := range strs {
		rows, err := gdb.Query("select infohash,name from meta_info where name like '%" + s + "%'")
		if err != nil {
			loggo.Error("Query sqlite3 fail %v", err)
		}
		defer rows.Close()

		for rows.Next() {

			var infohash string
			var name string
			err = rows.Scan(&infohash, &name)
			if err != nil {
				loggo.Error("Scan sqlite3 fail %v", err)
			}

			_, ok := retmap[infohash]
			if ok {
				continue
			}
			retmap[infohash] = name

			ret = append(ret, FindData{infohash, name})
		}
	}

	return ret
}
