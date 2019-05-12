package dht

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shiyanhui/dht"
	_ "github.com/shiyanhui/dht"
)

var gspider *sql.DB

func Load() error {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "spider", 7})

	loggo.Info("sqlite3 Load start")

	gspider, err := sql.Open("sqlite3", "./spider.db")
	if err != nil {
		loggo.Error("open sqlite3 fail %v", err)
		return err
	}

	gspider.Exec("CREATE TABLE  IF NOT EXISTS [meta_info](" +
		"[infohash] CHAR(40) NOT NULL," +
		"[name] TEXT NOT NULL," +
		"[time] DATETIME NOT NULL," +
		"PRIMARY KEY([name], [infohash]) ON CONFLICT IGNORE);")

	num := GetSize(gspider)
	loggo.Info("sqlite3 size %v", num)

	go Crawl()

	return nil
}

func GetSize(db *sql.DB) int {

	rows, err := db.Query("select count(*) from meta_info")
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
			for _, f := range bt.Files {
				for _, fp := range f.Path {
					fps := fp.(string)
					InsertSpider(bt.InfoHash, fps)
				}
			}
		}
	}
}

func InsertSpider(infohash string, name string) {
	_, err := gspider.Exec("insert into meta_info(infohash, name, time) values('" + infohash + "', '" + name + "', DATETIME())")
	if err != nil {
		loggo.Error("insert sqlite3 fail %v", err)
	}

	gspider.Exec("delete from meta_info where date('now', '-30 day') > date(time)")

	num := GetSize(gspider)

	loggo.Info("InsertSpider size %v", num)
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
