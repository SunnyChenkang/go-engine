package spider

import (
	"github.com/esrrhs/go-engine/src/loggo"
	_ "github.com/mattn/go-sqlite3"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Threadnum   int
	Buffersize  int
	Sleeptimems int
	Deps        int
	FocusSpider bool
}

type DBInfo struct {
	Title string
	Name  string
	Url   string
}

type PageLinkInfo struct {
	UI   URLInfo
	Name string
}

type PageInfo struct {
	UI    URLInfo
	Title string
	Son   []PageLinkInfo
}

type URLInfo struct {
	Url  string
	Deps int
}

func Start(config Config, url []string) {
	loggo.Info("Spider Start  %v", url)

	crawl := make(chan *URLInfo, config.Buffersize)
	parse := make(chan *PageInfo, config.Buffersize)
	save := make(chan *DBInfo, config.Buffersize)

	var jobs int32

	var find sync.Map

	for _, u := range url {
		atomic.AddInt32(&jobs, 1)
		crawl <- &URLInfo{u, 0}
	}

	var jobsCrawlerTotal int32
	var jobsCrawlerFail int32

	for i := 0; i < config.Threadnum; i++ {
		go Crawler(config, find, &jobs, crawl, parse, &jobsCrawlerTotal, &jobsCrawlerFail)
		go Parser(config, find, &jobs, crawl, parse, save)
		go Saver(&jobs, save)
	}

	for {
		time.Sleep(time.Second)
		if jobs == 0 {
			time.Sleep(time.Second)
			if jobs == 0 {
				break
			}
		}
	}

	loggo.Info("Spider jobs done cral %v, failed %v", jobsCrawlerTotal, jobsCrawlerFail)

	close(crawl)
	close(parse)
	close(save)

	loggo.Info("Spider end %v", GetSize())
}

func Crawler(config Config, find sync.Map, jobs *int32, crawl <-chan *URLInfo, parse chan<- *PageInfo, jobsCrawlerTotal *int32, jobsCrawlerTotalFail *int32) {
	loggo.Info("Crawler start")
	for job := range crawl {
		//loggo.Info("receive crawl job %v", job)

		_, ok := find.LoadOrStore(job.Url, nil)
		if !ok {
			if job.Deps < config.Deps {
				atomic.AddInt32(jobsCrawlerTotal, 1)
				pg := simplecrawl(job)
				if pg != nil {
					//loggo.Info("crawl job ok %v %v %v", job, pg.Title, len(pg.Son))
					atomic.AddInt32(jobs, 1)
					parse <- pg
				} else {
					atomic.AddInt32(jobsCrawlerTotalFail, 1)
				}
			}
		}

		atomic.AddInt32(jobs, -1)

		time.Sleep(time.Duration(config.Sleeptimems) * time.Millisecond)
	}
	loggo.Info("Crawler end")
}

func Parser(config Config, find sync.Map, jobs *int32, crawl chan<- *URLInfo, parse <-chan *PageInfo, save chan<- *DBInfo) {
	loggo.Info("Parser start")

	for job := range parse {
		//loggo.Info("receive parse job %v %v", job.Title, job.UI.Url)

		for _, s := range job.Son {

			sonurl := s.UI.Url

			if strings.HasPrefix(sonurl, "#") {
				continue
			}

			if sonurl == "/" {
				continue
			}

			ss := strings.ToLower(sonurl)

			ok := false
			if strings.HasPrefix(ss, "thunder://") || strings.HasPrefix(ss, "magnet://") {
				ok = true
			}

			if strings.HasSuffix(ss, ".mp4") || strings.HasSuffix(ss, ".rmvb") || strings.HasSuffix(ss, ".mkv") {
				ok = true
			}

			if ok {
				di := DBInfo{job.Title, s.Name, sonurl}
				atomic.AddInt32(jobs, 1)
				save <- &di

				//loggo.Info("receive parse ok %v %v %v", job.Title, s.Name, sonurl)
			} else {
				valid := false
				if strings.HasPrefix(ss, "http://") || strings.HasPrefix(ss, "https://") {
					valid = true
				}

				if strings.HasPrefix(ss, "/") {
					tmp := strings.TrimRight(job.UI.Url, "/")
					sonurl = tmp + sonurl
					valid = true
				}

				if valid {
					_, finded := find.Load(sonurl)
					if !finded {
						if config.FocusSpider {
							dstURL, dsterr := url.Parse(sonurl)
							srcURL, srcerr := url.Parse(job.UI.Url)

							if dsterr == nil && srcerr == nil {
								dstParams := strings.Split(dstURL.Host, ".")
								srcParams := strings.Split(srcURL.Host, ".")

								if len(dstParams) >= 2 && len(srcParams) >= 2 && dstParams[len(dstParams)-1] == srcParams[len(srcParams)-1] && dstParams[len(dstParams)-2] == srcParams[len(srcParams)-2] {
									atomic.AddInt32(jobs, 1)
									tmp := URLInfo{sonurl, s.UI.Deps}
									crawl <- &tmp

									//loggo.Info("parse spawn job %v %v", job.UI.Url, sonurl)
								}
							}
						} else {
							atomic.AddInt32(jobs, 1)
							tmp := URLInfo{sonurl, s.UI.Deps}
							crawl <- &tmp

							//loggo.Info("parse spawn job %v %v", job.UI.Url, sonurl)
						}
					}
				}
			}
		}

		atomic.AddInt32(jobs, -1)
	}
	loggo.Info("Parser end")
}

func Saver(jobs *int32, save <-chan *DBInfo) {
	loggo.Info("Saver start")

	for job := range save {
		loggo.Info("receive save job %v %v %v", job.Title, job.Name, job.Url)

		InsertSpider(job.Title, job.Name, job.Url)

		atomic.AddInt32(jobs, -1)
	}

	loggo.Info("Saver end")
}
