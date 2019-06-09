package spider

import (
	"github.com/esrrhs/go-engine/src/common"
	"github.com/esrrhs/go-engine/src/loggo"
	"github.com/esrrhs/go-engine/src/shell"
	_ "github.com/mattn/go-sqlite3"
	"math"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Threadnum    int
	Buffersize   int
	Sleeptimems  int
	Deps         int
	FocusSpider  bool
	Crawlfunc    string // simple,puppeteer
	CrawlTimeout int
	CrawlRetry   int
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

func Ini() {
	if runtime.GOOS == "linux" {
		go startChrome()
		go getChrome()
		for i := 0; i < 10; i++ {
			if len(gSpiderData.chromeWSEndpoint) > 0 {
				break
			}
			time.Sleep(time.Second)
		}

		if len(gSpiderData.chromeWSEndpoint) <= 0 {
			panic("spider start chrome fail")
		}

		loggo.Info("spider start chrome %v", gSpiderData.chromeWSEndpoint)
	}
}

type SpiderData struct {
	chromeWSEndpoint string
}

var gSpiderData SpiderData

func getChrome() {

	for {
		ret := shell.Run(common.GetNodeDir()+"/get_chrome.sh", common.GetNodeDir())
		ret = strings.TrimSpace(ret)
		if len(ret) > 0 {
			if ret != gSpiderData.chromeWSEndpoint {
				gSpiderData.chromeWSEndpoint = ret
				loggo.Info("spider get chromeWSEndpoint %v", gSpiderData.chromeWSEndpoint)
			}
		}
		time.Sleep(time.Second)
	}
}

func startChrome() {
	for {
		shell.RunTimeout(common.GetNodeDir()+"/close_chrome.sh", 60)
		loggo.Info("spider restart chrome ")
		shell.Run(common.GetNodeDir()+"/start_chrome.sh", common.GetNodeDir())
		time.Sleep(time.Second)
	}
}

func Start(db *DB, config Config, url string) {
	loggo.Info("Spider Start  %v", url)

	var jobs int32

	jbd := LoadJob(url)
	if jbd == nil {
		loggo.Error("Spider job LoadJob fail %v", url)
		return
	}
	dbd := LoadDone(url)
	if dbd == nil {
		loggo.Error("Spider job LoadDone fail %v", url)
		return
	}

	old := GetJobSize(jbd)
	if old == 0 {
		InsertSpiderJob(jbd, url, 0)
		DeleteSpiderDone(dbd)
	}

	old = GetJobSize(jbd)
	if old == 0 {
		loggo.Error("Spider job no jobs %v", url)
		return
	}

	crawl := make(chan *URLInfo, config.Buffersize)
	parse := make(chan *PageInfo, config.Buffersize)
	save := make(chan *DBInfo, config.Buffersize)

	atomic.AddInt32(&jobs, int32(GetJobSize(jbd)))

	entry, deps := PopSpiderJob(jbd, int(math.Min(float64(old), float64(config.Buffersize))))
	if len(entry) == 0 {
		loggo.Error("Spider job no jobs %v", url)
		return
	}

	for i, u := range entry {
		crawl <- &URLInfo{u, deps[i]}
	}

	var jobsCrawlerTotal int32
	var jobsCrawlerFail int32

	for i := 0; i < config.Threadnum; i++ {
		go Crawler(jbd, dbd, config, &jobs, crawl, parse, &jobsCrawlerTotal, &jobsCrawlerFail,
			config.Crawlfunc, config.CrawlTimeout, config.CrawlRetry)
		go Parser(jbd, dbd, config, &jobs, crawl, parse, save)
		go Saver(db, &jobs, save)
	}

	for {
		tmpurls, tmpdeps := PopSpiderJob(jbd, 1024)
		if len(tmpurls) == 0 {
			time.Sleep(time.Second)
			if jobs <= 0 {
				time.Sleep(time.Second)
				if jobs <= 0 {
					break
				}
			}
		} else {
			for i, url := range tmpurls {
				crawl <- &URLInfo{url, tmpdeps[i]}
			}
		}
	}

	loggo.Info("Spider jobs done crawl %v, failed %v", jobsCrawlerTotal, jobsCrawlerFail)

	close(crawl)
	close(parse)
	close(save)

	loggo.Info("Spider end %v %v", GetSize(db), GetDoneSize(dbd))
}

func Crawler(jbd *JobDB, dbd *DoneDB, config Config, jobs *int32, crawl <-chan *URLInfo, parse chan<- *PageInfo,
	jobsCrawlerTotal *int32, jobsCrawlerTotalFail *int32, crawlfunc string, crawlTimeout int, crawlRetry int) {
	loggo.Info("Crawler start")
	for job := range crawl {
		//loggo.Info("receive crawl job %v", job)

		ok := HasDone(dbd, job.Url)
		if !ok {
			InsertSpiderDone(dbd, job.Url)
			if job.Deps < config.Deps {
				atomic.AddInt32(jobsCrawlerTotal, 1)
				var pg *PageInfo
				for t := 0; t < crawlRetry; t++ {
					if crawlfunc == "simple" {
						pg = simplecrawl(job)
					} else if crawlfunc == "puppeteer" {
						pg = puppeteercrawl(job, crawlTimeout)
					}
					if pg != nil {
						break
					}
				}
				if pg != nil {
					loggo.Info("crawl job ok %v %v %v", job.Url, pg.Title, len(pg.Son))
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

func Parser(jbd *JobDB, dbd *DoneDB, config Config, jobs *int32, crawl chan<- *URLInfo, parse <-chan *PageInfo, save chan<- *DBInfo) {
	loggo.Info("Parser start")

	for job := range parse {
		//loggo.Info("receive parse job %v %v", job.Title, job.UI.Url)

		srcURL, err := url.Parse(job.UI.Url)
		if err != nil {
			continue
		}

		for _, s := range job.Son {
			sonurl := s.UI.Url

			if strings.HasPrefix(sonurl, "#") {
				continue
			}

			if sonurl == "/" {
				continue
			}

			if strings.Contains(sonurl, "javascript:void(0)") {
				continue
			}

			ss := strings.ToLower(sonurl)

			ok := false
			if strings.HasPrefix(ss, "thunder://") || strings.HasPrefix(ss, "magnet:?") ||
				strings.HasPrefix(ss, "ed2k://") {
				ok = true
			}

			if strings.HasSuffix(ss, ".mp4") || strings.HasSuffix(ss, ".rmvb") || strings.HasSuffix(ss, ".mkv") ||
				strings.HasSuffix(ss, ".avi") || strings.HasSuffix(ss, ".mpg") || strings.HasSuffix(ss, ".mpeg") ||
				strings.HasSuffix(ss, ".wmv") ||
				strings.HasSuffix(ss, ".torrent") {
				ok = true
			}

			if ok {
				di := DBInfo{job.Title, s.Name, sonurl}
				atomic.AddInt32(jobs, 1)
				save <- &di

				//loggo.Info("receive parse ok %v %v %v", job.Title, s.Name, sonurl)
			} else {

				if s.UI.Deps >= config.Deps {
					continue
				}

				if strings.HasPrefix(ss, "http://") || strings.HasPrefix(ss, "https://") {

				} else if strings.HasPrefix(ss, "/") {
					sonurl = srcURL.Scheme + "://" + srcURL.Host + sonurl
				} else {
					dir := srcURL.Path

					dirIndex := strings.LastIndex(dir, "/")
					if dirIndex >= 0 {
						dir = dir[0:dirIndex]
					} else {
						dir = ""
					}
					sonurl = srcURL.Scheme + "://" + srcURL.Host + dir + "/" + sonurl

					mIndex := strings.Index(sonurl, "#")
					if mIndex >= 0 {
						sonurl = sonurl[0:mIndex]
					}
				}

				_, err := url.Parse(sonurl)
				if err != nil {
					continue
				}

				var tmp *URLInfo

				finded := HasDone(dbd, sonurl)
				if !finded {
					if config.FocusSpider {
						dstURL, dsterr := url.Parse(sonurl)

						if dsterr == nil {
							dstParams := strings.Split(dstURL.Host, ".")
							srcParams := strings.Split(srcURL.Host, ".")

							if len(dstParams) >= 2 && len(srcParams) >= 2 &&
								dstParams[len(dstParams)-1] == srcParams[len(srcParams)-1] &&
								dstParams[len(dstParams)-2] == srcParams[len(srcParams)-2] {
								tmp = &URLInfo{sonurl, s.UI.Deps}
							}
						}
					} else {
						tmp = &URLInfo{sonurl, s.UI.Deps}
					}
				}

				if tmp != nil {
					hasJob := HasJob(jbd, tmp.Url)
					if !hasJob {
						atomic.AddInt32(jobs, 1)
						InsertSpiderJob(jbd, tmp.Url, tmp.Deps)

						//loggo.Info("parse spawn job %v %v %v", job.UI.Url, sonurl, GetJobSize(src))
					}
				}
			}
		}

		atomic.AddInt32(jobs, -1)
	}
	loggo.Info("Parser end")
}

func Saver(db *DB, jobs *int32, save <-chan *DBInfo) {
	loggo.Info("Saver start")

	for job := range save {
		//loggo.Info("receive save job %v %v %v", job.Title, job.Name, job.Url)

		InsertSpider(db, job.Title, job.Name, job.Url)

		atomic.AddInt32(jobs, -1)
	}

	loggo.Info("Saver end")
}
