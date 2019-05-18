package spider

import (
	"github.com/PuerkitoBio/goquery"
	"github.com/esrrhs/go-engine/src/loggo"
	"net/http"
)

func simplecrawl(url string) *PageInfo {

	loggo.Info("crawl %v", url)

	res, err := http.Get(url)
	if err != nil {
		loggo.Error("crawl http Get fail %v %v", url, err)
		return nil
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		loggo.Error("crawl http StatusCode fail %v %v", url, res.StatusCode)
		return nil
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		loggo.Error("crawl http NewDocumentFromReader fail %v %v", url, err)
		return nil
	}

	pg := PageInfo{}
	doc.Find("title").Each(func(i int, s *goquery.Selection) {
		if pg.Title == "" {
			pg.Title = s.Text()
			loggo.Info("simple crawl title %v", pg.Title)
		}
	})

	// Find the items
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the band and title
		name := s.Text()
		href, ok := s.Attr("href")
		if ok {
			loggo.Info("simple crawl link %v %v %v", pg.Title, name, href)
			pgl := PageLinkInfo{href, name}
			pg.Son = append(pg.Son, pgl)
		}
	})

	if len(pg.Son) == 0 {
		html, _ := doc.Html()
		loggo.Info("simple crawl link %v  %v", url, html)
	}

	return &pg
}
