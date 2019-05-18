package spider

import (
	"github.com/PuerkitoBio/goquery"
	"github.com/axgle/mahonia"
	"github.com/esrrhs/go-engine/src/loggo"
	"net/http"
	"strings"
)

func simplecrawl(ui *URLInfo) *PageInfo {

	url := ui.Url
	//loggo.Info("crawl %v", url)

	res, err := http.Get(url)
	if err != nil {
		loggo.Warn("crawl http Get fail %v %v", url, err)
		return nil
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		loggo.Warn("crawl http StatusCode fail %v %v", url, res.StatusCode)
		return nil
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		loggo.Warn("crawl http NewDocumentFromReader fail %v %v", url, err)
		return nil
	}

	gb2312 := false
	doc.Find("META").Each(func(i int, s *goquery.Selection) {
		content, ok := s.Attr("content")
		if ok {
			if strings.Contains(content, "gb2312") {
				gb2312 = true
			}
		}
	})

	pg := PageInfo{}
	pg.UI = *ui
	doc.Find("title").Each(func(i int, s *goquery.Selection) {
		if pg.Title == "" {
			pg.Title = s.Text()
			pg.Title = strings.TrimSpace(pg.Title)
			if gb2312 {
				enc := mahonia.NewDecoder("gbk")
				pg.Title = enc.ConvertString(pg.Title)
			}
			//loggo.Info("simple crawl title %v", pg.Title)
		}
	})

	// Find the items
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the band and title
		name := s.Text()
		href, ok := s.Attr("href")
		if ok {
			href = strings.TrimSpace(href)
			name = strings.TrimSpace(name)
			name = strings.Replace(name, "\n", " ", -1)
			if gb2312 {
				enc := mahonia.NewDecoder("gbk")
				href = enc.ConvertString(href)
				name = enc.ConvertString(name)
			}
			//loggo.Info("simple crawl link %v %v %v %v", i, pg.Title, name, href)

			if len(href) > 0 {
				pgl := PageLinkInfo{URLInfo{href, ui.Deps + 1}, name}
				pg.Son = append(pg.Son, pgl)
			}
		}
	})

	//if len(pg.Son) == 0 {
	//	html, _ := doc.Html()
	//	loggo.Warn("simple crawl no link %v html:\n%v", url, html)
	//}

	return &pg
}
