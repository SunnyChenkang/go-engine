package texas

import (
	"bufio"
	"github.com/esrrhs/go-engine/src/loggo"
	"io"
	"os"
	"strconv"
	"strings"
)

func Load(file string) {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 7})

	LoadColor("texas_data_color.txt")
	LoadNormal("texas_data_normal.txt")
	LoadColor("texas_data_extra_color_6.txt")
	LoadNormal("texas_data_extra_normal_6.txt")
	LoadColor("texas_data_extra_color_5.txt")
	LoadNormal("texas_data_extra_normal_5.txt")

}

type KeyData struct {
	index   int
	postion int
	max     int64
	ty      int
}

var colorMap map[int64]KeyData
var normalMap map[int64]KeyData

func LoadNormal(file string) {
	loggo.Debug("start LoadNormal %v", file)

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil || io.EOF == err {
			break
		}

		params := strings.Split(line, " ")

		key, _ := strconv.ParseInt(params[0], 10, 64)
		i, _ := strconv.ParseInt(params[1], 10, 32)
		index, _ := strconv.ParseInt(params[2], 10, 32)
		max, _ := strconv.ParseInt(params[5], 10, 64)
		ty, _ := strconv.ParseInt(params[7], 10, 32)

		keyData := KeyData{int(index), int(i), max, int(ty)}
		normalMap[key] = keyData
	}

	loggo.Debug("end LoadNormal %v", file)
}

func LoadColor(file string) {
	loggo.Debug("start LoadColor %v", file)

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil || io.EOF == err {
			break
		}

		params := strings.Split(line, " ")

		key, _ := strconv.ParseInt(params[0], 10, 64)
		i, _ := strconv.ParseInt(params[1], 10, 32)
		index, _ := strconv.ParseInt(params[2], 10, 32)
		max, _ := strconv.ParseInt(params[5], 10, 64)
		ty, _ := strconv.ParseInt(params[7], 10, 32)

		keyData := KeyData{int(index), int(i), max, int(ty)}
		colorMap[key] = keyData
	}

	loggo.Debug("end LoadColor %v", file)
}
