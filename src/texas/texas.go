package texas

import (
	"bufio"
	"github.com/esrrhs/go-engine/src/loggo"
	"io"
	"os"
	"strconv"
	"strings"
)

func Load() {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 7})

	LoadColor("texas_data_color.txt")
	LoadNormal("texas_data_normal.txt")
	LoadColor("texas_data_extra_color_6.txt")
	LoadNormal("texas_data_extra_normal_6.txt")
	LoadColor("texas_data_extra_color_5.txt")
	LoadNormal("texas_data_extra_normal_5.txt")

	for i := 6; i >= 2; i-- {
		loadProbility(i, "texas_data_opt_"+strconv.Itoa(i)+".txt")
	}
}

type KeyData struct {
	index   int
	postion int
	max     int64
	ty      int
}

var colorMap map[int64]KeyData
var normalMap map[int64]KeyData

type ProbilityData struct {
	avg float32
	min float32
	max float32
}

var probilityMap []map[int64]ProbilityData
var optprobilityMap []map[int64]ProbilityData

func loadProbility(i int, file string) {

	loggo.Debug("start loadProbility %v", file)

	probilityMap1 := make(map[int64]ProbilityData)
	optprobilityMap1 := make(map[int64]ProbilityData)

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
		ty, _ := strconv.ParseInt(params[1], 10, 32)
		probility, _ := strconv.ParseFloat(params[2], 32)
		min, _ := strconv.ParseFloat(params[3], 32)
		max, _ := strconv.ParseFloat(params[4], 32)

		if ty == 0 {
			probilityMap1[key] = ProbilityData{float32(probility), float32(min), float32(max)}
		} else {
			optprobilityMap1[key] = ProbilityData{float32(probility), float32(min), float32(max)}
		}
	}

	probilityMap = append(probilityMap, probilityMap1)
	optprobilityMap = append(optprobilityMap, optprobilityMap1)

	loggo.Debug("end loadProbility %v", file)
}

func LoadNormal(file string) {
	loggo.Debug("start LoadNormal %v", file)

	if normalMap == nil {
		normalMap = make(map[int64]KeyData)
	}

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

	if colorMap == nil {
		colorMap = make(map[int64]KeyData)
	}

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

type Poke struct {
	color int8
	value int8
}

var huaseName = []string{"方", "梅", "红", "黑"}
var valueName = []string{"", "", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"}

func (p *Poke) ToByte() int8 {
	return (int8)(p.color<<4 | p.value)
}

func (p *Poke) ToString() string {
	if p.value == PokeValue_GUI && p.color == PokeColor_GUI {
		return "鬼"
	}
	return huaseName[p.color] + valueName[p.value]
}

func NewPoke(byteValue int8) Poke {
	return Poke{(int8)(byteValue >> 4), (int8)(byteValue % 16)}
}

const (
	PokeColor_FANG = 0
	PokeColor_MEI  = 1
	PokeColor_HONG = 2
	PokeColor_HEI  = 3
	PokeColor_GUI  = 5
)

const (
	PokeValue_2   = 2
	PokeValue_3   = 3
	PokeValue_4   = 4
	PokeValue_5   = 5
	PokeValue_6   = 6
	PokeValue_7   = 7
	PokeValue_8   = 8
	PokeValue_9   = 9
	PokeValue_10  = 10
	PokeValue_J   = 11
	PokeValue_Q   = 12
	PokeValue_K   = 13
	PokeValue_A   = 14
	PokeValue_GUI = 8
)

func StrToByteValue(str string) int8 {

	if str == "A" {
		return PokeValue_A
	} else if str == "K" {
		return PokeValue_K
	} else if str == "Q" {
		return PokeValue_Q
	} else if str == "J" {
		return PokeValue_J
	} else {
		ret, _ := strconv.ParseInt(str, 10, 8)
		return int8(ret)
	}
}

func StrToBytes(str string) []int8 {
	var ret []int8

	if len(str) == 0 {
		return ret
	}

	strs := strings.Split(str, ",")
	for _, s := range strs {
		ret = append(ret, StrToByte(s))
	}

	return ret
}

func StrToByte(str string) int8 {
	if strings.HasPrefix(str, "方") {
		p := Poke{PokeColor_FANG, StrToByteValue(string([]rune(str)[1:]))}
		return p.ToByte()
	} else if strings.HasPrefix(str, "梅") {
		p := Poke{PokeColor_MEI, StrToByteValue(string([]rune(str)[1:]))}
		return p.ToByte()
	} else if strings.HasPrefix(str, "红") {
		p := Poke{PokeColor_HONG, StrToByteValue(string([]rune(str)[1:]))}
		return p.ToByte()
	} else if strings.HasPrefix(str, "黑") {
		p := Poke{PokeColor_HEI, StrToByteValue(string([]rune(str)[1:]))}
		return p.ToByte()
	} else if strings.HasPrefix(str, "鬼") {
		p := Poke{PokeColor_GUI, PokeValue_GUI}
		return p.ToByte()
	}

	return 0
}

func BytesToStr(bytes []int8) string {
	return KeyToStr(GenCardBind(bytes))
}

func GenCardBind(bytes []int8) int64 {
	var ret int64

	for _, b := range bytes {
		ret = ret*100 + int64(b)
	}

	return ret
}

func KeyToPoke(k int64) []Poke {

	var cs []Poke

	if k > 1000000000000 {
		cs = append(cs, NewPoke((int8)(k%100000000000000/1000000000000)))
	}
	if k > 10000000000 {
		cs = append(cs, NewPoke((int8)(k%1000000000000/10000000000)))
	}
	if k > 100000000 {
		cs = append(cs, NewPoke((int8)(k%10000000000/100000000)))
	}
	if k > 1000000 {
		cs = append(cs, NewPoke((int8)(k%100000000/1000000)))
	}
	if k > 10000 {
		cs = append(cs, NewPoke((int8)(k%1000000/10000)))
	}
	if k > 100 {
		cs = append(cs, NewPoke((int8)(k%10000/100)))
	}
	if k > 1 {
		cs = append(cs, NewPoke((int8)(k%100/1)))
	}
	return cs
}

func KeyToStr(key int64) string {
	var ret string

	cs := KeyToPoke(key)
	for _, cs := range cs {
		ret += cs.ToString()
	}

	return ret
}
