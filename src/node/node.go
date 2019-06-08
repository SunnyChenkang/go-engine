package node

import (
	"context"
	"fmt"
	"github.com/esrrhs/go-engine/src/loggo"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

var gNodeDir string

func Ini() {

	sysType := runtime.GOOS
	loggo.Info("sysType %v", sysType)

	GOPATH := os.Getenv("GOPATH")
	loggo.Info("GOPATH %v", GOPATH)

	gpaths := strings.Split(GOPATH, ";")

	dir := ""
	for _, p := range gpaths {
		tmp := p + "/src/github.com/esrrhs/go-engine/node/" + sysType + "/node_modules/"
		tmp = filepath.Clean(tmp)
		tmp = filepath.ToSlash(tmp)
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			dir = tmp
			break
		}
	}

	if len(dir) <= 0 {
		return
	}
	loggo.Info("dir %v", dir)

	gNodeDir = dir
}

func Run(script string, timeout int, param ...string) string {
	if len(gNodeDir) <= 0 {
		Ini()
		if len(gNodeDir) <= 0 {
			loggo.Error("node Run no node dir %v %v %v", script, timeout, fmt.Sprint(param))
			return ""
		}
	}
	d := time.Now().Add(time.Duration(timeout) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)

	defer cancel() // releases resources if slowOperation completes before timeout elapses

	loggo.Info("node Run start %v %v %v ", script, timeout, fmt.Sprint(param))

	tmp := gNodeDir + "/" + script
	tmp = filepath.Clean(tmp)
	tmp = filepath.ToSlash(tmp)

	var tmpparam []string
	tmpparam = append(tmpparam, tmp)
	tmpparam = append(tmpparam, param...)

	begin := time.Now()
	cmd := exec.CommandContext(ctx, "node", tmpparam...)
	out, err := cmd.CombinedOutput()
	outstr := string(out)
	if err != nil {
		loggo.Warn("node Run fail %v %v %v", cmd.Args, outstr, ctx.Err())
		return ""
	}

	loggo.Info("node Run ok %v %v", cmd.Args, time.Now().Sub(begin))
	loggo.Info("%v", outstr)

	return outstr
}
