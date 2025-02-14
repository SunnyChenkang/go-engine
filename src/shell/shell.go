package shell

import (
	"context"
	"fmt"
	"github.com/esrrhs/go-engine/src/loggo"
	"os/exec"
	"path/filepath"
	"time"
)

func Run(script string, silent bool, param ...string) string {

	script = filepath.Clean(script)
	script = filepath.ToSlash(script)

	if !silent {
		loggo.Info("shell Run start %v %v ", script, fmt.Sprint(param))
	}

	var tmpparam []string
	tmpparam = append(tmpparam, script)
	tmpparam = append(tmpparam, param...)

	begin := time.Now()
	cmd := exec.Command("sh", tmpparam...)
	out, err := cmd.CombinedOutput()
	outstr := string(out)
	if err != nil {
		loggo.Warn("shell Run fail %v %v", cmd.Args, outstr)
		return ""
	}

	if !silent {
		loggo.Info("shell Run ok %v %v", cmd.Args, time.Now().Sub(begin))
		loggo.Info("%v", outstr)
	}

	return outstr
}

func RunTimeout(script string, silent bool, timeout int, param ...string) string {

	script = filepath.Clean(script)
	script = filepath.ToSlash(script)

	d := time.Now().Add(time.Duration(timeout) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)

	defer cancel() // releases resources if slowOperation completes before timeout elapses
	if !silent {
		loggo.Info("shell Run start %v %v %v ", script, timeout, fmt.Sprint(param))
	}

	var tmpparam []string
	tmpparam = append(tmpparam, script)
	tmpparam = append(tmpparam, param...)

	begin := time.Now()
	cmd := exec.CommandContext(ctx, "sh", tmpparam...)
	out, err := cmd.CombinedOutput()
	outstr := string(out)
	if err != nil {
		loggo.Warn("shell Run fail %v %v %v", cmd.Args, outstr, ctx.Err())
		return ""
	}

	if !silent {
		loggo.Info("shell Run ok %v %v", cmd.Args, time.Now().Sub(begin))
		loggo.Info("%v", outstr)
	}
	return outstr
}
