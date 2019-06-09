package node

import (
	"context"
	"fmt"
	"github.com/esrrhs/go-engine/src/common"
	"github.com/esrrhs/go-engine/src/loggo"
	"os/exec"
	"path/filepath"
	"time"
)

func Run(script string, silent bool, timeout int, param ...string) string {

	d := time.Now().Add(time.Duration(timeout) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)

	defer cancel() // releases resources if slowOperation completes before timeout elapses

	if !silent {
		loggo.Info("node Run start %v %v %v ", script, timeout, fmt.Sprint(param))
	}

	tmp := common.GetNodeDir() + "/" + script
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
		if !silent {
			loggo.Warn("node Run fail %v %v %v", cmd.Args, outstr, ctx.Err())
		}
		return ""
	}

	if !silent {
		loggo.Info("node Run ok %v %v", cmd.Args, time.Now().Sub(begin))
		loggo.Info("%v", outstr)
	}

	return outstr
}
