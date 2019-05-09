package texas

import (
	"github.com/esrrhs/go-engine/src/loggo"
)

func Load(file string) {

	loggo.Ini(loggo.Config{loggo.LEVEL_DEBUG, "test", 7})

	loggo.Debug("start load %v", file)

}
