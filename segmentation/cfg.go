package segmentation

import (
	"github.com/astaxie/beego/config"
)

var (
	INPUT_FILE   = ""
	MATCH_STR    = ""
	INDEX_STR    = ""
	TYPE_STR     = ""
	OUT_PATH     = ""
	SEG_MAX_LINE = 0
	SEG_MAX_SIZE = 0
	FILDS_ARRAY  = ""
)

func LoadCfg() error {
	iniconf, err := config.NewConfig("ini", "config.ini")

	if nil != err {
		return err
	}

	INPUT_FILE = iniconf.String("input_file")
	MATCH_STR = iniconf.String("match_str")

	INDEX_STR = iniconf.String("index_str")
	TYPE_STR = iniconf.String("type_str")
	OUT_PATH = iniconf.String("out_path")
	SEG_MAX_LINE, _ = iniconf.Int("seg_max_line")
	SEG_MAX_SIZE, _ = iniconf.Int("seg_max_size")

	FILDS_ARRAY = iniconf.String("filds_array")

	return nil
}
