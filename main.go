package main

import (
	"fmt"
	"mywork/estool/segmentation"
	"strings"
)

func main() {

	segmentation.LoadCfg()

	fmt.Println("输入的文件名:", segmentation.INPUT_FILE)
	fmt.Println("分割规则:", segmentation.MATCH_STR)
	fmt.Println("es的索引名:", segmentation.INDEX_STR)
	fmt.Println("es的type名:", segmentation.TYPE_STR)
	fmt.Println("输出目录:", segmentation.OUT_PATH)
	fmt.Println("分割文件的最大行数:", segmentation.SEG_MAX_LINE)
	fmt.Println("分割文件的最大大小:", segmentation.SEG_MAX_SIZE)
	fmt.Println("第个分割出来对应的key值:", segmentation.FILDS_ARRAY)

	var seg segmentation.Segmentation
	//err := seg.InitRead("xh-2.txt", "([A-Za-z0-9._\\-@]+?)[\t\n\r\n ]+([!-~]+?)[\t\n\r\n ]+", "name", "password")
	//indexjson := "{\"index\":{\"_index\":\"test\",\"_type\":\"account\"}}"
	//err := seg.Init("xh-2.txt", "([\\S]+)[\\s]+([\\S]+)", "test", "account", "out", 50000, 5*1024*1024, "username", "password")
	filds := strings.Split(segmentation.FILDS_ARRAY, ",")
	err := seg.Init(segmentation.INPUT_FILE, segmentation.MATCH_STR, segmentation.INDEX_STR, segmentation.TYPE_STR, segmentation.OUT_PATH, segmentation.SEG_MAX_LINE, segmentation.SEG_MAX_SIZE, filds...)
	if nil != err {
		fmt.Println(err)
		return
	}

	err = seg.Run(4)
	if nil != err {
		fmt.Println(err)
		return
	}

	seg.Quit()
}
