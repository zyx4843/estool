package segmentation

import (
	"bytes"
	"fmt"
	"mywork/estool/segmentation/jsonproc"
	"mywork/estool/segmentation/mmapfile"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const (
	READ_BUF_LEN   = 1024 * 1024
	MAX_THREAD_NUM = 1000
)

type Segmentation struct {
	readFile   *mmapfile.MmapReadFile
	regexpObj  *regexp.Regexp
	indexStr   string //索引
	typeStr    string
	outPath    string   //输出目录
	segMaxLine int      //分割文件的最大行数
	fileSize   int64    //文件大小
	filds      []string //字段的key值
	segMaxSize int      //分割文件的最大大小
}

//
func (this *Segmentation) Init(fileName, matchstr, _index, _type, outPath string, segMaxLine, segMaxSize int, fildstrs ...string) error {

	file, err := os.Open(fileName)
	if nil != err {
		return err
	}
	fi, err := file.Stat()
	if nil != err {
		file.Close()
		return err
	}

	this.fileSize = fi.Size()
	file.Close()

	if 5 > this.fileSize {
		return fmt.Errorf("file too small")
	}

	fileArray := strings.Split(fileName, ".")
	path := fmt.Sprintf("%v%c%v", outPath, os.PathSeparator, fileArray[0])
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return err
	}

	this.readFile, err = mmapfile.NewMmapReadFile(fileName)
	if nil != err {
		return err
	}

	this.regexpObj, err = regexp.Compile(matchstr)
	if err != nil {
		return err
	}

	this.outPath = path
	this.indexStr = _index
	this.typeStr = _type
	this.segMaxLine = segMaxLine
	this.filds = fildstrs
	this.segMaxSize = segMaxSize

	runtime.GOMAXPROCS(runtime.NumCPU())

	return nil
}

func (this *Segmentation) Quit() {

	if this.readFile != nil {
		this.readFile.Close()
		this.readFile = nil
	}

	if nil != this.regexpObj {
		this.regexpObj = nil
	}
}

func (this *Segmentation) Run(routineNum int) error {

	if MAX_THREAD_NUM < routineNum {
		return fmt.Errorf("go thread num too big")
	}

	if this.fileSize < int64(10*routineNum) {
		return fmt.Errorf("go thread num too big %v than filesize %v", routineNum, this.fileSize)
	}

	starttime := time.Now().UnixNano()

	for mmapPos := int64(0); mmapPos < this.fileSize; {
		//大文件分次映射大块
		_, err := this.readFile.Seek(mmapPos, 0)
		if err != nil {
			return err
		}
		readbuf, err := this.readFile.Read(mmapfile.DEFAULT_MMAP_BUF_LEN)
		if err != nil {
			return err
		}

		segSize := (len(readbuf) + routineNum - 1) / routineNum

		chs := make([]chan int, routineNum)
		for i := 0; i < routineNum; i++ {
			//大块分区域给不同协程去处理
			chs[i] = make(chan int)
			go this.doThread(i, readbuf, i*segSize, segSize, chs[i])
		}

		for _, c := range chs {
			<-c
		}

		if len(readbuf) < mmapfile.DEFAULT_MMAP_BUF_LEN {
			//已经到文件结尾了
			break
		} else {
			lastIndex := bytes.LastIndexByte(readbuf, '\n')
			mmapPos += int64(lastIndex)
		}
	}

	fmt.Println("160M json time ", time.Now().UnixNano()-starttime)

	return nil
}

//文件区块的开始位置和区块大小
func (this *Segmentation) doThread(SegNo int, readbuf []byte, segStart, segSize int, ch chan int) {

	defer func() {
		ch <- 1
	}()

	if 0 != segStart {
		lastIndex := bytes.LastIndexByte(readbuf[segStart-segSize:segStart], '\n')
		if -1 == lastIndex {
			panic(fmt.Errorf("找不到行开始或结束"))
			return
		}
		segStart = segStart - segSize + lastIndex + 1
		segSize += segSize - lastIndex - 1
	}

	if segStart+segSize > len(readbuf) {
		//最后一个区域
		segSize = len(readbuf) - segStart
	}

	/*es := jsonproc.NewEsInput("http://192.168.190.139:9200")
	if nil == es {
		panic("es连不上")
		return
	}*/
	jsonfile := jsonproc.NewJsonFile(this.indexStr, this.typeStr, this.outPath, SegNo, this.segMaxLine, this.segMaxSize)

	segpos := 0
	readpos := 0
	for segpos < segSize {
		readpos = segStart + segpos
		end := bytes.IndexByte(readbuf[readpos:segStart+segSize], '\n')
		if 0 > end {
			end = segSize - segpos
			//fmt.Println("not find \\n ", readpos, segStart+segSize)
		}
		indexs := this.regexpObj.FindSubmatchIndex(readbuf[readpos : readpos+end])
		//fmt.Println(indexs)
		if nil == indexs {
			segpos += end + 1
			//fmt.Println("not match", string(readbuf[readpos:readpos+end]))
			continue
		}

		if len(this.filds)*2+2 != len(indexs) {
			segpos += indexs[0]
			fmt.Println("regexp error :", this.filds, len(indexs))
			break
		}

		jsonstr := "{\"" + this.filds[0] + "\":\"" + string(readbuf[readpos+indexs[2]:readpos+indexs[3]]) + "\""
		for i := 1; i < len(this.filds); i++ {
			jsonstr += ",\"" + this.filds[i] + "\":\"" + string(readbuf[readpos+indexs[i*2+2]:readpos+indexs[i*2+3]]) + "\""
		}
		jsonstr += "}\n"

		//es.AddData("go", "account", jsonstr)
		err := jsonfile.AddLine(jsonstr)
		if err != nil {
			fmt.Println("thread %v addline fail:", err)
			break
		}

		segpos += end + 1
	}

	jsonfile.Close()

	//es.Quit()
}
