package jsonproc

import (
	"fmt"
	. "github.com/edsrzf/mmap-go"
	"os"
)

const (
	DEFAULT_ES_FILE_SIZE = 5 * 1024 * 1024
)

type JsonFile struct {
	//indexStr string //索引名称
	//typeStr  string //
	indexPreix  string //bulk时的操作
	path        string
	maxline     int //最大行数，达到最大行数就保存为一个文件
	curline     int //当前添加的行数
	fileNo      int //已经保存的文件序号
	filePreix   int //文件名前缀
	maxFileSize int //文件最大大小,默认为5M
	writeFile   *os.File
	mmapBuf     MMap
	copyBuf     []byte
	curSize     int //当前文件大小
}

func NewJsonFile(_index, _type, outpath string, filePreix, maxline, maxFileSize int) *JsonFile {
	jsonFile := new(JsonFile)
	//jsonFile.indexStr = _index
	//jsonFile.typeStr = _type
	jsonFile.indexPreix = fmt.Sprintf("{\"index\":{\"_index\":\"%v\", \"_type\":\"%v\"}}\n", _index, _type)
	jsonFile.path = outpath
	jsonFile.maxline = maxline
	jsonFile.curline = 0
	jsonFile.fileNo = 0
	jsonFile.filePreix = filePreix
	jsonFile.maxFileSize = maxFileSize
	jsonFile.writeFile = nil
	jsonFile.mmapBuf = nil
	jsonFile.curSize = 0

	return jsonFile
}

func (this *JsonFile) Close() error {
	err := this.closeMmapfile()

	return err
}

func (this *JsonFile) openMmapfile() error {

	//this.curline = 0
	//this.curSize = 0
	//this.mmapBuf = make([]byte, this.maxFileSize)
	//return nil

	var err error
	filename := fmt.Sprintf("%v%c%v_%v.txt", this.path, os.PathSeparator, this.filePreix, this.fileNo)
	this.fileNo++
	this.writeFile, err = os.Create(filename)
	if err != nil {
		return err
	}

	/*_, err = this.writeFile.Seek(int64(this.maxFileSize-1), 0)
	if err != nil {
		return err
	}
	_, err = this.writeFile.Write([]byte(" "))
	if err != nil {
		return err
	}*/

	this.mmapBuf, err = MapRegion(this.writeFile, this.maxFileSize, RDWR, 0, 0)
	if err != nil {
		return err
	}

	this.copyBuf = make([]byte, this.maxFileSize)
	this.curline = 0
	this.curSize = 0

	return nil
}

func (this *JsonFile) closeMmapfile() error {
	if this.mmapBuf != nil {
		copy(this.mmapBuf, this.copyBuf)
		err := this.mmapBuf.Unmap()
		if err != nil {
			return err
		}
		this.mmapBuf = nil
	}

	if this.writeFile != nil {
		err := this.writeFile.Truncate(int64(this.curSize))
		if err != nil {
			fmt.Println(err)
		}
		err = this.writeFile.Close()
		if err != nil {
			return err
		}
		this.writeFile = nil
	}

	return nil
}

func (this *JsonFile) AddLine(line string) error {

	if nil == this.mmapBuf {
		err := this.openMmapfile()
		if err != nil {
			return err
		}
	}

	if this.curSize+len(this.indexPreix)+len(line) > this.maxFileSize || this.curline+1 > this.maxline {
		err := this.closeMmapfile()
		if err != nil {
			return err
		}
		err = this.openMmapfile()
		if err != nil {
			return err
		}
	}

	//copy(this.mmapBuf[this.curSize:], this.indexPreix)
	copy(this.copyBuf[this.curSize:], this.indexPreix)
	this.curSize += len(this.indexPreix)
	//copy(this.mmapBuf[this.curSize:], line)
	copy(this.copyBuf[this.curSize:], line)
	this.curSize += len(line)
	this.curline++

	return nil
}
