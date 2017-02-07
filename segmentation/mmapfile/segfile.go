package mmapfile

import (
	"bytes"
	"fmt"
	. "github.com/edsrzf/mmap-go"
	"os"
)

/**********************
其实可以不用分割
***********************/

const (
	MAX_SEG_FILE_SIZE = 200 * 1024 * 1024 //每个文件最大值(一次性内存映射的值)
)

//分割后的文件名是“源文件名.seg*.tmp”
func SegFile(filename string, segSize int64) error {

	if segSize%PAGE_SIZE != 0 {
		return fmt.Errorf("分割的文件大小请选择页的倍数")
	}

	file, err := os.Open(filename)
	if nil != err {
		return err
	}

	defer file.Close()

	fi, err := file.Stat()
	if nil != err {
		return err
	}

	if fi.Size() < segSize {
		return nil
	}

	segNum := int((fi.Size() + segSize - 1) / segSize)

	remainBuf := make([]byte, 0)
	for i := 0; i < segNum-1; i++ {
		readbuf, err := MapRegion(file, int(segSize), RDONLY, 0, segSize*int64(i))
		if err != nil {
			return err
		}
		lastIndex := bytes.LastIndexByte(readbuf, '\n')
		if -1 == lastIndex {
			return fmt.Errorf("文件内容格式不对")
		}
		err = createFile(filename+fmt.Sprintf(".seg%v.tmp", i), remainBuf, readbuf[:lastIndex])
		if nil != err {
			return err
		}
		remainBuf := make([]byte, len(readbuf)-lastIndex)
		copy(remainBuf, readbuf[lastIndex:])
	}

	//最后一个文件
	remainSize := int(fi.Size() % segSize)
	lastbuf := make([]byte, 0)
	if remainSize != 0 {
		lastbuf, err = MapRegion(file, remainSize, RDONLY, 0, segSize*int64(segNum-1))
		if err != nil {
			return err
		}
	}

	if len(remainBuf) != 0 || len(lastbuf) != 0 {
		err = createFile(filename+fmt.Sprintf(".seg%v.tmp", segNum-1), remainBuf, lastbuf)
		if nil != err {
			return err
		}
	}

	return nil
}

func createFile(filename string, oldBuf, newBuf []byte) error {
	writefile, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer writefile.Close()

	writeSize := len(oldBuf) + len(newBuf)
	_, err = writefile.Seek(int64(writeSize-1), 0)
	if err != nil {
		return err
	}
	_, err = writefile.Write([]byte(" "))
	if err != nil {
		return err
	}

	writebuf, err := MapRegion(writefile, writeSize, RDWR, 0, 0)
	if err != nil {
		return err
	}

	copy(writebuf, oldBuf)
	copy(writebuf[len(oldBuf):], newBuf)

	writebuf.Unmap()

	return nil
}
