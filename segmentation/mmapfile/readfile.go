package mmapfile

import (
	"fmt"
	"os"
	//"syscall"
	. "github.com/edsrzf/mmap-go"
)

const (
	DEFAULT_MMAP_BUF_LEN = 200 * 1024 * 1024
	PAGE_SIZE            = 65536 //window 64KB, and Unixes is 4KB, so use 64KB
)

type MmapReadFile struct {
	file       *os.File
	filesize   int64
	mmapBuf    MMap
	mmapOffset int64
	mmapLen    int
	readOffset int64
}

//@param name filename
func NewMmapReadFile(name string) (*MmapReadFile, error) {
	var err error
	mmapFile := new(MmapReadFile)

	mmapFile.file, err = os.Open(name)
	if nil != err {
		//
		return nil, err
	}
	fi, err := mmapFile.file.Stat()
	if nil != err {
		return nil, err
	}

	mmapFile.filesize = fi.Size()
	mmapFile.mmapBuf = nil
	mmapFile.mmapOffset = 0
	mmapFile.mmapLen = DEFAULT_MMAP_BUF_LEN
	mmapFile.readOffset = 0

	if mmapFile.filesize < int64(mmapFile.mmapLen) {
		mmapFile.mmapLen = int(mmapFile.filesize)
	}

	return mmapFile, nil
}

func (this *MmapReadFile) Close() error {
	if nil != this.mmapBuf {
		err := this.mmapBuf.Unmap()
		if nil != err {
			return err
		}
	}
	return this.file.Close()
}

func (this *MmapReadFile) Size() int64 {
	return this.filesize
}

func (this *MmapReadFile) GetCurOffset() int64 {
	return this.readOffset
}

//@param length "want read []byte len"
//@return "if return len < length is read EOF"
func (this *MmapReadFile) Read(length int) ([]byte, error) {
	if length <= 0 {
		return make([]byte, 0), fmt.Errorf("read length not has negative or zero")
	}

	if (int64(length) + this.readOffset) > this.filesize {
		length = int(this.filesize - this.readOffset)
	}

	if nil != this.mmapBuf {
		if (int64(length) + this.readOffset) > (int64(len(this.mmapBuf)) + this.mmapOffset) {
			err := this.mmapBuf.Unmap()
			if nil != err {
				return make([]byte, 0), err
			}

			mmapOffset := this.readOffset
			mmaplen := length

			remainderOffset := mmapOffset % PAGE_SIZE
			if 0 != remainderOffset {
				mmapOffset -= remainderOffset
				mmaplen += int(remainderOffset)
			}

			if (mmaplen > this.mmapLen) || (mmapOffset+int64(this.mmapLen) > this.filesize) {
				this.mmapBuf, err = MapRegion(this.file, mmaplen, RDONLY, 0, mmapOffset)
				//syscall.Mmap(int(this.file.Fd()), this.readOffset, length, syscall.PROT_READ, syscall.MAP_PRIVATE)
				if err != nil {
					return make([]byte, 0), err
				}
				this.mmapOffset = mmapOffset
				this.readOffset += int64(length)

				return this.mmapBuf[remainderOffset:], nil
			} else {
				this.mmapBuf, err = MapRegion(this.file, this.mmapLen, RDONLY, 0, mmapOffset)
				//this.mmapBuf, err = syscall.Mmap(int(this.file.Fd()), this.readOffset, this.mmapLen, syscall.PROT_READ, syscall.MAP_PRIVATE)
				if err != nil {
					return make([]byte, 0), err
				}

				this.mmapOffset = mmapOffset
				this.readOffset += int64(length)

				return this.mmapBuf[remainderOffset : remainderOffset+int64(length)], nil
			}
		} else {
			startpos := this.readOffset - this.mmapOffset
			endpos := startpos + int64(length)
			this.readOffset += int64(length)

			return this.mmapBuf[startpos:endpos], nil
		}
	} else {
		mmapOffset := this.readOffset
		mmaplen := length

		remainderOffset := mmapOffset % PAGE_SIZE
		if 0 != remainderOffset {
			mmapOffset -= remainderOffset
			mmaplen += int(remainderOffset)
		}

		var err error
		if (mmaplen > this.mmapLen) || (mmapOffset+int64(this.mmapLen) > this.filesize) {
			this.mmapBuf, err = MapRegion(this.file, mmaplen, RDONLY, 0, mmapOffset)
			//this.mmapBuf, err = syscall.Mmap(int(this.file.Fd()), this.readOffset, length, syscall.PROT_READ, syscall.MAP_PRIVATE)
			if err != nil {
				return make([]byte, 0), err
			}

			this.mmapOffset = mmapOffset
			this.readOffset += int64(length)

			return this.mmapBuf[remainderOffset:], nil
		} else {
			this.mmapBuf, err = MapRegion(this.file, this.mmapLen, RDONLY, 0, mmapOffset)
			//this.mmapBuf, err = syscall.Mmap(int(this.file.Fd()), this.readOffset, this.mmapLen, syscall.PROT_READ, syscall.MAP_PRIVATE)
			if err != nil {
				return make([]byte, 0), err
			}

			this.mmapOffset = mmapOffset
			this.readOffset += int64(length)

			return this.mmapBuf[remainderOffset : remainderOffset+int64(length)], nil
		}
	}

}

//@param offset "must be positive, best Multiple views of page size"
//@param whence "offset where: 0 offset begin, 1 offset current, 2 offset end"
//@return int64 "new offset"
func (this *MmapReadFile) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return this.readOffset, fmt.Errorf("offset not has negative")
	}

	var err error
	if 0 == whence {
		if offset > this.filesize {
			//return this.readOffset, fmt.Errorf("offset larger than filezie")
			err = fmt.Errorf("offset larger than filezie")
		} else {
			this.readOffset = offset
			//return this.readOffset, nil
		}
	} else if 1 == whence {
		if (offset + this.readOffset) > this.filesize {
			//return this.readOffset, fmt.Errorf("new offset larger than filezie")
			err = fmt.Errorf("new offset larger than filezie")
		}

		this.readOffset = +offset
		//return this.readOffset, nil
	} else if 2 == whence {
		if offset > this.filesize {
			err = fmt.Errorf("offset larger than filezie")
		} else {
			this.readOffset = this.filesize - offset
		}
	} else {
		err = fmt.Errorf("param whence=%v is no support", whence)
	}

	return this.readOffset, err
}
