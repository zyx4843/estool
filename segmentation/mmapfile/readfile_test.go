package mmapfile

import (
	"os"
	"testing"
)

func TestMmapfile(t *testing.T) {
	f, err := os.Create("test.dat")
	for i := 0; i < 10; i++ {
		f.Write([]byte("0123456789ABCDEF"))
	}
	f.Close()

	file, err := NewMmapReadFile("test.dat")
	if err != nil {
		t.Fatalf("new NewMmapReadFile fail: %v", err)
	}

	defer func() {
		err = file.Close()
		if err != nil {
			t.Fatalf("close MmapReadFile fail: %v", err)
		}
		//os.Remove("test.dat")
	}()

	if 16*10 != file.Size() {
		t.Fatalf("MmapReadFile file size error %v != %v", 16*10, file.Size())
	}

	buff, err := file.Read(10)
	if err != nil {
		t.Fatalf("NewMmapReadFile read fail: %v", err)
	}

	if buff[3] != '3' {
		t.Fatalf("MmapReadFile read buffer error %c != 3", buff[3])
	}

	if 10 != file.GetCurOffset() {
		t.Fatalf("MmapReadFile GetCurOffset error %v != 10", file.GetCurOffset())
	}

	pos, err := file.Seek(16, 2)
	if err != nil {
		t.Fatalf("NewMmapReadFile seek fail: %v", err)
	}

	if pos != 9*16 {
		t.Fatalf("NewMmapReadFile seek return pos error : %v", pos)
	}

	buff, err = file.Read(10)
	if err != nil {
		t.Fatalf("NewMmapReadFile read fail: %v", err)
	}

	if buff[3] != '3' {
		t.Fatalf("MmapReadFile read buffer error %c != 3", buff[3])
	}
}
