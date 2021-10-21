package innobase

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	fileOffsetPageChecksum int = 0 // 页的检验和，4 字节
	fileOffsetPage int = 4 // 页号，4 字节
	fileOffsetPagePrev int = 8 // 上一页的页号，4 字节
	fileOffsetPageNext int = 12 // 下一页的页号，4 字节
	fileOffsetPageLsn int = 16 // 页的最新 Lsn，8 字节
	fileOffsetPageType int = 24 // 页的类型，2 字节
	fileOffsetPageFlushedLsn int = 26 // 被刷新到磁盘的 Lsn， 8 字节
	fileOffsetSpaceId int = 34 // 表空间 ID，4 字节
)

const (
	fileHeaderSize uint8 = 38
)

const (
	pageSize16 uint16 = 16384
)

const (
	size2 uint8 = 2
	size4 uint8 = 4
	size8 uint8 = 8
	size16 uint8 = 16
)

type File struct {
	path string
	pageSize uint16
	fileHandler *os.File
}

func NewFile(path string) *File {
	f := &File{
		pageSize: pageSize16,
	}

	// 初始化对象调用 SetPath() 的时候不会出错，所以不处理错误
	_ = f.SetPath(path)

	return f
}

func (file *File)SetPath(path string) error {
	errPrefix := "File::SetPath()"


	path = strings.TrimSpace(path)
	if file.path == path {
		return nil
	}

	file.path = path

	if file.fileHandler != nil {
		err := file.fileHandler.Close()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		file.fileHandler = nil
	}

	return nil
}

func (file *File)GetPath() string {
	return file.path
}

func (file *File)GetFileHandler() (*os.File, error) {
	errPrefix := "File::GetFileHandler()"
	err := file.initFileHandler()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", errPrefix, err)
	}

	return file.fileHandler, nil
}

func (file *File)initFileHandler() error {
	errPrefix := "File::initFileHandler()"
	if file.fileHandler == nil {
		fp, err := os.Open(file.path)
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		file.fileHandler = fp
	}

	return nil
}

func (file *File)getPageCount() (uint32, error) {
	errPrefix := "File::getPageCount()"

	_, err := file.isReadable()
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	fileInfo, err := os.Stat(file.path)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	size := fileInfo.Size()
	if size <= 0 {
		return 0, fmt.Errorf("%s: [file size is zero]", errPrefix)
	}

	pageCount := uint32(size / int64(file.pageSize))

	return pageCount, nil
}

func (file *File)GetSpaceId() (int32, error)  {
	errPrefix := "TableSpace::GetSpaceId()"
	fileOffset := fileOffsetSpaceId
	fileId, err := file.getInt32Value(int32(fileOffset))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return fileId, nil
}

func (file *File)getInt16Value(offset int32) (int16, error) {
	errPrefix := "File::getInt16Value()"
	value, err := file.getIntValue(offset, size2)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	int16Value := value.(int16)

	return int16Value, nil
}

func (file *File)getInt32Value(offset int32) (int32, error) {
	errPrefix := "File::getInt32Value()"
	value, err := file.getIntValue(offset, size4)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return value.(int32), nil
}

func (file *File)getInt64Value(offset int32) (int64, error) {
	errPrefix := "File::getInt64Value()"
	value, err := file.getIntValue(offset, size8)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return value.(int64), nil
}

func (file *File)getIntValue(offset int32, size uint8) (interface{}, error) {
	errPrefix := "File::getIntValue()"
	if offset < 0 {
		return 0, fmt.Errorf("%s: [invalid offset %d]", errPrefix, offset)
	}
	if size <= 0 {
		return 0, fmt.Errorf("%s: [invalid size %d]", errPrefix, size)
	}

	if err := file.initFileHandler(); err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	if _, err := file.fileHandler.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	var err error
	var retValue interface{}
	switch size {
	case 1:
		value := int8(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 2:
		value := int16(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 4:
		value := int32(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 8:
		value := int64(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	default:
		return 0, fmt.Errorf("%s: [unsupport size %d]", errPrefix, size)
	}

	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return retValue, nil
}

func (file *File)isReadable() (bool, error) {
	if file.path == "" {
		return false, fmt.Errorf("getPageCount: path is empty")
	}

	_, err := os.Stat(file.path)
	if err != nil {
		return false, err
	}

	if os.IsNotExist(err) {
		return false, err
	}

	if os.IsPermission(err) {
		return false, err
	}

	return true, nil
}
