package innobase

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	fileOffsetPageChecksum uint8 = 0 // 页的检验和，4 字节
	fileOffsetPageNo uint8 = 4 // 页号，4 字节
	fileOffsetPagePrev uint8 = 8 // 上一页的页号，4 字节
	fileOffsetPageNext uint8 = 12 // 下一页的页号，4 字节
	fileOffsetPageLsn uint8 = 16 // 页的最新 Lsn，8 字节
	fileOffsetPageType uint8 = 24 // 页的类型，2 字节
	fileOffsetPageFlushedLsn uint8 = 26 // 被刷新到磁盘的 Lsn， 8 字节
	fileOffsetSpaceId uint8 = 34 // 表空间 ID，4 字节
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

const (
	spaceIdSize uint8 = size4
	pageNoSize uint8 = size4
)

const (
	pageTypeAllocated uint16 = 0
	pageTypeUndoLog uint16 = 2
	pageTypeInode uint16 = 3
	pageTypeIBufFreeList uint16 = 4
	pageTypeIBufBitmap uint16 = 5
	pageTypeSys uint16 = 6
	pageTypeTrxSys uint16 = 7
	pageTypeFSP uint16 = 8
	pageTypeXDES uint16 = 9
	pageTypeBlob uint16 = 10
	pageTypeZBlob uint16 = 11
	pageTypeZBlob2 uint16 = 12
	pageTypeUnknown uint16 = 13
	pageTypeCompressed uint16 = 14
	pageTypeEncrptyed uint16 = 15
	pageTypeCompressedAndEncrypted uint16 = 16
	pageTypeEncryptedRTree uint16 = 17
	pageTypeRTree uint16 = 17854
	pageTypeIndex uint16 = 17855
)

var pageTypeMap = map[uint16]string {
	pageTypeAllocated: "Freshly Allocated",
	pageTypeUndoLog: "Undo Log",
	pageTypeInode: "Inode",
	pageTypeIBufFreeList: "Change Buffer Free List",
	pageTypeIBufBitmap: "Change Buffer Bitmap",
	pageTypeSys: "System Page",
	pageTypeTrxSys: "Transaction Page",
	pageTypeFSP: "File Space Header",
	pageTypeXDES: "Extent Descriptor",
	pageTypeBlob: "Uncompressed Blob Page",
	pageTypeZBlob: "First Compressed Blob",
	pageTypeZBlob2: "Subsequent Compressed Blob",
	pageTypeUnknown: "Unknown Page",
	pageTypeCompressed: "Compressed Page",
	pageTypeEncrptyed: "Encrypted Page",
	pageTypeCompressedAndEncrypted: "Compressed And Encrypted Page",
	pageTypeEncryptedRTree: "Encrypted RTree Page",
	pageTypeRTree: "RTree Page",
	pageTypeIndex: "BTree Page",
}

type File struct {
	path string
	pageSize uint16
	fileHandler *os.File
	pageNo uint32
}

func NewFile(path string) *File {
	f := &File{
		pageSize: pageSize16,
		pageNo: 1,
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

func (file *File)SetPageNo(pageNo uint32) error {
	errPrefix := "File::SetPageNo"
	if err := file.CheckPageNo(pageNo, errPrefix); err != nil {
		return err
	}

	file.pageNo = pageNo

	return nil
}

func (file *File)GetPath() string {
	return file.path
}

func (file *File)GetFileHandler() (*os.File, error) {
	errPrefix := "File::GetFileHandler()"
	err := file.initFileHandler()
	if err != nil {
		return nil, fmt.Errorf("%s: [%s]", errPrefix, err)
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

func (file *File)GetSpaceId() (uint32, error)  {
	errPrefix := "TableSpace::GetSpaceId()"
	spaceId, err := file.getUint32Header(uint16(fileOffsetSpaceId))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return spaceId, nil
}

func (file *File)GetPageNo() (uint32, error) {
	errPrefix := "TableSpace::GetPageNo()"

	filePageNo, err := file.getUint32Header(uint16(fileOffsetPageNo))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return filePageNo, nil
}

func (file *File)GetPageType() (uint16, error) {
	errPrefix := "TableSpace::GetPageType()"

	pageType, err := file.getUint16Header(uint16(fileOffsetPageType))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return pageType, nil
}

func (file *File)IsBTreePage(pageType uint16) bool {
	return pageType == pageTypeIndex
}

func (file *File)CheckPageNo(pageNo uint32, errPrefix string) error {
	if pageNo < 1 {
		return fmt.Errorf("%s: [invalid page no %d]", errPrefix, pageNo)
	}

	return nil
}

func (file *File)getUint16Header(fieldOffset uint16) (uint16, error) {
	errPrefix := "File::getInt16Value()"
	value, err := file.getUintHeader(fieldOffset, size2)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return value.(uint16), nil
}

func (file *File)getUint32Header(fieldOffset uint16) (uint32, error) {
	errPrefix := "File::getInt32Value()"
	value, err := file.getUintHeader(fieldOffset, size4)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return value.(uint32), nil
}

func (file *File)getUint64Header(fieldOffset uint16) (uint64, error) {
	errPrefix := "File::getInt64Value()"
	value, err := file.getUintHeader(fieldOffset, size8)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return value.(uint64), nil
}

func (file *File)getUintHeader(fieldOffset uint16, size uint8) (interface{}, error) {
	errPrefix := "File::getIntValue()"
	if fieldOffset < 0 {
		return 0, fmt.Errorf("%s: [invalid field offset %d]", errPrefix, fieldOffset)
	}
	if size <= 0 {
		return 0, fmt.Errorf("%s: [invalid size %d]", errPrefix, size)
	}

	if err := file.initFileHandler(); err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	offset := int64(file.pageNo - 1) * int64(file.pageSize) + int64(fieldOffset)
	if _, err := file.fileHandler.Seek(offset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	var err error
	var retValue interface{}
	switch size {
	case 1:
		value := uint8(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 2:
		value := uint16(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 4:
		value := uint32(0)
		err = binary.Read(file.fileHandler, binary.BigEndian, &value)
		retValue = value
	case 8:
		value := uint64(0)
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

func (file *File)getPageHeaderAbsoluteOffset(fieldOffset uint16) int64 {
	return int64(file.pageNo - 1) * int64(file.pageSize) + int64(fileHeaderSize) + int64(fieldOffset)
}