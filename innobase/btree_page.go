package innobase

import "fmt"

const (
	pageOffsetNSlots uint16 = 0 // 页中分组（槽）的数量，2 字节
	pageOffsetHeapTop uint16 = 2 // 未使用的空间的首地址，如果插入新记录时，不能重用已删除记录，则插入到这个位置，2 字节
	pageOffsetNHeap uint16 = 4 // 页中记录的数量（有效记录数量、infimum、supremum、deleted_mark 为 1 的记录数量之和，第 15 位存储了访记录所属页的格式），2 字节
	pageOffsetFree uint16 = 6 // 已删除记录链表的首地址，2 字节
	pageOffsetGarbage uint16 = 8 // 已删除记录占用的总字节数，2 字节
	pageOffsetLastInsert uint16 = 10 // 页中最后插入记录在页中的偏移量，2 字节
	pageOffsetDirection uint16 = 12 // 页中最后插入记录的方向，2 字节
	pageOffsetNDirection uint16 = 14 // 同一个方向连续插入的记录数量，2 字节
	pageOffsetNRecs uint16 = 16 // 页中有效记录数量（不包含 infimum、supremum、deleted_mark 为 1 的记录），2 字节
	pageOffsetMaxTrxId uint16 = 18 // 最后修改页的事务的 ID（只有二级索引、Change Buffer中的页会有值），8 字节
	pageOffsetPageLevel uint16 = 26 // 页在索引中的层级（叶子节点层级为 0），2 字节
	pageOffsetIndexId uint16 = 28 // 页所属的索引 ID，8 字节
	pageOffsetSegTop uint16 = 36 // 页所属索引的非叶子节点段的头信息的地址（只有 B+ 树索引的根页面中会有值，Change Buffer 的根页面中不会有值），10 字节
	pageOffsetSegLeaf uint16 = 46 // 页所属索引的叶子节点的头信息的地址（只有 B+ 树索引的根页面中会有值，Change Buffer 的根页面中不会有值），10 字节
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

type BTreePage struct {
	file *File
}

func newBTreePage(file *File) BTreePage {
	return BTreePage{
		file: file,
	}
}

func (page *BTreePage)GetPageLevel(pageNo uint32) (int16, error) {
	errPrefix := "TableSpace::GetPageLevel()"
	if pageNo < 1 {
		return 0, fmt.Errorf("%s: [invalid page no %d]", errPrefix, pageNo)
	}

	file := page.file
	pageLevelOffset := (pageNo - 1) * uint32(file.pageSize) + uint32(fileHeaderSize) + uint32(pageOffsetPageLevel)
	level, err := file.getInt16Value(int32(pageLevelOffset))
	if err != nil {
		return 0, fmt.Errorf("%s, %s", errPrefix, err)
	}

	return level, nil
}

func (page *BTreePage)GetPageNo(pageNo uint32) (int32, error) {
	errPrefix := "TableSpace::GetPageNo()"
	if pageNo < 1 {
		return 0, fmt.Errorf("%s: [invalid page no: %d]", errPrefix, pageNo)
	}

	file := page.file
	numOffset := (pageNo - 1) * uint32(file.pageSize) + uint32(fileOffsetPage)
	filePageNo, err := file.getInt32Value(int32(numOffset))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return filePageNo, nil
}

func (page *BTreePage)GetPageType(pageNo uint32) (uint16, error) {
	errPrefix := "TableSpace::GetPageType()"
	if pageNo < 1 {
		return 0, fmt.Errorf("%s: invalid page no: %d", errPrefix, pageNo)
	}

	file := page.file
	typeOffset := (pageNo - 1) * uint32(file.pageSize) + uint32(fileOffsetPageType)
	pageType, err := file.getInt16Value(int32(typeOffset))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return uint16(pageType), nil
}

func (page *BTreePage)GetIndexId(pageNo uint32) (int64, error) {
	errPrefix := "TableSpace::GetIndexId()"
	if pageNo < 1 {
		return 0, fmt.Errorf("%s: [invalid page no %d]", errPrefix, pageNo)
	}

	file := page.file
	indexOffset := (pageNo - 1) * uint32(file.pageSize) + uint32(fileHeaderSize) + uint32(pageOffsetIndexId)
	indexId, err := file.getInt64Value(int32(indexOffset))
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return indexId, nil
}