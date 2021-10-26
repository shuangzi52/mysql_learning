package innobase

import "fmt"

const (
	pageOffsetNSlots uint16 = 38 // 页中分组（槽）的数量，2 字节
	pageOffsetHeapTop uint16 = 40 // 未使用的空间的首地址，如果插入新记录时，不能重用已删除记录，则插入到这个位置，2 字节
	pageOffsetNHeap uint16 = 42 // 页中记录的数量（有效记录数量、infimum、supremum、deleted_mark 为 1 的记录数量之和，第 15 位存储了访记录所属页的格式），2 字节
	pageOffsetFree uint16 = 44 // 已删除记录链表的首地址，2 字节
	pageOffsetGarbage uint16 = 46 // 已删除记录占用的总字节数，2 字节
	pageOffsetLastInsert uint16 = 48 // 页中最后插入记录在页中的偏移量，2 字节
	pageOffsetDirection uint16 = 50 // 页中最后插入记录的方向，2 字节
	pageOffsetNDirection uint16 = 52 // 同一个方向连续插入的记录数量，2 字节
	pageOffsetNRecs uint16 = 54 // 页中有效记录数量（不包含 infimum、supremum、deleted_mark 为 1 的记录），2 字节
	pageOffsetMaxTrxId uint16 = 56 // 最后修改页的事务的 ID（只有二级索引、Change Buffer中的页会有值），8 字节
	pageOffsetPageLevel uint16 = 64 // 页在索引中的层级（叶子节点层级为 0），2 字节
	pageOffsetIndexId uint16 = 66 // 页所属的索引 ID，8 字节
	pageOffsetSegLeaf uint16 = 74 // 页所属索引的叶子节点的头信息的地址（只有 B+ 树索引的根页面中会有值，Change Buffer 的根页面中不会有值），10 字节
	pageOffsetSegTop uint16 = 84 // 页所属索引的非叶子节点段的头信息的地址（只有 B+ 树索引的根页面中会有值，Change Buffer 的根页面中不会有值），10 字节
)

type BTreePage struct {
	file *File
}

func NewBTreePage(file *File) BTreePage {
	return BTreePage{
		file: file,
	}
}

func (page *BTreePage)GetPageLevel() (uint16, error) {
	errPrefix := "BTreePage::GetPageLevel()"
	level, err := page.file.getUint16Header(pageOffsetPageLevel)
	if err != nil {
		return 0, fmt.Errorf("%s, [%s]", errPrefix, err)
	}

	return level, nil
}

func (page *BTreePage)GetSlotsCount() (uint16, error) {
	errPrefix := "BTreePage::GetSlotsCount()"
	slotsCount, err := page.file.getUint16Header(pageOffsetNSlots)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return slotsCount, nil
}

func (page *BTreePage)GetHeapTop() (uint16, error) {
	errPrefix := "BTreePage::GetHeapTop()"
	heapTop, err := page.file.getUint16Header(pageOffsetHeapTop)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return heapTop, nil
}

func (page *BTreePage)GetHeapCount() (uint16, error) {
	errPrefix := "BTreePage::GetHeapCount()"
	heapCount, err := page.file.getUint16Header(pageOffsetNHeap)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return heapCount, nil
}

func (page *BTreePage)GetRecordCount() (uint16, error)  {
	errPrefix := "BTreePage::GetRecordCount()"
	recordCount, err := page.file.getUint16Header(pageOffsetNRecs)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return recordCount, nil
}

func (page *BTreePage)GetLastInsertDirection() (uint16, error) {
	errPrefix := "BTreePage::GetLastInsertDirection()"
	direction, err := page.file.getUint16Header(pageOffsetDirection)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return direction, nil
}

func (page *BTreePage)GetDirectionInsertCount() (uint16, error) {
	errPrefix := "BTreePage::GetDirectionInsertCount()"
	insertCount, err := page.file.getUint16Header(pageOffsetNDirection)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return insertCount, err
}

func (page *BTreePage)GetLastInsertOffset() (uint16, error) {
	errPrefix := "BTreePage::GetLastInsertOffset()"
	insertOffset, err := page.file.getUint16Header(pageOffsetLastInsert)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return insertOffset, nil
}

func (page *BTreePage)GetGarbageSize() (uint16, error) {
	errPrefix := "BTreePage::GetGarbageSize()"
	size, err := page.file.getUint16Header(pageOffsetGarbage)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return size, nil
}

func (page *BTreePage)GetFreeOffset() (uint16, error) {
	errPrefix := "BTreePage::GetFreeOffset()"
	freeOffset, err := page.file.getUint16Header(pageOffsetFree)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return freeOffset, nil
}

func (page *BTreePage)GetMaxTrxId() (uint64, error) {
	errPrefix := "BTreePage::GetMaxTrxId()"
	trxId, err := page.file.getUint64Header(pageOffsetMaxTrxId)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return trxId, nil
}

func (page *BTreePage)GetBtrSegTop() (uint32, uint32, uint16, error) {
	errPrefix := "BTreePage::GetBtrSegTop()"
	spaceId, pageNo, pageOffset, err := page.getBtrInodeHeader(pageOffsetSegTop)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return spaceId, pageNo, pageOffset, nil
}

func (page *BTreePage)GetBtrSegLeaf() (uint32, uint32, uint16, error) {
	errPrefix := "BTreePage::GetBtrSegLeaf()"
	spaceId, pageNo, pageOffset, err := page.getBtrInodeHeader(pageOffsetSegLeaf)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return spaceId, pageNo, pageOffset, nil
}

func (page *BTreePage)getBtrInodeHeader(inodeStartOffset uint16) (uint32, uint32, uint16, error) {
	errPrefix := "BTree::getBtrInodeHeader()"
	segTopSpaceId, err := page.file.getUint32Header(inodeStartOffset)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: [read space id: %s]", errPrefix, err)
	}

	pageNoOffset := inodeStartOffset + uint16(spaceIdSize)
	segTopPageNo, err := page.file.getUint32Header(pageNoOffset)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: [read page no: %s]", errPrefix, err)
	}

	inodeOffset := pageNoOffset + uint16(pageNoSize)
	segTopInodeOffset, err := page.file.getUint16Header(inodeOffset)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s: [read inode offset: %s]", errPrefix, err)
	}

	return segTopSpaceId, segTopPageNo, segTopInodeOffset, nil
}

func (page *BTreePage)GetIndexId() (uint64, error) {
	errPrefix := "BTreePage::GetIndexId()"

	file := page.file
	indexId, err := file.getUint64Header(pageOffsetIndexId)
	if err != nil {
		return 0, fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	return indexId, nil
}