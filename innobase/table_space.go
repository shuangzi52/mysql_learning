package innobase

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type TableSpace struct {

}

func NewTableSpace() TableSpace {
	return TableSpace {}
}

func (space *TableSpace)Stats(path string) error {
	errPrefix := "TableSpace::Stats()"

	file := NewFile(path)
	page := NewBTreePage(file)

	pageCount, err := file.getPageCount()
	if err != nil {
		return fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	// 表空间统计信息
	stats := map[string]uint32 {
		"space_id": 0,
		"total_page": pageCount,
	}

	// 索引统计信息
	pageTypePrefix := "page_type_"
	indexStats := map[uint64]map[string]int {}

	// 页面类型统计信息
	pageTypeStats := map[uint16]int32{}

	// 读取表空间 ID
	spaceId, err := file.GetSpaceId()
	if err != nil {
		return fmt.Errorf("%s: [%s]", errPrefix, err)
	}
	stats["space_id"] = uint32(spaceId)

	for pageNo := uint32(1); pageNo < pageCount; pageNo++ {
		if err := file.SetPageNo(pageNo); err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		// 读取页类型
		pageType, err := file.GetPageType()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		pageTypeStats[pageType]++

		if !file.IsBTreePage(pageType) {
			continue
		}

		// 读取索引 ID
		indexId, err := page.GetIndexId()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		if indexId <= 0 {
			continue
		}

		if _, exists := indexStats[indexId]; !exists {
			indexStats[indexId] = map[string]int{}
		}

		// 读取 PAGE_LEVEL
		pageLevel, err := page.GetPageLevel()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		levelKey := fmt.Sprintf("level_%d_page", pageLevel)
		indexStats[indexId][levelKey]++

		// 独立表空间中各类型页面数量
		pageTypeKey := fmt.Sprintf("%s%d", pageTypePrefix, pageType)
		indexStats[indexId][pageTypeKey]++

		// 读取页号
		_, err = file.GetPageNo()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		/* fmt.Printf("page no = %d, level = %d, type = %d\n", pageNo, pageLevel, 0)
		if i >= 100 {
			break
		} */
	}

	fmt.Printf("Stats (%s):\n", file.GetPath())
	keys := make([]string, 0, len(stats))
	for key, _ := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("    %s: %v\n", key, stats[key])
	}
	fmt.Println()

	fmt.Println("Page Type Stats:")
	for pageType, typeCount := range pageTypeStats {
		fmt.Printf("    %s (%d): %d\n", pageTypeMap[uint16(pageType)], pageType, typeCount)
	}
	fmt.Println()

	fmt.Printf("Index Stats (%d indexes):\n", len(indexStats))
	for indexId, singleIndexStats := range indexStats {
		fmt.Printf("    %d: \n", indexId)
		keys := make([]string, 0, len(singleIndexStats))
		for indexId, _ := range singleIndexStats {
			keys = append(keys, indexId)
		}
		sort.Strings(keys)

		singleIndexPaddingPrefix := "        "
		isPrinted := false
		for _, key := range keys {
			if strings.HasPrefix(key, pageTypePrefix) {
				pageType, err := strconv.Atoi(strings.Replace(key, pageTypePrefix, "", len(pageTypePrefix)))
				if err == nil {
					fmt.Printf("%s%s (%d): %d\n", singleIndexPaddingPrefix, pageTypeMap[uint16(pageType)], pageType, singleIndexStats[key])
					isPrinted = true
				}
			}

			if !isPrinted {
				fmt.Printf("%s%s: %v\n", singleIndexPaddingPrefix, key, singleIndexStats[key])
			}
		}
	}
	fmt.Println()

	return nil
}

func (space *TableSpace)IndexHeader(path string) error {
	errPrefix := "TableSpace::indexDetail()"

	file := NewFile(path)
	page := NewBTreePage(file)

	if err := file.SetPath(path); err != nil {
		return fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	pageCount, err := file.getPageCount()
	if err != nil {
		return fmt.Errorf("%s: [%s]", errPrefix, err)
	}

	for pageNo := uint32(1); pageNo < pageCount; pageNo++ {
		if err := file.SetPageNo(pageNo); err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		// 读取页类型
		pageType, err := file.GetPageType()
		pageTypeDesc := pageTypeMap[pageType]

		fmt.Printf("页号 = %d, 页类型 = %s", pageNo, pageTypeDesc)
		if pageType != pageTypeIndex {
			fmt.Printf("\n")
			continue
		} else {
			fmt.Printf(", ")
		}

		// 读取页所属的索引 ID
		indexId, err := page.GetIndexId()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("索引 ID = %d, ", indexId)

		// 读取节点所在层级
		level, err := page.GetPageLevel()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("索引层级 = %d, ", level)

		// 读取分组（槽）的数量
		slotsCount, err := page.GetSlotsCount()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("分组 = %d, ", slotsCount)

		// 读取记录数量（含 infimum、supremum、已标记删除记录、正常记录）
		nHeap, err := page.GetHeapCount()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		nHeap = nHeap - 32768
		fmt.Printf("记录 = %d, ", nHeap)

		// 读取正常记录数量
		nRecords, err := page.GetRecordCount()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("有效记录 = %d, ", nRecords)

		// 读取空白空间首地址（未插入过记录的空间）
		heapTop, err := page.GetHeapTop()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("未使用空间首地址 = %d, ", heapTop)

		// 读取垃圾链表首地址
		free, err := page.GetFreeOffset()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("垃圾链表首地址 = %d, ", free)

		// 读取垃圾链表占用字节数
		garbage, err := page.GetGarbageSize()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("垃圾链表占用空间 = %d, ", garbage)

		// 读取最后插入记录的位置
		lastInsert, err := page.GetLastInsertOffset()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("最后插入记录数据地址 = %d, ", lastInsert)

		// 读取插入记录的方向
		direction, err := page.GetLastInsertDirection()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("插入方向 = %d, ", direction)

		// 读取同一个方向上插入的记录数量
		directionCount, err := page.GetDirectionInsertCount()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("同一方向插入记录数 = %d, ", directionCount)

		// 读取修改页的最大事务 ID，只有主键索引有该值
		maxTrxId, err := page.GetMaxTrxId()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		fmt.Printf("最后修改页的事务 ID = %d", maxTrxId)

		// 读取索引非叶子节点段的 inode 存储信息
		topSpaceID, topPageNo, topPageOffset, err := page.GetBtrSegTop()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		if topPageOffset > 0 {
			fmt.Printf(", 内结点段 [表空间 = %d, 页号 = %d, 页内位置 = %d]", topSpaceID, topPageNo, topPageOffset)
		}

		// 读取索引叶子节点段的 inode 存储信息
		leafSpaceId, leafPageNo, leafPageOffset, err := page.GetBtrSegLeaf()
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		if leafPageOffset > 0 {
			fmt.Printf(", 叶子节点段 [表空间 = %d, 页号 = %d, 页内位置 = %d]", leafSpaceId, leafPageNo, leafPageOffset)
		}

		fmt.Println()
	}

	return nil
}