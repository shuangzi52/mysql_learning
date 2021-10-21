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

func (space *TableSpace) Stats(path string) error {
	errPrefix := "TableSpace::Stats()"

	file := NewFile(path)
	page := newBTreePage(file)

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
	indexStats := map[int64]map[string]int {}

	// 页面类型统计信息
	pageTypeStats := map[uint32]int32{}

	// 读取表空间 ID
	spaceId, err := file.GetSpaceId()
	if err != nil {
		return fmt.Errorf("%s: [%s]", errPrefix, err)
	}
	stats["space_id"] = uint32(spaceId)

	for i := uint32(0); i < pageCount; i++ {
		pageNo := i + 1

		// 读取页类型
		pageType, err := page.GetPageType(pageNo)
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}
		pageTypeStats[uint32(pageType)]++

		// 读取索引 ID
		indexId, err := page.GetIndexId(pageNo)
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
		pageLevel, err := page.GetPageLevel(pageNo)
		if err != nil {
			return fmt.Errorf("%s: [%s]", errPrefix, err)
		}

		levelKey := fmt.Sprintf("level_%d_page", pageLevel)
		indexStats[indexId][levelKey]++

		// 独立表空间中各类型页面数量
		pageTypeKey := fmt.Sprintf("%s%d", pageTypePrefix, pageType)
		indexStats[indexId][pageTypeKey]++

		// 读取页号
		_, err = page.GetPageNo(pageNo)
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