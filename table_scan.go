package main

import (
	ib "./innobase"
	"fmt"
)

func main()  {
	space := ib.NewTableSpace()
	/*
	_ = space.SetPath("/usr/local/mysql/data/csch/t2.ibd")
	err := space.Stats()
	if err != nil {
		fmt.Println(err)
	}

	_ = space.SetPath("/usr/local/mysql/data/ibdata1")
	err = space.Stats()
	if err != nil {
		fmt.Println(err)
	}
	 */

	path := "/usr/local/mysql/data/csch/t3.ibd"
	err := space.Stats(path)
	if err != nil {
		fmt.Println(err)
	}
}