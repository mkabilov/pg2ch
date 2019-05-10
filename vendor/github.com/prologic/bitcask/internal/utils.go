package internal

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func GetDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

func ParseIds(fns []string) ([]int, error) {
	var ids []int
	for _, fn := range fns {
		fn = filepath.Base(fn)
		ext := filepath.Ext(fn)
		if ext != ".data" {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	return ids, nil
}
