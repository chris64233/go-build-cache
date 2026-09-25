package buildcache

import "strconv"

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func intsToString(xs []int) string {
	if len(xs) == 0 {
		return "[]"
	}
	s := "["
	for i, x := range xs {
		if i > 0 {
			s += ","
		}
		s += strconv.Itoa(x)
	}
	return s + "]"
}
