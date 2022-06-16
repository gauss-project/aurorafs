package filestore

func pageFile(list []FileView, page1 Page) []FileView {
	if len(list) == 0 {
		return list
	}
	if page1.PageNum == 0 || page1.PageSize == 0 {
		return list
	}
	start := page1.PageSize * (page1.PageNum - 1)
	if len(list)-start >= page1.PageSize {
		list = list[start : start+page1.PageSize]
		return list
	}
	if len(list) < start {
		return nil
	} else {
		list = list[start:]
	}
	return list
}
