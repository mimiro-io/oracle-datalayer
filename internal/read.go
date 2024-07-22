package layer

import common "github.com/mimiro-io/common-datalayer"

func (d *Dataset) Changes(since string, limit int, latestOnly bool) (common.EntityIterator, common.LayerError) {
	//TODO implement me
	panic("implement me")
}

func (d *Dataset) Entities(from string, limit int) (common.EntityIterator, common.LayerError) {
	//TODO implement me
	panic("implement me")
}
