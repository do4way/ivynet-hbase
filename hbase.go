package ivyhbase

import (
	"context"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

//HShell ...
type HShell struct {
	client gohbase.Client
	table  string
}

//HResult the result object from hbase.
type HResult struct {
	Value     []byte
	Timestamp time.Time
}

//PutRow insert a row data.
func (h *HShell) PutRow(rowkey string, values map[string]map[string][]byte) error {
	putRequest, err := hrpc.NewPutStr(context.Background(), h.table, rowkey, values)
	if err != nil {
		return err
	}
	_, err = h.client.Put(putRequest)
	return err
}

//PutColumnFamily insert a columnfamily data into the table.
func (h *HShell) PutColumnFamily(rowkey string, cf string, data map[string][]byte) error {
	values := map[string]map[string][]byte{cf: data}
	putRequest, err := hrpc.NewPutStr(context.Background(), h.table, rowkey, values)
	if err != nil {
		return err
	}
	_, err = h.client.Put(putRequest)
	return err
}

//PutCell insert a cell data into the table.
func (h *HShell) PutCell(rowkey string, cf string, cName string, value string) error {
	values := map[string]map[string][]byte{cf: map[string][]byte{cName: []byte(value)}}
	putRequest, err := hrpc.NewPutStr(context.Background(), h.table, rowkey, values)
	if err != nil {
		return err
	}
	_, err = h.client.Put(putRequest)
	return err

}

//GetCell ...
func (h *HShell) GetCell(rowkey string, cf string, cName string) ([]*HResult, error) {
	family := map[string][]string{cf: []string{cName}}
	getRequest, err := hrpc.NewGetStr(context.Background(), h.table, rowkey, hrpc.Families(family))
	if err != nil {
		return nil, err
	}
	getResp, err := h.client.Get(getRequest)
	if err != nil {
		return nil, err
	}
	rst := make([]*HResult, len(getResp.Cells))
	for i, cell := range getResp.Cells {
		ms := int64(*cell.Timestamp / 1000)
		rst[i] = &HResult{
			Value:     cell.Value,
			Timestamp: time.Unix(ms, int64(*cell.Timestamp)-ms*1000),
		}
	}
	return rst, nil
}

//NewHShell ...
func NewHShell(table string) *HShell {
	hbcfg := ReadConfig()
	return &HShell{
		client: gohbase.NewClient(hbcfg.zkquorum),
		table:  table,
	}
}
