package pkread

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func NewPKReadReqBody(t *testing.T) PKReadBody {
	t.Helper()
	param := PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	return param
}

func NewOperationID(t *testing.T, size int) *string {
	opID := RandString(size)
	return &opID
}

func NewFilters(t *testing.T, prefix string, numFilters int) *[]Filter {
	t.Helper()

	filters := make([]Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		filters[i] = Filter{Column: &col, Value: &val}
	}
	return &filters
}

func NewFilter(t *testing.T, column *string, value *string) *[]Filter {
	t.Helper()
	filter := make([]Filter, 1)
	filter[0] = Filter{Column: column, Value: value}
	return &filter
}

func NewReadColumns(t *testing.T, prefix string, numReadColumns int) *[]string {
	t.Helper()
	readColumns := make([]string, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		readColumns[i] = prefix + fmt.Sprintf("%d", i)
	}
	return &readColumns
}

func NewReadColumn(t *testing.T, col string) *[]string {
	t.Helper()
	readColumns := make([]string, 1)
	readColumns[0] = col
	return &readColumns
}

func NewPKReadURL(db string, table string) string {
	url := fmt.Sprintf("%s%s", DB_OPS_EP_GROUP, DB_OPERATION)
	url = strings.Replace(url, ":"+DB_PP, db, 1)
	url = strings.Replace(url, ":"+TABLE_PP, table, 1)
	return url
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
