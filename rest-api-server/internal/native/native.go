package native

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L./../../../data-access-rondb/build/ -lrdrclient
#cgo LDFLAGS: -L/usr/local/mysql/lib -lndbclient
#include <stdlib.h>
#include "./../../../data-access-rondb/src/rdrslib.h"
*/
import "C"

func HelloWorld() {
	C.helloWorld()
	// fmt.Printf("%d\n", int64(ret.ret_code))
	// fmt.Printf("%s\n", C.GoString(ret.message))
}
