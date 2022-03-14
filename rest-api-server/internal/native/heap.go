package native

import "C"
import (
	"unsafe"
)

const BUFFER_SIZE = 512

func init() {
	// if BUFFER_SIZE%C.ADDRESS_SIZE != 0 {
	// panic(fmt.Sprintf("Buffer size must be multiple of %s", C.ADDRESS_SIZE))
	// }
}
func GetBuffer() unsafe.Pointer {
	return C.malloc(C.size_t(BUFFER_SIZE))
}
