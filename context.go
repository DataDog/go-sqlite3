package sqlite3

/*
#include <sqlite3.h>
#include <stdlib.h>
// These wrappers are necessary because SQLITE_TRANSIENT
// is a pointer constant, and cgo doesn't translate them correctly.

static inline void my_result_text(sqlite3_context *ctx, char *p, int np) {
	sqlite3_result_text(ctx, p, np, SQLITE_TRANSIENT);
}

static inline void my_result_blob(sqlite3_context *ctx, void *p, int np) {
	sqlite3_result_blob(ctx, p, np, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"math"
	"reflect"
	"unsafe"
)

const i64 = unsafe.Sizeof(int(0)) > 4

type ZeroBlobLength int32
type Context C.sqlite3_context

// ResultBool sets the result of an SQL function.
func (c *Context) ResultBool(b bool) {
	if b {
		c.ResultInt(1)
	} else {
		c.ResultInt(0)
	}
}

// ResultBlob sets the result of an SQL function.
// (See sqlite3_result_blob, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultBlob(b []byte) {
	if i64 && len(b) > math.MaxInt32 {
		C.sqlite3_result_error_toobig((*C.sqlite3_context)(c))
		return
	}
	var p *byte
	if len(b) > 0 {
		p = &b[0]
	}
	C.my_result_blob((*C.sqlite3_context)(c), unsafe.Pointer(p), C.int(len(b)))
}

// ResultDouble sets the result of an SQL function.
// (See sqlite3_result_double, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultDouble(d float64) {
	C.sqlite3_result_double((*C.sqlite3_context)(c), C.double(d))
}

// ResultInt sets the result of an SQL function.
// (See sqlite3_result_int, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultInt(i int) {
	if i64 && (i > math.MaxInt32 || i < math.MinInt32) {
		C.sqlite3_result_int64((*C.sqlite3_context)(c), C.sqlite3_int64(i))
	} else {
		C.sqlite3_result_int((*C.sqlite3_context)(c), C.int(i))
	}
}

// ResultInt64 sets the result of an SQL function.
// (See sqlite3_result_int64, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultInt64(i int64) {
	C.sqlite3_result_int64((*C.sqlite3_context)(c), C.sqlite3_int64(i))
}

// ResultNull sets the result of an SQL function.
// (See sqlite3_result_null, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultNull() {
	C.sqlite3_result_null((*C.sqlite3_context)(c))
}

// ResultText sets the result of an SQL function.
// (See sqlite3_result_text, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultText(s string) {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	cs, l := (*C.char)(unsafe.Pointer(h.Data)), C.int(h.Len)
	C.my_result_text((*C.sqlite3_context)(c), cs, l)
}

// ResultZeroblob sets the result of an SQL function.
// (See sqlite3_result_zeroblob, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultZeroblob(n ZeroBlobLength) {
	C.sqlite3_result_zeroblob((*C.sqlite3_context)(c), C.int(n))
}

type IndexInfo C.sqlite3_index_info
