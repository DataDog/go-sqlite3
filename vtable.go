// From https://github.com/gwenn/gosqlite/blob/master/vtab.go

package sqlite3

/*
#cgo LDFLAGS: -ldl
#cgo CFLAGS: -I.
#cgo CFLAGS: -std=gnu99
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE -DSQLITE_THREADSAFE
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4_UNICODE61
#cgo CFLAGS: -DSQLITE_TRACE_SIZE_LIMIT=15
#cgo CFLAGS: -DSQLITE_ENABLE_COLUMN_METADATA=1
#cgo CFLAGS: -Wno-deprecated-declarations -Wno-c99-extensions
#include <sqlite3-binding.h>
#include <stdlib.h>
#include <string.h>
#ifdef __CYGWIN__
# include <errno.h>
#endif
#ifndef SQLITE_OPEN_READWRITE
# define SQLITE_OPEN_READWRITE 0
#endif
#ifndef SQLITE_OPEN_FULLMUTEX
# define SQLITE_OPEN_FULLMUTEX 0
#endif
#ifndef SQLITE_DETERMINISTIC
# define SQLITE_DETERMINISTIC 0
#endif

int goSqlite3CreateModule(sqlite3 *db, const char *zName, uintptr_t pClientData);

static inline char *my_mprintf(char *zFormat, char *arg) {
	return sqlite3_mprintf(zFormat, arg);
}
*/
import "C"

import (
	"reflect"
	"unsafe"
)

type sqliteModule struct {
	c      *SQLiteConn
	name   string
	module Module
}

type sqliteVTab struct {
	module *sqliteModule
	vTab   VTab
}

type sqliteVTabCursor struct {
	vTab       *sqliteVTab
	vTabCursor VTabCursor
}

// Mprintf is like fmt.Printf but implements some additional formatting options
// that are useful for constructing SQL statements.
// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf(format string, arg string) string {
	zSQL := mPrintf(format, arg)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}

func mPrintf(format, arg string) *C.char { // TODO may return nil when no memory...
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	return C.my_mprintf(cf, ca)
}

//export goMInit
func goMInit(db, pClientData unsafe.Pointer, argc int, argv **C.char, pzErr **C.char, isCreate int) C.uintptr_t {
	m := lookupHandle(uintptr(pClientData)).(*sqliteModule)
	if m.c.db != (*C.sqlite3)(db) {
		*pzErr = mPrintf("%s", "Inconsistent db handles")
		return 0
	}
	args := make([]string, argc)
	var A []*C.char
	slice := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(argv)), Len: argc, Cap: argc}
	a := reflect.NewAt(reflect.TypeOf(A), unsafe.Pointer(&slice)).Elem().Interface()
	for i, s := range a.([]*C.char) {
		args[i] = C.GoString(s)
	}
	var vTab VTab
	var err error
	if isCreate == 1 {
		vTab, err = m.module.Create(m.c, args)
	} else {
		vTab, err = m.module.Connect(m.c, args)
	}

	if err != nil {
		*pzErr = mPrintf("%s", err.Error())
		return 0
	}
	vt := sqliteVTab{m, vTab}
	*pzErr = nil
	return C.uintptr_t(newHandle(m.c, &vt))
}

//export goVRelease
func goVRelease(pVTab unsafe.Pointer, isDestroy int) *C.char {
	vt := lookupHandle(uintptr(pVTab)).(*sqliteVTab)
	var err error
	if isDestroy == 1 {
		err = vt.vTab.Destroy()
	} else {
		err = vt.vTab.Disconnect()
	}
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goVOpen
func goVOpen(pVTab unsafe.Pointer, pzErr **C.char) C.uintptr_t {
	vt := lookupHandle(uintptr(pVTab)).(*sqliteVTab)
	vTabCursor, err := vt.vTab.Open()
	if err != nil {
		*pzErr = mPrintf("%s", err.Error())
		return 0
	}
	vtc := sqliteVTabCursor{vt, vTabCursor}
	*pzErr = nil
	return C.uintptr_t(newHandle(vt.module.c, &vtc))
}

//export goVBestIndex
func goVBestIndex(pVTab unsafe.Pointer, icp unsafe.Pointer) *C.char {
	vt := lookupHandle(uintptr(pVTab)).(*sqliteVTab)
	i := (*IndexInfo)(icp)
	err := vt.vTab.BestIndex(i)
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goVClose
func goVClose(pCursor unsafe.Pointer) *C.char {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	err := vtc.vTabCursor.Close()
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goMDestroy
func goMDestroy(pClientData unsafe.Pointer) {
	m := lookupHandle(uintptr(pClientData)).(*sqliteModule)
	m.module.DestroyModule()
}

//export goVFilter
func goVFilter(pCursor unsafe.Pointer) *C.char {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	err := vtc.vTabCursor.Filter()
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goVNext
func goVNext(pCursor unsafe.Pointer) *C.char {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	err := vtc.vTabCursor.Next()
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goVEof
func goVEof(pCursor unsafe.Pointer) C.int {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	err := vtc.vTabCursor.EOF()
	if err {
		return 1
	}
	return 0
}

//export goVColumn
func goVColumn(pCursor, cp unsafe.Pointer, col int) *C.char {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	c := (*Context)(cp)
	err := vtc.vTabCursor.Column(c, col)
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	return nil
}

//export goVRowid
func goVRowid(pCursor unsafe.Pointer, pRowid *C.sqlite3_int64) *C.char {
	vtc := lookupHandle(uintptr(pCursor)).(*sqliteVTabCursor)
	rowid, err := vtc.vTabCursor.Rowid()
	if err != nil {
		return mPrintf("%s", err.Error())
	}
	*pRowid = C.sqlite3_int64(rowid)
	return nil
}

// Module is a "virtual table module", it defines the implementation of a virtual tables.
// (See http://sqlite.org/c3ref/module.html)
type Module interface {
	Create(c *SQLiteConn, args []string) (VTab, error)  // See http://sqlite.org/vtab.html#xcreate
	Connect(c *SQLiteConn, args []string) (VTab, error) // See http://sqlite.org/vtab.html#xconnect
	DestroyModule()                                     // See http://sqlite.org/c3ref/create_module.html
}

// VTab describes a particular instance of the virtual table.
// (See http://sqlite.org/c3ref/vtab.html)
type VTab interface {
	BestIndex(*IndexInfo) error // See http://sqlite.org/vtab.html#xbestindex
	Disconnect() error          // See http://sqlite.org/vtab.html#xdisconnect
	Destroy() error             // See http://sqlite.org/vtab.html#sqlite3_module.xDestroy
	Open() (VTabCursor, error)  // See http://sqlite.org/vtab.html#xopen
}

// VTabExtended lists optional/extended functions.
// (See http://sqlite.org/c3ref/vtab.html)
type VTabExtended interface {
	VTab
	Update( /*int argc, sqlite3_value **argv, */ rowid int64) error

	Begin() error
	Sync() error
	Commit() error
	Rollback() error

	//FindFunction(nArg int, name string /*, void (**pxFunc)(sqlite3_context*,int,sqlite3_value**), void **ppArg*/) error
	Rename(newName string) error

	Savepoint(i int) error
	Release(i int) error
	RollbackTo(i int) error
}

// VTabCursor describes cursors that point into the virtual table and are used to loop through the virtual table.
// (See http://sqlite.org/c3ref/vtab_cursor.html)
type VTabCursor interface {
	Close() error                                                                 // See http://sqlite.org/vtab.html#xclose
	Filter( /*idxNum int, idxStr string, int argc, sqlite3_value **argv*/ ) error // See http://sqlite.org/vtab.html#xfilter
	Next() error                                                                  // See http://sqlite.org/vtab.html#xnext
	EOF() bool                                                                    // See http://sqlite.org/vtab.html#xeof
	// col is zero-based so the first column is numbered 0
	Column(c *Context, col int) error // See http://sqlite.org/vtab.html#xcolumn
	Rowid() (int64, error)            // See http://sqlite.org/vtab.html#xrowid
}

// DeclareVTab declares the Schema of a virtual table.
// (See http://sqlite.org/c3ref/declare_vtab.html)
func (c *SQLiteConn) DeclareVTab(sql string) error {
	zSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(zSQL))
	rv := C.sqlite3_declare_vtab(c.db, zSQL)
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}

// CreateModule registers a virtual table implementation.
// (See http://sqlite.org/c3ref/create_module.html)
func (c *SQLiteConn) CreateModule(moduleName string, module Module) error {
	mname := C.CString(moduleName)
	defer C.free(unsafe.Pointer(mname))
	udm := sqliteModule{c, moduleName, module}
	rv := C.goSqlite3CreateModule(c.db, mname, C.uintptr_t(newHandle(c, &udm)))
	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}
