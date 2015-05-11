package boltconn

import (
	"sync"
)

type boltConnMap struct {
	m     map[string]*BoltConn
	mutex sync.Mutex
}

func newBoltConnMap() *boltConnMap {
	return &boltConnMap{
		m: map[string]*BoltConn{},
	}
}

func (this *boltConnMap) get(dbPath string) *BoltConn {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return this.m[dbPath]
}

func (this *boltConnMap) set(dbPath string, conn *BoltConn) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if conn == nil {
		return
	}

	this.m[dbPath] = conn
	return
}

func (this *boltConnMap) unset(dbPath string) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if _, ok := this.m[dbPath]; ok {
		delete(this.m, dbPath)
	}
	return
}
