package boltconn

import (
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
)

var (
	connMap = newBoltConnMap()
)

type BoltConn struct {
	mutex  sync.Mutex
	db     *bolt.DB
	dbPath string
}

var (
	errBucketNotFound = fmt.Errorf("bucket not found")
	errKeyNotFound    = fmt.Errorf("key not found")
)

func New(dbPath string) (*BoltConn, error) {
	if c := connMap.get(dbPath); c != nil {
		return c, nil
	}

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	c := &BoltConn{
		db:     db,
		dbPath: dbPath,
	}

	connMap.set(dbPath, c)
	return c, nil
}

func (this *BoltConn) createBucketIfNotExists(tx *bolt.Tx, name []byte) (*bolt.Bucket, error) {
	return tx.CreateBucketIfNotExists(name)
}

func (this *BoltConn) Get(bucketName, key []byte) ([]byte, error) {
	var value []byte
	err := this.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errBucketNotFound
		}

		data := b.Get(key)
		if data == nil {
			return errKeyNotFound
		}

		value = data
		return nil
	})
	return value, err
}

func (this *BoltConn) Put(bucketName, key, value []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return this.db.Update(func(tx *bolt.Tx) error {
		b, err := this.createBucketIfNotExists(tx, bucketName)
		if err != nil {
			return err
		}

		return b.Put(key, value)
	})
}

func (this *BoltConn) Delete(bucketName, key []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b != nil {
			return b.Delete(key)
		}

		return nil
	})
}

func (this *BoltConn) DeleteBucket(bukcetName []byte) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bukcetName)
	})
}

func (this *BoltConn) Close() error {
	connMap.unset(this.dbPath)
	return this.db.Close()
}

func IsNotFound(err error) bool {
	return err == errBucketNotFound || err == errKeyNotFound
}
