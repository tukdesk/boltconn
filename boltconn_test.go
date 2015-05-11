package boltconn

import (
	"bytes"
	"os"
	"testing"
)

const (
	testDbPath = "_test.db"
)

func setUp() (*BoltConn, error) {
	os.RemoveAll(testDbPath)
	return New(testDbPath)
}

func tearDown(c *BoltConn) {
	c.Close()
	os.RemoveAll(testDbPath)
}

func TestBoltConn(t *testing.T) {
	c, err := setUp()
	if err != nil {
		t.Error(err)
		return
	}

	defer tearDown(c)

	bucketName := []byte("bucket")

	keyA := []byte("a")
	valueA := []byte("val_a")
	keyB := []byte("b")
	valueB := []byte("val_b")
	keyC := []byte("c")
	valueC := []byte("val_c")

	if err = c.Put(bucketName, keyA, valueA); err != nil {
		t.Error(err)
	}

	if err = c.Put(bucketName, keyB, valueB); err != nil {
		t.Error(err)
	}

	if err = c.Put(bucketName, keyC, valueC); err != nil {
		t.Error(err)
	}

	gotA, err := c.Get(bucketName, keyA)
	if err != nil {
		t.Error(err)
	}

	gotB, err := c.Get(bucketName, keyB)
	if err != nil {
		t.Error(err)
	}

	gotC, err := c.Get(bucketName, keyC)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(valueA, gotA) {
		t.Errorf("expected %s, got %s", string(valueA), string(gotA))
	}

	if !bytes.Equal(valueB, gotB) {
		t.Errorf("expected %s, got %s", string(valueB), string(gotB))
	}

	if !bytes.Equal(valueC, gotC) {
		t.Errorf("expected %s, got %s", string(valueC), string(gotC))
	}

	// key not found
	_, err = c.Get([]byte("not_found_bucket"), keyA)
	if err != errBucketNotFound {
		t.Errorf("expected bucket not found, got %s", err)
	}

	if !IsNotFound(err) {
		t.Error("expected to be true")
	}

	_, err = c.Get(bucketName, []byte("not_found_key"))
	if err != errKeyNotFound {
		t.Errorf("expected key not found, got %s", err)
	}

	if !IsNotFound(err) {
		t.Error("expected to be true")
	}

	// delete key
	if err = c.Delete(bucketName, keyA); err != nil {
		t.Error(err)
	}

	_, err = c.Get(bucketName, keyA)
	if err != errKeyNotFound {
		t.Errorf("expected key not found, got %s", err)
	}

	if !IsNotFound(err) {
		t.Error("expected to be true")
	}

	if _, err = c.Get(bucketName, keyB); err != nil {
		t.Error(err)
	}

	// delete bucket
	if err = c.DeleteBucket(bucketName); err != nil {
		t.Error(err)
	}

	_, err = c.Get(bucketName, keyB)
	if err != errBucketNotFound {
		t.Errorf("expected bucket not found, got %s", err)
	}

	if !IsNotFound(err) {
		t.Error("expected to be true")
	}

}

func TestConnMap(t *testing.T) {
	dbA := "_test_a.db"
	dbB := "_test_b.db"

	defer func() {
		os.RemoveAll(dbA)
		os.RemoveAll(dbB)
	}()

	connA1, err := New(dbA)
	if err != nil {
		t.Error(err)
	}

	connA2, err := New(dbA)
	if err != nil {
		t.Error(err)
	}

	if connA1 != connA2 {
		t.Error("expected to be one")
	}

	if err := connA1.Close(); err != nil {
		t.Error(err)
	}

	connA3, err := New(dbA)
	if err != nil {
		t.Error(err)
	}

	if connA1 == connA3 {
		t.Error("expected to be another one")
	}

	connB, err := New(dbB)
	if err != nil {
		t.Error(err)
	}

	if connA3 == connB {
		t.Error("expected to be diffrent ones")
	}
}
