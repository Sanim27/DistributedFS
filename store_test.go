package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "momsbestpicture"
	PathKey := CASPathTransformFunc(key)
	expectedFilename := "6804429f74181a63c50c3d81d733a12f14a353ff"
	expectedPathName := "68044/29f74/181a6/3c50c/3d81d/733a1/2f14a/353ff"
	if PathKey.Pathname != expectedPathName {
		t.Errorf("have %s , want %s", PathKey.Pathname, expectedPathName)
	}
	if PathKey.Filename != expectedFilename {
		t.Errorf("have %s , want %s", PathKey.Filename, expectedFilename)
	}
}

// func TestStoreDeleteKey(t *testing.T) {
// 	opts := StoreOpts{
// 		PathTransformFunc: CASPathTransformFunc,
// 	}
// 	s := NewStore(opts)
// 	key := "momsspecials"
// 	data := []byte("some jpg bytes")
// 	if err := s.writeStream(key, bytes.NewReader(data)); err != nil {
// 		t.Error(err)
// 	}

// 	if err := s.Delete(key); err != nil {
// 		t.Error(err)
// 	}
// }

func TestStore(t *testing.T) {
	s := newStore()
	defer teardown(t, s)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("foo_%d", i)

		data := []byte("some jpg bytes")
		if _, err := s.writeStream(key, bytes.NewReader(data)); err != nil {
			t.Error(err)
		}

		r, err := s.Read(key)
		if err != nil {
			t.Error(err)
		}

		if ok := s.Has(key); !ok {
			t.Errorf("Expected to have key %s", key)
		}

		b, err := io.ReadAll(r)

		fmt.Println(string(b))

		if string(b) != string(data) {
			t.Errorf("want %s have %s", data, b)
		}
		fmt.Println(string(b))

		if err := s.Delete(key); err != nil {
			t.Error(err)
		}
		if ok := s.Has(key); ok {
			t.Errorf("Expected to Not have key %s", key)
		}
	}
}

func newStore() *Store {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	return NewStore(opts)
}

func teardown(t *testing.T, s *Store) {
	if err := s.Clear(); err != nil {
		t.Error(err)
	}
}
