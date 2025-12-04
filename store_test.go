package main

import (
	"bytes"
	"fmt"
	"io"

	// "errors"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "momsbestpicture"
	pathName := CASPathTranformFunc(key)
	fmt.Println(pathName)
}

func TestStore(t *testing.T) {
	s := newStore()
	defer teardown(t,s)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("fook_%d",i)
		data := []byte("some jpg bytes")

		if _,err := s.writestream(key,bytes.NewReader(data)); err != nil {
			t.Error(err)
		}
		if ok := s.Has(key); !ok {
			t.Errorf("expected to have key %s", key)
		}
		r, err := s.Read(key)
		if err != nil {
			t.Error(err)
		}
		b, _ := io.ReadAll(r)
		if string(b) != string(data) {
			t.Errorf("want %s have %s", data, b)
		}
		
		if err := s.Delete(key); err != nil {
			t.Error(err)
		}
		if ok := s.Has(key); ok {
			t.Errorf("expected to not have key %s", key)
		}
		
	}
	}
//Helper function
func newStore() *Store {
	opts:= StoreOpts{
		PathTransformFunc: CASPathTranformFunc,

	}
	return NewStore(opts)
}

func teardown(t *testing.T, s *Store) {
	if err := s.Clear(); err != nil {
		t.Error(err)
	}
}