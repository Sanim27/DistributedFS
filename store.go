package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
)

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	blockSize := 5
	sliceLen := len(hashStr) / blockSize

	paths := make([]string, sliceLen)
	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, i*blockSize+blockSize
		paths[i] = hashStr[from:to]
	}
	return PathKey{
		Pathname: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	Pathname string
	Filename string
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.Pathname, p.Filename)
}

type StoreOpts struct {
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransferFunc = func(key string) PathKey {
	return PathKey{
		Pathname: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

// func (s *Store) Has(key string) bool {
// 	pathKey := s.PathTransformFunc(key)

// 	_, err := os.Stat(pathKey.FullPath())
// 	if err == fs.ErrNotExist {
// 		return false
// 	}
// 	return true
// }

func (s *Store) Has(key string) bool {
	pathKey := s.PathTransformFunc(key)

	_, err := os.Stat(pathKey.FullPath())
	if err == nil {
		// file exists
		return true
	}

	// if the error means "file not found"
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}

	// for other unexpected errors
	return false
}

func (p PathKey) firstPathName() string {
	paths := strings.Split(p.FullPath(), "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	return os.RemoveAll(pathKey.firstPathName())
}

func NewStore(opts StoreOpts) *Store {
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf, err
}

func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	f, err := os.Open(pathKey.FullPath())
	if err != nil {
		return nil, err
	}
	return f, err
}

func (s *Store) writeStream(key string, r io.Reader) error {
	PathKey := s.PathTransformFunc(key)

	if err := os.MkdirAll(PathKey.Pathname, os.ModePerm); err != nil {
		return err
	}

	fullpath := PathKey.FullPath()

	f, err := os.Create(fullpath)
	if err != nil {
		return err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	log.Printf("written (%d) bytes to disk: %s", n, fullpath)
	return nil
}
