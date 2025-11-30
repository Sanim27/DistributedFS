package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	// "io/fs"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "ggnetwork"

func CASPathTranformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr)/blocksize

	paths := make ([]string,sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i *blocksize,(i * blocksize) + blocksize
		paths[i] = hashStr[from:to]
	}
	return PathKey{
		PathName: strings.Join(paths,"/"),
		Filename: hashStr,
	}
	// return  strings.Join(paths, "/")
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName,"/")
	if len(paths) == 0{
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName,p.Filename)
}

type StoreOpts struct {
	// Root is the folder name of the rood, containing all 
	// folders/files of the system
	Root string
	PathTransformFunc  PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Has(key string) bool {
	PathKey := s.StoreOpts.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, PathKey.FullPath())
	_, err := os.Stat(fullPathWithRoot)

	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.StoreOpts.Root)
}
func (s *Store) Delete(key string) error {
	PathKey:= s.StoreOpts.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", PathKey.Filename)
	}()
	firstPathNameWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, PathKey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)

}

func (s *Store) Write(key string, r io.Reader) error {
	return s.writestream(key,r)
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf,f)

	return buf, err
}

func (s *Store) readStream(key string) (io.ReadCloser,error) {
	pathKey := s.StoreOpts.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.FullPath())
	return os.Open(fullPathWithRoot)
}

//efficiently copy data from the network connection directly 
// to the file system without having to load the entire file 
// into the computer's memory (RAM) first.
func (s *Store) writestream(key string, r io.Reader) error {
	pathKey := CASPathTranformFunc(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return err
	}

	fullPathWithRoot :=fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.FullPath()) 

	f, err := os.Create(fullPathWithRoot)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}
	log.Printf("written (%d) bytes to disk: %s",n, fullPathWithRoot)
	
	return nil

}