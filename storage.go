package main

import (
	// "bytes"
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
	hash := sha1.Sum([]byte(key)) // converts key of string type to byte slice and computes SHA-1 hash of those bytes
	hashStr := hex.EncodeToString(hash[:]) // converts array to slice and converts from byte to hexa

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

func (s *Store) Write(key string, r io.Reader) (int64,error) {
	return s.writeStream(key,r)
}

// FIXME: Instead of copying directly to a reader, we first copy 
// this into a buffer. Maybe just return the file from the readstream.
func (s *Store) Read(key string) (int64,io.Reader, error) {
	return s.readStream(key)
}

func (s *Store) readStream(key string) (int64,io.ReadCloser,error) {
	pathKey := s.StoreOpts.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.FullPath())

	file,err:= os.Open(fullPathWithRoot)
	if err != nil {
		return 0,nil,err
	}
	fi, err := file.Stat()
	if err != nil {
		return 0,nil,err
	}
	return fi.Size(),file,nil
}
func (s *Store) WriteDecrypt(encKey []byte, key string, r io.Reader) (int64, error) {
    f, err := s.openFileForWriting(key)
    if err != nil {
        return 0, err
    }

    n, err := copyDecrypt(encKey, r, f)
    return int64(n), err
}

func (s *Store) openFileForWriting(key string) (*os.File, error) {
    pathKey := s.StoreOpts.PathTransformFunc(key)
    pathNameWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.PathName)
    
    if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
        return nil, err
    }

    fullPathWithRoot := fmt.Sprintf("%s/%s", s.StoreOpts.Root, pathKey.FullPath())

    return os.Create(fullPathWithRoot)
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
    f, err := s.openFileForWriting(key)
    if err != nil {
        return 0, err
    }
    
    return io.Copy(f, r)
}