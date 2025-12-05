package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
)
// generates buffer of 32 random bytes and convert to hex string
func generateID() string {
	buf := make([]byte, 32)
	io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}

//takes string of any length and converts into fixed 128-bit hash
func hashKey(key string) string {
	hash := md5.Sum([]byte(key))
	return hex.EncodeToString(hash[:])
}

//generates random encryption key of size 32 bytes
func newEncryptionKey() []byte {
	keyBuf := make([]byte, 32)
	io.ReadFull(rand.Reader, keyBuf)
	return keyBuf
}

//reads data in 32kb chunks from src, encrypts/decrypts 
// each chunk using xor cipher,
//  writes result to dst and continues until EOF.
func copyStream(stream cipher.Stream, blockSize int, src io.Reader, dst io.Writer) (int, error) {
	var (
		buf = make([]byte, 32*1024)
		nw  = blockSize
	)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			stream.XORKeyStream(buf, buf[:n])
			nn, err := dst.Write(buf[:n])
			if err != nil {
				return 0, err
			}
			nw += nn
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	return nw, nil
}


func copyDecrypt(key []byte, src io.Reader, dst io.Writer) (int, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, err
	}

	// Read the IV from the given io.Reader which, in our case should be the
	// the block.BlockSize() bytes we read.
	iv := make([]byte, block.BlockSize())
	if _, err := src.Read(iv); err != nil {
		return 0, err
	}

	stream := cipher.NewCTR(block, iv)
	return copyStream(stream, block.BlockSize(), src, dst)
}

// 
func copyEncrypt(key []byte, src io.Reader, dst io.Writer) (int, error) {
	block, err := aes.NewCipher(key) // creates an aes cipher object using 32 bytes key but blocksize is 16 bytes
	if err != nil {
		return 0, err
	}

	// creates a random 16 byte number used to add randomness to encryption 
	// (called as Initialization Vector (IV))
	// Its because encrypting same message twice should produce different results
	iv := make([]byte, block.BlockSize()) 
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return 0, err
	}

	// prepend the IV to the file because the receiver needs the IV to decrypt later.
	if _, err := dst.Write(iv); err != nil {
		return 0, err
	}

	// creates a counter mode stream cipher which works on any size 
	// while block cipher works on fixed size
	stream := cipher.NewCTR(block, iv) 
	return copyStream(stream, block.BlockSize(), src, dst)
}