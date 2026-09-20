package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
)

func key(s string) []byte { h := sha256.Sum256([]byte(s)); return h[:] }
func Encrypt(plain, password string) (string, error) {
	b, err := aes.NewCipher(key(password))
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	n := make([]byte, g.NonceSize())
	if _, err = io.ReadFull(rand.Reader, n); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(g.Seal(n, n, []byte(plain), nil)), nil
}
func Decrypt(value, password string) (string, error) {
	b, err := aes.NewCipher(key(password))
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	v, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	if len(v) < g.NonceSize() {
		return "", errors.New("invalid encrypted value")
	}
	p, err := g.Open(nil, v[:g.NonceSize()], v[g.NonceSize():], nil)
	return string(p), err
}
