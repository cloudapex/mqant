// Copyright 2021 mqant Author. All Rights Reserved.
//

//    AES
//密码学中的高级加密标准（Advanced Encryption Standard，AES），
//又称Rijndael加密法，是美国联邦政府采用的一种区块加密标准。
//这个标准用来替代原先的DES（Data Encryption Standard），
//已经被多方分析且广为全世界所使用。AES中常见的有三种解决方案，
//分别为AES-128、AES-192和AES-256。
//如果采用真正的128位加密技术甚至256位加密技术，蛮力攻击要取得成功需要耗费相当长的时间。

// Package
package aes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/liangdas/mqant/log"
)

func AESEncrypt(src []byte) (encrypted []byte, e error) {
	if len(src) > MSG_PKG_MAX_LEN {
		log.Warning("协议包超超长|Lenth=%d", len(src))
		return encrypted, errors.New(fmt.Sprintf("协议包超过最大包长:%d", len(src)))
	}

	switch AES_MODEL {
	case ECB:
		return AES_ECB_Encrypt(src, []byte(AES_KEY16))
		//return AES_ECB_EncryptEx(src, []byte(AES_KEY16))
	case CBC:
		return AES_CBC_Encrypt(src, []byte(AES_KEY16))
	default:
		return nil, errors.New(fmt.Sprintf("模式错误:%s", AES_MODEL))
	}
}

func AESDecrypt(src []byte) (decrypted []byte, e error) {
	switch AES_MODEL {
	case ECB:
		return AES_ECB_Decrypt(src, []byte(AES_KEY16))
		//return AES_ECB_DecryptEx(src,[]byte(AES_KEY16))
	case CBC:
		return AES_CBC_Decrypt(src, []byte(AES_KEY16))
	default:
		return nil, errors.New(fmt.Sprintf("模式错误:%s", AES_MODEL))
	}

}

// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ////////////////////////////////////////////////电码本模式 ECB模式///////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 加密
func AES_ECB_Encrypt(src []byte, key []byte) (encrypted []byte, e error) {
	//	log.Debug("ECB")
	if len(key) == 0 {
		dest := make([]byte, len(src))
		copy(dest, src)
		return dest, nil
	}

	cipher, err := aes.NewCipher(ECB_GenerateKey(key))
	if err != nil {
		return nil, err
	}
	length := (len(src) + aes.BlockSize) / aes.BlockSize
	plain := make([]byte, length*aes.BlockSize)
	copy(plain, src)
	pad := byte(len(plain) - len(src))
	for i := len(src); i < len(plain); i++ {
		plain[i] = pad
	}
	encrypted = make([]byte, len(plain))
	// 分组 分块 加密
	for bs, be := 0, cipher.BlockSize(); bs <= len(src); bs, be = bs+cipher.BlockSize(), be+cipher.BlockSize() {
		cipher.Encrypt(encrypted[bs:be], plain[bs:be])
	}

	if true {
		// 对B64的编解码
		newSrc := base64.StdEncoding.EncodeToString(encrypted)
		encrypted = []byte(newSrc)
	}

	return encrypted, nil
}

// 解密
func AES_ECB_Decrypt(encrypted []byte, key []byte) (decrypted []byte, e error) {
	defer func() {
		if r := recover(); r != nil {
			decrypted = nil
			e = errors.New(fmt.Sprintf("解密失败"))
			return
		}
	}()

	cipher, err := aes.NewCipher(ECB_GenerateKey(key))
	if err != nil {
		return nil, err
	}

	if true {
		//对B64的编解码
		dataDecode, err_ds := base64.StdEncoding.DecodeString(string(encrypted))
		if err_ds != nil {
			log.Warning("B64Decode失败|解密|%v", err_ds)
		}
		encrypted = dataDecode
	}

	decrypted = make([]byte, len(encrypted))
	// 分组 分块 解密
	for bs, be := 0, cipher.BlockSize(); bs < len(encrypted); bs, be = bs+cipher.BlockSize(), be+cipher.BlockSize() {
		cipher.Decrypt(decrypted[bs:be], encrypted[bs:be])
	}

	trim := 0
	if len(decrypted) > 0 {
		trim = len(decrypted) - int(decrypted[len(decrypted)-1])
	}

	return decrypted[:trim], nil
}

// 产生Key
func ECB_GenerateKey(key []byte) (newKey []byte) {
	newKey = make([]byte, aes.BlockSize)
	copy(newKey, key)
	for i := aes.BlockSize; i < len(key); {
		for j := 0; j < aes.BlockSize && i < len(key); j, i = j+1, i+1 {
			newKey[j] ^= key[i]
		}
	}
	return newKey
}

///////////////////////////////////

// 加密
type ecbEncrypter ecb

// NewECBEncrypter returns a BlockMode which encrypts in electronic code book
// mode, using the given Block.
func NewECBEncrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbEncrypter)(newECB(b))
}
func (x *ecbEncrypter) BlockSize() int { return x.blockSize }

func (x *ecbEncrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Encrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

type ecb struct {
	b         cipher.Block
	blockSize int
}

func newECB(b cipher.Block) *ecb {
	return &ecb{
		b:         b,
		blockSize: b.BlockSize(),
	}
}

type ecbDecrypter ecb

// NewECBDecrypter returns a BlockMode which decrypts in electronic code book
// mode, using the given Block.
func NewECBDecrypter(b cipher.Block) cipher.BlockMode {
	return (*ecbDecrypter)(newECB(b))
}
func (x *ecbDecrypter) BlockSize() int { return x.blockSize }
func (x *ecbDecrypter) CryptBlocks(dst, src []byte) {
	if len(src)%x.blockSize != 0 {
		panic("crypto/cipher: input not full blocks")
	}
	if len(dst) < len(src) {
		panic("crypto/cipher: output smaller than input")
	}
	for len(src) > 0 {
		x.b.Decrypt(dst, src[:x.blockSize])
		src = src[x.blockSize:]
		dst = dst[x.blockSize:]
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////密码分组链接模式 CBC模式///////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// CBC 加密
func AES_CBC_Encrypt(src []byte, key []byte) (encrypted []byte, e error) {

	//	log.Debug("加密|CBC")
	// 分组秘钥
	// NewCipher该函数限制了输入k的长度必须为16, 24或者32
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// 获取秘钥块的长度
	blockSize := block.BlockSize()
	// 补全码
	src = PKCS7Padding(src, blockSize)
	// 加密模式
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	// 创建数组
	cryted := make([]byte, len(src))
	// 加密
	blockMode.CryptBlocks(cryted, src)
	//return []byte(base64.StdEncoding.EncodeToString(cryted)),nil
	return cryted, nil
}

// CBC 解密
func AES_CBC_Decrypt(src []byte, key []byte) (decrypted []byte, e error) {

	// 转成字节数组
	//crytedByte, _ := base64.StdEncoding.DecodeString(string(src))

	// 分组秘钥
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// 获取秘钥块的长度
	blockSize := block.BlockSize()
	// 加密模式
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	// 创建数组
	orig := make([]byte, len(src))
	// 解密
	blockMode.CryptBlocks(orig, src)
	// 去补全码
	orig = PKCS7UnPadding(orig)
	return orig, nil
}

//////////////////////////////////////////////////////////////////////

// 补码
// AES加密数据块分组长度必须为128bit(byte[16])，密钥长度可以是128bit(byte[16])、192bit(byte[24])、256bit(byte[32])中的任意一个。
func PKCS7Padding(ciphertext []byte, blocksize int) []byte {
	padding := blocksize - len(ciphertext)%blocksize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// 去码
func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	//填充
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)

	return append(ciphertext, padtext...)
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	// 去掉最后一个字节 unpadding 次
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}
