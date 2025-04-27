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

const MSG_PKG_MAX_LEN = 1024 * 8 * 2 //最大包长，8192

const (
	AES_MODEL string = "OFF"
	AES_KEY16        = "3D334C30D5E6CEDD"
	AES_KEY24        = "E065323B9EA400D3C23E56D8"
	AES_KEY32        = "F9D8CEB63D334C30D5E6CEDDFCB942CE"
	ECB              = "ECB"
	CBC              = "CBC"
	CRT              = "CRT"
	OFB              = "OFB"
	RSA              = "RSA"
)
