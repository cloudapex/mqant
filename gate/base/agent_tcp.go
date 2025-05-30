package gatebase

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/mqtools/aes"
)

func NewTCPAgent() gate.Agent {
	return &TCPAgent{}
}

type TCPAgent struct {
	agentBase
}

// 读取数据并解码出Pack
func (this *TCPAgent) OnReadDecodingPack() (*gate.Pack, error) {
	pkgLenData := make([]byte, gate.PACK_HEAD_TOTAL_LEN_SIZE)
	_, err := io.ReadFull(this.r, pkgLenData)
	if err != nil {
		return nil, err
	}
	pkgLen := binary.LittleEndian.Uint16(pkgLenData)

	bodyData := make([]byte, pkgLen-gate.PACK_HEAD_TOTAL_LEN_SIZE)
	_, err = io.ReadFull(this.r, bodyData)
	if err != nil {
		return nil, err
	}
	if this.gate.Options().EncryptKey != "" {
		cbc, err := aes.AES_ECB_Decrypt(bodyData, []byte(this.gate.Options().EncryptKey))
		if err != nil {
			return nil, fmt.Errorf("decrypt cbc, err:%v", err)
		}
		b64Data, err := base64.StdEncoding.DecodeString(string(cbc))
		if err != nil {
			return nil, fmt.Errorf("decrypt base64, err:%v", err)
		}
		bodyData = b64Data
	}
	topicLen := binary.LittleEndian.Uint16(bodyData[0:gate.PACK_HEAD_MSG_ID_LEN_SIZE])

	return &gate.Pack{
		Topic: string(bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE : gate.PACK_HEAD_MSG_ID_LEN_SIZE+topicLen]),
		Body:  bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE+topicLen:],
	}, nil
}
