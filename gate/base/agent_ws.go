package gatebase

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/mqtools/aes"
)

func NewWSAgent() gate.IAgent {
	return &WSAgent{}
}

type WSAgent struct {
	agentBase
}

// 读取数据并解码出Pack
func (this *WSAgent) OnReadDecodingPack() (*gate.Pack, error) {
	_, datas, err := this.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if len(datas) == 0 {
		return nil, nil
	}
	if len(datas) < gate.PACK_HEAD_TOTAL_LEN_SIZE {
		return nil, fmt.Errorf("package len tool small")
	}
	pkgLen := binary.LittleEndian.Uint16(datas[0:gate.PACK_HEAD_TOTAL_LEN_SIZE])
	if pkgLen != uint16(len(datas)) {
		return nil, fmt.Errorf("package len notmatch headLen")
	}

	bodyData := datas[gate.PACK_HEAD_TOTAL_LEN_SIZE:]
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
	idLen := binary.LittleEndian.Uint16(bodyData[0:gate.PACK_HEAD_MSG_ID_LEN_SIZE])

	return &gate.Pack{
		Topic: string(bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE : gate.PACK_HEAD_MSG_ID_LEN_SIZE+idLen]),
		Body:  bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE+idLen:],
	}, nil
}
