package raft

import (
	"encoding/binary"
	"sync"
	"time"
)

type Parts []*Part

type Part struct {
	ID         uint16
	UploadTime time.Time
	MD5        string
	Size       uint64
	Inode      uint64
}

type Multipart struct {
	// session fields
	Id       string
	Key      string
	InitTime time.Time
	Parts    Parts

	mu sync.RWMutex
}

func MultipartFromBytes(raw []byte) *Multipart {
	var unmarshalStr = func(data []byte) (string, int) {
		var n int
		var lengthU64 uint64
		lengthU64, n = binary.Uvarint(data)
		return string(data[n : n+int(lengthU64)]), n + int(lengthU64)
	}
	var offset, n int
	// decode id
	var id string
	id, n = unmarshalStr(raw)
	offset += n
	// decode key
	var key string
	key, n = unmarshalStr(raw[offset:])
	offset += n
	// decode init time
	var initTimeI64 int64
	initTimeI64, n = binary.Varint(raw[offset:])
	offset += n
	// decode parts
	var partsLengthU64 uint64
	partsLengthU64, n = binary.Uvarint(raw[offset:])
	offset += n
	var parts = PartsFromBytes(raw[offset : offset+int(partsLengthU64)])

	var muSession = &Multipart{
		Id:       id,
		Key:      key,
		InitTime: time.Unix(0, initTimeI64),
		Parts:    parts,
	}
	return muSession
}

func PartsFromBytes(raw []byte) Parts {
	var offset, n int
	var numPartsU64 uint64
	numPartsU64, n = binary.Uvarint(raw)
	offset += n
	var muParts = make([]*Part, int(numPartsU64))
	for i := 0; i < int(numPartsU64); i++ {
		var partLengthU64 uint64
		partLengthU64, n = binary.Uvarint(raw[offset:])
		offset += n
		part := PartFromBytes(raw[offset : offset+int(partLengthU64)])
		muParts[i] = part
		offset += int(partLengthU64)
	}
	return muParts
}

func PartFromBytes(raw []byte) *Part {
	var offset, n int
	// decode ID
	var u64ID uint64
	u64ID, n = binary.Uvarint(raw)
	offset += n
	// decode upload time
	var uploadTimeI64 int64
	uploadTimeI64, n = binary.Varint(raw[offset:])
	offset += n
	// decode MD5
	var md5Len uint64
	md5Len, n = binary.Uvarint(raw[offset:])
	offset += n
	var md5Content = string(raw[offset : offset+int(md5Len)])
	offset += int(md5Len)
	// decode size
	var sizeU64 uint64
	sizeU64, n = binary.Uvarint(raw[offset:])
	offset += n
	// decode inode
	var inode uint64
	inode, n = binary.Uvarint(raw[offset:])

	var muPart = &Part{
		ID:         uint16(u64ID),
		UploadTime: time.Unix(0, uploadTimeI64),
		MD5:        md5Content,
		Size:       sizeU64,
		Inode:      inode,
	}
	return muPart
}
