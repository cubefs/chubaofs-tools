package raft

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type Extend struct {
	Inode   uint64
	DataMap map[string][]byte
	mu      sync.RWMutex
}

func NewExtend(inode uint64) *Extend {
	return &Extend{Inode: inode, DataMap: make(map[string][]byte)}
}

func (e *Extend) Put(key, value []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.DataMap[string(key)] = value
}

func NewExtendFromBytes(raw []byte) (*Extend, error) {
	var err error
	var buffer = bytes.NewBuffer(raw)
	// decode inode
	var inode uint64
	if inode, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	var ext = NewExtend(inode)
	// decode number of key-value pairs
	var numKV uint64
	if numKV, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	var readBytes = func() ([]byte, error) {
		var length uint64
		if length, err = binary.ReadUvarint(buffer); err != nil {
			return nil, err
		}
		var data = make([]byte, length)
		if _, err = buffer.Read(data); err != nil {
			return nil, err
		}
		return data, nil
	}
	for i := 0; i < int(numKV); i++ {
		var k, v []byte
		if k, err = readBytes(); err != nil {
			return nil, err
		}
		if v, err = readBytes(); err != nil {
			return nil, err
		}
		ext.Put(k, v)
	}
	return ext, nil
}
