// Code generated by protoc-gen-gopb. DO NOT EDIT.
// Code generated by wpb. DO NOT EDIT.

package server

import (
	"errors"

	"github.com/walleframe/walle/util/protowire"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Player struct {
	Uid  int64  `json:"uid,omitempty"`
	Name string `json:"name,omitempty"`
}

func (x *Player) Reset() {
	*x = Player{}
}

// MarshalObject marshal data to []byte
func (x *Player) MarshalObject() (data []byte, err error) {
	data = make([]byte, 0, x.MarshalSize())
	return x.MarshalObjectTo(data)
}

// MarshalSize calc marshal data need space
func (x *Player) MarshalSize() (size int) {
	if x.Uid != 0 {
		// 1 = protowire.SizeTag(1)
		size += 1 + protowire.SizeVarint(uint64(x.Uid))
	}
	if len(x.Name) > 0 {
		// 1 = protowire.SizeTag(2)
		size += 1 + protowire.SizeBytes(len(x.Name))
	}
	return
}

// MarshalObjectTo marshal data to []byte
func (x *Player) MarshalObjectTo(buf []byte) (data []byte, err error) {
	data = buf
	if x.Uid != 0 {
		// data = protowire.AppendTag(data, 1, protowire.VarintType) => 00001000
		data = append(data, 0x8)
		data = protowire.AppendVarint(data, uint64(x.Uid))
	}
	if len(x.Name) > 0 {
		// data = protowire.AppendTag(data, 2, protowire.BytesType) => 00010010
		data = append(data, 0x12)
		data = protowire.AppendString(data, x.Name)
	}
	return
}

// UnmarshalObject unmarshal data from []byte
func (x *Player) UnmarshalObject(data []byte) (err error) {
	index := 0
	for index < len(data) {
		num, typ, cnt := protowire.ConsumeTag(data[index:])
		if num == 0 {
			err = errors.New("invalid tag")
			return
		}

		index += cnt
		switch num {
		case 1:
			v, cnt := protowire.ConsumeVarint(data[index:])
			if cnt < 1 {
				err = errors.New("parse Player.Uid ID:1 : invalid varint value")
				return
			}
			index += cnt
			x.Uid = int64(v)
		case 2:
			v, cnt := protowire.ConsumeString(data[index:])
			if cnt < 1 {
				err = errors.New("parse Player.Name ID:2 : invalid len value")
				return
			}
			index += cnt
			x.Name = v
		default: // skip fields
			cnt = protowire.ConsumeFieldValue(num, typ, data[index:])
			if cnt < 0 {
				return protowire.ParseError(cnt)
			}
			index += cnt
		}
	}

	return
}

func (x *Player) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt64("Uid", x.Uid)
	enc.AddString("Name", x.Name)
	return nil
}

type ZapArrayPlayer []*Player

func (x ZapArrayPlayer) MarshalLogArray(ae zapcore.ArrayEncoder) error {
	for _, v := range x {
		ae.AppendObject(v)
	}
	return nil
}

func LogArrayPlayer(name string, v []*Player) zap.Field {
	return zap.Array(name, ZapArrayPlayer(v))
}
