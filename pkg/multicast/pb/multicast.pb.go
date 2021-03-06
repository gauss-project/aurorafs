// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: multicast.proto

package pb

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type GIDs struct {
	Gid [][]byte `protobuf:"bytes,1,rep,name=gid,proto3" json:"gid,omitempty"`
}

func (m *GIDs) Reset()         { *m = GIDs{} }
func (m *GIDs) String() string { return proto.CompactTextString(m) }
func (*GIDs) ProtoMessage()    {}
func (*GIDs) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{0}
}
func (m *GIDs) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *GIDs) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_GIDs.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *GIDs) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GIDs.Merge(m, src)
}
func (m *GIDs) XXX_Size() int {
	return m.Size()
}
func (m *GIDs) XXX_DiscardUnknown() {
	xxx_messageInfo_GIDs.DiscardUnknown(m)
}

var xxx_messageInfo_GIDs proto.InternalMessageInfo

func (m *GIDs) GetGid() [][]byte {
	if m != nil {
		return m.Gid
	}
	return nil
}

type FindGroupReq struct {
	Gid   []byte   `protobuf:"bytes,1,opt,name=gid,proto3" json:"gid,omitempty"`
	Limit int32    `protobuf:"varint,2,opt,name=limit,proto3" json:"limit,omitempty"`
	Ttl   int32    `protobuf:"varint,3,opt,name=ttl,proto3" json:"ttl,omitempty"`
	Paths [][]byte `protobuf:"bytes,4,rep,name=paths,proto3" json:"paths,omitempty"`
}

func (m *FindGroupReq) Reset()         { *m = FindGroupReq{} }
func (m *FindGroupReq) String() string { return proto.CompactTextString(m) }
func (*FindGroupReq) ProtoMessage()    {}
func (*FindGroupReq) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{1}
}
func (m *FindGroupReq) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *FindGroupReq) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_FindGroupReq.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *FindGroupReq) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FindGroupReq.Merge(m, src)
}
func (m *FindGroupReq) XXX_Size() int {
	return m.Size()
}
func (m *FindGroupReq) XXX_DiscardUnknown() {
	xxx_messageInfo_FindGroupReq.DiscardUnknown(m)
}

var xxx_messageInfo_FindGroupReq proto.InternalMessageInfo

func (m *FindGroupReq) GetGid() []byte {
	if m != nil {
		return m.Gid
	}
	return nil
}

func (m *FindGroupReq) GetLimit() int32 {
	if m != nil {
		return m.Limit
	}
	return 0
}

func (m *FindGroupReq) GetTtl() int32 {
	if m != nil {
		return m.Ttl
	}
	return 0
}

func (m *FindGroupReq) GetPaths() [][]byte {
	if m != nil {
		return m.Paths
	}
	return nil
}

type FindGroupResp struct {
	Addresses [][]byte `protobuf:"bytes,1,rep,name=addresses,proto3" json:"addresses,omitempty"`
}

func (m *FindGroupResp) Reset()         { *m = FindGroupResp{} }
func (m *FindGroupResp) String() string { return proto.CompactTextString(m) }
func (*FindGroupResp) ProtoMessage()    {}
func (*FindGroupResp) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{2}
}
func (m *FindGroupResp) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *FindGroupResp) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_FindGroupResp.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *FindGroupResp) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FindGroupResp.Merge(m, src)
}
func (m *FindGroupResp) XXX_Size() int {
	return m.Size()
}
func (m *FindGroupResp) XXX_DiscardUnknown() {
	xxx_messageInfo_FindGroupResp.DiscardUnknown(m)
}

var xxx_messageInfo_FindGroupResp proto.InternalMessageInfo

func (m *FindGroupResp) GetAddresses() [][]byte {
	if m != nil {
		return m.Addresses
	}
	return nil
}

type MulticastMsg struct {
	Id         uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	CreateTime int64  `protobuf:"varint,2,opt,name=createTime,proto3" json:"createTime,omitempty"`
	Origin     []byte `protobuf:"bytes,3,opt,name=origin,proto3" json:"origin,omitempty"`
	Gid        []byte `protobuf:"bytes,4,opt,name=gid,proto3" json:"gid,omitempty"`
	Data       []byte `protobuf:"bytes,5,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *MulticastMsg) Reset()         { *m = MulticastMsg{} }
func (m *MulticastMsg) String() string { return proto.CompactTextString(m) }
func (*MulticastMsg) ProtoMessage()    {}
func (*MulticastMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{3}
}
func (m *MulticastMsg) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *MulticastMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_MulticastMsg.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *MulticastMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MulticastMsg.Merge(m, src)
}
func (m *MulticastMsg) XXX_Size() int {
	return m.Size()
}
func (m *MulticastMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_MulticastMsg.DiscardUnknown(m)
}

var xxx_messageInfo_MulticastMsg proto.InternalMessageInfo

func (m *MulticastMsg) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *MulticastMsg) GetCreateTime() int64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

func (m *MulticastMsg) GetOrigin() []byte {
	if m != nil {
		return m.Origin
	}
	return nil
}

func (m *MulticastMsg) GetGid() []byte {
	if m != nil {
		return m.Gid
	}
	return nil
}

func (m *MulticastMsg) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type Notify struct {
	Status int32    `protobuf:"varint,1,opt,name=status,proto3" json:"status,omitempty"`
	Gids   [][]byte `protobuf:"bytes,2,rep,name=gids,proto3" json:"gids,omitempty"`
}

func (m *Notify) Reset()         { *m = Notify{} }
func (m *Notify) String() string { return proto.CompactTextString(m) }
func (*Notify) ProtoMessage()    {}
func (*Notify) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{4}
}
func (m *Notify) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Notify) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Notify.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Notify) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Notify.Merge(m, src)
}
func (m *Notify) XXX_Size() int {
	return m.Size()
}
func (m *Notify) XXX_DiscardUnknown() {
	xxx_messageInfo_Notify.DiscardUnknown(m)
}

var xxx_messageInfo_Notify proto.InternalMessageInfo

func (m *Notify) GetStatus() int32 {
	if m != nil {
		return m.Status
	}
	return 0
}

func (m *Notify) GetGids() [][]byte {
	if m != nil {
		return m.Gids
	}
	return nil
}

type GroupMsg struct {
	Gid  []byte `protobuf:"bytes,1,opt,name=gid,proto3" json:"gid,omitempty"`
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	Type int32  `protobuf:"varint,3,opt,name=type,proto3" json:"type,omitempty"`
	Err  string `protobuf:"bytes,4,opt,name=err,proto3" json:"err,omitempty"`
}

func (m *GroupMsg) Reset()         { *m = GroupMsg{} }
func (m *GroupMsg) String() string { return proto.CompactTextString(m) }
func (*GroupMsg) ProtoMessage()    {}
func (*GroupMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_eedbde62517e047e, []int{5}
}
func (m *GroupMsg) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *GroupMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_GroupMsg.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *GroupMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GroupMsg.Merge(m, src)
}
func (m *GroupMsg) XXX_Size() int {
	return m.Size()
}
func (m *GroupMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_GroupMsg.DiscardUnknown(m)
}

var xxx_messageInfo_GroupMsg proto.InternalMessageInfo

func (m *GroupMsg) GetGid() []byte {
	if m != nil {
		return m.Gid
	}
	return nil
}

func (m *GroupMsg) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *GroupMsg) GetType() int32 {
	if m != nil {
		return m.Type
	}
	return 0
}

func (m *GroupMsg) GetErr() string {
	if m != nil {
		return m.Err
	}
	return ""
}

func init() {
	proto.RegisterType((*GIDs)(nil), "multicast.GIDs")
	proto.RegisterType((*FindGroupReq)(nil), "multicast.FindGroupReq")
	proto.RegisterType((*FindGroupResp)(nil), "multicast.FindGroupResp")
	proto.RegisterType((*MulticastMsg)(nil), "multicast.MulticastMsg")
	proto.RegisterType((*Notify)(nil), "multicast.Notify")
	proto.RegisterType((*GroupMsg)(nil), "multicast.GroupMsg")
}

func init() { proto.RegisterFile("multicast.proto", fileDescriptor_eedbde62517e047e) }

var fileDescriptor_eedbde62517e047e = []byte{
	// 327 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x91, 0x3f, 0x4b, 0x33, 0x41,
	0x10, 0xc6, 0x73, 0xff, 0xc2, 0x9b, 0xe1, 0x5e, 0x95, 0x45, 0x64, 0x8b, 0xb0, 0x84, 0xab, 0xd2,
	0x68, 0xa3, 0x9f, 0x40, 0xc4, 0x60, 0x11, 0x8b, 0x45, 0x2c, 0x2c, 0x84, 0x4d, 0x76, 0x3d, 0x17,
	0x92, 0xdc, 0xb9, 0x3b, 0x29, 0x02, 0x7e, 0x08, 0x3f, 0x96, 0x65, 0x4a, 0x4b, 0x49, 0xbe, 0x88,
	0xec, 0xe4, 0xe2, 0x05, 0xec, 0x9e, 0x79, 0x78, 0x76, 0xe6, 0x37, 0xb3, 0x70, 0x3c, 0x5f, 0xce,
	0xd0, 0x4e, 0x95, 0xc7, 0x8b, 0xda, 0x55, 0x58, 0xb1, 0xde, 0xaf, 0x51, 0x70, 0x48, 0x47, 0x77,
	0x37, 0x9e, 0x9d, 0x40, 0x52, 0x5a, 0xcd, 0xa3, 0x41, 0x32, 0xcc, 0x65, 0x90, 0xc5, 0x33, 0xe4,
	0xb7, 0x76, 0xa1, 0x47, 0xae, 0x5a, 0xd6, 0xd2, 0xbc, 0xb5, 0x89, 0xa8, 0x49, 0xb0, 0x53, 0xc8,
	0x66, 0x76, 0x6e, 0x91, 0xc7, 0x83, 0x68, 0x98, 0xc9, 0x5d, 0x11, 0x72, 0x88, 0x33, 0x9e, 0x90,
	0x17, 0x64, 0xc8, 0xd5, 0x0a, 0x5f, 0x3d, 0x4f, 0xa9, 0xfb, 0xae, 0x28, 0xce, 0xe1, 0xff, 0x41,
	0x7f, 0x5f, 0xb3, 0x3e, 0xf4, 0x94, 0xd6, 0xce, 0x78, 0x6f, 0x7c, 0x03, 0xd2, 0x1a, 0xc5, 0x3b,
	0xe4, 0xe3, 0x3d, 0xf5, 0xd8, 0x97, 0xec, 0x08, 0xe2, 0x86, 0x26, 0x95, 0xb1, 0xd5, 0x4c, 0x00,
	0x4c, 0x9d, 0x51, 0x68, 0x1e, 0xec, 0xdc, 0x10, 0x51, 0x22, 0x0f, 0x1c, 0x76, 0x06, 0xdd, 0xca,
	0xd9, 0xd2, 0x2e, 0x88, 0x2c, 0x97, 0x4d, 0xb5, 0x5f, 0x2b, 0x6d, 0xd7, 0x62, 0x90, 0x6a, 0x85,
	0x8a, 0x67, 0x64, 0x91, 0x2e, 0xae, 0xa0, 0x7b, 0x5f, 0xa1, 0x7d, 0x59, 0x85, 0x3e, 0x1e, 0x15,
	0x2e, 0x3d, 0xcd, 0xce, 0x64, 0x53, 0x85, 0x57, 0xa5, 0xd5, 0x9e, 0xc7, 0x04, 0x4e, 0xba, 0x78,
	0x84, 0x7f, 0xb4, 0x5e, 0xe0, 0xfd, 0x7b, 0xbe, 0xfd, 0x9c, 0xb8, 0x9d, 0x13, 0x3c, 0x5c, 0xd5,
	0xa6, 0xb9, 0x1e, 0xe9, 0xf0, 0xd2, 0x38, 0x47, 0x84, 0x3d, 0x19, 0xe4, 0x75, 0xff, 0x73, 0x23,
	0xa2, 0xf5, 0x46, 0x44, 0xdf, 0x1b, 0x11, 0x7d, 0x6c, 0x45, 0x67, 0xbd, 0x15, 0x9d, 0xaf, 0xad,
	0xe8, 0x3c, 0xc5, 0xf5, 0x64, 0xd2, 0xa5, 0x4f, 0xbe, 0xfc, 0x09, 0x00, 0x00, 0xff, 0xff, 0xbf,
	0x5b, 0x9b, 0x5b, 0xf7, 0x01, 0x00, 0x00,
}

func (m *GIDs) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *GIDs) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *GIDs) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Gid) > 0 {
		for iNdEx := len(m.Gid) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Gid[iNdEx])
			copy(dAtA[i:], m.Gid[iNdEx])
			i = encodeVarintMulticast(dAtA, i, uint64(len(m.Gid[iNdEx])))
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *FindGroupReq) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *FindGroupReq) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *FindGroupReq) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Paths) > 0 {
		for iNdEx := len(m.Paths) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Paths[iNdEx])
			copy(dAtA[i:], m.Paths[iNdEx])
			i = encodeVarintMulticast(dAtA, i, uint64(len(m.Paths[iNdEx])))
			i--
			dAtA[i] = 0x22
		}
	}
	if m.Ttl != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.Ttl))
		i--
		dAtA[i] = 0x18
	}
	if m.Limit != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.Limit))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Gid) > 0 {
		i -= len(m.Gid)
		copy(dAtA[i:], m.Gid)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Gid)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *FindGroupResp) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *FindGroupResp) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *FindGroupResp) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Addresses) > 0 {
		for iNdEx := len(m.Addresses) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Addresses[iNdEx])
			copy(dAtA[i:], m.Addresses[iNdEx])
			i = encodeVarintMulticast(dAtA, i, uint64(len(m.Addresses[iNdEx])))
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *MulticastMsg) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *MulticastMsg) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *MulticastMsg) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Data) > 0 {
		i -= len(m.Data)
		copy(dAtA[i:], m.Data)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Data)))
		i--
		dAtA[i] = 0x2a
	}
	if len(m.Gid) > 0 {
		i -= len(m.Gid)
		copy(dAtA[i:], m.Gid)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Gid)))
		i--
		dAtA[i] = 0x22
	}
	if len(m.Origin) > 0 {
		i -= len(m.Origin)
		copy(dAtA[i:], m.Origin)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Origin)))
		i--
		dAtA[i] = 0x1a
	}
	if m.CreateTime != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.CreateTime))
		i--
		dAtA[i] = 0x10
	}
	if m.Id != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.Id))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *Notify) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Notify) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Notify) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Gids) > 0 {
		for iNdEx := len(m.Gids) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Gids[iNdEx])
			copy(dAtA[i:], m.Gids[iNdEx])
			i = encodeVarintMulticast(dAtA, i, uint64(len(m.Gids[iNdEx])))
			i--
			dAtA[i] = 0x12
		}
	}
	if m.Status != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.Status))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *GroupMsg) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *GroupMsg) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *GroupMsg) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Err) > 0 {
		i -= len(m.Err)
		copy(dAtA[i:], m.Err)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Err)))
		i--
		dAtA[i] = 0x22
	}
	if m.Type != 0 {
		i = encodeVarintMulticast(dAtA, i, uint64(m.Type))
		i--
		dAtA[i] = 0x18
	}
	if len(m.Data) > 0 {
		i -= len(m.Data)
		copy(dAtA[i:], m.Data)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Data)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Gid) > 0 {
		i -= len(m.Gid)
		copy(dAtA[i:], m.Gid)
		i = encodeVarintMulticast(dAtA, i, uint64(len(m.Gid)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintMulticast(dAtA []byte, offset int, v uint64) int {
	offset -= sovMulticast(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *GIDs) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Gid) > 0 {
		for _, b := range m.Gid {
			l = len(b)
			n += 1 + l + sovMulticast(uint64(l))
		}
	}
	return n
}

func (m *FindGroupReq) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Gid)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	if m.Limit != 0 {
		n += 1 + sovMulticast(uint64(m.Limit))
	}
	if m.Ttl != 0 {
		n += 1 + sovMulticast(uint64(m.Ttl))
	}
	if len(m.Paths) > 0 {
		for _, b := range m.Paths {
			l = len(b)
			n += 1 + l + sovMulticast(uint64(l))
		}
	}
	return n
}

func (m *FindGroupResp) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Addresses) > 0 {
		for _, b := range m.Addresses {
			l = len(b)
			n += 1 + l + sovMulticast(uint64(l))
		}
	}
	return n
}

func (m *MulticastMsg) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Id != 0 {
		n += 1 + sovMulticast(uint64(m.Id))
	}
	if m.CreateTime != 0 {
		n += 1 + sovMulticast(uint64(m.CreateTime))
	}
	l = len(m.Origin)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	l = len(m.Gid)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	return n
}

func (m *Notify) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Status != 0 {
		n += 1 + sovMulticast(uint64(m.Status))
	}
	if len(m.Gids) > 0 {
		for _, b := range m.Gids {
			l = len(b)
			n += 1 + l + sovMulticast(uint64(l))
		}
	}
	return n
}

func (m *GroupMsg) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Gid)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	if m.Type != 0 {
		n += 1 + sovMulticast(uint64(m.Type))
	}
	l = len(m.Err)
	if l > 0 {
		n += 1 + l + sovMulticast(uint64(l))
	}
	return n
}

func sovMulticast(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozMulticast(x uint64) (n int) {
	return sovMulticast(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *GIDs) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: GIDs: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GIDs: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gid", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Gid = append(m.Gid, make([]byte, postIndex-iNdEx))
			copy(m.Gid[len(m.Gid)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *FindGroupReq) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: FindGroupReq: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FindGroupReq: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gid", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Gid = append(m.Gid[:0], dAtA[iNdEx:postIndex]...)
			if m.Gid == nil {
				m.Gid = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Limit", wireType)
			}
			m.Limit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Limit |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Ttl", wireType)
			}
			m.Ttl = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Ttl |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Paths", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Paths = append(m.Paths, make([]byte, postIndex-iNdEx))
			copy(m.Paths[len(m.Paths)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *FindGroupResp) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: FindGroupResp: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FindGroupResp: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Addresses", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Addresses = append(m.Addresses, make([]byte, postIndex-iNdEx))
			copy(m.Addresses[len(m.Addresses)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *MulticastMsg) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: MulticastMsg: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: MulticastMsg: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CreateTime", wireType)
			}
			m.CreateTime = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CreateTime |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Origin", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Origin = append(m.Origin[:0], dAtA[iNdEx:postIndex]...)
			if m.Origin == nil {
				m.Origin = []byte{}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gid", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Gid = append(m.Gid[:0], dAtA[iNdEx:postIndex]...)
			if m.Gid == nil {
				m.Gid = []byte{}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Notify) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Notify: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Notify: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Status", wireType)
			}
			m.Status = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Status |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gids", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Gids = append(m.Gids, make([]byte, postIndex-iNdEx))
			copy(m.Gids[len(m.Gids)-1], dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *GroupMsg) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: GroupMsg: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GroupMsg: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gid", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Gid = append(m.Gid[:0], dAtA[iNdEx:postIndex]...)
			if m.Gid == nil {
				m.Gid = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			m.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Type |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Err", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMulticast
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMulticast
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Err = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMulticast(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMulticast
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMulticast(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMulticast
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMulticast
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMulticast
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupMulticast
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthMulticast
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthMulticast        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMulticast          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMulticast = fmt.Errorf("proto: unexpected end of group")
)
