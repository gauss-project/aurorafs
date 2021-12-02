// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: traffic.proto

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

type EmitCheque struct {
	Address      []byte `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty"`
	SignedCheque []byte `protobuf:"bytes,2,opt,name=SignedCheque,proto3" json:"SignedCheque,omitempty"`
}

func (m *EmitCheque) Reset()         { *m = EmitCheque{} }
func (m *EmitCheque) String() string { return proto.CompactTextString(m) }
func (*EmitCheque) ProtoMessage()    {}
func (*EmitCheque) Descriptor() ([]byte, []int) {
	return fileDescriptor_50e185a42cb2d3c6, []int{0}
}
func (m *EmitCheque) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *EmitCheque) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_EmitCheque.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *EmitCheque) XXX_Merge(src proto.Message) {
	xxx_messageInfo_EmitCheque.Merge(m, src)
}
func (m *EmitCheque) XXX_Size() int {
	return m.Size()
}
func (m *EmitCheque) XXX_DiscardUnknown() {
	xxx_messageInfo_EmitCheque.DiscardUnknown(m)
}

var xxx_messageInfo_EmitCheque proto.InternalMessageInfo

func (m *EmitCheque) GetAddress() []byte {
	if m != nil {
		return m.Address
	}
	return nil
}

func (m *EmitCheque) GetSignedCheque() []byte {
	if m != nil {
		return m.SignedCheque
	}
	return nil
}

func init() {
	proto.RegisterType((*EmitCheque)(nil), "traffic.EmitCheque")
}

func init() { proto.RegisterFile("traffic.proto", fileDescriptor_50e185a42cb2d3c6) }

var fileDescriptor_50e185a42cb2d3c6 = []byte{
	// 131 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x29, 0x4a, 0x4c,
	0x4b, 0xcb, 0x4c, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x87, 0x72, 0x95, 0xbc, 0xb8,
	0xb8, 0x5c, 0x73, 0x33, 0x4b, 0x9c, 0x33, 0x52, 0x0b, 0x4b, 0x53, 0x85, 0x24, 0xb8, 0xd8, 0x13,
	0x53, 0x52, 0x8a, 0x52, 0x8b, 0x8b, 0x25, 0x18, 0x15, 0x18, 0x35, 0x78, 0x82, 0x60, 0x5c, 0x21,
	0x25, 0x2e, 0x9e, 0xe0, 0xcc, 0xf4, 0xbc, 0xd4, 0x14, 0x88, 0x4a, 0x09, 0x26, 0xb0, 0x34, 0x8a,
	0x98, 0x93, 0xcc, 0x89, 0x47, 0x72, 0x8c, 0x17, 0x1e, 0xc9, 0x31, 0x3e, 0x78, 0x24, 0xc7, 0x38,
	0xe1, 0xb1, 0x1c, 0xc3, 0x85, 0xc7, 0x72, 0x0c, 0x37, 0x1e, 0xcb, 0x31, 0x44, 0x31, 0x15, 0x24,
	0x25, 0xb1, 0x81, 0x6d, 0x36, 0x06, 0x04, 0x00, 0x00, 0xff, 0xff, 0xa7, 0xd9, 0xc8, 0x95, 0x8a,
	0x00, 0x00, 0x00,
}

func (m *EmitCheque) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *EmitCheque) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *EmitCheque) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.SignedCheque) > 0 {
		i -= len(m.SignedCheque)
		copy(dAtA[i:], m.SignedCheque)
		i = encodeVarintTraffic(dAtA, i, uint64(len(m.SignedCheque)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintTraffic(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintTraffic(dAtA []byte, offset int, v uint64) int {
	offset -= sovTraffic(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *EmitCheque) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovTraffic(uint64(l))
	}
	l = len(m.SignedCheque)
	if l > 0 {
		n += 1 + l + sovTraffic(uint64(l))
	}
	return n
}

func sovTraffic(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTraffic(x uint64) (n int) {
	return sovTraffic(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *EmitCheque) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTraffic
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
			return fmt.Errorf("proto: EmitCheque: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: EmitCheque: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraffic
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
				return ErrInvalidLengthTraffic
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTraffic
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = append(m.Address[:0], dAtA[iNdEx:postIndex]...)
			if m.Address == nil {
				m.Address = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field SignedCheque", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraffic
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
				return ErrInvalidLengthTraffic
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTraffic
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.SignedCheque = append(m.SignedCheque[:0], dAtA[iNdEx:postIndex]...)
			if m.SignedCheque == nil {
				m.SignedCheque = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTraffic(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTraffic
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
func skipTraffic(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTraffic
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
					return 0, ErrIntOverflowTraffic
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
					return 0, ErrIntOverflowTraffic
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
				return 0, ErrInvalidLengthTraffic
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTraffic
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTraffic
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTraffic        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTraffic          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTraffic = fmt.Errorf("proto: unexpected end of group")
)