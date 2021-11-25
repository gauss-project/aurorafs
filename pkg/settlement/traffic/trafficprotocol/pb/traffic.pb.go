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

type SignedCheque struct {
	Cheque    *Cheque `protobuf:"bytes,1,opt,name=cheque,proto3" json:"cheque,omitempty"`
	Signature []byte  `protobuf:"bytes,2,opt,name=Signature,proto3" json:"Signature,omitempty"`
}

func (m *SignedCheque) Reset()         { *m = SignedCheque{} }
func (m *SignedCheque) String() string { return proto.CompactTextString(m) }
func (*SignedCheque) ProtoMessage()    {}
func (*SignedCheque) Descriptor() ([]byte, []int) {
	return fileDescriptor_50e185a42cb2d3c6, []int{0}
}
func (m *SignedCheque) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SignedCheque) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SignedCheque.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SignedCheque) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SignedCheque.Merge(m, src)
}
func (m *SignedCheque) XXX_Size() int {
	return m.Size()
}
func (m *SignedCheque) XXX_DiscardUnknown() {
	xxx_messageInfo_SignedCheque.DiscardUnknown(m)
}

var xxx_messageInfo_SignedCheque proto.InternalMessageInfo

func (m *SignedCheque) GetCheque() *Cheque {
	if m != nil {
		return m.Cheque
	}
	return nil
}

func (m *SignedCheque) GetSignature() []byte {
	if m != nil {
		return m.Signature
	}
	return nil
}

type Cheque struct {
	Recipient        []byte `protobuf:"bytes,1,opt,name=Recipient,proto3" json:"Recipient,omitempty"`
	Beneficiary      []byte `protobuf:"bytes,2,opt,name=Beneficiary,proto3" json:"Beneficiary,omitempty"`
	CumulativePayout uint64 `protobuf:"varint,3,opt,name=CumulativePayout,proto3" json:"CumulativePayout,omitempty"`
}

func (m *Cheque) Reset()         { *m = Cheque{} }
func (m *Cheque) String() string { return proto.CompactTextString(m) }
func (*Cheque) ProtoMessage()    {}
func (*Cheque) Descriptor() ([]byte, []int) {
	return fileDescriptor_50e185a42cb2d3c6, []int{1}
}
func (m *Cheque) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Cheque) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Cheque.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Cheque) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Cheque.Merge(m, src)
}
func (m *Cheque) XXX_Size() int {
	return m.Size()
}
func (m *Cheque) XXX_DiscardUnknown() {
	xxx_messageInfo_Cheque.DiscardUnknown(m)
}

var xxx_messageInfo_Cheque proto.InternalMessageInfo

func (m *Cheque) GetRecipient() []byte {
	if m != nil {
		return m.Recipient
	}
	return nil
}

func (m *Cheque) GetBeneficiary() []byte {
	if m != nil {
		return m.Beneficiary
	}
	return nil
}

func (m *Cheque) GetCumulativePayout() uint64 {
	if m != nil {
		return m.CumulativePayout
	}
	return 0
}

func init() {
	proto.RegisterType((*SignedCheque)(nil), "traffic.SignedCheque")
	proto.RegisterType((*Cheque)(nil), "traffic.Cheque")
}

func init() { proto.RegisterFile("traffic.proto", fileDescriptor_50e185a42cb2d3c6) }

var fileDescriptor_50e185a42cb2d3c6 = []byte{
	// 208 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x29, 0x4a, 0x4c,
	0x4b, 0xcb, 0x4c, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x87, 0x72, 0x95, 0x42, 0xb9,
	0x78, 0x82, 0x33, 0xd3, 0xf3, 0x52, 0x53, 0x9c, 0x33, 0x52, 0x0b, 0x4b, 0x53, 0x85, 0xd4, 0xb9,
	0xd8, 0x92, 0xc1, 0x2c, 0x09, 0x46, 0x05, 0x46, 0x0d, 0x6e, 0x23, 0x7e, 0x3d, 0x98, 0x46, 0x88,
	0x82, 0x20, 0xa8, 0xb4, 0x90, 0x0c, 0x17, 0x27, 0x48, 0x63, 0x62, 0x49, 0x69, 0x51, 0xaa, 0x04,
	0x93, 0x02, 0xa3, 0x06, 0x4f, 0x10, 0x42, 0x40, 0xa9, 0x84, 0x8b, 0xcd, 0x19, 0xae, 0x2e, 0x28,
	0x35, 0x39, 0xb3, 0x20, 0x33, 0x35, 0xaf, 0x04, 0x6c, 0x26, 0x4f, 0x10, 0x42, 0x40, 0x48, 0x81,
	0x8b, 0xdb, 0x29, 0x35, 0x2f, 0x35, 0x2d, 0x33, 0x39, 0x33, 0xb1, 0xa8, 0x12, 0x6a, 0x0e, 0xb2,
	0x90, 0x90, 0x16, 0x97, 0x80, 0x73, 0x69, 0x6e, 0x69, 0x4e, 0x62, 0x49, 0x66, 0x59, 0x6a, 0x40,
	0x62, 0x65, 0x7e, 0x69, 0x89, 0x04, 0xb3, 0x02, 0xa3, 0x06, 0x4b, 0x10, 0x86, 0xb8, 0x93, 0xcc,
	0x89, 0x47, 0x72, 0x8c, 0x17, 0x1e, 0xc9, 0x31, 0x3e, 0x78, 0x24, 0xc7, 0x38, 0xe1, 0xb1, 0x1c,
	0xc3, 0x85, 0xc7, 0x72, 0x0c, 0x37, 0x1e, 0xcb, 0x31, 0x44, 0x31, 0x15, 0x24, 0x25, 0xb1, 0x81,
	0xbd, 0x6e, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0x49, 0x01, 0x2e, 0x03, 0x0b, 0x01, 0x00, 0x00,
}

func (m *SignedCheque) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SignedCheque) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SignedCheque) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Signature) > 0 {
		i -= len(m.Signature)
		copy(dAtA[i:], m.Signature)
		i = encodeVarintTraffic(dAtA, i, uint64(len(m.Signature)))
		i--
		dAtA[i] = 0x12
	}
	if m.Cheque != nil {
		{
			size, err := m.Cheque.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTraffic(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Cheque) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Cheque) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Cheque) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.CumulativePayout != 0 {
		i = encodeVarintTraffic(dAtA, i, uint64(m.CumulativePayout))
		i--
		dAtA[i] = 0x18
	}
	if len(m.Beneficiary) > 0 {
		i -= len(m.Beneficiary)
		copy(dAtA[i:], m.Beneficiary)
		i = encodeVarintTraffic(dAtA, i, uint64(len(m.Beneficiary)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Recipient) > 0 {
		i -= len(m.Recipient)
		copy(dAtA[i:], m.Recipient)
		i = encodeVarintTraffic(dAtA, i, uint64(len(m.Recipient)))
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
func (m *SignedCheque) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Cheque != nil {
		l = m.Cheque.Size()
		n += 1 + l + sovTraffic(uint64(l))
	}
	l = len(m.Signature)
	if l > 0 {
		n += 1 + l + sovTraffic(uint64(l))
	}
	return n
}

func (m *Cheque) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Recipient)
	if l > 0 {
		n += 1 + l + sovTraffic(uint64(l))
	}
	l = len(m.Beneficiary)
	if l > 0 {
		n += 1 + l + sovTraffic(uint64(l))
	}
	if m.CumulativePayout != 0 {
		n += 1 + sovTraffic(uint64(m.CumulativePayout))
	}
	return n
}

func sovTraffic(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTraffic(x uint64) (n int) {
	return sovTraffic(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *SignedCheque) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: SignedCheque: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SignedCheque: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Cheque", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraffic
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTraffic
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTraffic
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Cheque == nil {
				m.Cheque = &Cheque{}
			}
			if err := m.Cheque.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Signature", wireType)
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
			m.Signature = append(m.Signature[:0], dAtA[iNdEx:postIndex]...)
			if m.Signature == nil {
				m.Signature = []byte{}
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
func (m *Cheque) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: Cheque: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Cheque: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Recipient", wireType)
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
			m.Recipient = append(m.Recipient[:0], dAtA[iNdEx:postIndex]...)
			if m.Recipient == nil {
				m.Recipient = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Beneficiary", wireType)
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
			m.Beneficiary = append(m.Beneficiary[:0], dAtA[iNdEx:postIndex]...)
			if m.Beneficiary == nil {
				m.Beneficiary = []byte{}
			}
			iNdEx = postIndex
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CumulativePayout", wireType)
			}
			m.CumulativePayout = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraffic
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CumulativePayout |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
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
