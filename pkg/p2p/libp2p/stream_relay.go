package libp2p

import (
	"bytes"
	"errors"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
	"github.com/gauss-project/aurorafs/pkg/routetab/pb"
	"io"
	"sync"
)

var _ p2p.Stream = (*relayStream)(nil)

type relayStream struct {
	p2p.Stream
	buf            bytes.Buffer
	relayData      *pb.RouteRelayReq
	chWrite        chan chWrite
	o              sync.Once
	chClose        chan struct{}
	w              protobuf.Writer
	r              protobuf.Reader
	reallyDataChan chan []byte
}

type chWrite struct {
	data []byte
	err  chan error
	resp chan bool
}

func newRelayStream(s p2p.Stream, data *pb.RouteRelayReq, reallyDataChan chan []byte) *relayStream {
	srv := &relayStream{
		Stream:         s,
		relayData:      data,
		chWrite:        make(chan chWrite, 1),
		chClose:        make(chan struct{}, 1),
		w:              protobuf.NewWriter(s),
		r:              protobuf.NewReader(s),
		reallyDataChan: reallyDataChan,
	}
	go func() {
		for {
			select {
			case <-srv.chClose:
				return
			case ch := <-srv.chWrite:
				// wait target resp
				err := srv.Pack(ch.data)
				if err != nil {
					ch.err <- err
				}
				ch.resp <- true
			}
		}
	}()
	return srv
}

func (s *relayStream) Unpack() error {
	resp := &pb.RouteRelayResp{}
	err := s.r.ReadMsg(resp)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	_, err = s.buf.Write(resp.Data)
	return err
}

func (s *relayStream) Pack(p []byte) error {
	s.relayData.Data = p
	err := s.w.WriteMsg(s.relayData)
	if err != nil {
		return err
	}
	return s.Unpack()
}

func (s *relayStream) Read(p []byte) (n int, err error) {
	if s.buf.Len() > 0 {
		return s.buf.Read(p)
	}
	if s.reallyDataChan != nil {
		_, err = s.buf.Write(<-s.reallyDataChan)
	} else {
		err = s.Unpack()
	}
	if err != nil {
		return
	}
	return s.buf.Read(p)
}

func (s *relayStream) Write(p []byte) (n int, err error) {
	errChan := make(chan error, 1)
	resp := make(chan bool, 1)
	s.chWrite <- chWrite{
		data: p,
		err:  errChan,
		resp: resp,
	}
	select {
	case err = <-errChan:
		return 0, err
	case <-resp:
		return
	}
}

func (s *relayStream) Reset() error {
	s.o.Do(func() {
		close(s.chClose)
	})
	return s.Stream.Reset()
}

func (s *relayStream) FullClose() error {
	s.o.Do(func() {
		close(s.chClose)
	})
	return s.Stream.FullClose()
}
