package libp2p

import (
	"bytes"
	"github.com/gauss-project/aurorafs/pkg/p2p"
)

var _ p2p.Stream = (*virtualStream)(nil)

type virtualStream struct {
	p2p.Stream
	buf    bytes.Buffer
	writer p2p.WriterChan
	read   p2p.ReaderChan
	done   chan struct{}
}

func newVirtualStream(s p2p.Stream, w p2p.WriterChan, r p2p.ReaderChan, done chan struct{}) *virtualStream {
	srv := &virtualStream{
		Stream: s,
		writer: w,
		read:   r,
		done:   done,
	}
	return srv
}

func (s *virtualStream) Read(p []byte) (int, error) {
	if s.buf.Len() == 0 {
		select {
		case d := <-s.read.R:
			_, err := s.buf.Write(d)
			if err != nil {
				return 0, err
			}
		case err := <-s.read.Err:
			return 0, err
		}
	}
	return s.buf.Read(p)
}

func (s *virtualStream) Write(p []byte) (n int, err error) {
	s.writer.W <- p
	err = <-s.writer.Err
	return
}

func (s *virtualStream) Reset() error {
	close(s.done)
	return nil
}

func (s *virtualStream) FullClose() error {
	close(s.done)
	return nil
}
