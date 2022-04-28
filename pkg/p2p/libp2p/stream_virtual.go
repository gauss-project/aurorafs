package libp2p

import (
	"bytes"

	"github.com/gauss-project/aurorafs/pkg/p2p"
	"go.uber.org/atomic"
)

var _ p2p.Stream = (*virtualStream)(nil)

type virtualStream struct {
	p2p.Stream
	buf              bytes.Buffer
	writer           *p2p.WriterChan
	reader           *p2p.ReaderChan
	done             chan struct{}
	realStreamClosed *atomic.Bool
}

func newVirtualStream(s p2p.Stream) *virtualStream {
	srv := &virtualStream{
		Stream: s,
		writer: &p2p.WriterChan{
			W:   make(chan []byte, 1),
			Err: make(chan error, 1),
		},
		reader: &p2p.ReaderChan{
			R:   make(chan []byte, 1),
			Err: make(chan error, 1),
		},
		done:             make(chan struct{}, 1),
		realStreamClosed: atomic.NewBool(false),
	}
	return srv
}

func (s *virtualStream) Read(p []byte) (int, error) {
	if s.buf.Len() == 0 {
		if s.realStreamClosed.Load() {
			return 0, p2p.ErrStreamClosed
		}
		select {
		case d := <-s.reader.R:
			_, err := s.buf.Write(d)
			if err != nil {
				return 0, err
			}
		case err := <-s.reader.Err:
			return 0, err
		}
	}
	return s.buf.Read(p)
}

func (s *virtualStream) Write(p []byte) (n int, err error) {
	if s.realStreamClosed.Load() {
		return 0, p2p.ErrStreamClosed
	}
	s.writer.W <- p
	err = <-s.writer.Err
	return
}

func (s *virtualStream) Reset() error {
	close(s.done)
	s.realStreamClosed.Store(true)
	return nil
}

func (s *virtualStream) FullClose() error {
	close(s.done)
	s.realStreamClosed.Store(true)
	return nil
}

func (s *virtualStream) UpdateStatRealStreamClosed() {
	s.realStreamClosed.Store(true)
}

func (s *virtualStream) Reader() *p2p.ReaderChan {
	return s.reader
}

func (s *virtualStream) Writer() *p2p.WriterChan {
	return s.writer
}

func (s *virtualStream) Done() chan struct{} {
	return s.done
}

func (s *virtualStream) RealStream() p2p.Stream {
	return s.Stream
}
