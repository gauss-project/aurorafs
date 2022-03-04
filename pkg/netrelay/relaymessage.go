package netrelay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/netrelay/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
)

const (
	protocolName       = "netrelay"
	protocolVersion    = "2.0.0"
	streamRelayHttpReq = "httpreq"
)

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamRelayHttpReq,
				Handler: s.relayHttpReq,
			},
		},
	}
}

func (s *Service) relayHttpReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	var httpResp pb.RelayHttpResp

	w, r := protobuf.NewWriterAndReader(stream)

	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			stream.FullClose()
		}
	}()

	reqWriter := func(resp pb.RelayHttpResp) error {
		if err = w.WriteMsgWithContext(ctx, &resp); err != nil {
			s.logger.Errorf("[relayMessage-relayHttpReq] write hash message: %w", err)
			return fmt.Errorf("[relayMessage-relayHttpReq] write hash message: %w", err)
		}
		return nil
	}

	var httpReq pb.RelayHttpReq
	if err = r.ReadMsgWithContext(ctx, &httpReq); err != nil {
		s.logger.Errorf("[relayMessage-relayHttpReq] read http req message: %w", err)
		return fmt.Errorf("[relayMessage-relayHttpReq] read http req message: %w", err)
	}

	s.logger.Tracef("[relayMessage-relayHttpReq] got http info req.")

	urls := strings.Split(httpReq.Url, "/")

	addr, ok := s.getDomainAddr(urls[1], urls[2])
	if !ok {
		httpResp.Status = http.StatusBadGateway
		httpResp.Body = []byte("Bad Gateway")
		return reqWriter(httpResp)
	}

	cli := &http.Client{}

	url := addr + strings.ReplaceAll(httpReq.Url, "/"+urls[1]+"/"+urls[2], "")
	req, err := http.NewRequest(string(httpReq.Method), url, bytes.NewReader(httpReq.Body))
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("[relayMessage-relayHttpReq] Header reading failed:%v", err.Error()))
		return reqWriter(httpResp)
	}

	reqHeaderMp := make(map[string]string)
	respHeaderMp := make(map[string]string)
	err = json.Unmarshal(httpReq.Header, &reqHeaderMp)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("[relayMessage-relayHttpReq] Header reading failed:%v", err.Error()))
		return reqWriter(httpResp)
	}

	for k, v := range reqHeaderMp {
		req.Header.Set(k, v)

	}
	res, err := cli.Do(req)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("[relayMessage-relayHttpReq] Http request failed:%v", err.Error()))
		return nil
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("[relayMessage-relayHttpReq] Error in returning http body extraction:%v", err.Error()))
		return reqWriter(httpResp)
	}

	for k, v := range res.Header {
		respHeaderMp[k] = v[0]
	}
	headerByte, err := json.Marshal(respHeaderMp)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("[relayMessage-relayHttpReq] Return Header parsing failed:%v", err.Error()))
		return reqWriter(httpResp)
	}
	httpResp.Header = headerByte
	httpResp.Body = body
	httpResp.Status = int32(res.StatusCode)

	return reqWriter(httpResp)
}

func (s *Service) SendHttp(ctx context.Context, address boson.Address, req pb.RelayHttpReq) (Response pb.RelayHttpResp, err error) {
	var stream p2p.Stream
	if s.route.IsNeighbor(address) {
		stream, err = s.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamRelayHttpReq)
	} else {
		stream, err = s.streamer.NewRelayStream(ctx, address, nil, protocolName, protocolVersion, streamRelayHttpReq, false)
	}
	if err != nil {
		s.logger.Errorf("[relayMessage] new stream: %w", err)
		return Response, err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)

	if err = w.WriteMsgWithContext(ctx, &req); err != nil {
		return Response, fmt.Errorf("[pyramid info] write message: %w", err)
	}

	if err = r.ReadMsgWithContext(ctx, &Response); err != nil {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("stream is closed")
		}
		return Response, fmt.Errorf("[relaymessage] read message: %w", err)
	}
	return Response, nil

}