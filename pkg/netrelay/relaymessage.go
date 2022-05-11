package netrelay

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/aurora"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/multicast/model"
	"github.com/gauss-project/aurorafs/pkg/netrelay/pb"
	"github.com/gauss-project/aurorafs/pkg/p2p"
	"github.com/gauss-project/aurorafs/pkg/p2p/protobuf"
)

const (
	protocolName         = "netrelay"
	protocolVersion      = "2.0.0"
	streamRelayHttpReq   = "httpreq"   // v1 http proxy
	streamRelayHttpReqV2 = "httpreqv2" // v2 http proxy support ws
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
			{
				Name:    streamRelayHttpReqV2,
				Handler: s.onRelayHttpReqV2,
			},
		},
	}
}

// Deprecated
func (s *Service) relayHttpReq(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	var httpResp pb.RelayHttpResp

	w, r := protobuf.NewWriterAndReader(stream)

	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	reqWriter := func(resp pb.RelayHttpResp) error {
		if err = w.WriteMsgWithContext(ctx, &resp); err != nil {
			s.logger.Errorf("relayHttpReq: write resp to %s err %w", p.Address, err)
			return fmt.Errorf("write resp: %w", err)
		}
		return nil
	}

	var httpReq pb.RelayHttpReq
	if err = r.ReadMsgWithContext(ctx, &httpReq); err != nil {
		s.logger.Errorf("relayHttpReq: read req from %s err %w", p.Address, err)
		return fmt.Errorf("read req: %w", err)
	}

	s.logger.Tracef("relayHttpReq: from %s got req: %s", p.Address, httpReq.Url)

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
		httpResp.Body = []byte(fmt.Sprintf("on req convert to http.request err: %v", err.Error()))
		return reqWriter(httpResp)
	}

	reqHeaderMp := make(map[string]string)
	respHeaderMp := make(map[string]string)
	err = json.Unmarshal(httpReq.Header, &reqHeaderMp)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("on req parase header err: %v", err.Error()))
		return reqWriter(httpResp)
	}

	for k, v := range reqHeaderMp {
		req.Header.Set(k, v)

	}
	res, err := cli.Do(req)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("on req do real request err: %v", err.Error()))
		return nil
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("on req read real resp.Body err: %v", err.Error()))
		return reqWriter(httpResp)
	}

	for k, v := range res.Header {
		respHeaderMp[k] = v[0]
	}
	headerByte, err := json.Marshal(respHeaderMp)
	if err != nil {
		httpResp.Status = http.StatusInternalServerError
		httpResp.Body = []byte(fmt.Sprintf("on req parase real resp header err: %v", err.Error()))
		return reqWriter(httpResp)
	}
	httpResp.Header = headerByte
	httpResp.Body = body
	httpResp.Status = int32(res.StatusCode)

	return reqWriter(httpResp)
}

// SendHttp Deprecated see relayHttpReqV2
func (s *Service) SendHttp(ctx context.Context, address boson.Address, req pb.RelayHttpReq) (Response pb.RelayHttpResp, err error) {
	var stream p2p.Stream
	if s.route.IsNeighbor(address) {
		stream, err = s.streamer.NewStream(ctx, address, nil, protocolName, protocolVersion, streamRelayHttpReq)
		if err != nil {
			err = fmt.Errorf("new stream err: %s", err)
		}
	} else {
		stream, err = s.streamer.NewRelayStream(ctx, address, nil, protocolName, protocolVersion, streamRelayHttpReq, false)
		if err != nil {
			err = fmt.Errorf("new realay stream err: %s", err)
		}
	}
	if err != nil {
		s.logger.Errorf("SendHttp to %s %s", address, err)
		return Response, err
	}
	defer func() {
		if err != nil {
			s.logger.Errorf("SendHttp to %s %s", address, err)
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)

	if err = w.WriteMsgWithContext(ctx, &req); err != nil {
		return Response, fmt.Errorf("write message: %w", err)
	}

	if err = r.ReadMsgWithContext(ctx, &Response); err != nil {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("stream is closed")
		}
		return Response, fmt.Errorf("read message: %w", err)
	}
	return Response, nil

}

// Deprecated
func (s *Service) getDomainAddr(groupName, domainName string) (string, bool) {
	for _, v := range s.groups {
		if v.Name == groupName {
			for _, domain := range v.AgentHttp {
				if domain.Domain == domainName {
					return domain.Addr, true
				}
			}
		}
	}
	s.logger.Errorf("domain %v not found in group %s", domainName, groupName)
	return "", false
}

func (s *Service) getDomainAddrWithScheme(scheme, groupName, domainName string) (string, bool) {
	for _, v := range s.groups {
		if v.Name == groupName {
			var agents []model.ConfigNetDomain
			switch scheme {
			case "ws", "wss":
				agents = v.AgentWS
			case "http", "https":
				agents = v.AgentHttp
			}
			for _, domain := range agents {
				if domain.Domain == domainName {
					return domain.Addr, true
				}
			}
		}
	}
	s.logger.Errorf("domain %v not found in group %s", domainName, groupName)
	return "", false
}

func (s *Service) onRelayHttpReqV2(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	defer func() {
		if err != nil {
			s.logger.Tracef("onRelayHttpReqV2 from %s err %s", p.Address, err)
			_ = stream.Reset()
		} else {
			_ = stream.Close()
			s.logger.Tracef("onRelayHttpReqV2 from %s stream close", p.Address)
		}
	}()

	buf := bufio.NewReader(stream)
	req, err := http.ReadRequest(buf)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	defer req.Body.Close()

	url := strings.ReplaceAll(req.URL.String(), aurora.RelayPrefixHttp, "")
	urls := strings.Split(url, "/")

	var reqWS bool
	if req.Header.Get("Connection") == "Upgrade" && req.Header.Get("Upgrade") == "websocket" {
		req.URL.Scheme = "ws"
		reqWS = true
	} else {
		req.URL.Scheme = "http"
	}
	addr, ok := s.getDomainAddrWithScheme(req.URL.Scheme, urls[1], urls[2])
	if !ok {
		return errors.New("domain parse err")
	}
	req.URL, err = req.URL.Parse(addr + strings.ReplaceAll(url, "/"+urls[1]+"/"+urls[2], ""))
	if err != nil {
		return err
	}
	s.logger.Infof("onRelayHttpReqV2 from %s request to %s", p.Address, req.URL)

	req.Host = req.URL.Host
	req.RequestURI = req.URL.RequestURI()

	if !reqWS {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			_, _ = stream.Write([]byte(err.Error()))
			return err
		}
		// resp.Write writes whatever response we obtained for our
		// request back to the stream.
		return resp.Write(stream)
	} else {
		var remoteConn net.Conn
		switch req.URL.Scheme {
		case "ws":
			remoteConn, err = net.Dial("tcp", req.URL.Host)
		case "wss":
			remoteConn, err = tls.Dial("tcp", req.URL.Host, &tls.Config{
				InsecureSkipVerify: true,
			})
		}
		if err != nil {
			_, _ = stream.Write([]byte(err.Error()))
			return err
		}
		defer remoteConn.Close()
		b, _ := httputil.DumpRequest(req, false)
		_, err = remoteConn.Write(b)
		if err != nil {
			_, _ = stream.Write([]byte(err.Error()))
			return err
		}
		// response
		respErrCh := make(chan error, 1)
		go func() {
			_, err = io.Copy(stream, remoteConn)
			s.logger.Tracef("onRelayHttpReqV2 from %s io.copy resp err %v", p.Address, err)
			respErrCh <- err
		}()
		// request
		reqErrCh := make(chan error, 1)
		go func() {
			_, err = io.Copy(remoteConn, stream)
			s.logger.Tracef("onRelayHttpReqV2 from %s io.copy req err %v", p.Address, err)
			reqErrCh <- err
		}()
		select {
		case err = <-respErrCh:
			return err
		case err = <-reqErrCh:
			return err
		}
	}
}
