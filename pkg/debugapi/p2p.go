package debugapi

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/gauss-project/aurorafs/pkg/boson"
	"github.com/gauss-project/aurorafs/pkg/crypto"
	"github.com/gauss-project/aurorafs/pkg/jsonhttp"
	"github.com/gauss-project/aurorafs/pkg/logging"
	"github.com/multiformats/go-multiaddr"
)

type publicIP struct {
	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
}

type addressesResponse struct {
	Overlay   boson.Address         `json:"overlay"`
	Underlay  []multiaddr.Multiaddr `json:"underlay"`
	NATRoute  []string              `json:"nat_route"`
	PublicIP  publicIP              `json:"public_ip"`
	NetworkID uint64                `json:"network_id"`
	PublicKey string                `json:"public_key"`
}

func GetPublicIp(loger logging.Logger) *publicIP {
	var (
		ip4ServiceUrl = "https://api.ipify.org"
		ip6ServiceUrl = "https://api6.ipify.org"

		ip4Content = new(bytes.Buffer)
		ip6Content = new(bytes.Buffer)
	)
	client := http.Client{
		Timeout: time.Second * 2,
	}
	ip4Resp, err := client.Get(ip4ServiceUrl)
	if err != nil {
		loger.Debugf("debug api: p2p request public ipv4: %v", err)
	} else {
		if ip4Resp.StatusCode < 200 || ip4Resp.StatusCode >= 300 {
			loger.Debugf("debug api: http service(%s) report http code %s(%d)", ip4ServiceUrl, ip4Resp.Status, ip4Resp.StatusCode)
		} else {
			_, err = ip4Content.ReadFrom(ip4Resp.Body)
			if err != nil {
				loger.Debugf("debug api: p2p parse ipv4 service response: %v", err)
			}
		}
	}
	ip6Resp, err := client.Get(ip6ServiceUrl)
	if err != nil {
		loger.Debugf("debug api: p2p request public ipv6: %v", err)
	} else {
		if ip6Resp.StatusCode < 200 || ip6Resp.StatusCode >= 300 {
			loger.Debugf("debug api: http service(%s) report http code %s(%d)", ip6ServiceUrl, ip6Resp.Status, ip6Resp.StatusCode)
		} else {
			_, err = ip6Content.ReadFrom(ip6Resp.Body)
			if err != nil {
				loger.Debugf("debug api: p2p parse ipv6 service response: %v", err)
			}
		}
	}
	return &publicIP{IPv4: ip4Content.String(), IPv6: ip6Content.String()}
}

var pubIP = &publicIP{
	IPv4: "",
	IPv6: "",
}

func (s *Service) addressesHandler(w http.ResponseWriter, r *http.Request) {
	// initialize variable to json encode as [] instead null if p2p is nil
	underlay := make([]multiaddr.Multiaddr, 0)
	natAddresses := make([]string, 0)
	// addresses endpoint is exposed before p2p service is configured
	// to provide information about other addresses.
	if s.p2p != nil {
		u, err := s.p2p.Addresses()
		if err != nil {
			s.logger.Debugf("debug api: p2p addresses: %v", err)
			jsonhttp.InternalServerError(w, err)
			return
		}
		underlay = u

		n, err := s.p2p.NATAddresses()
		if err != nil {
			s.logger.Debugf("debug api: p2p nat addresses: %v", err)
			jsonhttp.InternalServerError(w, err)
			return
		}

		for _, a := range n {
			natAddresses = append(natAddresses, a.String())
		}
	}

	key := "debugapi_addressesHandler"
	get, _ := s.cache.Get(s.cacheCtx, key)
	if get == nil {
		ch := make(chan struct{}, 1)
		go func(ch chan struct{}) {
			ip := GetPublicIp(s.logger)
			pubIP.IPv6 = ip.IPv6
			pubIP.IPv4 = ip.IPv4
			_ = s.cache.Set(s.cacheCtx, key, 1, time.Second*5)
			close(ch)
		}(ch)
		select {
		case <-time.After(time.Millisecond * 200):
		case <-ch:
		}
	}

	jsonhttp.OK(w, addressesResponse{
		Overlay:   s.overlay,
		Underlay:  underlay,
		NATRoute:  natAddresses,
		PublicIP:  *pubIP,
		NetworkID: s.nodeOptions.NetworkID,
		PublicKey: hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(&s.publicKey)),
	})
}
