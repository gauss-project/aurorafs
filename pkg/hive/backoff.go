package hive

import (
	"github.com/ethersphere/bee/pkg/swarm"
	"sync"
	"time"
)

var keyPrefix = "blocklist-"

const invalid_retry_interval = int64(30 * time.Second)
// timeNow is used to deterministically mock time.Now() in tests.
var timeNow = time.Now

type Backoff struct {
	addresses sync.Map
}

func NewBackoff() Backoff {
	return Backoff{
		addresses: sync.Map{},
	}
}

type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Exp       int64		`json:"exp"`
}

func (b *Backoff) updateAddr(addr swarm.Address) bool{
	blocked := false
	hash_addr := addr.ByteString()
	 entry_,ok := b.addresses.Load(hash_addr)
	 if ok {
	 	entry := entry_.(Entry)
	 	if entry.Exp == 0 { //it is ok to connect
	 		if time.Since(entry.Timestamp).Seconds() <= float64( invalid_retry_interval){
				entry.Timestamp = timeNow()
				entry.Exp =  1
			}else{
				entry.Timestamp = timeNow()
				entry.Exp =  0
			}
		}else{ // in block now
			reconnect_interval := timeNow().Sub(entry.Timestamp).Nanoseconds()
			if  reconnect_interval <= entry.Exp * invalid_retry_interval {
				if entry.Exp * invalid_retry_interval - reconnect_interval < int64(20*time.Second) {
					//reset
					entry.Timestamp = timeNow()
					entry.Exp =  entry.Exp + 1
				}
				blocked = true
			}else{ // out of blocktime, reset now
				entry.Timestamp = timeNow()
				entry.Exp =  0
			}
		}
		 b.addresses.Store(hash_addr,entry)
	 }else{
	 	b.addresses.Store(hash_addr,Entry{
			Timestamp : timeNow(),
			Exp :  0 ,
		})
	 }

	return blocked
}