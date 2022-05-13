package subscribe

import (
	"github.com/gauss-project/aurorafs/pkg/rpc"
	"time"
)

type INotifier interface {
	Notify(key string, data interface{}) error
	Err() <-chan error
}

// NewNotifier wrap *rpc.INotifier to meet the interface named INotifier
func NewNotifier(n *rpc.Notifier, sub *rpc.Subscription) *Notifier {
	return &Notifier{
		notifier: n,
		sub:      sub,
	}
}

type Notifier struct {
	notifier *rpc.Notifier
	sub      *rpc.Subscription
}

func (n *Notifier) Notify(_ string, data interface{}) error {
	return n.notifier.Notify(n.sub.ID, data)
}

func (n *Notifier) Err() <-chan error {
	return n.sub.Err()
}

// NewNotifierWithDelay wrap *rpc.INotifier to add some extra features
func NewNotifierWithDelay(notifier *rpc.Notifier, sub *rpc.Subscription, delay int, sendArray bool) *NotifierWithDelay {
	var n = &NotifierWithDelay{
		sendArray: sendArray,
		notifier:  notifier,
		sub:       sub,
		ch: make(chan struct {
			key string
			msg interface{}
		}, 10),
		msgMap: make(map[string]interface{}),
	}
	go n.process(delay)
	return n
}

type NotifierWithDelay struct {
	sendArray bool
	notifier  *rpc.Notifier
	sub       *rpc.Subscription
	msgMap    map[string]interface{}
	ch        chan struct {
		key string
		msg interface{}
	}
}

func (n *NotifierWithDelay) Notify(key string, data interface{}) error {
	n.ch <- struct {
		key string
		msg interface{}
	}{key: key, msg: data}
	return nil
}

func (n *NotifierWithDelay) Err() <-chan error {
	return n.sub.Err()
}

func (n *NotifierWithDelay) process(delay int) {
	ticker := time.NewTicker(time.Duration(delay) * time.Second)
	for {
		select {
		case msg := <-n.ch:
			n.msgMap[msg.key] = msg.msg
		case <-ticker.C:
			if len(n.msgMap) == 0 {
				continue
			}
			if !n.sendArray {
				for k, msg := range n.msgMap {
					_ = n.notifier.Notify(n.sub.ID, msg)
					delete(n.msgMap, k)
				}
				continue
			}
			msgList := make([]interface{}, 0, len(n.msgMap))
			for k, msg := range n.msgMap {
				msgList = append(msgList, msg)
				delete(n.msgMap, k)
			}
			_ = n.notifier.Notify(n.sub.ID, msgList)
		case <-n.sub.Err():
			return
		}
	}
}

// NewNotifierWithMsgChan return a notifier which will use method named Notify to send message to a field named MsgChan in the notifier
func NewNotifierWithMsgChan() *NotifierWithMsgChan {
	n := &NotifierWithMsgChan{
		MsgChan: make(chan interface{}, 10),
		ErrChan: make(chan error),
	}
	return n
}

type NotifierWithMsgChan struct {
	MsgChan chan interface{} // msg will be sent to this chan
	ErrChan chan error       // close this chan if you want to unsubscribe
}

func (n *NotifierWithMsgChan) Notify(_ string, data interface{}) error {
	n.MsgChan <- data
	return nil
}

func (n *NotifierWithMsgChan) Err() <-chan error {
	return n.ErrChan
}
