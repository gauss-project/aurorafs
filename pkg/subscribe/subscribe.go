package subscribe

import (
	"fmt"
	"reflect"
	"sync"
)

type subInfo struct {
	key      string
	notifier INotifier
}

// subPub is a wrapper for the publish-subscribe function
type subPub struct {
	keyToNotifier sync.Map

	subInfoChan   chan subInfo
	unsubInfoChan chan subInfo
}

type SubPub interface {
	// Subscribe a subscription,creates a mapping from nameSpace_kind_param to INotifier
	Subscribe(iNotifier INotifier, nameSpace string, kind string, param string) error
	// Publish finds the notifier corresponding to the key nameSpace_kind_param, and then calls the Notify method of
	// notify to publish message
	Publish(nameSpace string, kind string, param string, message interface{}) error
	// PublishArray finds the notifier corresponding to the key nameSpace_kind_param, and then calls the Notify method of
	// notify to publish a group of message
	PublishArray(nameSpace string, kind string, field string, messageList []interface{}) error
}

// NewSubPub return a subPub
func NewSubPub() *subPub {
	s := &subPub{
		subInfoChan:   make(chan subInfo, 50),
		unsubInfoChan: make(chan subInfo, 50),
	}
	go s.process()
	return s
}

// Subscribe a subscription, use NewNotifier to warp the *rpc.INotifier and get a INotifier interface
func (s *subPub) Subscribe(iNotifier INotifier, nameSpace string, kind string, param string) error {
	var key string
	if param != "" {
		key = fmt.Sprintf("%s_%s_%s", nameSpace, kind, param)
	} else {
		key = fmt.Sprintf("%s_%s", nameSpace, kind)
	}

	info := subInfo{
		key:      key,
		notifier: iNotifier,
	}

	s.subInfoChan <- info

	go func() {
		<-iNotifier.Err()
		s.unsubInfoChan <- info
	}()

	return nil
}

// a goroutine to process subscription and unsubscription, start after you call NewSubPub
func (s *subPub) process() {
	for {
		select {
		case info := <-s.subInfoChan:
			var slice []*subInfo
			v, ok := s.keyToNotifier.Load(info.key)
			if !ok {
				slice = make([]*subInfo, 0, 1)
			} else {
				slice = v.([]*subInfo)
			}
			slice = append(slice, &info)
			s.keyToNotifier.Store(info.key, slice)
		case info := <-s.unsubInfoChan:
			v, ok := s.keyToNotifier.Load(info.key)
			if !ok {
				continue
			}
			slice := v.([]*subInfo)
			cSlice := make([]*subInfo, len(slice))
			copy(cSlice, slice)
			for j := 0; j < len(cSlice); j++ {
				if cSlice[j].notifier == info.notifier {
					cSlice = append(cSlice[:j], cSlice[j+1:]...)
				}
			}
			if len(cSlice) == 0 {
				s.keyToNotifier.Delete(info.key)
			} else {
				s.keyToNotifier.Store(info.key, cSlice)
			}
		}
	}
}

// Publish the message, nameSpace kind param is that you use when you call Subscribe
func (s *subPub) Publish(nameSpace string, kind string, param string, message interface{}) error {
	keyPrefix := fmt.Sprintf("%s_%s", nameSpace, kind)

	keyList := make([]string, 0, 2)
	keyList = append(keyList, keyPrefix)
	if param != "" {
		keyList = append(keyList, keyPrefix+"_"+param)
	}

	for i := 0; i < len(keyList); i++ {
		v, ok := s.keyToNotifier.Load(keyList[i])
		if !ok {
			continue
		}
		slice := v.([]*subInfo)
		for _, sub := range slice {
			_ = sub.notifier.Notify(keyList[i], message)
		}
	}
	return nil
}

// PublishArray publish a group of message, nameSpace kind is that you use when you call Subscribe.
// the element of messageList should be a struct that has a Field named field. The Field should be
// a string type or implement iString interface so that we can get a string as param.
//
// NOTE:
// type Str1 struct{ str string }
// func (s *Str1) String() string { return s.str }
// *Str1 implements iString interface, but Str1 doesn't.
//
// type Str2 struct{ str string }
// func (s Str2) String() string { return s.str }
// *Str2 implements iString interface, Str2 does also.
func (s *subPub) PublishArray(nameSpace string, kind string, field string, messageList []interface{}) error {

	messageMap := make(map[string][]interface{})
	keyAll := fmt.Sprintf("%s_%s", nameSpace, kind)

	for _, message := range messageList {
		param := getKeyInStruct(field, message)

		if param != "" {
			keySpec := fmt.Sprintf("%s_%s_%s", nameSpace, kind, param)

			slice, ok := messageMap[keySpec]
			if !ok {
				slice = make([]interface{}, 0, 1)
			}
			slice = append(slice, message)
			messageMap[keySpec] = slice
		}

		slice, ok := messageMap[keyAll]
		if !ok {
			slice = make([]interface{}, 0, 1)
		}
		slice = append(slice, message)
		messageMap[keyAll] = slice
	}

	for key, msgList := range messageMap {
		v, ok := s.keyToNotifier.Load(key)
		if !ok {
			continue
		}
		subList := v.([]*subInfo)
		for _, sub := range subList {
			for i := 0; i < len(msgList); i++ {
				_ = sub.notifier.Notify(key, msgList[i])
			}
		}
	}
	return nil
}

// get the param from the field named f in struct s
func getKeyInStruct(f string, s interface{}) string {
	value := reflect.ValueOf(s)

	var fieldVal reflect.Value
	if value.Kind() != reflect.Struct {
		if value.Kind() != reflect.Ptr {
			return ""
		}
		v := value.Elem()
		if v.Kind() != reflect.Struct {
			return ""
		}
		fieldVal = v.FieldByName(f)
	} else {
		fieldVal = value.FieldByName(f)
	}

	if !fieldVal.IsValid() {
		return ""
	}

	if fieldVal.Kind() == reflect.String {
		return fieldVal.String()
	}

	fieldInterface := fieldVal.Interface()
	key, ok := fieldInterface.(iString)
	if ok {
		return key.String()
	}

	return ""
}

type iString interface {
	String() string
}
