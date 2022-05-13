package subscribe

import (
	"fmt"
	"testing"
	"time"
)

type S struct {
	str string
}

func (k S) String() string {
	return k.str
}

type InfoA struct {
	Key  string
	Info string
}

type InfoB struct {
	Key  S
	Info string
}

type TestNotifier struct {
	message []interface{}
	ch      chan error
}

func NewTestNotifier() *TestNotifier {
	return &TestNotifier{
		ch: make(chan error),
	}
}

func (tn *TestNotifier) Notify(_ string, data interface{}) error {
	tn.message = append(tn.message, data)
	return nil
}

func (tn *TestNotifier) Err() <-chan error {
	return tn.ch
}

func (tn *TestNotifier) Close() {
	close(tn.ch)
}

type Sub struct {
	key string

	nameSpace string
	kind      string
	param     string

	notifier *TestNotifier
}

type Pub struct {
	nameSpace string
	kind      string
	param     string

	field string

	InfoType        InfoType
	InfoString      string
	InfoStructListA []interface{}
	InfoStructListB []interface{}
}

type InfoType int

const (
	String InfoType = iota
	StructArrayA
	StructArrayB
)

func TestSubPub(t *testing.T) {
	SubList := []*Sub{
		{key: "ns1_k1", nameSpace: "ns1", kind: "k1", notifier: NewTestNotifier()},
		{key: "ns1_k1_p1", nameSpace: "ns1", kind: "k1", param: "p1", notifier: NewTestNotifier()},
		{key: "ns1_k1_p2", nameSpace: "ns1", kind: "k1", param: "p2", notifier: NewTestNotifier()},

		{key: "ns1_k1", nameSpace: "ns1", kind: "k1", notifier: NewTestNotifier()},
		{key: "ns1_k1_p1", nameSpace: "ns1", kind: "k1", param: "p1", notifier: NewTestNotifier()},
		{key: "ns1_k1_p2", nameSpace: "ns1", kind: "k1", param: "p2", notifier: NewTestNotifier()},

		{key: "ns1_k2", nameSpace: "ns1", kind: "k2", notifier: NewTestNotifier()},
		{key: "ns1_k2_p1", nameSpace: "ns1", kind: "k2", param: "p1", notifier: NewTestNotifier()},
		{key: "ns1_k2_p2", nameSpace: "ns1", kind: "k2", param: "p2", notifier: NewTestNotifier()},
	}

	messageList := []*Pub{
		{nameSpace: "ns1", kind: "k1", param: "p1", InfoType: String, InfoString: "info1"},
		{nameSpace: "ns1", kind: "k1", param: "p2", InfoType: String, InfoString: "info2"},
		{nameSpace: "ns1", kind: "k1", InfoType: StructArrayA, field: "Key", InfoStructListA: []interface{}{InfoA{Key: "p1", Info: "info3"}, InfoA{Key: "p2", Info: "info4"}}},

		{nameSpace: "ns1", kind: "k2", param: "p1", InfoType: String, InfoString: "info5"},
		{nameSpace: "ns1", kind: "k2", param: "p2", InfoType: String, InfoString: "info6"},
		{nameSpace: "ns1", kind: "k2", InfoType: StructArrayB, field: "Key", InfoStructListB: []interface{}{InfoB{Key: S{str: "p1"}, Info: "info7"}, InfoB{Key: S{str: "p2"}, Info: "info8"}}},
	}

	sp := NewSubPub()

	// subscribe subscription
	for _, sub := range SubList {
		err := sp.Subscribe(sub.notifier, sub.nameSpace, sub.kind, sub.param)
		if err != nil {
			t.Fatalf("subscribe err:%v,nameSpace:%s, kind:%s, param:%s", err, sub.nameSpace, sub.kind, sub.param)
		}
	}

	time.Sleep(300 * time.Millisecond)

	// publish message
	for _, message := range messageList {
		switch message.InfoType {
		case String:
			err := sp.Publish(message.nameSpace, message.kind, message.param, message.InfoString)
			if err != nil {
				t.Fatalf("subscribe err:%v,nameSpace:%s, kind:%s, param:%s, message:%v", err, message.nameSpace, message.kind, message.param, message.InfoString)
			}
		case StructArrayA:
			err := sp.PublishArray(message.nameSpace, message.kind, message.field, message.InfoStructListA)
			if err != nil {
				t.Fatalf("subscribe err:%v,nameSpace:%s, kind:%s, field:%s, message:%v", err, message.nameSpace, message.kind, message.field, message.InfoString)
			}
		case StructArrayB:
			err := sp.PublishArray(message.nameSpace, message.kind, message.field, message.InfoStructListB)
			if err != nil {
				t.Fatalf("subscribe err:%v,nameSpace:%s, kind:%s, field:%s, message:%v", err, message.nameSpace, message.kind, message.field, message.InfoString)
			}
		}
	}

	// check subscription
	for _, sub := range SubList {
		v, ok := sp.keyToNotifier.Load(sub.key)
		if !ok {
			t.Fatalf("subscribe fail,no subscribtion found,key:%s", sub.key)
		}
		slice := v.([]*subInfo)
		isFind := false
		for _, s := range slice {
			if s.key != sub.key {
				t.Fatalf("subscribe fail,keys are not match,key(%s)!=key(%s)", sub.key, s.key)
			}
			if s.notifier == sub.notifier {
				if isFind == false {
					isFind = true
				} else {
					t.Fatalf("repeat subscription,key:%s", sub.key)
				}
			}
		}
		if !isFind {
			t.Fatalf("subscribe fail,subscribtion not found,key:%s", sub.key)
		}
	}

	sp.keyToNotifier.Range(func(k interface{}, v interface{}) bool {
		slice := v.([]*subInfo)
		for _, s := range slice {
			isFind := false
			for _, sub := range SubList {
				if sub.notifier == s.notifier {
					isFind = true
					if sub.key != s.key {
						t.Fatalf("subscribe fail,keys are not match,key(%s)!=key(%s)", sub.key, s.key)
					}
				}
			}
			if !isFind {
				t.Fatalf("subscribe fail,subscribtion not found,key:%s", s.key)
			}
		}
		return true
	})

	// check message
	for _, message := range messageList {
		isFind := false
		sp.keyToNotifier.Range(func(k interface{}, v interface{}) bool {
			key := k.(string)
			slice := v.([]*subInfo)
			for _, sub := range slice {
				notifier := sub.notifier.(*TestNotifier)
				for _, msg := range notifier.message {
					switch msg.(type) {
					case string:
						if message.InfoType != String {
							continue
						}
						str := msg.(string)
						if str == message.InfoString {
							isFind = true
							keyAll := fmt.Sprintf("%s_%s_%s", message.nameSpace, message.kind, message.param)
							keySpec := fmt.Sprintf("%s_%s", message.nameSpace, message.kind)
							if key == keyAll || key == keySpec {

							} else {
								t.Fatalf("message publish error, nameSpace:%s, kind:%s, param:%s, messageKey:%s", message.nameSpace, message.kind, message.param, key)
							}
						}
					case InfoA:
						if message.InfoType != StructArrayA {
							continue
						}
						infoA := msg.(InfoA)
						for _, i := range message.InfoStructListA {
							iA := i.(InfoA)
							if infoA == iA {
								isFind = true
								keyAll := fmt.Sprintf("%s_%s", message.nameSpace, message.kind)
								keySpec := fmt.Sprintf("%s_%s_%s", message.nameSpace, message.kind, iA.Key)
								if key == keyAll || key == keySpec {

								} else {
									t.Fatalf("message publish error, nameSpace:%s, kind:%s, param:%s, messageKey:%s", message.nameSpace, message.kind, iA.Key, key)
								}
							}
						}
					case InfoB:
						if message.InfoType != StructArrayB {
							continue
						}
						infoB := msg.(InfoB)
						for _, i := range message.InfoStructListB {
							iB := i.(InfoB)
							if infoB == iB {
								isFind = true
								keyAll := fmt.Sprintf("%s_%s", message.nameSpace, message.kind)
								keySpec := fmt.Sprintf("%s_%s_%s", message.nameSpace, message.kind, iB.Key)
								if key == keyAll || key == keySpec {

								} else {
									t.Fatalf("message publish error, nameSpace:%s, kind:%s, param:%s, messageKey:%s", message.nameSpace, message.kind, iB.Key, key)
								}
							}
						}
					}
				}
			}
			return true
		})
		if !isFind {
			t.Fatalf("message publish fail,message not found,message:%#v", message)
		}
	}

	// unsubscribe all subscription
	for _, sub := range SubList {
		close(sub.notifier.ch)
		time.Sleep(300 * time.Millisecond)

		v, ok := sp.keyToNotifier.Load(sub.key)
		if !ok {
			continue
		}
		slice := v.([]*subInfo)
		isFind := false
		for _, s := range slice {
			if s.notifier == sub.notifier {
				if isFind == false {
					isFind = true
				}
			}
		}
		if isFind {
			t.Fatalf("unsubscribe fail,subscribtion not delete,key:%s", sub.key)
		}
	}
}

type Str1 struct{ str string }

func (s Str1) String() string { return s.str }

type Str2 struct{ str string }

func (s *Str2) String() string { return s.str }

type Str3 struct{}

func (s Str3) String() {}

type Str4 struct{}

type SA struct {
	Key string

	Str1 Str1
	Str2 Str2
	Str3 Str3
	Str4 Str4

	Str1Ptr *Str1
	Str2Ptr *Str2
	Str3Ptr *Str3
	Str4Ptr *Str4
}

func TestGetKeyInStruct(t *testing.T) {

	cases := []struct {
		name     string
		expected string

		f string
		s interface{}
	}{
		// ---------------------------------------------------------------------
		{name: "test1", expected: "", f: "", s: "this is not a struct"},
		{name: "test2", expected: "", f: "field", s: "this is not a struct"},

		// ---------------------------------------------------------------------
		{name: "test3", expected: "", f: "", s: SA{Key: "key"}},
		{name: "test4", expected: "", f: "field", s: SA{Key: "key"}},
		{name: "test5", expected: "key", f: "Key", s: SA{Key: "key"}},

		{name: "test6", expected: "", f: "", s: SA{Str1: Str1{str: "key"}}},
		{name: "test7", expected: "", f: "field", s: SA{Str1: Str1{str: "key"}}},
		{name: "test8", expected: "key", f: "Str1", s: SA{Str1: Str1{str: "key"}}},

		{name: "test9", expected: "", f: "", s: SA{Str2: Str2{str: "key"}}},
		{name: "test10", expected: "", f: "field", s: SA{Str2: Str2{str: "key"}}},
		//{name: "test11", expected: "key", f: "Str2", s: SA{Str2: Str2{str: "key"}}},

		{name: "test12", expected: "", f: "", s: SA{Str3: Str3{}}},
		{name: "test13", expected: "", f: "field", s: SA{Str3: Str3{}}},
		{name: "test14", expected: "", f: "Str3", s: SA{Str3: Str3{}}},

		{name: "test15", expected: "", f: "", s: SA{Str4: Str4{}}},
		{name: "test16", expected: "", f: "field", s: SA{Str4: Str4{}}},
		{name: "test17", expected: "", f: "Str4", s: SA{Str4: Str4{}}},

		{name: "test18", expected: "", f: "", s: SA{Str1Ptr: &Str1{str: "key"}}},
		{name: "test19", expected: "", f: "field", s: SA{Str1Ptr: &Str1{str: "key"}}},
		{name: "test20", expected: "key", f: "Str1Ptr", s: SA{Str1Ptr: &Str1{str: "key"}}},

		{name: "test21", expected: "", f: "", s: SA{Str2Ptr: &Str2{str: "key"}}},
		{name: "test22", expected: "", f: "field", s: SA{Str2Ptr: &Str2{str: "key"}}},
		{name: "test23", expected: "key", f: "Str2Ptr", s: SA{Str2Ptr: &Str2{str: "key"}}},

		{name: "test24", expected: "", f: "", s: SA{Str3Ptr: &Str3{}}},
		{name: "test25", expected: "", f: "field", s: SA{Str3Ptr: &Str3{}}},
		{name: "test26", expected: "", f: "Str3Ptr", s: SA{Str3Ptr: &Str3{}}},

		{name: "test27", expected: "", f: "", s: SA{Str4Ptr: &Str4{}}},
		{name: "test28", expected: "", f: "field", s: SA{Str4Ptr: &Str4{}}},
		{name: "test29", expected: "", f: "Str4Ptr", s: SA{Str4Ptr: &Str4{}}},

		// ---------------------------------------------------------------------
		{name: "test30", expected: "", f: "", s: &SA{Key: "key"}},
		{name: "test31", expected: "", f: "field", s: &SA{Key: "key"}},
		{name: "test32", expected: "key", f: "Key", s: &SA{Key: "key"}},

		{name: "test33", expected: "", f: "", s: &SA{Str1: Str1{str: "key"}}},
		{name: "test34", expected: "", f: "field", s: &SA{Str1: Str1{str: "key"}}},
		{name: "test35", expected: "key", f: "Str1", s: &SA{Str1: Str1{str: "key"}}},

		{name: "test36", expected: "", f: "", s: &SA{Str2: Str2{str: "key"}}},
		{name: "test37", expected: "", f: "field", s: &SA{Str2: Str2{str: "key"}}},
		//{name: "test38", expected: "key", f: "Str2", s: &SA{Str2: Str2{str: "key"}}},

		{name: "test39", expected: "", f: "", s: &SA{Str3: Str3{}}},
		{name: "test40", expected: "", f: "field", s: &SA{Str3: Str3{}}},
		{name: "test41", expected: "", f: "Str3", s: &SA{Str3: Str3{}}},

		{name: "test42", expected: "", f: "", s: &SA{Str4: Str4{}}},
		{name: "test43", expected: "", f: "field", s: &SA{Str4: Str4{}}},
		{name: "test44", expected: "", f: "Str4", s: &SA{Str4: Str4{}}},

		{name: "test45", expected: "", f: "", s: &SA{Str1Ptr: &Str1{str: "key"}}},
		{name: "test46", expected: "", f: "field", s: &SA{Str1Ptr: &Str1{str: "key"}}},
		{name: "test47", expected: "key", f: "Str1Ptr", s: &SA{Str1Ptr: &Str1{str: "key"}}},

		{name: "test48", expected: "", f: "", s: &SA{Str2Ptr: &Str2{str: "key"}}},
		{name: "test49", expected: "", f: "field", s: &SA{Str2Ptr: &Str2{str: "key"}}},
		{name: "test50", expected: "key", f: "Str2Ptr", s: &SA{Str2Ptr: &Str2{str: "key"}}},

		{name: "test51", expected: "", f: "", s: &SA{Str3Ptr: &Str3{}}},
		{name: "test52", expected: "", f: "field", s: &SA{Str3Ptr: &Str3{}}},
		{name: "test53", expected: "", f: "Str3Ptr", s: &SA{Str3Ptr: &Str3{}}},

		{name: "test54", expected: "", f: "", s: &SA{Str4Ptr: &Str4{}}},
		{name: "test55", expected: "", f: "field", s: &SA{Str4Ptr: &Str4{}}},
		{name: "test56", expected: "", f: "Str4Ptr", s: &SA{Str4Ptr: &Str4{}}},

		{name: "test57", expected: "", f: ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			param := getKeyInStruct(c.f, c.s)
			if param != c.expected {
				t.Fatalf("%s error,\"%s\"(param)!=\"%s\"(expected)", c.name, param, c.expected)
			}
		})
	}

}
