package main

import (
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"testing"
)

func TestTopicValidation(t *testing.T) {
	for _, name := range []string{"a", "orders.created-v1_2", strings.Repeat("a", 249)} {
		if e := validateTopic(name); e != nil {
			t.Errorf("valid %q: %v", name, e)
		}
	}
	for _, name := range []string{"", ".", "..", "a/b", "订单", "with space", strings.Repeat("a", 250)} {
		if validateTopic(name) == nil {
			t.Errorf("accepted %q", name)
		}
	}
}
func TestConnectionValidation(t *testing.T) {
	cases := []struct {
		name  string
		c     connection
		valid bool
	}{
		{"local", connection{Brokers: "localhost:9092"}, true},
		{"multiple", connection{Brokers: "a:9092, b:9093\n[::1]:9092"}, true},
		{"empty", connection{}, false}, {"missing port", connection{Brokers: "localhost"}, false},
		{"zero", connection{Brokers: "localhost:0"}, false}, {"overflow", connection{Brokers: "localhost:65536"}, false},
		{"scheme", connection{Brokers: "http://localhost:9092"}, false},
		{"bad sasl", connection{Brokers: "a:9092", SASL: "unknown"}, false},
		{"credentials required", connection{Brokers: "a:9092", SASL: "plain"}, false},
		{"plain", connection{Brokers: "a:9092", SASL: "plain", Username: "u", Password: "p"}, true},
		{"scram256", connection{Brokers: "a:9092", SASL: "scram-sha-256", Username: "u", Password: "p"}, true},
		{"scram512", connection{Brokers: "a:9092", SASL: "scram-sha-512", Username: "u", Password: "p"}, true},
		{"system TLS", connection{Brokers: "a:9092", TLS: true}, true},
		{"invalid CA", connection{Brokers: "a:9092", TLS: true, CA: "bad"}, false},
		{"TLS required", connection{Brokers: "a:9092", CA: "bad"}, false},
		{"missing key", connection{Brokers: "a:9092", TLS: true, Cert: "bad"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.c.options()
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v err=%v", c.valid, err)
			}
		})
	}
}
func TestNullAndEmptyAreDifferent(t *testing.T) {
	if strPtr(nil) != nil {
		t.Fatal("nil lost")
	}
	p := strPtr([]byte{})
	if p == nil || *p != "" {
		t.Fatal("empty lost")
	}
}
func TestOffsetPrecision(t *testing.T) {
	o := offsetDTO(map[int32]kadm.ListedOffset{0: {Offset: 9223372036854775807}})
	b, err := json.Marshal(o)
	if err != nil || !strings.Contains(string(b), `"9223372036854775807"`) {
		t.Fatalf("%s %v", b, err)
	}
}
func TestMessageBudgetIncludesHeadersAndKey(t *testing.T) {
	key, value := "key", "value"
	if n := requestBytes(request{Key: &key, Value: &value, Headers: []header{{"head", "body"}}}); n != 16 {
		t.Fatalf("got %d", n)
	}
}

func TestConnectionOptionsConstructClient(t *testing.T) {
	opts, err := (connection{Brokers: "localhost:9092"}).options()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}
