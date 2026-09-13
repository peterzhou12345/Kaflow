package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Runs only against an explicitly supplied test broker. Own topics/groups are
// unique, and cleanup is registered before writes so failed assertions clean up.
func TestKafkaIntegration(t *testing.T) {
	broker := os.Getenv("KAFLOW_TEST_BROKERS")
	if broker == "" {
		t.Skip("set KAFLOW_TEST_BROKERS to an isolated real Kafka broker")
	}
	a := newApp()
	defer a.close()
	h := a.handler("127.0.0.1:17893", "integration-token")
	call := func(action string, body map[string]any, want int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest("POST", "/api/"+action, strings.NewReader(string(b)))
		r.Host = "127.0.0.1:17893"
		r.Header.Set("Authorization", "Bearer integration-token")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s: status %d body %s", action, w.Code, w.Body.String())
		}
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.Data
	}
	var connected struct {
		Session string `json:"session"`
	}
	json.Unmarshal(call("connect", map[string]any{"connection": map[string]any{"brokers": broker}}, 200), &connected)
	id := connected.Session
	topic := fmt.Sprintf("kaflow-test-%d", time.Now().UnixNano())
	group := topic + "-consumer"
	admin := kadm.NewClient(a.sessions[id].client)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin.DeleteGroup(ctx, group)
		admin.DeleteTopic(ctx, topic)
	}()
	payload := func() map[string]any { return map[string]any{"session": id, "topic": topic} }
	p := payload()
	p["partitions"] = 2
	p["replicas"] = 1
	call("create", p, 200)
	if !strings.Contains(string(call("metadata", payload(), 200)), topic) {
		t.Fatal("created topic absent")
	}
	values := []any{`{"hello":"Kafka 世界"}`, "", nil, `<script>alert("xss")</script>`}
	for i, v := range values {
		p = payload()
		p["partition"] = 0
		p["key"] = fmt.Sprint(i)
		p["value"] = v
		p["headers"] = []map[string]string{{"key": "source", "value": "integration"}}
		var result struct {
			Offset string `json:"offset"`
		}
		json.Unmarshal(call("produce", p, 200), &result)
		if result.Offset != fmt.Sprint(i) {
			t.Fatalf("offset %s", result.Offset)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var commit kadm.Offsets
	commit.Add(kadm.Offset{Topic: topic, Partition: 0, At: 1, LeaderEpoch: -1})
	response, err := admin.CommitOffsets(ctx, group, commit)
	if err != nil {
		t.Fatal(err)
	}
	if err = response.Error(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"earliest", "latest", "offset"} {
		p = payload()
		p["mode"] = mode
		p["offset"] = "2"
		p["limit"] = 100
		p["partition"] = 0
		var result struct {
			Messages []message `json:"messages"`
			Partial  bool      `json:"partial"`
		}
		json.Unmarshal(call("messages", p, 200), &result)
		want := 4
		if mode == "offset" {
			want = 2
		}
		if len(result.Messages) != want || result.Partial {
			t.Fatalf("%s records=%d partial=%v", mode, len(result.Messages), result.Partial)
		}
		for _, m := range result.Messages {
			if m.Offset == "1" && (m.Value == nil || *m.Value != "") {
				t.Fatal("empty value lost")
			}
			if m.Offset == "2" && m.Value != nil {
				t.Fatal("tombstone lost")
			}
			if len(m.Headers) != 1 || m.Headers[0].Value != "integration" {
				t.Fatal("header lost")
			}
		}
	}
	p = payload()
	p["mode"] = "latest"
	p["limit"] = 1
	p["partition"] = 0
	var latest struct {
		Messages []message `json:"messages"`
	}
	json.Unmarshal(call("messages", p, 200), &latest)
	if len(latest.Messages) != 1 || latest.Messages[0].Offset != "3" {
		t.Fatal("latest tail incorrect")
	}
	p = payload()
	p["mode"] = "earliest"
	p["limit"] = 100
	p["partition"] = 1
	var empty struct {
		Messages []message `json:"messages"`
	}
	json.Unmarshal(call("messages", p, 200), &empty)
	if len(empty.Messages) != 0 {
		t.Fatal("empty partition not empty")
	}
	offsets := string(call("offsets", payload(), 200))
	if !strings.Contains(offsets, `"Offset":"4"`) {
		t.Fatalf("bad offsets %s", offsets)
	}
	groups := string(call("groups", payload(), 200))
	if !strings.Contains(groups, group) || !strings.Contains(groups, `"Lag":"3"`) || !strings.Contains(groups, `"At":"1"`) {
		t.Fatalf("bad group lag / browse changed commit: %s", groups)
	}
	p = payload()
	p["confirm"] = "wrong"
	call("delete", p, 400)
	if !strings.Contains(string(call("metadata", payload(), 200)), topic) {
		t.Fatal("failed confirmation deleted topic")
	}
	p = payload()
	p["confirm"] = topic
	call("delete", p, 200)
	deadline := time.Now().Add(10 * time.Second)
	for strings.Contains(string(call("metadata", payload(), 200)), `"Topic":"`+topic+`"`) {
		if time.Now().After(deadline) {
			t.Fatal("topic remains after deletion")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	call("disconnect", payload(), 200)
	call("metadata", payload(), 400)
}
