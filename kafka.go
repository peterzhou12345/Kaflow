package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// normalizePEM accepts certificates copied from indented YAML/properties
// blocks while preserving PEM headers and base64 content.
func normalizePEM(value string) string {
	lines := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
}

type connection struct {
	Brokers    string `json:"brokers"`
	TLS        bool   `json:"tls"`
	CA         string `json:"ca"`
	Cert       string `json:"cert"`
	PrivateKey string `json:"privateKey"`
	SkipHostnameVerify bool `json:"skipHostnameVerify"`
	SASL       string `json:"sasl"`
	Username   string `json:"username"`
	Password   string `json:"password"`
}
type request struct {
	Connection connection `json:"connection"`
	Session    string     `json:"session"`
	Topic      string     `json:"topic"`
	Partitions int32      `json:"partitions"`
	Replicas   int16      `json:"replicas"`
	Partition  *int32     `json:"partition"`
	Mode       string     `json:"mode"`
	Offset     string     `json:"offset"`
	Limit      int        `json:"limit"`
	Key        *string    `json:"key"`
	Value      *string    `json:"value"`
	Headers    []header   `json:"headers"`
	Confirm    string     `json:"confirm"`
}
type header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type session struct {
	config connection
	client *kgo.Client
	mu     sync.RWMutex
	closed bool
}
type app struct {
	mu       sync.Mutex
	sessions map[string]*session
	next     uint64
}

func newApp() *app { return &app{sessions: map[string]*session{}} }
func (a *app) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		s.mu.Lock()
		s.closed = true
		s.client.Close()
		s.mu.Unlock()
	}
}
func (c connection) options() ([]kgo.Opt, error) {
	brokers := strings.FieldsFunc(c.Brokers, func(r rune) bool { return r == ',' || r == '\n' || r == ' ' })
	if len(brokers) == 0 {
		return nil, fmt.Errorf("请填写 Broker 地址")
	}
	for _, b := range brokers {
		h, p, e := net.SplitHostPort(b)
		n, _ := strconv.Atoi(p)
		if e != nil || h == "" || n < 1 || n > 65535 {
			return nil, fmt.Errorf("Broker 格式应为 host:port：%s", b)
		}
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID("kaflow"), kgo.DialTimeout(5 * time.Second), kgo.RequestTimeoutOverhead(5 * time.Second), kgo.RecordDeliveryTimeout(15 * time.Second), kgo.FetchMaxBytes(4 << 20), kgo.FetchMaxPartitionBytes(1 << 20)}
	if c.TLS {
		// Kafka configurations commonly use an empty endpoint identification algorithm
		// with private-cluster certificates whose SANs are internal service names.
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: c.SkipHostnameVerify} // #nosec G402: explicit user-selected compatibility option
		if c.CA != "" {
			pool, err := x509.SystemCertPool()
			if err != nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM([]byte(normalizePEM(c.CA))) {
				return nil, fmt.Errorf("CA 证书不是有效的 PEM")
			}
			cfg.RootCAs = pool
		}
		if c.Cert != "" || c.PrivateKey != "" {
			cert, err := tls.X509KeyPair([]byte(normalizePEM(c.Cert)), []byte(normalizePEM(c.PrivateKey)))
			if err != nil {
				return nil, fmt.Errorf("客户端证书: %w", err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	} else if c.CA != "" || c.Cert != "" || c.PrivateKey != "" {
		return nil, fmt.Errorf("填写证书时必须启用 TLS")
	}
	switch c.SASL {
	case "", "none":
	case "plain", "scram-sha-256", "scram-sha-512":
		if c.Username == "" || c.Password == "" {
			return nil, fmt.Errorf("SASL 需要用户名和密码")
		}
		switch c.SASL {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism()))
		}
	default:
		return nil, fmt.Errorf("不支持的 SASL 机制")
	}
	return opts, nil
}

var topicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func validateTopic(t string) error {
	if !topicPattern.MatchString(t) || t == "." || t == ".." {
		return fmt.Errorf("Topic 名称需为 1–249 位字母、数字、点、下划线或横线")
	}
	return nil
}
func (a *app) execute(ctx context.Context, action string, r request) (any, error) {
	if action == "connect" {
		opts, err := r.Connection.options()
		if err != nil {
			return nil, err
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		if err = cl.Ping(ctx); err != nil {
			cl.Close()
			return nil, fmt.Errorf("连接失败（检查地址、认证与 advertised.listeners）：%w", err)
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if len(a.sessions) >= 16 {
			cl.Close()
			return nil, fmt.Errorf("连接数已达上限，请先断开不用的连接")
		}
		a.next++
		id := strconv.FormatUint(a.next, 10)
		a.sessions[id] = &session{config: r.Connection, client: cl}
		return map[string]any{"session": id}, nil
	}
	a.mu.Lock()
	s := a.sessions[r.Session]
	if action == "disconnect" {
		delete(a.sessions, r.Session)
	}
	a.mu.Unlock()
	if s == nil {
		return nil, fmt.Errorf("连接已断开，请重新连接")
	}
	if action == "disconnect" {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		s.client.Close()
		return true, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fmt.Errorf("连接已断开")
	}
	admin := kadm.NewClient(s.client)
	switch action {
	case "metadata":
		md, err := admin.Metadata(ctx)
		if err != nil {
			return nil, err
		}
		return md, nil
	case "create":
		if err := validateTopic(r.Topic); err != nil {
			return nil, err
		}
		if r.Partitions < 1 || r.Partitions > 10000 || r.Replicas < 1 {
			return nil, fmt.Errorf("分区数需为 1–10000，副本数至少为 1")
		}
		return admin.CreateTopic(ctx, r.Partitions, r.Replicas, nil, r.Topic)
	case "delete":
		if err := validateTopic(r.Topic); err != nil {
			return nil, err
		}
		if r.Confirm != r.Topic {
			return nil, fmt.Errorf("请输入完整 Topic 名称确认删除")
		}
		return admin.DeleteTopic(ctx, r.Topic)
	case "groups":
		gs, err := admin.ListGroups(ctx)
		if err != nil {
			return nil, err
		}
		if len(gs) == 0 {
			return []any{}, nil
		}
		lags, err := admin.Lag(ctx, gs.Groups()...)
		if err != nil {
			return nil, err
		}
		return groupDTO(lags), nil
	case "offsets":
		if err := validateTopic(r.Topic); err != nil {
			return nil, err
		}
		start, err := admin.ListStartOffsets(ctx, r.Topic)
		if err != nil {
			return nil, err
		}
		if err = start.Error(); err != nil {
			return nil, err
		}
		end, err := admin.ListEndOffsets(ctx, r.Topic)
		if err != nil {
			return nil, err
		}
		if err = end.Error(); err != nil {
			return nil, err
		}
		return map[string]any{"start": offsetDTO(start[r.Topic]), "end": offsetDTO(end[r.Topic])}, nil
	case "produce":
		if err := validateTopic(r.Topic); err != nil {
			return nil, err
		}
		if requestBytes(r) > 1<<20 {
			return nil, fmt.Errorf("单条消息不能超过 1 MiB")
		}
		opts, err := s.config.options()
		if err != nil {
			return nil, err
		}
		if r.Partition != nil {
			if *r.Partition < 0 {
				return nil, fmt.Errorf("分区号不能为负")
			}
			opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		defer cl.Close()
		rec := &kgo.Record{Topic: r.Topic}
		if r.Key != nil {
			rec.Key = []byte(*r.Key)
		}
		if r.Value != nil {
			rec.Value = []byte(*r.Value)
		}
		if r.Partition != nil {
			rec.Partition = *r.Partition
		}
		for _, h := range r.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
		}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			return nil, err
		}
		return map[string]any{"partition": rec.Partition, "offset": strconv.FormatInt(rec.Offset, 10)}, nil
	case "messages":
		return s.messages(ctx, r, admin)
	default:
		return nil, fmt.Errorf("未知操作")
	}
}

type message struct {
	Partition   int32     `json:"partition"`
	Offset      string    `json:"offset"`
	Timestamp   time.Time `json:"timestamp"`
	Key         *string   `json:"key"`
	Value       *string   `json:"value"`
	KeyBase64   string    `json:"keyBase64"`
	ValueBase64 string    `json:"valueBase64"`
	Headers     []header  `json:"headers"`
}

func strPtr(b []byte) *string {
	if b == nil {
		return nil
	}
	s := string(b)
	return &s
}
func (s *session) messages(ctx context.Context, r request, admin *kadm.Client) (any, error) {
	if err := validateTopic(r.Topic); err != nil {
		return nil, err
	}
	if r.Limit < 1 || r.Limit > 1000 {
		return nil, fmt.Errorf("条数需为 1–1000")
	}
	if r.Mode != "latest" && r.Mode != "earliest" && r.Mode != "offset" {
		return nil, fmt.Errorf("无效读取模式")
	}
	var specified int64
	if r.Mode == "offset" {
		n, e := strconv.ParseInt(r.Offset, 10, 64)
		if e != nil || n < 0 {
			return nil, fmt.Errorf("Offset 必须为非负整数")
		}
		specified = n
	}
	starts, err := admin.ListStartOffsets(ctx, r.Topic)
	if err != nil {
		return nil, err
	}
	if err = starts.Error(); err != nil {
		return nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, r.Topic)
	if err != nil {
		return nil, err
	}
	if err = ends.Error(); err != nil {
		return nil, err
	}
	offsets := map[int32]kgo.Offset{}
	targets := map[int32]int64{}
	for p, end := range ends[r.Topic] {
		if r.Partition != nil && *r.Partition != p {
			continue
		}
		start := starts[r.Topic][p].Offset
		n := start
		if r.Mode == "latest" {
			n = end.Offset - int64(r.Limit)
			if n < start {
				n = start
			}
		}
		if r.Mode == "offset" {
			n = specified
			if n < start {
				n = start
			}
		}
		if n < end.Offset {
			offsets[p] = kgo.NewOffset().At(n)
			targets[p] = end.Offset
		}
	}
	if r.Partition != nil {
		if _, ok := ends[r.Topic][*r.Partition]; !ok {
			return nil, fmt.Errorf("分区不存在")
		}
	}
	records := []message{}
	if len(offsets) == 0 {
		return map[string]any{"messages": records, "partial": false}, nil
	}
	opts, err := s.config.options()
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.Topic: offsets}), kgo.FetchMaxWait(250*time.Millisecond))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	readCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	partial := false
	bytes := 0
	for len(targets) > 0 && len(records) < r.Limit {
		fetches := cl.PollRecords(readCtx, r.Limit-len(records))
		if readCtx.Err() != nil {
			partial = len(targets) > 0
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, errs[0].Err
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			target, ok := targets[p.Partition]
			if !ok {
				return
			}
			for _, rec := range p.Records {
				if rec.Offset >= target {
					delete(targets, p.Partition)
					break
				}
				size := len(rec.Key) + len(rec.Value)
				for _, h := range rec.Headers {
					size += len(h.Key) + len(h.Value)
				}
				if bytes+size > 8<<20 {
					partial = true
					cancel()
					break
				}
				bytes += size
				h := []header{}
				for _, v := range rec.Headers {
					h = append(h, header{v.Key, string(v.Value)})
				}
				records = append(records, message{rec.Partition, strconv.FormatInt(rec.Offset, 10), rec.Timestamp, strPtr(rec.Key), strPtr(rec.Value), base64.StdEncoding.EncodeToString(rec.Key), base64.StdEncoding.EncodeToString(rec.Value), h})
				if rec.Offset+1 >= target {
					delete(targets, p.Partition)
				}
			}
		})
	}
	sort.SliceStable(records, func(i, j int) bool { return records[i].Timestamp.After(records[j].Timestamp) })
	return map[string]any{"messages": records, "partial": partial, "limited": len(targets) > 0 && len(records) >= r.Limit}, nil
}

// Offset strings preserve all 64 bits across the browser JSON boundary.
func offsetDTO(offsets map[int32]kadm.ListedOffset) map[int32]any {
	out := map[int32]any{}
	for p, o := range offsets {
		out[p] = map[string]any{"Offset": strconv.FormatInt(o.Offset, 10)}
	}
	return out
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func groupDTO(groups kadm.DescribedGroupLags) []any {
	out := []any{}
	for _, g := range groups {
		lag := map[string]any{}
		for topic, parts := range g.Lag {
			ps := map[int32]any{}
			for p, l := range parts {
				ps[p] = map[string]any{"Commit": map[string]string{"At": strconv.FormatInt(l.Commit.At, 10)}, "End": map[string]string{"Offset": strconv.FormatInt(l.End.Offset, 10)}, "Lag": strconv.FormatInt(l.Lag, 10), "Err": errorText(l.Err)}
			}
			lag[topic] = ps
		}
		out = append(out, map[string]any{"Group": g.Group, "State": g.State, "Members": g.Members, "Lag": lag, "DescribeErr": errorText(g.DescribeErr), "FetchErr": errorText(g.FetchErr)})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].(map[string]any)["Group"].(string) < out[j].(map[string]any)["Group"].(string)
	})
	return out
}
func requestBytes(r request) int {
	n := 0
	if r.Key != nil {
		n += len(*r.Key)
	}
	if r.Value != nil {
		n += len(*r.Value)
	}
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}
