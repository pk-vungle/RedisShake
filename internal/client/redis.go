package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"RedisShake/internal/client/proto"
	"RedisShake/internal/log"
)

type Redis struct {
	conn        net.Conn
	reader      *bufio.Reader
	writer      *bufio.Writer
	protoReader *proto.Reader
	protoWriter *proto.Writer
}

type TlsConfig struct {
	CACertFilePath string `mapstructure:"ca_cert" default:""`
	CertFilePath   string `mapstructure:"cert" default:""`
	KeyFilePath    string `mapstructure:"key" default:""`
}

func NewRedisClient(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig TlsConfig, replica bool) *Redis {
	r, err := NewRedisClientSafe(ctx, address, username, password, Tls, tlsConfig, replica)
	if err != nil {
		log.Panicf(err.Error())
	}
	return r
}

// NewRedisClientSafe creates a Redis client and returns an error instead of panicking.
// The dial timeout is 30 seconds to handle slow or remote connections.
func NewRedisClientSafe(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig TlsConfig, replica bool) (*Redis, error) {
	r := new(Redis)
	var conn net.Conn
	var dialer = &net.Dialer{
		Timeout:   5 * time.Minute,
		KeepAlive: 5 * time.Minute,
	}
	ctxWithDeadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var err error
	if Tls {
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    getTlsConfig(tlsConfig),
		}
		conn, err = tlsDialer.DialContext(ctxWithDeadline, "tcp", address)
	} else {
		conn, err = dialer.DialContext(ctxWithDeadline, "tcp", address)
	}
	if err != nil {
		return nil, fmt.Errorf("dial failed. address=[%s], tls=[%v], err=[%v]", address, Tls, err)
	}

	r.conn = conn
	// Increase the size of the underlying TCP send cache to avoid short-write errors
	r.reader = bufio.NewReader(conn)
	r.writer = bufio.NewWriterSize(conn, 32*1024) // size is 32KiB
	r.protoReader = proto.NewReader(r.reader)
	r.protoWriter = proto.NewWriter(r.writer)

	// auth
	if password != "" {
		var reply string
		if username != "" {
			reply, err = r.doSafe("auth", username, password)
		} else {
			reply, err = r.doSafe("auth", password)
		}
		if err != nil {
			r.conn.Close()
			return nil, fmt.Errorf("auth failed: %v", err)
		}
		if reply != "OK" {
			r.conn.Close()
			return nil, fmt.Errorf("auth failed with reply: %s", reply)
		}
	}

	// ping to test connection
	reply, err := r.doSafe("ping")
	if err != nil {
		r.conn.Close()
		return nil, fmt.Errorf("ping failed: %v", err)
	}
	if reply != "PONG" {
		r.conn.Close()
		return nil, fmt.Errorf("ping failed with reply: %s", reply)
	}

	// get best replica
	if replica {
		reply, err = r.doSafe("info", "replication")
		if err != nil {
			r.conn.Close()
			return nil, fmt.Errorf("info replication failed: %v", err)
		}
		replicaInfo := getReplicaAddr(reply, address)
		log.Infof("best replica: %s", replicaInfo.BestReplica)
		r.conn.Close()
		return NewRedisClientSafe(ctx, replicaInfo.BestReplica, username, password, Tls, tlsConfig, false)
	}

	return r, nil
}

// NewRedisClientWithRetry creates a Redis client, retrying up to maxRetries times on failure.
func NewRedisClientWithRetry(ctx context.Context, address string, username string, password string, Tls bool, tlsConfig TlsConfig, replica bool, maxRetries int) *Redis {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			waitTime := time.Duration(attempt) * 5 * time.Second
			log.Warnf("connection to [%s] failed (attempt %d/%d): %v, retrying in %v...", address, attempt, maxRetries, lastErr, waitTime)
			select {
			case <-ctx.Done():
				log.Panicf("context cancelled while retrying connection to [%s]", address)
			case <-time.After(waitTime):
			}
		}
		c, err := NewRedisClientSafe(ctx, address, username, password, Tls, tlsConfig, replica)
		if err == nil {
			return c
		}
		lastErr = err
	}
	log.Panicf("connection to [%s] failed after %d retries: %v", address, maxRetries, lastErr)
	return nil
}

func getTlsConfig(tlsConfig TlsConfig) *tls.Config {
	if tlsConfig.CACertFilePath == "" || tlsConfig.CertFilePath == "" || tlsConfig.KeyFilePath == "" {
		return &tls.Config{InsecureSkipVerify: true}
	}

	// Use mutual authentication (mTLS)
	cert, err := tls.LoadX509KeyPair(tlsConfig.CertFilePath, tlsConfig.KeyFilePath)
	if err != nil {
		log.Panicf("load tls cert failed. cert=[%s], key=[%s], err=[%v]", tlsConfig.CertFilePath, tlsConfig.KeyFilePath, err)
	}
	caCert, err := os.ReadFile(tlsConfig.CACertFilePath)
	if err != nil {
		log.Panicf("read ca cert failed. ca_cert=[%s], err=[%v]", tlsConfig.CACertFilePath, err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return &tls.Config{
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
}

type Replica struct {
	Addr   string
	Offset string
}

type RedisReplicaInfo struct {
	Role        string
	BestReplica string
}

func getReplicaAddr(info, addr string) RedisReplicaInfo {
	infoReplica := RedisReplicaInfo{}
	replicas := make([]Replica, 0)
	slaveInfoRegexp := regexp.MustCompile(`slave\d+:ip=.*`)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "role:slave"):
			infoReplica.Role = "slave"
			infoReplica.BestReplica = addr
			return infoReplica
		case strings.HasPrefix(line, "role:master"):
			infoReplica.Role = "master"
		case slaveInfoRegexp.MatchString(line):
			slaveInfo := strings.Split(line, ":")
			s1 := slaveInfo[1]
			slaveInfo = strings.Split(s1, ",")
			replica := Replica{}
			var host string
			var port string
			var offset string
			for _, item := range slaveInfo {
				if strings.HasPrefix(item, "ip=") {
					host = strings.Split(item, "=")[1]
				}
				if strings.HasPrefix(item, "port=") {
					port = strings.Split(item, "=")[1]
				}
				if strings.HasPrefix(item, "offset=") {
					offset = strings.Split(item, "=")[1]
				}
			}
			replica.Addr = host + ":" + port
			replica.Offset = offset
			replicas = append(replicas, replica)
		}
	}
	if len(replicas) == 0 {
		log.Panicf("no replica found, should not set `prefer_replica` to true")
	}
	best := replicas[0]
	for _, replica := range replicas {
		if replica.Offset > best.Offset {
			best = replica
		}
	}
	infoReplica.BestReplica = best.Addr
	return infoReplica
}

// doSafe is an internal helper that sends a command and returns the string reply or an error.
func (r *Redis) doSafe(args ...interface{}) (string, error) {
	argsInterface := make([]interface{}, len(args))
	for i, item := range args {
		argsInterface[i] = item
	}
	if err := r.protoWriter.WriteArgs(argsInterface); err != nil {
		return "", err
	}
	if err := r.writer.Flush(); err != nil {
		return "", err
	}
	reply, err := r.protoReader.ReadReply()
	if err != nil {
		return "", err
	}
	s, ok := reply.(string)
	if !ok {
		return "", fmt.Errorf("unexpected reply type %T", reply)
	}
	return s, nil
}

// SendSafe writes a command to the connection and returns an error instead of panicking.
func (r *Redis) SendSafe(args ...interface{}) error {
	argsInterface := make([]interface{}, len(args))
	for i, item := range args {
		argsInterface[i] = item
	}
	if err := r.protoWriter.WriteArgs(argsInterface); err != nil {
		return err
	}
	return r.writer.Flush()
}

// ScanSafe runs a SCAN command and returns an error instead of panicking.
// When match is non-empty it is passed as the MATCH argument (e.g. "scc1:*").
func (r *Redis) ScanSafe(cursor uint64, match string, count int) (newCursor uint64, keys []string, err error) {
	args := []interface{}{"scan", strconv.FormatUint(cursor, 10)}
	if match != "" {
		args = append(args, "match", match)
	}
	args = append(args, "count", count)
	if err = r.SendSafe(args...); err != nil {
		return 0, nil, err
	}
	reply, err := r.Receive()
	if err != nil {
		return 0, nil, err
	}
	array, ok := reply.([]interface{})
	if !ok || len(array) != 2 {
		return 0, nil, fmt.Errorf("unexpected SCAN reply: %v", reply)
	}
	newCursor, err = strconv.ParseUint(array[0].(string), 10, 64)
	if err != nil {
		return 0, nil, err
	}
	for _, item := range array[1].([]interface{}) {
		keys = append(keys, item.(string))
	}
	return newCursor, keys, nil
}

func (r *Redis) DoWithStringReply(args ...interface{}) string {
	r.Send(args...)

	replyInterface, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	reply := replyInterface.(string)
	return reply
}

func (r *Redis) Do(args ...interface{}) interface{} {
	r.Send(args...)

	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	return reply
}

func (r *Redis) Send(args ...interface{}) {
	argsInterface := make([]interface{}, len(args))
	for inx, item := range args {
		argsInterface[inx] = item
	}
	err := r.protoWriter.WriteArgs(argsInterface)
	if err != nil {
		log.Panicf(err.Error())
	}
	r.Flush()
}

// SendBytesBuff send bytes to buffer, need to call Flush() to send the buffer
func (r *Redis) SendBytesBuff(buf []byte) {
	_, err := r.writer.Write(buf)
	if err != nil {
		log.Panicf(err.Error())
	}
}

func (r *Redis) Flush() {
	err := r.writer.Flush()
	if err != nil {
		log.Panicf(err.Error())
	}
}

func (r *Redis) Receive() (interface{}, error) {
	return r.protoReader.ReadReply()
}

func (r *Redis) ReceiveString() string {
	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}
	return reply.(string)
}

func (r *Redis) Peek() (byte, error) {
	bytes, err := r.protoReader.Peek(1)
	if err != nil {
		return 0, err
	}
	return bytes[0], nil
}

func (r *Redis) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *Redis) ReadByte() (byte, error) {
	return r.reader.ReadByte()
}

func (r *Redis) ReadString(delim byte) (string, error) {
	return r.reader.ReadString(delim)
}

func (r *Redis) Close() {
	if err := r.conn.Close(); err != nil {
		log.Infof("close redis conn err: %s\n", err.Error())
	}
}

/* Commands */

func (r *Redis) Scan(cursor uint64, count int) (newCursor uint64, keys []string) {
	r.Send("scan", strconv.FormatUint(cursor, 10), "count", count)
	reply, err := r.Receive()
	if err != nil {
		log.Panicf(err.Error())
	}

	array := reply.([]interface{})
	if len(array) != 2 {
		log.Panicf("scan return length error. ret=%v", reply)
	}

	// cursor
	newCursor, err = strconv.ParseUint(array[0].(string), 10, 64)
	if err != nil {
		log.Panicf(err.Error())
	}
	// keys
	keys = make([]string, 0)
	for _, item := range array[1].([]interface{}) {
		keys = append(keys, item.(string))
	}
	return
}
