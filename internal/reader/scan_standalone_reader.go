package reader

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

type ScanReaderOptions struct {
	Cluster         bool             `mapstructure:"cluster" default:"false"`
	Address         string           `mapstructure:"address" default:""`
	Username        string           `mapstructure:"username" default:""`
	Password        string           `mapstructure:"password" default:""`
	Tls             bool             `mapstructure:"tls" default:"false"`
	TlsConfig       client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	Scan            bool             `mapstructure:"scan" default:"true"`
	KSN             bool             `mapstructure:"ksn" default:"false"`
	DBS             []int            `mapstructure:"dbs"`
	PreferReplica   bool             `mapstructure:"prefer_replica" default:"false"`
	Count           int              `mapstructure:"count" default:"1"`
	SkipUnknownType []string         `mapstructure:"skip_unknown_type" default:"[]"`
	// MaxRetries controls how many times to retry on connection failure (0 = no retry).
	MaxRetries int `mapstructure:"max_retries" default:"3"`
	// DumpThrottleMs adds a sleep of this many milliseconds between each dump
	// batch to reduce network/CPU pressure on the source. 0 = no throttle.
	DumpThrottleMs int `mapstructure:"dump_throttle_ms" default:"0"`
	// ScanKeyPattern is passed as the MATCH argument to SCAN.
	// Use when allow_key_prefix covers a small fraction of total keys to avoid
	// sending DUMP for every key in the dataset. Example: "scc1:*"
	// Leave empty to scan all keys (default behaviour).
	ScanKeyPattern string `mapstructure:"scan_key_pattern" default:""`
	// NeedDumpQueueSize controls the buffer depth of the internal scan→dump queue
	// per node. Reducing this dramatically lowers memory usage for large clusters.
	// The scan goroutine blocks (backpressure) when the queue is full — no data
	// is lost. Default 100000 (100K). The old hardcoded value was 100M.
	NeedDumpQueueSize int `mapstructure:"need_dump_queue_size" default:"100000"`
}

type dbKey struct {
	db  int
	key string
}

type scanStandaloneReader struct {
	ctx           context.Context
	dbs           []int
	opts          *ScanReaderOptions
	ch            chan *entry.Entry
	needDumpQueue *utils.UniqueQueue
	subWG         sync.WaitGroup
	isValkey      bool

	stat struct {
		Name              string `json:"name"`
		ScanFinished      bool   `json:"scan_finished"`
		ScanDbId          int    `json:"scan_dbId"`
		ScanCursor        uint64 `json:"scan_cursor"`
		ScanPercentByDbId string `json:"scan_percent"`
		NeedUpdateCount   int64  `json:"need_update_count"`
		ScanMatchedKeys   int64  `json:"scan_matched_keys"`
	}
}

func NewScanStandaloneReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	r.dbs = opts.DBS
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	queueSize := opts.NeedDumpQueueSize
	if queueSize <= 0 {
		queueSize = 100000
	}
	r.needDumpQueue = utils.NewUniqueQueue(queueSize)
	log.Infof("[%s] scanStandaloneReader init finished. dbs=[%v]", r.stat.Name, r.dbs)
	return r
}

func (r *scanStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	if r.opts.KSN {
		r.subWG.Add(1)
		go r.subscribe()
		r.subWG.Wait()
	}
	if r.opts.Scan {
		go r.scan()
	}
	go r.dumpAndRestore()
	return []chan *entry.Entry{r.ch}
}

func (r *scanStandaloneReader) subscribe() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	log.Infof("[%s] scanStandaloneReader subscribe started. dbs=[%v]", r.stat.Name, r.dbs)
	if len(r.dbs) == 0 {
		c.Send("psubscribe", "__keyevent@*__:*")
		_, err := c.Receive()
		if err != nil {
			log.Panicf(err.Error())
		}
	} else {
		args := []interface{}{"psubscribe"}
		for _, db := range r.dbs {
			args = append(args, fmt.Sprintf("__keyevent@%v__:*", db))
		}
		c.Send(args...)
		for range r.dbs {
			_, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
			}
		}
	}

	// wait
	r.subWG.Done()

	regex := regexp.MustCompile(`\d+`)
	for {
		select {
		case <-r.ctx.Done():
			log.Infof("[%s] scanStandaloneReader subscribe finished.", r.stat.Name)
			r.needDumpQueue.Close()
			return
		default:
			resp, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
			}
			respSlice := resp.([]interface{})
			key := respSlice[3].(string)
			dbId := regex.FindString(respSlice[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf(err.Error())
			}
			// handle del action
			eventSlice := strings.Split(respSlice[2].(string), ":")
			if eventSlice[1] == "del" {
				e := entry.NewEntry()
				e.DbId = dbIdInt
				e.Argv = []string{"DEL", key}
				r.ch <- e
				continue
			}
			r.needDumpQueue.Put(dbKey{db: dbIdInt, key: key})
		}
	}
}

func (r *scanStandaloneReader) connectWithRetry(name string) *client.Redis {
	var lastErr error
	for attempt := 0; attempt <= r.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			waitTime := time.Duration(attempt) * 5 * time.Second
			log.Warnf("[%s] %s connection failed (attempt %d/%d): %v, retrying in %v...", r.stat.Name, name, attempt, r.opts.MaxRetries, lastErr, waitTime)
			select {
			case <-r.ctx.Done():
				log.Panicf("[%s] context cancelled during %s reconnect", r.stat.Name, name)
			case <-time.After(waitTime):
			}
		}
		c, err := client.NewRedisClientSafe(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
		if err == nil {
			return c
		}
		lastErr = err
	}
	log.Panicf("[%s] %s connection failed after %d retries: %v", r.stat.Name, name, r.opts.MaxRetries, lastErr)
	return nil
}

func (r *scanStandaloneReader) scan() {
	c := r.connectWithRetry("scan")
	defer c.Close()

	dbs := r.dbs
	if len(r.dbs) == 0 {
		c.Send("info", "keyspace")
		info, err := c.Receive()
		if err != nil {
			log.Panicf(err.Error())
		}
		dbs = utils.ParseDBs(info.(string))
	}

	for _, dbId := range dbs {
		if dbId != 0 {
			reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId))
			if reply != "OK" {
				log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
			}
		}

		var cursor uint64 = 0
		count := r.opts.Count
		for {
			select {
			case <-r.ctx.Done():
				log.Infof("[%s] scanStandaloneReader scan finished.", r.stat.Name)
				r.needDumpQueue.Close()
				return
			default:
			}

			// Retry SCAN at the current cursor on transient connection errors.
			var keys []string
			var scanErr error
			for attempt := 0; attempt <= r.opts.MaxRetries; attempt++ {
				if attempt > 0 {
					c.Close()
					c = r.connectWithRetry("scan-retry")
					if dbId != 0 {
						if reply := c.DoWithStringReply("SELECT", strconv.Itoa(dbId)); reply != "OK" {
							log.Panicf("[%s] scan retry SELECT failed. db=[%d]", r.stat.Name, dbId)
						}
					}
				}
				cursor, keys, scanErr = c.ScanSafe(cursor, r.opts.ScanKeyPattern, count)
				if scanErr == nil {
					break
				}
				log.Warnf("[%s] SCAN failed (attempt %d/%d): %v", r.stat.Name, attempt+1, r.opts.MaxRetries+1, scanErr)
			}
			if scanErr != nil {
				log.Panicf("[%s] SCAN failed after %d retries: %v", r.stat.Name, r.opts.MaxRetries, scanErr)
			}

			for _, key := range keys {
				r.needDumpQueue.Put(dbKey{dbId, key}) // pass value not pointer
				r.stat.ScanMatchedKeys++
				if r.stat.ScanMatchedKeys == 1 {
					log.Infof("[%s] first matched key: db=[%d] key=[%s]", r.stat.Name, dbId, key)
				}
			}

			// stat
			r.stat.ScanCursor = cursor
			r.stat.ScanDbId = dbId
			r.stat.ScanPercentByDbId = fmt.Sprintf("%.2f%%", float64(bits.Reverse64(cursor))/float64(^uint(0))*100)

			if cursor == 0 {
				break
			}
		}
	}
	r.stat.ScanFinished = true
	log.Infof("[%s] scan complete: matched_keys=[%d]", r.stat.Name, r.stat.ScanMatchedKeys)
	if !r.opts.KSN {
		r.needDumpQueue.Close()
	}
}

// dumpBatchItem tracks a single key within a mini-batch pipeline.
type dumpBatchItem struct {
	dbId      int
	key       string
	hasSelect bool // true when a SELECT was sent before DUMP for this item
}

// dumpAndRestore replaces the separate dump()+restore() goroutines.
// It processes keys from needDumpQueue in mini-batches using a pipelined
// DUMP+PTTL pattern, and retries the entire batch on connection failure.
//
// Retry semantics: on any send/receive error the connection is closed,
// a new one is dialled, and the same batch is re-sent from scratch.
// Keys that disappear between retries are silently skipped (DUMP returns nil).
func (r *scanStandaloneReader) dumpAndRestore() {
	const batchSize = 64

	c := r.connectWithRetry("dump")
	defer c.Close()
	r.isValkey = c.IsValkey()
	log.Infof("[%s] detected server type: %s", r.stat.Name, map[bool]string{true: "Valkey", false: "Redis"}[r.isValkey])
	if r.opts.PreferReplica {
		c.Do("READONLY")
		log.Infof("running dumpAndRestore() in read-only mode")
	}

	nowDbId := 0

	for {
		// Collect a mini-batch from the queue.
		batch := make([]dumpBatchItem, 0, batchSize)
		item, ok := <-r.needDumpQueue.Ch
		if !ok {
			break
		}
		r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
		first := item.(dbKey)
		if nowDbId != first.db {
			batch = append(batch, dumpBatchItem{dbId: first.db, hasSelect: true})
			nowDbId = first.db
		}
		batch = append(batch, dumpBatchItem{dbId: first.db, key: first.key})

	fillBatch:
		for len(batch) < batchSize {
			select {
			case item, ok := <-r.needDumpQueue.Ch:
				if !ok {
					break fillBatch
				}
				k := item.(dbKey)
				if nowDbId != k.db {
					batch = append(batch, dumpBatchItem{dbId: k.db, hasSelect: true})
					nowDbId = k.db
				}
				batch = append(batch, dumpBatchItem{dbId: k.db, key: k.key})
			default:
				break fillBatch
			}
		}

		// Process the batch with retry.
		for attempt := 0; ; attempt++ {
			err := r.processDumpBatch(c, batch)
			if err == nil {
				break
			}
			if attempt >= r.opts.MaxRetries {
				log.Panicf("[%s] dump batch failed after %d retries: %v", r.stat.Name, r.opts.MaxRetries, err)
			}
			waitTime := time.Duration(attempt+1) * 5 * time.Second
			log.Warnf("[%s] dump batch failed (attempt %d/%d): %v, reconnecting in %v...", r.stat.Name, attempt+1, r.opts.MaxRetries+1, err, waitTime)
			select {
			case <-r.ctx.Done():
				log.Panicf("[%s] context cancelled during dump retry", r.stat.Name)
			case <-time.After(waitTime):
			}
			c.Close()
			c = r.connectWithRetry("dump-retry")
			if r.opts.PreferReplica {
				c.Do("READONLY")
			}
			// Fresh connection starts at db 0; rebuild SELECT markers for this batch.
			batch = rebuildBatchSelects(batch)
		}
		// Track the last db in this batch so the next batch knows where we are.
		for i := len(batch) - 1; i >= 0; i-- {
			if !batch[i].hasSelect {
				nowDbId = batch[i].dbId
				break
			}
		}

		// Throttle source reads to reduce network/CPU pressure.
		if r.opts.DumpThrottleMs > 0 {
			select {
			case <-r.ctx.Done():
				break
			case <-time.After(time.Duration(r.opts.DumpThrottleMs) * time.Millisecond):
			}
		}
	}
	log.Infof("[%s] scanStandaloneReader dumpAndRestore finished.", r.stat.Name)
	close(r.ch)
}

// rebuildBatchSelects strips existing SELECT placeholders and re-inserts them
// assuming the connection starts at db 0 (i.e. after a fresh reconnect).
func rebuildBatchSelects(batch []dumpBatchItem) []dumpBatchItem {
	result := make([]dumpBatchItem, 0, len(batch))
	curDb := 0
	for _, item := range batch {
		if item.hasSelect {
			continue // drop old placeholder, we'll re-insert as needed
		}
		if curDb != item.dbId {
			result = append(result, dumpBatchItem{dbId: item.dbId, hasSelect: true})
			curDb = item.dbId
		}
		result = append(result, item)
	}
	return result
}

// processDumpBatch pipelines DUMP+PTTL (and optional TYPE) for all items in
// the batch, receives responses, and emits entries to r.ch.
// Returns a non-nil error on any connection failure so the caller can retry.
func (r *scanStandaloneReader) processDumpBatch(c *client.Redis, batch []dumpBatchItem) error {
	// Send phase
	for _, item := range batch {
		if item.hasSelect {
			if err := c.SendSafe("SELECT", strconv.Itoa(item.dbId)); err != nil {
				return fmt.Errorf("SELECT send: %w", err)
			}
			continue
		}
		if err := c.SendSafe("DUMP", item.key); err != nil {
			return fmt.Errorf("DUMP send key=[%s]: %w", item.key, err)
		}
		if err := c.SendSafe("PTTL", item.key); err != nil {
			return fmt.Errorf("PTTL send key=[%s]: %w", item.key, err)
		}
		if len(r.opts.SkipUnknownType) > 0 {
			if err := c.SendSafe("TYPE", item.key); err != nil {
				return fmt.Errorf("TYPE send key=[%s]: %w", item.key, err)
			}
		}
	}

	// Receive phase
	for _, item := range batch {
		if item.hasSelect {
			reply, err := c.Receive()
			if err != nil {
				return fmt.Errorf("SELECT receive: %w", err)
			}
			if reply != "OK" {
				return fmt.Errorf("SELECT reply not OK: %v", reply)
			}
			continue
		}

		iDump, err1 := c.Receive()
		iPttl, err2 := c.Receive()

		if len(r.opts.SkipUnknownType) > 0 {
			iType, err3 := c.Receive()
			if err3 != nil {
				return fmt.Errorf("TYPE receive key=[%s]: %w", item.key, err3)
			}
			typeStr := iType.(string)
			skip := false
			for _, skipType := range r.opts.SkipUnknownType {
				if strings.EqualFold(typeStr, skipType) {
					skip = true
					break
				}
			}
			if skip {
				log.Infof("skip restore key=[%s] type=[%s]", item.key, typeStr)
				continue
			}
		}

		if errors.Is(err1, proto.Nil) {
			continue // key expired/deleted
		}
		if err1 != nil {
			return fmt.Errorf("DUMP receive key=[%s]: %w", item.key, err1)
		}
		if err2 != nil {
			return fmt.Errorf("PTTL receive key=[%s]: %w", item.key, err2)
		}

		dump := iDump.(string)
		pttl := 0
		switch v := iPttl.(type) {
		case int64:
			pttl = int(v)
			if pttl == 0 {
				pttl = 1
			}
		case string:
			return fmt.Errorf("unexpected string pttl for key=[%s]: %s", item.key, v)
		default:
			return fmt.Errorf("unexpected pttl type %T for key=[%s]", iPttl, item.key)
		}

		if pttl == -2 {
			continue // key not exist
		}
		if pttl == -1 {
			pttl = 0 // no expire
		}

		if uint64(len(dump)) > config.Opt.Advanced.TargetRedisProtoMaxBulkLen {
			log.Warnf("key=[%s] dump len=[%d] exceeds target_redis_proto_max_bulk_len, falling back to individual commands. "+
				"rdb_restore_command_behavior setting may not work correctly for this key.", item.key, len(dump))
			typeByte := dump[0]
			anotherReader := strings.NewReader(dump[1 : len(dump)-10])
			o := types.ParseObject(anotherReader, typeByte, item.key, r.isValkey)
			cmdC := o.Rewrite()
			for cmd := range cmdC {
				e := entry.NewEntry()
				e.DbId = item.dbId
				e.Argv = cmd
				r.ch <- e
			}
			if pttl != 0 {
				e := entry.NewEntry()
				e.DbId = item.dbId
				e.Argv = []string{"PEXPIRE", item.key, strconv.Itoa(pttl)}
				r.ch <- e
			}
		} else {
			argv := []string{"RESTORE", item.key, strconv.Itoa(pttl), dump}
			if config.Opt.Advanced.RDBRestoreCommandBehavior == "rewrite" {
				argv = append(argv, "replace")
			}
			r.ch <- &entry.Entry{
				DbId: item.dbId,
				Argv: argv,
			}
		}
	}
	return nil
}

func (r *scanStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *scanStandaloneReader) StatusString() string {
	if r.stat.ScanFinished {
		return fmt.Sprintf("need_update_count=[%d], matched_keys=[%d]", r.stat.NeedUpdateCount, r.stat.ScanMatchedKeys)
	}
	return fmt.Sprintf("scan_dbid=[%d], scan_percent=[%s], need_update_count=[%d], matched_keys=[%d]", r.stat.ScanDbId, r.stat.ScanPercentByDbId, r.stat.NeedUpdateCount, r.stat.ScanMatchedKeys)
}

func (r *scanStandaloneReader) StatusConsistent() bool {
	return r.stat.ScanFinished && r.stat.NeedUpdateCount == 0
}
