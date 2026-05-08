package reader

import (
	"context"
	"fmt"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
)

type scanClusterReader struct {
	readers  []Reader
	statusId int
}

func NewScanClusterReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	addresses := discoverClusterNodesWithRetry(ctx, opts)

	rd := &scanClusterReader{}
	for _, address := range addresses {
		theOpts := *opts
		theOpts.Address = address
		rd.readers = append(rd.readers, NewScanStandaloneReader(ctx, &theOpts))
	}
	return rd
}

// discoverClusterNodesWithRetry calls GetRedisClusterNodes, retrying up to
// opts.MaxRetries times so a transient startup connectivity blip doesn't abort the run.
func discoverClusterNodesWithRetry(ctx context.Context, opts *ScanReaderOptions) []string {
	var lastErr error
	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			waitTime := time.Duration(attempt) * 5 * time.Second
			log.Warnf("[cluster] node discovery failed (attempt %d/%d): %v, retrying in %v...", attempt, opts.MaxRetries, lastErr, waitTime)
			select {
			case <-ctx.Done():
				log.Panicf("[cluster] context cancelled during node discovery")
			case <-time.After(waitTime):
			}
		}
		addresses, err := discoverClusterNodesSafe(ctx, opts)
		if err == nil {
			return addresses
		}
		lastErr = err
	}
	log.Panicf("[cluster] node discovery failed after %d retries: %v", opts.MaxRetries, lastErr)
	return nil
}

// discoverClusterNodesSafe tests connectivity first (returns an error for retry),
// then calls GetRedisClusterNodes. GetRedisClusterNodes uses log.Panicf (os.Exit)
// internally, so we gate it behind a connectivity check that CAN return an error.
func discoverClusterNodesSafe(ctx context.Context, opts *ScanReaderOptions) ([]string, error) {
	// Probe the seed node. This is the step most likely to fail transiently.
	c, err := client.NewRedisClientSafe(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	if err != nil {
		return nil, fmt.Errorf("seed node unreachable: %w", err)
	}
	c.Close()
	// Node is up — proceed with full discovery.
	addresses, _ := utils.GetRedisClusterNodes(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, opts.PreferReplica)
	return addresses, nil
}

func (rd *scanClusterReader) StartRead(ctx context.Context) []chan *entry.Entry {
	chs := make([]chan *entry.Entry, 0)
	for _, r := range rd.readers {
		ch := r.StartRead(ctx)
		chs = append(chs, ch[0])
	}
	return chs
}

func (rd *scanClusterReader) Status() interface{} {
	stat := make([]interface{}, 0)
	for _, r := range rd.readers {
		stat = append(stat, r.Status())
	}
	return stat
}

func (rd *scanClusterReader) StatusString() string {
	rd.statusId += 1
	rd.statusId %= len(rd.readers)
	return fmt.Sprintf("src-%d, %s", rd.statusId, rd.readers[rd.statusId].StatusString())
}

func (rd *scanClusterReader) StatusConsistent() bool {
	for _, r := range rd.readers {
		if !r.StatusConsistent() {
			return false
		}
	}
	return true
}
