package jobs

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"fmt"

	"github.com/lytics/escp/esbulk"
	"github.com/lytics/escp/esindex"
	"github.com/lytics/escp/esscroll"
	log "github.com/lytics/escp/logging"
)

func ParseUrl(u string) (*url.URL, error) {
	if len(u) == 0 {
		return nil, fmt.Errorf("no url provided")
	}
	if strings.HasSuffix(u, "/") {
		u = u[:len(u)-1]
	}
	if !strings.HasPrefix(u, "http") {
		u = "http://" + u
	}
	return url.Parse(u)
}

type SourceConfig struct {
	IndexName     string                 // the sorce index name to read from.
	Host          *url.URL               // the source index url to read from example: http://es1:9200
	ScrollTimeout time.Duration          // time to keep scroll alive between requests
	ScrollPage    int                    // size of scroll pages (will actually be per source shard)
	ScrollDocs    int                    // number of `docs` to buffer in memory from scroll
	Filter        map[string]interface{} // an es filter to apply to the source scroll (experimental)
}

func (s *SourceConfig) URL() string {
	if s.Host.Scheme == "" {
		s.Host.Scheme = "http"
	}
	return fmt.Sprintf("%s/%s", s.Host.String(), s.IndexName)
}

type DesConfig struct {
	IndexName string     //The target index name
	Hosts     []*url.URL //set of hosts to use during the copy, to send Bulk requests too.

	CreateDelay       time.Duration // after creating a new target index, sleep this long before writing data.
	RefreshInt        time.Duration // the refresh interval to use on the new index
	Shards            int           // how many shards to use with the next index
	DelayRefresh      bool          // Disable refreshing until the copy has completed
	SkipCreate        bool          //Just start writting to the index, don't bother to create the index
	DelayReplicaton   bool          //turn off ES replication until after the copy has finished.
	ReplicationFactor int           //if delayreplication is set the replicaiton setting will be set to this after coping.
	MaxSeg            int           //if indexing is delayed, the max number of segments for the optimized index

	BulkSize   int // The pulk batch size to use when submitting writes to des index bulk queue.
	NumWorkers int //number of parallel bulk upload buffers to use; 0 = len(hosts)*2
}

func (d *DesConfig) URLs() []string {
	res := []string{}
	for _, h := range d.Hosts {
		if h.Scheme == "" {
			h.Scheme = "http"
		}
		res = append(res, h.String())
	}
	return res
}

func (d *DesConfig) PrimaryURL() string {
	// Use the first destination host as the "primary" node to talk too
	if urls := d.URLs(); len(urls) > 0 {
		return fmt.Sprintf("%s/%s", urls[0], d.IndexName)
	}
	return ""
}

func Copy(ctx context.Context, src *SourceConfig, des *DesConfig, logger log.Logger, logevery time.Duration) error {
	srcUrl := src.URL()
	priDesUrl := des.PrimaryURL()

	idxmeta, err := esindex.Get(srcUrl)
	if err != nil {
		return fmt.Errorf("failed getting source index metadata: %v", err)
	}

	// Copy over shards setting if it wasn't explicitly set
	if des.Shards == 0 {
		des.Shards = *idxmeta.Settings.Index.Shards
	}

	// Copy over refreshint if it wasn't set in options but was set on the source
	// index
	refreshint := ""
	if des.RefreshInt == 0 {
		if idxmeta.Settings.Index.RefreshInterval != "" {
			refreshint = idxmeta.Settings.Index.RefreshInterval
		} else {
			refreshint = "1s" // default
		}
	} else {
		refreshint = fmt.Sprintf("%v", des.RefreshInt)
	}

	// Start the scroll first to make sure the source parameter is valid
	ess := esscroll.New(ctx, srcUrl, src.ScrollTimeout, src.ScrollPage, src.ScrollDocs, src.Filter, logevery, logger)
	resp, err := ess.Start()
	if err != nil {
		return fmt.Errorf("error starting scroll: %v", err)
	}

	// Create the destination index unless explicitly told not to
	if !des.SkipCreate {
		logger.Infof("Creating index %s with shards=%d refresh_interval=%s delay-refresh=%t", des.IndexName, des.Shards, refreshint, des.DelayRefresh)
		if des.Shards == 0 {
			des.Shards = *idxmeta.Settings.Index.Shards
		}
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{
			Shards:          &des.Shards,
			RefreshInterval: refreshint,
		}}}
		if des.DelayRefresh {
			m.Settings.Index.RefreshInterval = "-1" // Disable refreshing until the copy has completed
		}
		if des.DelayReplicaton {
			i := 0
			m.Settings.Index.Replicas = &i
		}
		if err := esindex.Create(priDesUrl, &m); err != nil {
			logger.Errorf("index create failed:%v", err)
			return err
		}

		time.Sleep(des.CreateDelay)
	}

	desmeta, err := esindex.Get(priDesUrl)
	if err != nil {
		logger.Infof("error loading destination index settings. err:%v", err)
	}
	b, err := json.Marshal(desmeta)
	if err != nil {
		logger.Infof("error marshalling index settings. err:%v", err)
	}

	logger.Infof("Copying %d documents from %s to %s/%s destination index settings: %v bulksize:%v",
		resp.Total, srcUrl, des.Hosts, des.IndexName, string(b), esscroll.IECFormat(uint64(des.BulkSize)))

	indexer := esbulk.New(ctx, des.URLs(), des.IndexName, des.BulkSize, des.NumWorkers, resp.Hits, logger)
	if err := <-indexer.Err(); err != nil {
		return fmt.Errorf("Error indexing: %v", err)
	}

	if err := resp.Err(); err != nil {
		logger.Errorf("Error searching: %v", err)
	}

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if des.DelayRefresh {
		logger.Infof("Copy completed. Refreshing index. This may take some time.")
		if err := esindex.Optimize(priDesUrl, des.MaxSeg); err != nil {
			return fmt.Errorf("Error optimizing index: %v", err)
		}
		logger.Infof("Optimize completed. Setting refresh interval to %s", refreshint)

		// update refresh setting
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{RefreshInterval: refreshint}}}
		if err := esindex.Update(priDesUrl, &m); err != nil {
			return fmt.Errorf("Error enabling refreshing: %v", err)
		}
	}

	if des.DelayReplicaton {
		// update refresh setting
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{Replicas: &des.ReplicationFactor}}}
		if err := esindex.Update(priDesUrl, &m); err != nil {
			return fmt.Errorf("Error enabling replicas[%v]: %v", des.ReplicationFactor, err)
		}
	}

	desmeta, err = esindex.Get(priDesUrl)
	if err != nil {
		return fmt.Errorf("error loading destination index settings. err:%v", err)
	}
	b, err = json.Marshal(desmeta)
	if err != nil {
		return fmt.Errorf("error marshalling index settings. err:%v", err)
	}
	logger.Infof("copy job completed: destination index settngs: idx:%v settings:%v", priDesUrl, string(b))
	return nil
}
