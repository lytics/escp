package jobs

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/lytics/escp/esdiff"
	"github.com/lytics/escp/esindex"
	"github.com/lytics/escp/esscroll"
	log "github.com/lytics/escp/logging"
)

var ErrMissMatch = fmt.Errorf("missmatched results")

type ValidationResults struct {
	Total       int
	Checked     int
	Missing     int
	MissMatched int
	Matched     int
	Details     []string
}

func (v *ValidationResults) String() string {
	return fmt.Sprintf("Checked %d/%d (%.1f%%) documents; missing=%d mismatched=%d matched=%d",
		v.Checked, v.Total, (float64(v.Checked)/float64(v.Total))*100.0,
		v.Missing, v.MissMatched, v.Matched)
}

func Validate(ctx context.Context, src *SourceConfig, des *DesConfig, denom int, logger log.Logger, logevery time.Duration) (*ValidationResults, error) {
	dice := rand.New(rand.NewSource(time.Now().UnixNano()))
	vr := &ValidationResults{}
	desIdxUrl := fmt.Sprintf("%s/%s", des.Hosts[0], des.IndexName)
	srcUrl := fmt.Sprintf("%s/%s", src.Host, src.IndexName)

	// Make sure the totals are the same before we do a bunch of work
	srccnt, err := esindex.GetDocCount(srcUrl)
	if err != nil {
		return vr, fmt.Errorf("error getting src doc count: %v", err)
	}
	descnt, err := esindex.GetDocCount(desIdxUrl)
	if err != nil {
		return vr, fmt.Errorf("error getting des doc count: %v", err)
	}
	if srccnt != descnt {
		logger.Warnf("Source and target have different document totals: %d vs. %d", srccnt, descnt)
		vr.Details = []string{fmt.Sprintf("DocCountMissMatch: %d vs. %d", srccnt, descnt)}
		return vr, ErrMissMatch
	}

	// Start the scroll first to make sure the source parameter is valid
	ess := esscroll.New(ctx, srcUrl, src.ScrollTimeout, src.ScrollPage, src.ScrollDocs, src.Filter, logevery, logger)
	resp, err := ess.Start()
	if err != nil {
		return vr, fmt.Errorf("error starting scroll: %v", err)
	}

	vr.Total = int(resp.Total)

	logger.Infof("Scrolling over %d documents from %v \n", resp.Total, srcUrl)

	for doc := range resp.Hits {
		if denom == 1 || dice.Intn(denom) == 0 {
			vr.Checked++
			diff, err := esdiff.Check(doc, desIdxUrl, logger)
			if err != nil {
				return vr, fmt.Errorf("fatal escheck error: %v", err)
			}
			switch diff {
			case "":
				vr.Matched++
			case esdiff.DiffMissing:
				vr.Missing++
				vr.Details = append(vr.Details, fmt.Sprintf("MissingDoc:%v", doc.ID))
			default:
				vr.MissMatched++
				vr.Details = append(vr.Details, fmt.Sprintf("DocMissMatch:%v", doc.ID))
			}
		}
	}
	if resp.Err() != nil {
		return vr, fmt.Errorf("scoll error:%v", resp.Err())
	}

	if vr.Missing+vr.MissMatched > 0 {
		return vr, ErrMissMatch
	}
	return vr, nil
}
