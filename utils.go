package db_bench

import (
	"github.com/stretchr/testify/suite"
	"sort"
	"testing"
)

func printStats(t *testing.T, suiteName string, stats *suite.SuiteInformation) {

	t.Logf("=== %s", suiteName)

	keys := make([]string, 0, len(stats.TestStats))
	for k := range stats.TestStats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var total float64

	for _, k := range keys {
		stat := stats.TestStats[k]
		duration := stat.End.Sub(stat.Start)
		if stat.Passed && duration.Milliseconds() == 0 {
			t.Logf("%s: SKIPPED", k)
		} else {
			t.Logf("%s: %d ms (%.3f s)", k, duration.Milliseconds(), duration.Seconds())
		}
		total += duration.Seconds()
	}

	t.Logf("total: %.3f s", total)
}
