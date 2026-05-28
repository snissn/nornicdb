package observability

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestCatalogBindHelpersObserveWithTenantLabels(t *testing.T) {
	ctx := context.Background()

	t.Run("bolt bind message duration", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewBoltMetrics(te.Registry)
		bag.BindMessageDuration("run").Observe(ctx, 0.002)

		metric := onlyMetric(t, te, "nornicdb_bolt_message_duration_seconds")
		require.Equal(t, map[string]string{"op": "run"}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("cypher observe query duration with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.ObserveQueryDuration(ctx, "write", "neo4j", 0.003)

		metric := onlyMetric(t, te, "nornicdb_cypher_query_duration_seconds")
		require.Equal(t, map[string]string{"op_type": "write", "database": "neo4j"}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("http observe request duration with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewHTTPMetrics(te.Registry, true)
		require.True(t, bag.TenantLabelsEnabled())
		bag.ObserveRequestDuration(ctx, "GET", "/db/{name}", "2xx", "neo4j", 0.004)

		metric := onlyMetric(t, te, "nornicdb_http_request_duration_seconds")
		require.Equal(t, map[string]string{
			"method":        "GET",
			"path_template": "/db/{name}",
			"status_class":  "2xx",
			"database":      "neo4j",
		}, metricLabels(metric))
		require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
	})

	t.Run("search bind duration and request with tenant", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewSearchMetrics(te.Registry, true, nil)
		require.True(t, bag.TenantLabelsEnabled())
		bag.BindDuration("neo4j", "vector", "fuse").Observe(ctx, 0.005)
		bag.IncRequest("neo4j", "vector", "success")

		duration := onlyMetric(t, te, "nornicdb_search_duration_seconds")
		require.Equal(t, map[string]string{"database": "neo4j", "mode": "vector", "stage": "fuse"}, metricLabels(duration))
		require.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount())

		request := onlyMetric(t, te, "nornicdb_search_requests_total")
		require.Equal(t, map[string]string{"database": "neo4j", "mode": "vector", "result": "success"}, metricLabels(request))
		require.Equal(t, float64(1), request.GetCounter().GetValue())
	})
}

func onlyMetric(t *testing.T, te *TestEnv, name string) *dto.Metric {
	t.Helper()
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.Metric, 1, name)
			return mf.Metric[0]
		}
	}
	require.FailNowf(t, "missing metric", "metric family %s was not gathered", name)
	return nil
}

func metricLabels(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.Label))
	for _, label := range metric.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}
