package workers

import "time"

const (
	// CDC activity.
	totalSearch             = 50
	DefaultSearchCDCSubject = "distributor.search.new"

	// Distributor activity.
	DefaultWorkflowSearchDistributorDurableName = "distributor_searches"

	// BatchWorker activity
	DefaultWorkflowStreamName       = "workflow"
	DefaultWorkflowStreamSubjects   = "  distributor.>;  worker.>;collector.>"
	DefaultWorkflowStreamCleanupTtl = 3600 * time.Second

	DefaultWorkflowSearchWorkerDurableName = "batch_search_worker"
	TemplateSearchWorkerSubject            = "worker.%s.search.new.%d"
	DefaultWorkflowWorkerSubjects          = "worker"
	DefaultBatchSearchWorkerSpanScope      = "batch_search_worker"
	// BatchWorker activity
	DefaultWorkflowMetricsBucketName = "metrics"
	DefaultWorkflowMetricsScope      = "workflow.metrics"
)
