package scalers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	defaultScheduleDesiredReplicas = 1
	scheduleMetricType             = "External"
)

type scheduleScaler struct {
	metricType v2.MetricTargetType
	metadata   *scheduleMetadata
	logger     logr.Logger
}

type scheduleMetadata struct {
	interval        string
	start           string
	end             string
	timezone        string
	desiredReplicas int64
	scalerIndex     int
}

// NewScheduleScaler creates a new scheduleScaler
func NewScheduleScaler(config *ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	meta, parseErr := parseScheduleMetadata(config)
	if parseErr != nil {
		return nil, fmt.Errorf("error parsing schedule metadata: %w", parseErr)
	}

	return &scheduleScaler{
		metricType: metricType,
		metadata:   meta,
		logger:     InitializeLogger(config, "schedule_scaler"),
	}, nil
}

func parseScheduleMetadata(config *ScalerConfig) (*scheduleMetadata, error) {
	if len(config.TriggerMetadata) == 0 {
		return nil, fmt.Errorf("invalid Input Metadata. %s", config.TriggerMetadata)
	}

	meta := scheduleMetadata{}
	if val, ok := config.TriggerMetadata["timezone"]; ok && val != "" {
		meta.timezone = val
	} else {
		return nil, fmt.Errorf("no timezone specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["period"]; ok && val != "" {
		_, err := kedautil.ParseInterval(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing a period: %w", err)
		}
		meta.interval = val
	} else {
		return nil, fmt.Errorf("no start time specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["start"]; ok && val != "" {
		_, err := kedautil.ParseTime(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing start time: %w", err)
		}
		meta.start = val
	} else {
		return nil, fmt.Errorf("no start time specified. %s", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["end"]; ok && val != "" {
		_, err := kedautil.ParseTime(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing end time: %w", err)
		}
		meta.end = val
	} else {
		return nil, fmt.Errorf("no end time specified. %s", config.TriggerMetadata)
	}
	if meta.start == meta.end {
		return nil, fmt.Errorf("error parsing schedule. %s: start and end can not have exactly same time input", config.TriggerMetadata)
	}
	if val, ok := config.TriggerMetadata["desiredReplicas"]; ok && val != "" {
		metadataDesiredReplicas, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing desiredReplicas metadata. %s", config.TriggerMetadata)
		}

		meta.desiredReplicas = int64(metadataDesiredReplicas)
	} else {
		return nil, fmt.Errorf("no DesiredReplicas specified. %s", config.TriggerMetadata)
	}
	meta.scalerIndex = config.ScalerIndex
	return &meta, nil
}

func (s *scheduleScaler) Close(context.Context) error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *scheduleScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	var specReplicas int64 = 1
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, kedautil.NormalizeString(fmt.Sprintf("schedule-%s-%s-%s-%s", s.metadata.timezone, s.metadata.interval, s.metadata.start, s.metadata.end))),
		},
		Target: GetMetricTarget(s.metricType, specReplicas),
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: scheduleMetricType}
	return []v2.MetricSpec{metricSpec}
}

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *scheduleScaler) GetMetricsAndActivity(_ context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	defaultDesiredReplicas := int64(defaultScheduleDesiredReplicas)

	location, err := time.LoadLocation(s.metadata.timezone)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("unable to load timezone. Error: %w", err)
	}

	// Since we are considering the timestamp here and not the exact time, timezone does matter.
	currentTime := time.Now().Unix()
	parsedInterval, err := kedautil.ParseInterval(s.metadata.interval)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error initializing interval: %v", parsedInterval)
	}
	startTime, err := kedautil.ParseTime(s.metadata.start)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error initializing start time: %s", s.metadata.start)
	}
	endTime, err := kedautil.ParseTime(s.metadata.end)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error initializing start time: %s", s.metadata.end)
	}

	nextStartTime, startTimescheduleErr := kedautil.ParseNextTime(parsedInterval, startTime, location)
	if startTimescheduleErr != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error initializing start schedule: %w", startTimescheduleErr)
	}

	nextEndTime, endTimescheduleErr := kedautil.ParseNextTime(parsedInterval, endTime, location)
	if endTimescheduleErr != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error intializing end schedule: %w", endTimescheduleErr)
	}

	switch {
	case nextStartTime.Unix() < nextEndTime.Unix() && currentTime < nextStartTime.Unix():
		metric := GenerateMetricInMili(metricName, float64(defaultScheduleDesiredReplicas))
		return []external_metrics.ExternalMetricValue{metric}, false, nil
	case currentTime <= nextEndTime.Unix():
		metric := GenerateMetricInMili(metricName, float64(s.metadata.desiredReplicas))
		return []external_metrics.ExternalMetricValue{metric}, true, nil
	default:
		metric := GenerateMetricInMili(metricName, float64(defaultDesiredReplicas))
		return []external_metrics.ExternalMetricValue{metric}, false, nil
	}
}
