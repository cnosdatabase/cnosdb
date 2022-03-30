package prometheus

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cnosdb/cnosdb/storage/reads/datatypes"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/storage"
	"github.com/gogo/protobuf/types"
	remote "github.com/prometheus/prometheus/prompb"
)

const (
	// measurementName is the default name used if no Prometheus name can be found on write
	measurementName = "prom_metric_not_specified"

	// fieldName is the field all prometheus values get written to
	fieldName = "value"

	// fieldTagKey is the tag key that all field names use in the new storage processor
	fieldTagKey = "_field"

	// prometheusNameTag is the tag key that Prometheus uses for metric names
	prometheusNameTag = "__name__"

	// measurementTagKey is the tag key that all measurement names use in the new storage processor
	measurementTagKey = "_measurement"
)

// A DroppedValuesError is returned when the prometheus write request contains
// unsupported float64 values.
type DroppedValuesError struct {
	nan  uint64
	ninf uint64
	inf  uint64
}

// Error returns a descriptive error of the values dropped.
func (e DroppedValuesError) Error() string {
	return fmt.Sprintf("dropped unsupported Prometheus values: [NaN = %d, +Inf = %d, -Inf = %d]", e.nan, e.inf, e.ninf)
}

// WriteRequestToPoints converts a Prometheus remote write request of time series and their
// samples into Points that can be written into CnosDB
func WriteRequestToPoints(req *remote.WriteRequest) ([]models.Point, error) {
	var maxPoints int
	for _, ts := range req.Timeseries {
		maxPoints += len(ts.Samples)
	}
	points := make([]models.Point, 0, maxPoints)

	// Track any dropped values.
	var nan, inf, ninf uint64

	for _, ts := range req.Timeseries {
		measurement := measurementName

		tags := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			tags[l.Name] = l.Value
			if l.Name == prometheusNameTag {
				measurement = l.Value
			}
		}

		for _, s := range ts.Samples {
			if v := s.Value; math.IsNaN(v) {
				nan++
				continue
			} else if math.IsInf(v, -1) {
				ninf++
				continue
			} else if math.IsInf(v, 1) {
				inf++
				continue
			}

			// convert and append
			t := time.Unix(0, s.Timestamp*int64(time.Millisecond))
			fields := map[string]interface{}{fieldName: s.Value}
			p, err := models.NewPoint(measurement, models.NewTags(tags), fields, t)
			if err != nil {
				return nil, err
			}
			points = append(points, p)
		}
	}

	if nan+inf+ninf > 0 {
		return points, DroppedValuesError{nan: nan, inf: inf, ninf: ninf}
	}
	return points, nil
}

// ReadRequestToCnosDBStorageRequest converts a Prometheus remote read request into one using the
// new storage API that IFQL uses.
func ReadRequestToCnosDBStorageRequest(req *remote.ReadRequest, db, rp string) (*datatypes.ReadFilterRequest, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("Prometheus read endpoint currently only supports one query at a time")
	}
	q := req.Queries[0]

	src, err := types.MarshalAny(&storage.ReadSource{Database: db, RetentionPolicy: rp})
	if err != nil {
		return nil, err
	}

	sreq := &datatypes.ReadFilterRequest{
		ReadSource: src,
		Range: datatypes.TimestampRange{
			Start: time.Unix(0, q.StartTimestampMs*int64(time.Millisecond)).UnixNano(),
			End:   time.Unix(0, q.EndTimestampMs*int64(time.Millisecond)).UnixNano(),
		},
	}

	pred, err := predicateFromMatchers(q.Matchers)
	if err != nil {
		return nil, err
	}

	sreq.Predicate = pred
	return sreq, nil
}

// RemoveCnosDBSystemTags will remove tags that are CnosDB internal (_measurement and _field)
func RemoveCnosDBSystemTags(tags models.Tags) models.Tags {
	var t models.Tags
	for _, tt := range tags {
		if string(tt.Key) == measurementTagKey || string(tt.Key) == fieldTagKey {
			continue
		}
		t = append(t, tt)
	}

	return t
}

// predicateFromMatchers takes Prometheus label matchers and converts them to a storage
// predicate that works with the schema that is written in, which assumes a single field
// named value
func predicateFromMatchers(matchers []*remote.LabelMatcher) (*datatypes.Predicate, error) {
	left, err := nodeFromMatchers(matchers)
	if err != nil {
		return nil, err
	}
	right := fieldNode()

	return &datatypes.Predicate{
		Root: &datatypes.Node{
			NodeType: datatypes.NodeTypeLogicalExpression,
			Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
			Children: []*datatypes.Node{left, right},
		},
	}, nil
}

// fieldNode returns a datatypes.Node that will match that the fieldTagKey == fieldName
// which matches how Prometheus data is fed into the system
func fieldNode() *datatypes.Node {
	children := []*datatypes.Node{
		&datatypes.Node{
			NodeType: datatypes.NodeTypeTagRef,
			Value: &datatypes.Node_TagRefValue{
				TagRefValue: fieldTagKey,
			},
		},
		&datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: fieldName,
			},
		},
	}

	return &datatypes.Node{
		NodeType: datatypes.NodeTypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: datatypes.ComparisonEqual},
		Children: children,
	}
}

func nodeFromMatchers(matchers []*remote.LabelMatcher) (*datatypes.Node, error) {
	if len(matchers) == 0 {
		return nil, errors.New("expected matcher")
	} else if len(matchers) == 1 {
		return nodeFromMatcher(matchers[0])
	}

	left, err := nodeFromMatcher(matchers[0])
	if err != nil {
		return nil, err
	}

	right, err := nodeFromMatchers(matchers[1:])
	if err != nil {
		return nil, err
	}

	children := []*datatypes.Node{left, right}
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeLogicalExpression,
		Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
		Children: children,
	}, nil
}

func nodeFromMatcher(m *remote.LabelMatcher) (*datatypes.Node, error) {
	var op datatypes.Node_Comparison
	switch m.Type {
	case remote.LabelMatcher_EQ:
		op = datatypes.ComparisonEqual
	case remote.LabelMatcher_NEQ:
		op = datatypes.ComparisonNotEqual
	case remote.LabelMatcher_RE:
		op = datatypes.ComparisonRegex
	case remote.LabelMatcher_NRE:
		op = datatypes.ComparisonNotRegex
	default:
		return nil, fmt.Errorf("unknown match type %v", m.Type)
	}

	name := m.Name
	if m.Name == prometheusNameTag {
		name = measurementTagKey
	}

	left := &datatypes.Node{
		NodeType: datatypes.NodeTypeTagRef,
		Value: &datatypes.Node_TagRefValue{
			TagRefValue: name,
		},
	}

	var right *datatypes.Node

	if op == datatypes.ComparisonRegex || op == datatypes.ComparisonNotRegex {
		right = &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_RegexValue{
				// To comply with PromQL, see
				// https://github.com/prometheus/prometheus/blob/daf382e4a9f5ca380b2b662c8e60755a56675f14/pkg/labels/regexp.go#L30
				RegexValue: "^(?:" + m.Value + ")$",
			},
		}
	} else {
		right = &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: m.Value,
			},
		}
	}

	children := []*datatypes.Node{left, right}
	return &datatypes.Node{
		NodeType: datatypes.NodeTypeComparisonExpression,
		Value:    &datatypes.Node_Comparison_{Comparison: op},
		Children: children,
	}, nil
}

// ModelTagsToLabelPairs converts models.Tags to a slice of Prometheus label pairs
func ModelTagsToLabelPairs(tags models.Tags) []remote.Label {
	pairs := make([]remote.Label, 0, len(tags))
	for _, t := range tags {
		if string(t.Value) == "" {
			continue
		}
		pairs = append(pairs, remote.Label{
			Name:  string(t.Key),
			Value: string(t.Value),
		})
	}
	return pairs
}

// TagsToLabelPairs converts a map of CnosDB tags into a slice of Prometheus label pairs
func TagsToLabelPairs(tags map[string]string) []*remote.Label {
	pairs := make([]*remote.Label, 0, len(tags))
	for k, v := range tags {
		if v == "" {
			// If we select metrics with different sets of labels names,
			// CnosDB returns *all* possible tag names on all returned
			// series, with empty tag values on series where they don't
			// apply. In Prometheus, an empty label value is equivalent
			// to a non-existent label, so we just skip empty ones here
			// to make the result correct.
			continue
		}
		pairs = append(pairs, &remote.Label{
			Name:  k,
			Value: v,
		})
	}
	return pairs
}
