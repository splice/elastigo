package elastigo

import (
	"encoding/json"
)

// AggregateBucket holds information about a bucket-type aggregate
// as returned by an ES query.
//
// Given this JSON:
// "aggregations":{"tags":{"buckets":[{"key":"bass","doc_count":1},{"key":"drum","doc_count":1}]}}
//	The struct will hold:
// Name: "tags", KeyCount: {bass: 1}, {drum: 1}
type AggregateBucket struct {
	Name     string
	KeyCount map[string]int
}

// ExtractAggregates returns the result of an aggregate query.
// Any query can have an 'aggregates' section added to it, and the result of this
// comes back in a separate key: 'aggregates' (as opposed to 'hits', which is where
// the actual search results are in the response.
func ExtractAggregates(sr *SearchResult) ([]*AggregateBucket, error) {
	// unmarshal the aggregates (have to use a map and some manual steps
	// to be able to get the name of the aggregation, which is a key, not
	// a value).
	var m map[string]interface{}
	if err := json.Unmarshal(sr.Aggregations, &m); err != nil {
		return nil, err
	}
	var aggs []*AggregateBucket
	for k, v := range m {
		agg := AggregateBucket{
			Name:     k,
			KeyCount: make(map[string]int),
		}
		if vm, ok := v.(map[string]interface{}); ok {
			if buck, ok := vm["buckets"]; ok {
				// buck should be an array of maps having two keys each, "key"
				// and "doc_count"
				if ar, ok := buck.([]interface{}); ok {
					for _, kdc := range ar {
						if mkdc, ok := kdc.(map[string]interface{}); ok {
							if k, ok := mkdc["key"]; ok {
								if cnt, ok := mkdc["doc_count"]; ok {
									agg.KeyCount[k.(string)] = int(cnt.(float64))
								}
							}
						}
					}
				}
			}
		}
		if len(agg.KeyCount) > 0 {
			aggs = append(aggs, &agg)
		}
	}
	return aggs, nil
}
