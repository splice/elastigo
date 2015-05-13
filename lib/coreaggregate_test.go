package elastigo

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestExtractAggregates(t *testing.T) {
	s := `
	{
		"instruments": {
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count": 0,
			"buckets": [
				{
					"key": "violin",
					"doc_count": 3
				},
				{
					"key": "bongo",
					"doc_count": 2
				},
				{
					"key": "cello",
					"doc_count": 1
				},
				{
					"key": "drums",
					"doc_count": 4
				}
			]
		},
		"tags": {
			"doc_count_error_upper_bound": 0,
			"sum_other_doc_count": 0,
			"buckets": [
				{
					"key": "drum",
					"doc_count": 2
				},
				{
					"key": "house",
					"doc_count": 3
				},
				{
					"key": "bass",
					"doc_count": 1
				}
			]
		}
	}
	`

	sr := &SearchResult{Aggregations: []byte(s)}

	buckets, err := ExtractAggregates(sr)
	if err != nil {
		t.Fatal(err)
	}

	if len(buckets) != 2 {
		t.Fatalf("got %d aggregates, expected 2", len(buckets))
	}

	for _, bucket := range buckets {
		switch bucket.Name {
		case "tags":
			if len(bucket.KeyCount) == 3 {
				assert.Equal(t, 2, bucket.KeyCount["drum"])
				assert.Equal(t, 3, bucket.KeyCount["house"])
				assert.Equal(t, 1, bucket.KeyCount["bass"])
			} else {
				t.Errorf("got %d keys in 'tags' bucket, expected 3", len(bucket.KeyCount))
			}
		case "instruments":
			if len(bucket.KeyCount) == 4 {
				assert.Equal(t, 4, bucket.KeyCount["drums"])
				assert.Equal(t, 1, bucket.KeyCount["cello"])
				assert.Equal(t, 3, bucket.KeyCount["violin"])
				assert.Equal(t, 2, bucket.KeyCount["bongo"])
			} else {
				t.Errorf("got %d keys in 'instruments' bucket, expected 4", len(bucket.KeyCount))
			}
		}
	}
}
