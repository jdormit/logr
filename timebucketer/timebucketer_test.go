package timebucketer

import (
	"github.com/google/go-cmp/cmp"
	"github.com/jdormit/logr/timeseries"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	testCases := []struct {
		begin          time.Time
		end            time.Time
		numBuckets     int
		logLines       []timeseries.LogLine
		expectedOutput TimeBuckets
	}{
		{
			time.Unix(0, 0),
			time.Unix(10, 0),
			5,
			[]timeseries.LogLine{
				timeseries.LogLine{
					Timestamp: time.Unix(1, 0),
				},
				timeseries.LogLine{
					Timestamp: time.Unix(4, 0),
				},
				timeseries.LogLine{
					Timestamp: time.Unix(5, 0),
				},
			},
			TimeBuckets{
				{
					timeseries.LogLine{
						Timestamp: time.Unix(1, 0),
					},
				},
				nil,
				{
					timeseries.LogLine{
						Timestamp: time.Unix(4, 0),
					},
					timeseries.LogLine{
						Timestamp: time.Unix(5, 0),
					},
				},
				nil,
				nil,
			},
		},
	}
	for caseIdx, testCase := range testCases {
		buckets := Bucket(testCase.begin, testCase.end, testCase.numBuckets, testCase.logLines)
		if !cmp.Equal(buckets, testCase.expectedOutput) {
			t.Errorf("Error on case %d.\nExpected: %v\nActual: %v",
				caseIdx, testCase.expectedOutput, buckets)
		}
	}
}
