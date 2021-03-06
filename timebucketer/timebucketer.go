// Package timebucketer provides functionality to divide timeseries log lines into buckets
package timebucketer

import (
	"github.com/jdormit/logr/timeseries"
	"time"
)

type TimeBuckets [][]timeseries.LogLine

// The whichBucket function returns the index of the bucket in which timestamp belongs
// by performing a binary search over the timestamps of each bucket
func whichBucket(begin time.Time, end time.Time, numBuckets int, beginBucket int, endBucket int, timestamp time.Time) int {
	if beginBucket == endBucket {
		return beginBucket
	}

	currentBucket := (beginBucket + endBucket) / 2
	bucketDuration := end.Sub(begin) / time.Duration(numBuckets)
	low := begin.Add(bucketDuration * time.Duration(currentBucket))
	high := begin.Add(bucketDuration * time.Duration(currentBucket+1))

	if (timestamp.Equal(low) || timestamp.After(low)) && timestamp.Before(high) {
		return currentBucket
	} else if timestamp.Before(low) {
		return whichBucket(begin, end, numBuckets, beginBucket, currentBucket-1, timestamp)
	} else {
		return whichBucket(begin, end, numBuckets, currentBucket+1, endBucket, timestamp)
	}
}

// The Bucket function divides the input log lines into buckets.
// A bucket is an even slice of time such that there are `numBuckets`
// buckets between `begin` and `end`. In other words, Bucket will group
// the log lines into `numBuckets` groups, where log lines in the same bucket
// were all logged in the same slice of time.
func Bucket(begin time.Time, end time.Time, numBuckets int, logLines []timeseries.LogLine) TimeBuckets {
	buckets := make([][]timeseries.LogLine, numBuckets)
	for i := 0; i < len(logLines); i++ {
		logLine := logLines[i]
		bucket := whichBucket(begin, end, numBuckets, 0, numBuckets-1, logLine.Timestamp)
		buckets[bucket] = append(buckets[bucket], logLine)
	}
	return buckets
}
