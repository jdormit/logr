package demowriter

import (
	"fmt"
	"math/rand"
	"time"
)

func pickOne(stuff []string) string {
	return stuff[rand.Intn(len(stuff))]
}

func generateLogLine() string {
	ip := "127.0.0.1"
	user := "-"
	authUser := pickOne([]string{"james", "jill", "frank", "mary"})
	time := time.Now().Format("02/Jan/2006:15:04:05 -0700")
	method := pickOne([]string{"GET", "GET", "POST"})
	path := pickOne([]string{"/report", "/api/users", "/", "/api/posts"})
	request := fmt.Sprintf("%s %s HTTP/1.0", method, path)
	response := pickOne([]string{"200", "200", "200", "400", "500"})
	responseBytes := pickOne([]string{"123", "12", "234", "123"})

	logFmt := "%s %s %s [%s] \"%s\" %s %s"
	return fmt.Sprintf(logFmt, ip, user, authUser, time, request, response, responseBytes)
}

type DemoWriter struct {
	output     chan<- string
	terminated bool
}

func NewDemoWriter(output chan<- string) DemoWriter {
	return DemoWriter{output, false}
}

func (d *DemoWriter) Terminate() {
	d.terminated = true
}

// Start writes random log line strings to the demo output channel at random intervals,
// where each interval is guaranteed to be between minDuration and maxDuration
func (d *DemoWriter) Start(minDuration time.Duration, maxDuration time.Duration) {
	rand.Seed(time.Now().UnixNano())
	d.terminated = false
	for !d.terminated {
		nextDuration := minDuration + time.Duration(rand.Int63n(int64(maxDuration)-int64(minDuration)))
		timer := time.NewTimer(nextDuration)
		<-timer.C
		d.output <- generateLogLine()
	}
}
