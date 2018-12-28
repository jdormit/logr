package main

import (
	"fmt"
	"github.com/jdormit/logr/demowriter"
	"os"
	"os/signal"
	"time"
)

func main() {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)

	writerChan := make(chan string)
	writer := demowriter.NewDemoWriter(writerChan)

	minDuration := time.Duration(1) * time.Second
	maxDuration := time.Duration(5) * time.Second
	go writer.Start(minDuration, maxDuration)
	defer writer.Terminate()

	for {
		select {
		case <-interrupts:
			return
		case logLine := <-writerChan:
			fmt.Println(logLine)
		}
	}
}
