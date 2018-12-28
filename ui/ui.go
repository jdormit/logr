package ui

import (
	"fmt"
	"github.com/gizak/termui"
	"github.com/jdormit/logr/timebucketer"
	"github.com/jdormit/logr/timeseries"
	"log"
	"time"
)

const recoveredCountdown = 3

// Traffic is a length-`UIState.Granularity` list of traffic readings,
// representing the total traffic for each bucket of time in the current time window
type Traffic []int

type UIState struct {
	SectionCounts      []timeseries.Count
	StatusCounts       []timeseries.Count
	Traffic            Traffic
	Begin              time.Time
	Timescale          int
	Granularity        int
	Alert              bool
	Recovered          bool
	RecoveredCountdown int
	AlertThreshold     float64
	AlertInterval      int
}

func getEnd(begin time.Time, timescale int) time.Time {
	return begin.Add(time.Duration(timescale) * time.Minute)
}

func noData() (noData *termui.Paragraph) {
	noData = termui.NewParagraph("No data")
	noData.Height = 3
	noData.TextFgColor = termui.ColorBlack
	noData.Border = false
	return
}

func header(state *UIState) (header *termui.Paragraph) {
	end := getEnd(state.Begin, state.Timescale)
	header = termui.NewParagraph(fmt.Sprintf("Traffic Statistics from %s to %s",
		state.Begin.Format("15:04:05"), end.Format("15:04:05")))
	header.Height = 3
	header.TextFgColor = termui.ColorBlack
	header.Border = false
	return
}

func sectionGraph(state *UIState) termui.GridBufferer {
	return gaugesWithLabels(state.SectionCounts, "/%s")
}

func statusGraph(state *UIState) termui.GridBufferer {
	return gaugesWithLabels(state.StatusCounts, "%v")
}

func gaugesWithLabels(counts []timeseries.Count, labelFmt string) termui.GridBufferer {
	numCounts := len(counts)

	if numCounts == 0 {
		noData := noData()
		return termui.NewRow(termui.NewCol(12, 0, noData))
	}

	var totalCount float64 = 0
	for i := 0; i < numCounts; i++ {
		totalCount = totalCount + float64(counts[i].Count)
	}

	labels := make([]string, 0)
	gauges := make([]termui.GridBufferer, 0)
	for i := 0; i < numCounts; i++ {
		count := counts[i]
		percentage := float64(count.Count) / totalCount * 100.0
		labels = append(labels, fmt.Sprintf(labelFmt, count.Label))
		labels = append(labels, "")
		gauge := &termui.Gauge{
			Percent: int(percentage),
			Label:   fmt.Sprintf("%v (%.2f%%)", count.Count, percentage),
		}
		gauge.Height = 2
		gauge.PaddingBottom = 1
		gauge.PaddingRight = 2
		gauges = append(gauges, gauge)
	}
	labels = labels[:len(labels)-1]

	labelList := termui.NewList()
	labelList.Items = labels
	labelList.Height = 2 * len(labels)
	labelList.Border = false
	labelList.ItemFgColor = termui.ColorBlack
	labelList.PaddingTop = 0
	labelList.PaddingBottom = 0

	return termui.NewRow(
		termui.NewCol(3, 0, labelList),
		termui.NewCol(9, 0, gauges...))
}

func statusHeader() (header *termui.Paragraph) {
	header = termui.NewParagraph("Response Status Code Breakdown")
	header.Height = 3
	header.TextFgColor = termui.ColorBlack
	header.Border = false
	return
}

func sectionHeader() (header *termui.Paragraph) {
	header = termui.NewParagraph("Website Section Breakdown")
	header.Height = 3
	header.TextFgColor = termui.ColorBlack
	header.Border = false
	return
}

func summaryStats(state *UIState) (stats *termui.Paragraph) {
	statsStr := ""
	stats = termui.NewParagraph(statsStr)
	return
}

func trafficGraph(state *UIState) (graph termui.GridBufferer) {
	end := getEnd(state.Begin, state.Timescale)
	chart := termui.NewBarChart()
	chart.Data = state.Traffic
	labels := make([]string, state.Granularity)
	bucketDuration := end.Sub(state.Begin) / time.Duration(state.Granularity)
	for i := range labels {
		bucketTime := state.Begin.Add(bucketDuration * time.Duration(i))
		labels[i] = bucketTime.Format("15:04:05")
	}
	chart.DataLabels = labels
	chart.BorderLabel = fmt.Sprintf("Site Traffic (Hits per %.2f seconds)",
		bucketDuration.Seconds())
	chart.Height = 9
	chart.PaddingTop = 1
	chart.TextColor = termui.ColorBlack
	chart.BarColor = termui.ColorYellow
	chart.NumColor = termui.ColorBlack
	chart.BarWidth = termui.TermWidth()/state.Granularity - 1
	graph = chart
	return
}

func empty() termui.GridBufferer {
	block := termui.NewBlock()
	block.Height = 0
	block.Border = false
	return block
}

func alert(state *UIState) termui.GridBufferer {
	if state.Alert {
		message := fmt.Sprintf("Average traffic exceeded %v/second for over %v seconds!",
			state.AlertThreshold, state.AlertInterval)
		alert := termui.NewParagraph(message)
		alert.BorderFg = termui.ColorRed
		alert.TextFgColor = termui.ColorRed | termui.AttrBold
		alert.BorderLabel = "ALERT"
		alert.Height = 3
		return alert
	} else if state.Recovered {
		message := fmt.Sprintf("Alert recovered at %s",
			time.Now().Format("15:04:05"))
		alert := termui.NewParagraph(message)
		alert.BorderFg = termui.ColorGreen
		alert.TextFgColor = termui.ColorGreen | termui.AttrBold
		alert.BorderLabel = "Recovered"
		alert.Height = 3
		return alert
	} else {
		return empty()
	}
}

func currentTime() *termui.Paragraph {
	currentTime := termui.NewParagraph(fmt.Sprintf("Current time: %s",
		time.Now().Format("15:04:05")))
	currentTime.TextFgColor = termui.ColorBlack
	currentTime.Border = false
	currentTime.Height = 2
	return currentTime
}

func Render(state *UIState) {
	termui.ClearArea(termui.TermRect(), termui.ColorDefault)
	header := header(state)
	currentTime := currentTime()

	sectionHeader := sectionHeader()
	sectionGraph := sectionGraph(state)

	statusHeader := statusHeader()
	statusGraph := statusGraph(state)

	trafficChart := trafficGraph(state)

	alert := alert(state)

	grid := termui.NewGrid()
	grid.Width = termui.TermWidth()
	grid.AddRows(
		termui.NewRow(
			termui.NewCol(9, 0, header),
			termui.NewCol(3, 0, currentTime)),
		termui.NewRow(termui.NewCol(12, 0, trafficChart)),
		termui.NewRow(
			termui.NewCol(6, 0, sectionHeader),
			termui.NewCol(6, 0, statusHeader)),
		termui.NewRow(
			termui.NewCol(6, 0, sectionGraph),
			termui.NewCol(6, 0, statusGraph)),
		termui.NewRow(termui.NewCol(12, 0, alert)))
	grid.Align()
	termui.Render(grid)
}

func NextUIState(state *UIState, ts *timeseries.LogTimeSeries, now time.Time) *UIState {
	end := getEnd(state.Begin, state.Timescale)
	if end.Before(now) {
		state.Begin = now
		end = state.Begin.Add(time.Duration(state.Timescale) * time.Minute)
	}

	sectionCounts, err := ts.GetSectionCounts(state.Begin, end)
	if err != nil {
		log.Fatal(err)
	}
	state.SectionCounts = sectionCounts

	statusCounts, err := ts.GetStatusCounts(state.Begin, end)
	if err != nil {
		log.Fatal(err)
	}
	state.StatusCounts = statusCounts

	logLines, err := ts.GetLogLines(state.Begin, end)
	if err != nil {
		log.Fatal(err)
	}
	timeBuckets := timebucketer.Bucket(state.Begin, end, state.Granularity, logLines)
	traffic := make([]int, state.Granularity)
	for i, bucket := range timeBuckets {
		traffic[i] = len(bucket)
	}
	state.Traffic = traffic

	avgTraffic, err := ts.GetAverageTraffic(now.Add(time.Duration(state.AlertInterval)*-time.Second), now)
	if err != nil {
		log.Fatal(err)
	}
	if state.Recovered {
		if state.RecoveredCountdown == 0 {
			state.Recovered = false
		} else {
			state.RecoveredCountdown = state.RecoveredCountdown - 1
		}
	}
	if avgTraffic > state.AlertThreshold {
		state.Alert = true
	} else if state.Alert {
		state.Alert = false
		state.Recovered = true
		state.RecoveredCountdown = recoveredCountdown
	}

	return state
}

func GetInitialUIState(ts *timeseries.LogTimeSeries, timescale int, granularity int, alertThreshold float64, alertInterval int) (state *UIState, err error) {
	begin := time.Now()
	end := getEnd(begin, timescale)
	sectionCounts, err := ts.GetSectionCounts(begin, end)
	if err != nil {
		return
	}
	statusCounts, err := ts.GetStatusCounts(begin, end)
	if err != nil {
		return
	}
	logLines, err := ts.GetLogLines(begin, end)
	if err != nil {
		return
	}
	timeBuckets := timebucketer.Bucket(begin, end, granularity, logLines)
	traffic := make([]int, granularity)
	for i, bucket := range timeBuckets {
		traffic[i] = len(bucket)
	}
	state = &UIState{
		Timescale:      timescale,
		Begin:          begin,
		SectionCounts:  sectionCounts,
		StatusCounts:   statusCounts,
		Traffic:        traffic,
		Granularity:    granularity,
		AlertThreshold: alertThreshold,
		AlertInterval:  alertInterval,
	}
	return
}
