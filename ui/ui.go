package ui

import (
	"fmt"
	"github.com/gizak/termui"
	"github.com/jdormit/logr/timeseries"
	"time"
)

type TrafficAtTime struct {
	Timestamp time.Time
	Traffic   int
}

type UIState struct {
	SectionCounts []timeseries.Count
	StatusCounts  []timeseries.Count
	Traffic       []TrafficAtTime
	Begin         time.Time
	End           time.Time
	Timescale     int
}

func noData() (noData *termui.Paragraph) {
	noData = termui.NewParagraph("No data")
	noData.Height = 3
	noData.TextFgColor = termui.ColorBlack
	noData.Border = false
	return
}

func header(state UIState) (header *termui.Paragraph) {
	header = termui.NewParagraph(fmt.Sprintf("Traffic Statistics from %s to %s",
		state.Begin.Format("15:04:05"), state.End.Format("15:04:05")))
	header.Height = 3
	header.TextFgColor = termui.ColorBlack
	header.Border = false
	return
}

func sectionGraph(state UIState) termui.GridBufferer {
	return gaugesWithLabels(state.SectionCounts, "/%s")
}

func statusGraph(state UIState) termui.GridBufferer {
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

func summaryStats(state UIState) (stats *termui.Paragraph) {
	statsStr := ""
	stats = termui.NewParagraph(statsStr)
	return
}

func Render(state UIState) {
	header := header(state)

	sectionHeader := sectionHeader()
	sectionGraph := sectionGraph(state)

	statusHeader := statusHeader()
	statusGraph := statusGraph(state)

	grid := termui.NewGrid()
	grid.Width = termui.TermWidth()
	grid.AddRows(
		termui.NewRow(termui.NewCol(12, 0, header)),
		termui.NewRow(
			termui.NewCol(6, 0, sectionHeader),
			termui.NewCol(6, 0, statusHeader)),
		termui.NewRow(
			termui.NewCol(6, 0, sectionGraph),
			termui.NewCol(6, 0, statusGraph)))
	grid.Align()
	termui.Render(grid)
}
