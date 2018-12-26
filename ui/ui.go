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
	SectionCounts []timeseries.SectionCount
	StatusCounts  []timeseries.SectionCount
	Traffic       []TrafficAtTime
	Begin         time.Time
	End           time.Time
}

func header(state UIState) (header *termui.Paragraph) {
	header = termui.NewParagraph(fmt.Sprintf("Traffic Statistics from %s to %s",
		state.Begin.Format("15:04:05"), state.End.Format("15:04:05")))
	header.Height = 3
	header.TextFgColor = termui.ColorBlack
	header.Border = false
	return
}

func sectionGraph(state UIState) *termui.Row {
	numSectionCounts := len(state.SectionCounts)

	var totalCount float64 = 0
	for i := 0; i < numSectionCounts; i++ {
		totalCount = totalCount + float64(state.SectionCounts[i].Count)
	}

	labels := make([]string, numSectionCounts)
	gauges := make([]termui.GridBufferer, numSectionCounts)
	for i := 0; i < numSectionCounts; i++ {
		sectionCount := state.SectionCounts[i]
		percentage := float64(sectionCount.Count) / totalCount * 100.0
		labels = append(labels, sectionCount.Section)
		gauge := &termui.Gauge{
			Percent: int(percentage),
			Label: fmt.Sprintf("%d (%d%%)", sectionCount.Count, percentage),
		}
		gauge.Height = 2
		gauges = append(gauges, gauge)
	}

	labelList := termui.NewList()
	labelList.Items = labels
	labelList.Height = 2 * len(labels)

	return termui.NewRow(
		termui.NewCol(3, 0, labelList),
		termui.NewCol(3, 0, gauges...))
}

func Render(state UIState) {
	header := header(state)
	sectionGraph := sectionGraph(state)

	grid := termui.NewGrid()
	grid.Width = termui.TermWidth()
	grid.AddRows(
		termui.NewRow(termui.NewCol(12, 0, header)),
		sectionGraph)
	grid.Align()
	termui.Render(grid)
}
