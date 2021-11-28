package main

import (
	"flag"
	"log"

	"github.com/ChaosJiang/mode-assignment-general-v2-answer/timeSeries"
)

func main() {
    begin := flag.String("begin", "2021-05-12T19:00:00Z", "Begin hour timestamp")
    end := flag.String("end", "2021-05-13T10:00:00Z", "End hour timestamp")
    flag.Parse()

    err := timeSeries.GetHourlyAverage(*begin, *end)
    if err != nil {
        log.Fatal(err)
    }
}