package main

import (
	"flag"
	"log"

	"github.com/ChaosJiang/mode-assignment-general-v2-answer/timeSeries"
)

func main() {
    begin := flag.String("begin", "", "Begin hour timestamp")
    end := flag.String("end", "", "End hour timestamp")
    flag.Parse()

    err := timeSeries.GetHourlyAverage(*begin, *end)
    if err != nil {
        log.Fatal(err)
    }
}