package timeSeries

import (
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "runtime"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

type seriesBucket struct {
    total float64
    count float64
}

// GetHourlyAverage - Get average value of metric in each hourly bucket
func GetHourlyAverage(beginStr, endStr string) error {
    //Check if begin and end time string are in RFC3339 format
    beginTime, err := time.Parse(time.RFC3339, beginStr)
    if err != nil {
        return  err
    }
    endTime, err := time.Parse(time.RFC3339, endStr)
    if err != nil {
        return  err
    }
    // Begin time cannot be greater than end time
    if beginTime.After(endTime) {
        return fmt.Errorf("begin time is later than end time")
    }

    // cpu cores number
    cpuNum := runtime.NumCPU()
    runtime.GOMAXPROCS(cpuNum)

    // make channel size corresponding to cpu core numbers
    ch := make(chan struct{}, cpuNum)

    var wg sync.WaitGroup
    var mutex sync.Mutex

    hourlyBucket := map[string]*seriesBucket{}
    // time duration between each batch
    batchDuration := time.Hour * 12

    for batchBegin, batchEnd := beginTime, beginTime.Add(batchDuration); batchBegin.Before(endTime);
     batchBegin,batchEnd = batchEnd.Add(time.Hour*1), batchEnd.Add(batchDuration) {
        // Avoid batchEnd time overflow
        if batchEnd.After(endTime) {
            batchEnd = endTime
        }
        ch <- struct{}{}
        wg.Add(1)

        go func (t1, t2 time.Time)  {
            defer func ()  {
                wg.Done()
                <-ch
            }()
            lines, err := fetchSeriesWithDuration(t1.Format(time.RFC3339), t2.Add(time.Minute*59 + time.Second*59).
                Format(time.RFC3339))
            if err != nil {
                log.Println(err)
                return
            }
            // parse response data, and set into bucket
            batchBucket, err := parseSeries(lines)
            if err != nil {
                log.Println(err)
                return
            }
            // write lock
            mutex.Lock()
            for k, v := range batchBucket {
                hourlyBucket[k] = v
            }
            mutex.Unlock()
        }(batchBegin, batchEnd)
    }
     wg.Wait()
     close(ch)
    //  print hourly
     printHourlyBucket(hourlyBucket)
    return nil
}

// printHourlyBucket- print hourly bucket in increasing order
func printHourlyBucket(hourlyBucket map[string]*seriesBucket) {
    keys := make([]string, 0, len(hourlyBucket))
     // calculate the aerate value
     for k, _ := range hourlyBucket {
         keys = append(keys, k)
     }
     sort.Strings(keys)
     // print in increasing order
     for _, k := range keys {
        fmt.Printf("%s:00:00Z %.4f\n", k, hourlyBucket[k].total / hourlyBucket[k].count)
     }
}

// ParseSeries- Parse the response data from endpoint, then append to hourly bucket
func parseSeries(str string) (map[string]*seriesBucket, error) {
    strList := strings.Split(str, "\n")

    bucket := map[string]*seriesBucket{}
    for _, v:= range strList {
        line := strings.Fields(v)
        // skip illegal record
        if len(line) < 2 {
            continue
        } 
        // else {
        //     fmt.Println("some thing is wrong2")
        //     log.Print(line)
        // }
        key := line[0][:13]
        metric, err := strconv.ParseFloat(line[1], 64)
        if err!= nil {
            continue
        }
        if _,ok := bucket[key]; ok{
            bucket[key].total += metric
            bucket[key].count++
        } else {
            bucket[key] = &seriesBucket{total: metric, count: 1}
        }
    }
    return  bucket, nil
}

// FetchSeriesWithDuration -Fetch series data from url,
// in the specific time range between begin and end
func fetchSeriesWithDuration(begin, end string) (string, error) {
    req, err := http.NewRequest("GET", "https://tsserv.tinkermode.dev/data", nil)
    if err != nil {
        return "", err
    }
    // Add query parameters
    q := req.URL.Query()
    q.Add("begin", begin)
    q.Add("end", end)
    req.URL.RawQuery = q.Encode()

    timeout := 60 * time.Second
    client := &http.Client{
        Timeout: timeout,
    }
    // log.Printf(" fetch from: %s, to: %s", begin, end)
    // Do fetch job
    resp, err := client.Do(req)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return "", err
    }
    //log.Print(string(body))
    return string(body), nil
}