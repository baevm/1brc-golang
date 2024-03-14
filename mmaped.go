package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

var NEW_LINE = byte('\n')

func main() {
	f, err := os.OpenFile("./measurements.txt", os.O_RDWR, 0644)

	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	mmap, _ := mmap.Map(f, mmap.RDONLY, 0)
	defer mmap.Unmap()

	var linesCount atomic.Uint64
	result := NewMap[string, Stats]()
	buf := make([]byte, 0)

	workerCount := runtime.NumCPU() * 2
	jobs := make(chan []byte, workerCount)

	// Start workers
	fmt.Printf("Starting with %d workers...\n", workerCount)
	for w := 0; w < workerCount; w++ {
		go mmap_worker(jobs, &result)
	}

	startTime := time.Now()

	for _, line := range mmap {

		if line == NEW_LINE {
			jobs <- buf
			buf = make([]byte, 0)
		} else {
			buf = append(buf, line)
		}

		linesCount.Add(1)
	}

	close(jobs)

	resultFile, err := os.Create("result.txt")

	if err != nil {
		log.Fatalln(err)
	}

	writer := bufio.NewWriterSize(resultFile, 64*1024)

	fmt.Println("Writting results to file...")

	sortedCities := make([]string, 0, result.Size())

	result.Range(func(key string, value Stats) bool {
		sortedCities = append(sortedCities, key)
		return true
	})

	slices.Sort(sortedCities)

	for _, city := range sortedCities {
		value, _ := result.Get(city)
		avg := value.sum / float64(value.count)
		writer.WriteString(fmt.Sprintf("%s: %.2f/%.2f/%.2f\n", city, value.min, avg, value.max))
	}

	err = writer.Flush()
	if err != nil {
		log.Fatalln(err)
	}

	endTime := time.Since(startTime)
	fmt.Printf("\nTime took: %s\n", endTime)
}

func mmap_worker(jobs <-chan []byte, result *Map[string, Stats]) {
	for line := range jobs {

		cityTemp := bytes.Split(line, []byte(";"))
		city := string(cityTemp[0])

		if len(cityTemp) < 2 {
			fmt.Println(string(line), cityTemp)
		}

		temp, _ := strconv.ParseFloat(string(cityTemp[1]), 64)

		if prevVal, isOk := result.Get(city); isOk {
			if prevVal.min > temp {
				prevVal.min = temp
			} else if prevVal.max < temp {
				prevVal.max = temp
			}

			prevVal.count += 1
			prevVal.sum += temp

			result.Set(city, prevVal)
		} else {
			result.Set(city, Stats{
				min:   temp,
				max:   temp,
				count: 1,
				sum:   temp,
			})
		}
	}
}
