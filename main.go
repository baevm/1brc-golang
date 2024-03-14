package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Stats struct {
	min float64
	max float64

	count int64
	sum   float64
}

var separator = ";"

func worker(jobs <-chan string, result *Map[string, Stats]) {
	for line := range jobs {
		cityTemp := strings.Split(line, separator)
		city := string(cityTemp[0])

		temp, _ := strconv.ParseFloat(cityTemp[1], 64)

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

func main1() {
	/* PROFILING */
	profCpu, err := os.Create("./perf/cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(profCpu)
	defer pprof.StopCPUProfile()
	profMem, err := os.Create("./perf/mem.pprof")
	if err != nil {
		panic(err)
	}
	pprof.WriteHeapProfile(profMem)
	profMem.Close()
	/* PROFILING */

	f, err := os.Open("./measurements.txt")

	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	var linesCount atomic.Uint64
	result := NewMap[string, Stats]()

	workerCount := runtime.NumCPU() * 2
	jobs := make(chan string, workerCount)

	// Start workers
	fmt.Printf("Starting with %d workers...\n", workerCount)
	for w := 0; w < workerCount; w++ {
		go worker(jobs, &result)
	}

	reader := bufio.NewScanner(bufio.NewReader(f))

	startTime := time.Now()

	// Process file
	for reader.Scan() {
		line := reader.Text()

		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalln(err)
			}
		}

		jobs <- line

		if linesCount.Load() >= 100_000_000 {
			break
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
