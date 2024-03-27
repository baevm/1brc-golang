package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/valyala/fastjson/fastfloat"
)

type Stats struct {
	min float64
	max float64

	count int64
	sum   float64
}

var separator = ";"

func worker(jobs <-chan []string, result *Map[string, *Stats]) {
	for chunk := range jobs {
		for _, line := range chunk {

			cityTemp := strings.Split(line, separator)
			city := cityTemp[0]

			temp := fastfloat.ParseBestEffort(cityTemp[1])

			prevVal, isOk := result.Get(city)

			if isOk {
				prevVal.min = min(prevVal.min, temp)
				prevVal.max = max(prevVal.max, temp)
				prevVal.count += 1
				prevVal.sum += temp

			} else {
				result.Set(city, &Stats{
					min:   temp,
					max:   temp,
					count: 1,
					sum:   temp,
				})
			}
		}

	}
}

// check first 5kk lines and get all cities
// then start parsing min maxing all
func main() {
	/* PROFILING */
	// profCpu, err := os.Create("./perf/cpu.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// pprof.StartCPUProfile(profCpu)
	// defer pprof.StopCPUProfile()
	// profMem, err := os.Create("./perf/mem.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// pprof.WriteHeapProfile(profMem)
	// profMem.Close()
	/* PROFILING */

	f, err := os.Open("./measurements.txt")

	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	// var linesCount atomic.Uint64
	result := NewMap[string, *Stats]()

	workerCount := 5
	jobs := make(chan []string, workerCount)

	// Start workers
	fmt.Printf("Starting with %d workers...\n", workerCount)
	for w := 0; w < workerCount; w++ {
		go worker(jobs, &result)
	}

	reader := bufio.NewScanner(bufio.NewReader(f))
	lineChunks := make([]string, 0, 10000)

	startTime := time.Now()

	// Process file
	for reader.Scan() {
		line := reader.Text()

		if len(lineChunks) < 10000 {
			lineChunks = append(lineChunks, line)
		} else {
			jobs <- lineChunks
			lineChunks = make([]string, 0, 10000)
		}

		// if err != nil {
		// 	if err == io.EOF {
		// 		break
		// 	} else {
		// 		log.Fatalln(err)
		// 	}
		// }

		// if linesCount.Load() >= 100_000_000 {
		// 	break
		// }

		// linesCount.Add(1)
	}

	close(jobs)

	resultFile, err := os.Create("result.txt")

	if err != nil {
		log.Fatalln(err)
	}

	writer := bufio.NewWriterSize(resultFile, 64*1024)

	fmt.Println("Writting results to file...")

	sortedCities := make([]string, 0, result.Size())

	result.Range(func(key string, value *Stats) bool {
		sortedCities = append(sortedCities, key)
		return false
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
