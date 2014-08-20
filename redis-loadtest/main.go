package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

func main() {
	var threadCt, numberOfRequests, reqsSec int
	var host, filename string

	flag.IntVar(&threadCt, "t", 1, "Number of threads")
	flag.IntVar(&reqsSec, "r", 200, "Requests a second")
	flag.StringVar(&host, "h", ":6379", "Redis host")
	flag.StringVar(&filename, "f", "", "Source Data Filename (required)")
	flag.Parse()
	redisCmd := flag.Arg(0)
	redisKey := flag.Arg(1)

	if len(redisCmd) == 0 || len(redisKey) == 0 {
		log.Fatalln("[COMMAND] [KEY] are required arguments")
		return
	}
	if len(filename) == 0 {
		log.Fatal(filename, "-f [filename] is required.")
		return
	}

	var wg sync.WaitGroup
	wg.Add(threadCt)

	log.Println("Num Threads: ", threadCt)

	for i := 0; i < threadCt; i++ {
		go func() {
			throttle := time.Tick(time.Second / time.Duration(reqsSec))
			conn, err := redis.Dial("tcp", host)

			if err != nil {
				log.Fatal(err)
			}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			for {
				file.Seek(0, 0)
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					<-throttle
					_, err := conn.Do(redisCmd, redisKey, scanner.Text())

					if err != nil {
						log.Fatal(err)
					} else {
						numberOfRequests += 1
					}
				}

				if err := scanner.Err(); err != nil {
					log.Fatal(err)
				}
			}

			wg.Done()
		}()
	}

	printTick := time.Tick(time.Second)
	go func() {
		for {
			<-printTick
			log.Println("Req/s", numberOfRequests)
			numberOfRequests = 0
		}
	}()

	wg.Wait()
}
