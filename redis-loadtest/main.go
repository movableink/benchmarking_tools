package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"sync"

	"github.com/garyburd/redigo/redis"
)

func main() {
	var threadCt int
	var host, filename string

	flag.IntVar(&threadCt, "t", 1, "Number of threads")
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
			conn, err := redis.Dial("tcp", host)

			if err != nil {
				log.Fatal(err)
			}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				result, err := conn.Do(redisCmd, redisKey, scanner.Text())

				if err != nil {
					log.Fatal(err)
				} else {
					log.Println(result)
				}
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}

			wg.Done()
		}()
	}

	wg.Wait()
}
