package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan string)

	for i := 0; i < 10; i++ {
		go func() {
			ch <- "1"
			fmt.Printf("input\n")
		}()
	}

	for {
		time.Sleep(time.Second * 1)
		val := <-ch
		fmt.Printf("output %s\n", val)
	}
}
