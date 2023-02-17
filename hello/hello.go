package main

import "fmt"

func main() {
	fmt.Println("main start")
	ch := make(chan string, 1)
	ch <- "a" // 入 chan
	go func() {
		val := <-ch // 出 chan
		fmt.Println(val)
	}()
	fmt.Println("main end")
}
