package main

import (
	"github.com/reterVision/gogossip/client"
)

func main() {
	c1 := client.NewClient("server3.json")
	c1.Start(nil)
}
