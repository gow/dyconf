package main

import (
	"flag"
	"log"
)

func main() {
	var updateMethod = flag.String(
		"update-method",
		"push",
		"Method used to update the config values")
	flag.Parse()

	log.Println(*updateMethod)
}
