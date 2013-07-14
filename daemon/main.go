package main

import (
	"flag"
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	var updateMethod = flag.String(
		"update-method",
		"push",
		"Method used to update the config values")
	flag.Parse()

	log.Println(*updateMethod)
	daemon := new(dyconfDaemon)
	err := daemon.init("/tmp/qwerty1234")
	if err != nil {
		log.Fatal(err)
	}
}
