package main

import (
	"encoding/json"
	"fmt"
	"github.com/microlib/simple"
	"io/ioutil"
	"os"
)

var (
	logger simple.Logger
)

func main() {
	var pd []PositionDetail
	var p Position
	var pubd PublicationDetail
	var pub Publication

	logger.Level = "debug"
	file, _ := ioutil.ReadFile("publication.json")
	// update our schema
	err := json.Unmarshal(file, &pubd)
	if err != nil {
		logger.Error(fmt.Sprintf("Converting json %v", err))
	}

	pub = Publication{UID: 1, Name: "TEST", Type: "publication", Data: pubd}
	logger.Debug(fmt.Sprintf("Publication %v", pub))

	file, _ = ioutil.ReadFile("position.json")
	// update our schema
	err = json.Unmarshal(file, &pd)
	if err != nil {
		logger.Error(fmt.Sprintf("Converting json %v", err))
	}

	p = Position{UID: 1, Name: "TEST", Type: "position", Data: pd}
	logger.Info(fmt.Sprintf("Count %d", len(pd)))
	logger.Debug(fmt.Sprintf("PD[0] %v", p.Data[0]))

	os.Exit(0)
}
