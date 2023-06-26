package storage

import (
	"bufio"
	"log"
	"os"
)

type PromotionParser func(string) (*Promotion, error)

func Process(file *os.File, parse PromotionParser, processed chan *Promotion) {
	log.Println("Start processing file " + file.Name())

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		model, err := parse(line)
		if err != nil {
			log.Printf("Skipping current line...\n")
			continue
		}

		processed <- model
	}

	close(processed)
	log.Println("Processed file " + file.Name())
}
