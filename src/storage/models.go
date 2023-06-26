package storage

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type NotFound struct{}

func (e *NotFound) Error() string {
	return "Not Found"
}

type Promotion struct {
	Id             uuid.UUID
	Price          float64
	ExpirationDate time.Time
}

func NewPromotion(id uuid.UUID, price float64, expirationDate time.Time) *Promotion {
	return &Promotion{id, price, expirationDate}
}

func PromotionFromCsv(csv string) (*Promotion, error) {
	values := strings.Split(csv, ",")

	id, err := uuid.Parse(values[0])
	if err != nil {
		log.Printf("Failed to parse id: '%s'\n", values[0])
		return nil, err
	}

	price, err := strconv.ParseFloat(values[1], 64)
	if err != nil {
		log.Printf("Failed to parse price: '%s'\n", values[1])
		return nil, err
	}

	expirationDate, err := time.Parse("2006-01-02 15:04:05 -0700 MST", values[2])
	if err != nil {
		log.Printf("Failed to parse expiration date: '%s'\n", values[2])
		return nil, err
	}

	return NewPromotion(id, price, expirationDate), err
}
