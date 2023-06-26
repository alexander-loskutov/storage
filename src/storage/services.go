package storage

import (
	"github.com/google/uuid"
)

type PromotionsService struct {
	dao *PromotionsDao
}

func NewPromotionsService(dao *PromotionsDao) *PromotionsService {
	return &PromotionsService{dao}
}

func (svc *PromotionsService) GetById(id uuid.UUID) (*Promotion, error) {
	return svc.dao.GetById(id)
}

func (svc *PromotionsService) GetByIndex(index int) (*Promotion, error) {
	return svc.dao.GetByIndex(index)
}

func (svc *PromotionsService) Save(promotion *Promotion) {
	svc.dao.Upsert(promotion)
}

func (svc *PromotionsService) Rewrite(promotions chan *Promotion) {
	svc.dao.Rewrite(promotions)
}
