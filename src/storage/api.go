package storage

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Api struct {
	port       int
	promotions *PromotionsApi
}

func NewApi(cfg *Configuration, svc *PromotionsService) *Api {
	promotions := &PromotionsApi{svc}

	return &Api{cfg.Api.Port, promotions}
}

func (api *Api) Listen() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/promotions/:id", api.promotions.Get)

	router.Run(fmt.Sprintf("localhost:%d", api.port))
}

type PromotionsApi struct {
	svc *PromotionsService
}

type PromotionDto struct {
	Id             string  `json:"id"`
	Price          float64 `json:"price"`
	ExpirationDate string  `json:"expiration_date"`
}

func (api *PromotionsApi) Get(c *gin.Context) {
	paramId := c.Param("id")
	id, err := uuid.Parse(paramId)
	if err != nil {
		// Could not parse parameter as uuid, trying to parse as int
		index, err := strconv.Atoi(paramId)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("Failed to parse specified id: %s", paramId)})
			return
		}

		promotion, err := api.svc.GetByIndex(index)
		api.writeResponse(c, promotion, err)

		return
	}

	promotion, err := api.svc.GetById(id)
	api.writeResponse(c, promotion, err)
}

func (api *PromotionsApi) writeResponse(c *gin.Context, p *Promotion, err error) {
	paramId := c.Param("id")

	if err != nil {
		if errors.Is(err, &NotFound{}) {
			c.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("Promotion with id '%s' not found", paramId)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Failed to get promotion by index: %s. Error: %v", paramId, err)})
		}
		return
	}

	dto := &PromotionDto{
		Id:             strings.ToUpper(p.Id.String()),
		Price:          p.Price,
		ExpirationDate: p.ExpirationDate.In(time.Local).Format("2006-01-02 15:04:05"),
	}
	c.JSON(http.StatusOK, dto)
}
