package admin

import (
	"errors"
	"net/http"
	"strconv"

	"tankard_workdir/internal/repository"
	"tankard_workdir/internal/service"

	"github.com/gin-gonic/gin"
)

type updateExperimentRequest struct {
	Weight  *int32 `json:"weight"`
	Enabled *bool  `json:"enabled"`
}

// NewPatchExperiment возвращает обработчик PATCH /v1/admin/experiments/:id. Он сохраняет изменения и обновляет снимок маршрутизатора, поэтому новые веса применяются со следующего запроса без перезапуска и изменения клиентского API.
func NewPatchExperiment(router *service.StrategyRouter) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "experiment id must be a positive integer"})
			return
		}

		var req updateExperimentRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
			return
		}
		if req.Weight == nil && req.Enabled == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "at least one of weight/enabled is required"})
			return
		}

		if err := router.Update(c.Request.Context(), id, req.Weight, req.Enabled); err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update experiment: " + err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":      "updated",
			"experiments": router.Arms(),
		})
	}
}
