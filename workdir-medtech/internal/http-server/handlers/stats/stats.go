package stats

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Counter — источник метрики конфликтов.
type Counter interface {
	ConflictCount() int64
}

// New возвращает gin-хендлер GET /v1/stats с метрикой conflict_count.
func New(counter Counter) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"conflict_count": counter.ConflictCount(),
		})
	}
}
