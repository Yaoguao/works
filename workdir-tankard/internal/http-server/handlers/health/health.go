package health

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// New возвращает обработчик GET /health.
func New() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	}
}
