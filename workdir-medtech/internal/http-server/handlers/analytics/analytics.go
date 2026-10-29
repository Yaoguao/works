package analytics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/services"
)

const dateLayout = "2006-01-02"

// Reporter — интерфейс сервисного слоя аналитики.
type Reporter interface {
	NoShowReport(ctx context.Context, clinicID int64, from, to time.Time) (*services.NoShowReport, error)
	Reconcile(ctx context.Context, day time.Time) (*services.ReconcileResult, error)
}

// NoShow — GET /v1/analytics/no-show?clinic_id=&from=YYYY-MM-DD&to=YYYY-MM-DD.
// Данные берутся из ClickHouse.
func NoShow(r Reporter) gin.HandlerFunc {
	return func(c *gin.Context) {
		clinicID, err := strconv.ParseInt(c.Query("clinic_id"), 10, 64)
		if err != nil || clinicID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "clinic_id is required"})
			return
		}
		from, err := time.Parse(dateLayout, c.Query("from"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "from must be YYYY-MM-DD"})
			return
		}
		to, err := time.Parse(dateLayout, c.Query("to"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to must be YYYY-MM-DD"})
			return
		}
		if to.Before(from) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to must be >= from"})
			return
		}

		report, err := r.NoShowReport(c.Request.Context(), clinicID, from, to)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.JSON(http.StatusOK, report)
	}
}

// Reconcile — GET /v1/analytics/reconcile?day=YYYY-MM-DD.
// Сверяет count Postgres и ClickHouse за день и показывает расхождение.
func Reconcile(r Reporter) gin.HandlerFunc {
	return func(c *gin.Context) {
		day, err := time.Parse(dateLayout, c.Query("day"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "day must be YYYY-MM-DD"})
			return
		}

		res, err := r.Reconcile(c.Request.Context(), day)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.JSON(http.StatusOK, res)
	}
}
