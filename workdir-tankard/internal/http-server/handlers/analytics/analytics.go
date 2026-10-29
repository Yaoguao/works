package analytics

import (
	"net/http"
	"time"

	"tankard_workdir/internal/service"

	"github.com/gin-gonic/gin"
)

const dateLayout = "2006-01-02"

// NewAB возвращает обработчик GET /v1/analytics/ab?from=&to=&surface=. Он показывает показы, клики и конверсии для экспериментальных групп, а также CTR и CR.
func NewAB(svc *service.AnalyticsService) gin.HandlerFunc {
	return func(c *gin.Context) {
		from, to, ok := parseRange(c)
		if !ok {
			return
		}

		surface := c.Query("surface")

		rows, err := svc.Report(c.Request.Context(), from, to, surface)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build ab report: " + err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"from":    from.Format(dateLayout),
			"to":      to.Format(dateLayout),
			"surface": surface,
			"rows":    rows,
		})
	}
}

// NewRebuild возвращает обработчик POST /v1/analytics/ab/rebuild. Он пересчитывает ab_daily по исходным событиям.
func NewRebuild(svc *service.AnalyticsService) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := svc.BuildABDaily(c.Request.Context()); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rebuild ab_daily: " + err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "rebuilt"})
	}
}

// NewReconcile возвращает обработчик GET /v1/analytics/reconcile?day=. Он сравнивает recommendation_logs в Postgres с показами в ClickHouse.
func NewReconcile(svc *service.AnalyticsService) gin.HandlerFunc {
	return func(c *gin.Context) {
		day, err := parseDay(c.Query("day"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "day must be YYYY-MM-DD"})
			return
		}

		result, err := svc.Reconcile(c.Request.Context(), day)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to reconcile: " + err.Error()})
			return
		}

		c.JSON(http.StatusOK, result)
	}
}

// parseRange считывает from и to (по умолчанию — текущий день) и проверяет порядок дат.
func parseRange(c *gin.Context) (time.Time, time.Time, bool) {
	now := time.Now()

	var (
		from = now
		to   = now
		err  error
	)

	if raw := c.Query("from"); raw != "" {
		if from, err = parseDay(raw); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "from must be YYYY-MM-DD"})
			return time.Time{}, time.Time{}, false
		}
	}
	if raw := c.Query("to"); raw != "" {
		if to, err = parseDay(raw); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "to must be YYYY-MM-DD"})
			return time.Time{}, time.Time{}, false
		}
	}
	if to.Before(from) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "to must not be before from"})
		return time.Time{}, time.Time{}, false
	}

	return from, to, true
}

func parseDay(value string) (time.Time, error) {
	return time.Parse(dateLayout, value)
}
