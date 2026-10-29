package document

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/models"
	"medtech-practice/internal/services"
)

const maxUploadSize = 32 << 20 // 32 MB

type documentResponse struct {
	ID            int64     `json:"id"`
	ClinicID      int64     `json:"clinic_id"`
	PatientID     *int64    `json:"patient_id"`
	AppointmentID *int64    `json:"appointment_id,omitempty"`
	Bucket        string    `json:"bucket"`
	ObjectKey     string    `json:"object_key"`
	MimeType      string    `json:"mime_type"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	Visibility    string    `json:"visibility"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

func toResponse(d *models.Document) documentResponse {
	return documentResponse{
		ID:            d.ID,
		ClinicID:      d.ClinicID,
		PatientID:     d.PatientID,
		AppointmentID: d.AppointmentID,
		Bucket:        d.Bucket,
		ObjectKey:     d.ObjectKey,
		MimeType:      d.MimeType,
		SizeBytes:     d.SizeBytes,
		SHA256:        d.SHA256,
		Visibility:    d.Visibility,
		Status:        d.Status,
		CreatedAt:     d.CreatedAt,
	}
}

// Uploader — интерфейс сервисного слоя для загрузки документов.
type Uploader interface {
	Upload(ctx context.Context, p services.UploadDocumentParams) (*models.Document, bool, error)
}

// NewUpload возвращает gin-хендлер POST /v1/documents/upload (multipart):
// file + clinic_id, patient_id, appointment_id?, visibility, uploaded_by.
// 201 — метаданные созданы; 200 — идемпотентный повтор (тот же sha256+patient).
func NewUpload(uploader Uploader, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.document.NewUpload"

		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxUploadSize)

		fileHeader, err := c.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file is required: " + err.Error()})
			return
		}

		clinicID, err := strconv.ParseInt(c.PostForm("clinic_id"), 10, 64)
		if err != nil || clinicID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "clinic_id is required"})
			return
		}
		patientID, err := strconv.ParseInt(c.PostForm("patient_id"), 10, 64)
		if err != nil || patientID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "patient_id is required"})
			return
		}
		uploadedBy, err := strconv.ParseInt(c.PostForm("uploaded_by"), 10, 64)
		if err != nil || uploadedBy <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "uploaded_by is required"})
			return
		}
		visibility := c.PostForm("visibility")
		if visibility == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "visibility is required"})
			return
		}

		var appointmentID *int64
		if raw := c.PostForm("appointment_id"); raw != "" {
			id, perr := strconv.ParseInt(raw, 10, 64)
			if perr != nil || id <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid appointment_id"})
				return
			}
			appointmentID = &id
		}

		f, err := fileHeader.Open()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "cannot open file: " + err.Error()})
			return
		}
		defer f.Close()

		content, err := io.ReadAll(f)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read file: " + err.Error()})
			return
		}

		doc, created, err := uploader.Upload(c.Request.Context(), services.UploadDocumentParams{
			ClinicID:      clinicID,
			PatientID:     patientID,
			AppointmentID: appointmentID,
			Visibility:    visibility,
			UploadedBy:    uploadedBy,
			Filename:      fileHeader.Filename,
			Content:       content,
		})
		switch {
		case err == nil && created:
			c.JSON(http.StatusCreated, toResponse(doc))
		case err == nil:
			c.Header("X-Idempotent-Replay", "true")
			c.JSON(http.StatusOK, toResponse(doc))
		case errors.Is(err, services.ErrInvalidVisibility):
			c.JSON(http.StatusBadRequest, gin.H{"error": "visibility must be one of: patient, staff, clinic"})
		default:
			log.ErrorContext(c.Request.Context(), "upload failed",
				slog.String("op", op), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

// URLGetter — интерфейс сервисного слоя для выдачи pre-signed URL.
type URLGetter interface {
	DownloadURL(ctx context.Context, docID int64, role string, userPatientID, clinicID int64) (string, time.Duration, error)
}

// NewDownloadURL возвращает gin-хендлер GET /v1/documents/:id/download-url.
// Права упрощённо через query: role=patient|doctor|registrar + user_patient_id/clinic_id.
// 200 — {url, expires_in_seconds}; 403 — ACL не пропустил; 404 — нет/удалён.
func NewDownloadURL(getter URLGetter) gin.HandlerFunc {
	return func(c *gin.Context) {
		docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid document id"})
			return
		}

		role := c.Query("role")
		userPatientID, _ := strconv.ParseInt(c.Query("user_patient_id"), 10, 64)
		clinicID, _ := strconv.ParseInt(c.Query("clinic_id"), 10, 64)

		url, ttl, err := getter.DownloadURL(c.Request.Context(), docID, role, userPatientID, clinicID)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, gin.H{
				"url":                url,
				"expires_in_seconds": int(ttl.Seconds()),
			})
		case errors.Is(err, services.ErrInvalidRole):
			c.JSON(http.StatusBadRequest, gin.H{"error": "role must be one of: patient, doctor, registrar"})
		case errors.Is(err, services.ErrAccessDenied):
			c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
		case errors.Is(err, services.ErrDocumentNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "document not found"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

// Deleter — интерфейс сервисного слоя для soft-delete.
type Deleter interface {
	Delete(ctx context.Context, docID int64) (*models.Document, error)
}

// NewDelete возвращает gin-хендлер DELETE /v1/documents/:id (soft-delete).
func NewDelete(deleter Deleter) gin.HandlerFunc {
	return func(c *gin.Context) {
		docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid document id"})
			return
		}

		doc, err := deleter.Delete(c.Request.Context(), docID)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, toResponse(doc))
		case errors.Is(err, services.ErrDocumentNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "document not found"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}
