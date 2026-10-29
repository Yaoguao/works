package services

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
	"medtech-practice/internal/storage"
)

var (
	ErrDocumentNotFound  = errors.New("document not found")
	ErrAccessDenied      = errors.New("access denied")
	ErrInvalidRole       = errors.New("invalid role")
	ErrInvalidVisibility = errors.New("invalid visibility")
)

const (
	DocumentStatusUploaded = "uploaded"
	DocumentStatusDeleted  = "deleted"

	VisibilityPatient = "patient"
	VisibilityStaff   = "staff"
	VisibilityClinic  = "clinic"

	RolePatient   = "patient"
	RoleDoctor    = "doctor"
	RoleRegistrar = "registrar"
)

type DocumentsConfig struct {
	BucketConsents    string
	BucketAttachments string
	PresignTTL        time.Duration
}

// DocumentsService — документы пациентов: бинарники в MinIO, метаданные в Postgres.
// Postgres — источник истины по ACL и жизненному циклу, MinIO хранит только объекты.
type DocumentsService struct {
	pg      *pgx_driver.Postgres
	storage *storage.DocumentStorage
	minio   *minio.Client
	cfg     DocumentsConfig
	log     *slog.Logger
}

func NewDocumentsService(
	pg *pgx_driver.Postgres,
	st *storage.DocumentStorage,
	minioClient *minio.Client,
	cfg DocumentsConfig,
	log *slog.Logger,
) *DocumentsService {
	return &DocumentsService{
		pg:      pg,
		storage: st,
		minio:   minioClient,
		cfg:     cfg,
		log:     log,
	}
}

type UploadDocumentParams struct {
	ClinicID      int64
	PatientID     int64
	AppointmentID *int64
	Visibility    string
	UploadedBy    int64
	Filename      string
	Content       []byte
}

// Upload считает sha256, кладёт объект в MinIO и пишет метаданные в documents.
// Повторная загрузка того же файла (sha256+patient) возвращает существующую запись.
// created=false означает идемпотентный повтор.
func (s *DocumentsService) Upload(ctx context.Context, p UploadDocumentParams) (*models.Document, bool, error) {
	const op = "services.documents.Upload"

	switch p.Visibility {
	case VisibilityPatient, VisibilityStaff, VisibilityClinic:
	default:
		return nil, false, ErrInvalidVisibility
	}

	sum := sha256.Sum256(p.Content)
	sha := hex.EncodeToString(sum[:])

	if doc, err := s.storage.FindActiveByPatientAndSHA(ctx, s.pg, p.PatientID, sha); err == nil {
		return doc, false, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, false, fmt.Errorf("%s: idempotency lookup: %w", op, err)
	}

	bucket := s.pickBucket(p.Filename)
	objectKey := fmt.Sprintf(
		"clinic/%d/patient/%d/%s_%s",
		p.ClinicID, p.PatientID, uuid.NewString(), filepath.Base(p.Filename),
	)
	contentType := http.DetectContentType(p.Content)

	_, err := s.minio.PutObject(ctx, bucket, objectKey,
		bytes.NewReader(p.Content), int64(len(p.Content)),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return nil, false, fmt.Errorf("%s: put object: %w", op, err)
	}

	doc := &models.Document{
		ClinicID:      p.ClinicID,
		PatientID:     &p.PatientID,
		AppointmentID: p.AppointmentID,
		Bucket:        bucket,
		ObjectKey:     objectKey,
		MimeType:      contentType,
		SizeBytes:     int64(len(p.Content)),
		SHA256:        sha,
		Visibility:    p.Visibility,
		Status:        DocumentStatusUploaded,
		UploadedBy:    &p.UploadedBy,
	}

	id, err := s.storage.InsertDocument(ctx, s.pg, doc)
	if err != nil {
		s.removeObject(ctx, op, bucket, objectKey)
		return nil, false, err
	}
	if id == 0 {
		s.removeObject(ctx, op, bucket, objectKey)
		existing, gerr := s.storage.FindActiveByPatientAndSHA(ctx, s.pg, p.PatientID, sha)
		if gerr != nil {
			return nil, false, fmt.Errorf("%s: idempotency relookup: %w", op, gerr)
		}
		return existing, false, nil
	}

	doc.ID = id
	return doc, true, nil
}

// DownloadURL проверяет упрощённый ACL и отдаёт pre-signed GET URL (TTL из конфига):
//   - patient: только свои документы с visibility in ('patient','clinic');
//   - doctor/registrar: документы своей клиники с visibility in ('staff','clinic').
func (s *DocumentsService) DownloadURL(
	ctx context.Context,
	docID int64,
	role string,
	userPatientID int64,
	clinicID int64,
) (string, time.Duration, error) {
	const op = "services.documents.DownloadURL"

	doc, err := s.storage.GetDocumentByID(ctx, s.pg, docID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", 0, ErrDocumentNotFound
		}
		return "", 0, fmt.Errorf("%s: get document: %w", op, err)
	}
	if doc.Status == DocumentStatusDeleted {
		return "", 0, ErrDocumentNotFound
	}

	switch role {
	case RolePatient:
		if userPatientID <= 0 || doc.PatientID == nil || *doc.PatientID != userPatientID ||
			(doc.Visibility != VisibilityPatient && doc.Visibility != VisibilityClinic) {
			s.log.WarnContext(ctx, "document access denied",
				slog.String("op", op), slog.Int64("document_id", docID),
				slog.String("role", role), slog.Int64("user_patient_id", userPatientID))
			return "", 0, ErrAccessDenied
		}
	case RoleDoctor, RoleRegistrar:
		if clinicID <= 0 || doc.ClinicID != clinicID ||
			(doc.Visibility != VisibilityStaff && doc.Visibility != VisibilityClinic) {
			s.log.WarnContext(ctx, "document access denied",
				slog.String("op", op), slog.Int64("document_id", docID),
				slog.String("role", role), slog.Int64("clinic_id", clinicID))
			return "", 0, ErrAccessDenied
		}
	default:
		return "", 0, ErrInvalidRole
	}

	u, err := s.minio.PresignedGetObject(ctx, doc.Bucket, doc.ObjectKey, s.cfg.PresignTTL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("%s: presign: %w", op, err)
	}

	return u.String(), s.cfg.PresignTTL, nil
}

// Delete выполняет soft-delete: status='deleted' в метаданных.
// Объект в MinIO оставляем (восстановление/аудит), доступ закрывает статус в БД.
// Повторный DELETE того же документа — идемпотентный 200.
func (s *DocumentsService) Delete(ctx context.Context, docID int64) (*models.Document, error) {
	const op = "services.documents.Delete"

	doc, err := s.storage.GetDocumentByID(ctx, s.pg, docID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrDocumentNotFound
		}
		return nil, fmt.Errorf("%s: get document: %w", op, err)
	}
	if doc.Status == DocumentStatusDeleted {
		return doc, nil
	}

	if _, err := s.storage.MarkDeleted(ctx, s.pg, docID); err != nil {
		return nil, fmt.Errorf("%s: mark deleted: %w", op, err)
	}
	doc.Status = DocumentStatusDeleted

	return doc, nil
}

// pickBucket: согласия — в consents, остальное — в attachments.
func (s *DocumentsService) pickBucket(filename string) string {
	if strings.HasPrefix(strings.ToLower(filepath.Base(filename)), "consent") {
		return s.cfg.BucketConsents
	}
	return s.cfg.BucketAttachments
}

func (s *DocumentsService) removeObject(ctx context.Context, op, bucket, objectKey string) {
	if err := s.minio.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{}); err != nil {
		s.log.WarnContext(ctx, "failed to remove orphan object",
			slog.String("op", op), slog.String("bucket", bucket),
			slog.String("object_key", objectKey), slog.Any("error", err))
	}
}
