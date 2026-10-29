package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
)

const documentColumns = `id, clinic_id, patient_id, appointment_id, bucket, object_key,
	mime_type, size_bytes, sha256, visibility, status, uploaded_by, created_at`

func scanDocument(row pgx.Row) (*models.Document, error) {
	var d models.Document
	err := row.Scan(
		&d.ID, &d.ClinicID, &d.PatientID, &d.AppointmentID, &d.Bucket, &d.ObjectKey,
		&d.MimeType, &d.SizeBytes, &d.SHA256, &d.Visibility, &d.Status, &d.UploadedBy, &d.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

type DocumentStorage struct {
	db *pgx_driver.Postgres
}

func NewDocumentStorage(db *pgx_driver.Postgres) *DocumentStorage {
	return &DocumentStorage{db: db}
}

// GetDocumentByID возвращает документ по id или ErrNotFound.
func (s *DocumentStorage) GetDocumentByID(ctx context.Context, qe pgx_driver.QueryExecuter, id int64) (*models.Document, error) {
	const op = "storage.document.GetDocumentByID"

	sql, args, err := s.db.Select(documentColumns).
		From("documents").
		Where(squirrel.Eq{"id": id}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	doc, err := scanDocument(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return doc, nil
}

// FindActiveByPatientAndSHA ищет активный (не удалённый) документ пациента
// с тем же sha256 — основа идемпотентности повторной загрузки.
func (s *DocumentStorage) FindActiveByPatientAndSHA(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	patientID int64,
	sha256 string,
) (*models.Document, error) {
	const op = "storage.document.FindActiveByPatientAndSHA"

	sql, args, err := s.db.Select(documentColumns).
		From("documents").
		Where(squirrel.Eq{"patient_id": patientID, "sha256": sha256, "status": "uploaded"}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	doc, err := scanDocument(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return doc, nil
}

// InsertDocument вставляет метаданные. Идемпотентность держит частичный уникальный
// индекс uq_documents_patient_sha_active (status='uploaded'): при гонке конкурент
// получает 0 rows из RETURNING и обязан перечитать существующую запись.
func (s *DocumentStorage) InsertDocument(ctx context.Context, qe pgx_driver.QueryExecuter, d *models.Document) (int64, error) {
	const op = "storage.document.InsertDocument"

	sql, args, err := s.db.Insert("documents").
		Columns(
			"clinic_id", "patient_id", "appointment_id", "bucket", "object_key",
			"mime_type", "size_bytes", "sha256", "visibility", "uploaded_by",
		).
		Values(
			d.ClinicID, d.PatientID, d.AppointmentID, d.Bucket, d.ObjectKey,
			d.MimeType, d.SizeBytes, d.SHA256, d.Visibility, d.UploadedBy,
		).
		Suffix("ON CONFLICT (patient_id, sha256) WHERE status = 'uploaded' DO NOTHING RETURNING id").
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("%s: build query: %w", op, err)
	}

	var id int64
	err = qe.QueryRow(ctx, sql, args...).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Конфликт по индексу: запись уже создана конкурентной загрузкой.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return id, nil
}

// MarkDeleted выполняет soft-delete (status='deleted'), объект в MinIO не трогаем.
// Возвращает false, если документ не найден или уже удалён.
func (s *DocumentStorage) MarkDeleted(ctx context.Context, qe pgx_driver.QueryExecuter, id int64) (bool, error) {
	const op = "storage.document.MarkDeleted"

	sql, args, err := s.db.Update("documents").
		Set("status", "deleted").
		Where(squirrel.Eq{"id": id, "status": "uploaded"}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("%s: build query: %w", op, err)
	}

	tag, err := qe.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return tag.RowsAffected() > 0, nil
}
