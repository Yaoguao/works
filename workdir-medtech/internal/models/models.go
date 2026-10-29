package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Clinic — клиника.
type Clinic struct {
	ID        int64     `migrations:"id"`
	Name      string    `migrations:"name"`
	Timezone  string    `migrations:"timezone"`
	CreatedAt time.Time `migrations:"created_at"`
}

// User — пользователь системы.
type User struct {
	ID        int64     `migrations:"id"`
	ClinicID  *int64    `migrations:"clinic_id"`
	Email     string    `migrations:"email"`
	FullName  string    `migrations:"full_name"`
	Role      string    `migrations:"role"`
	CreatedAt time.Time `migrations:"created_at"`
}

// Patient — пациент клиники.
type Patient struct {
	ID        int64      `migrations:"id"`
	ClinicID  int64      `migrations:"clinic_id"`
	UserID    *int64     `migrations:"user_id"`
	FullName  string     `migrations:"full_name"`
	Phone     *string    `migrations:"phone"`
	BirthDate *time.Time `migrations:"birth_date"`
	CreatedAt time.Time  `migrations:"created_at"`
}

// Provider — врач.
type Provider struct {
	ID        int64     `migrations:"id"`
	ClinicID  int64     `migrations:"clinic_id"`
	UserID    *int64    `migrations:"user_id"`
	FullName  string    `migrations:"full_name"`
	Specialty string    `migrations:"specialty"`
	CreatedAt time.Time `migrations:"created_at"`
}

// Slot — временной слот врача.
type Slot struct {
	ID         int64      `migrations:"id"`
	ClinicID   int64      `migrations:"clinic_id"`
	ProviderID int64      `migrations:"provider_id"`
	StartsAt   time.Time  `migrations:"starts_at"`
	EndsAt     time.Time  `migrations:"ends_at"`
	Status     string     `migrations:"status"`
	HoldToken  *string    `migrations:"hold_token"`
	HoldUntil  *time.Time `migrations:"hold_until"`
	CreatedAt  time.Time  `migrations:"created_at"`
}

// Appointment — запись пациента.
type Appointment struct {
	ID             int64      `migrations:"id"`
	ClinicID       int64      `migrations:"clinic_id"`
	PatientID      int64      `migrations:"patient_id"`
	ProviderID     int64      `migrations:"provider_id"`
	SlotID         int64      `migrations:"slot_id"`
	Status         string     `migrations:"status"`
	StartsAt       time.Time  `migrations:"starts_at"`
	EndsAt         time.Time  `migrations:"ends_at"`
	NoShowScore    *float64   `migrations:"no_show_score"`
	ScoredAt       *time.Time `migrations:"scored_at"`
	ModelVersion   *string    `migrations:"model_version"`
	Version        int32      `migrations:"version"`
	IdempotencyKey *string    `migrations:"idempotency_key"`
	CreatedAt      time.Time  `migrations:"created_at"`
	UpdatedAt      time.Time  `migrations:"updated_at"`
}

// AppointmentStatusHistory — история переходов статусов записи.
type AppointmentStatusHistory struct {
	ID            int64     `migrations:"id"`
	AppointmentID int64     `migrations:"appointment_id"`
	FromStatus    *string   `migrations:"from_status"`
	ToStatus      string    `migrations:"to_status"`
	ChangedBy     *int64    `migrations:"changed_by"`
	Reason        *string   `migrations:"reason"`
	CreatedAt     time.Time `migrations:"created_at"`
}

// Document — метаданные документа в MinIO.
type Document struct {
	ID            int64     `migrations:"id"`
	ClinicID      int64     `migrations:"clinic_id"`
	PatientID     *int64    `migrations:"patient_id"`
	AppointmentID *int64    `migrations:"appointment_id"`
	Bucket        string    `migrations:"bucket"`
	ObjectKey     string    `migrations:"object_key"`
	MimeType      string    `migrations:"mime_type"`
	SizeBytes     int64     `migrations:"size_bytes"`
	SHA256        string    `migrations:"sha256"`
	Visibility    string    `migrations:"visibility"`
	Status        string    `migrations:"status"`
	UploadedBy    *int64    `migrations:"uploaded_by"`
	CreatedAt     time.Time `migrations:"created_at"`
}

// RescheduleProposal — предложение переноса записи.
type RescheduleProposal struct {
	ID             int64      `migrations:"id"`
	AppointmentID  int64      `migrations:"appointment_id"`
	OldSlotID      int64      `migrations:"old_slot_id"`
	ProposedSlotID int64      `migrations:"proposed_slot_id"`
	Status         string     `migrations:"status"`
	ProposedBy     *int64     `migrations:"proposed_by"`
	ExpiresAt      time.Time  `migrations:"expires_at"`
	CreatedAt      time.Time  `migrations:"created_at"`
	DecidedAt      *time.Time `migrations:"decided_at"`
}

// OutboxEvent — событие для последующей публикации в RabbitMQ.
type OutboxEvent struct {
	ID            int64           `migrations:"id"`
	EventID       uuid.UUID       `migrations:"event_id"`
	EventType     string          `migrations:"event_type"`
	AggregateType string          `migrations:"aggregate_type"`
	AggregateID   int64           `migrations:"aggregate_id"`
	Payload       json.RawMessage `migrations:"payload"`
	Status        string          `migrations:"status"`
	CreatedAt     time.Time       `migrations:"created_at"`
	SentAt        *time.Time      `migrations:"sent_at"`
}
