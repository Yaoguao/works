package analytics

import "time"

type FactAppointment struct {
	EventID       string    `ch:"event_id"`
	EventTime     time.Time `ch:"event_time"`
	ClinicID      uint64    `ch:"clinic_id"`
	ProviderID    uint64    `ch:"provider_id"`
	PatientID     uint64    `ch:"patient_id"`
	Specialty     string    `ch:"specialty"`
	Status        string    `ch:"status"`
	NoShowScore   float32   `ch:"no_show_score"`
	LeadTimeHours float32   `ch:"lead_time_hours"`
}

type DailyProviderStat struct {
	Day        time.Time `ch:"day"`
	ClinicID   uint64    `ch:"clinic_id"`
	ProviderID uint64    `ch:"provider_id"`
	Specialty  string    `ch:"specialty"`
	TotalAppts uint64    `ch:"total_appts"`
	Completed  uint64    `ch:"completed"`
	Cancelled  uint64    `ch:"cancelled"`
	NoShow     uint64    `ch:"no_show"`
}
