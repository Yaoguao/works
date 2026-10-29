CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_patient_sha_active
    ON documents(patient_id, sha256)
    WHERE status = 'uploaded';
