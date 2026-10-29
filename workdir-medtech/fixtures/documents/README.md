# Фикстуры документов MedTech

Текстовые файлы - имитация согласий/направлений/анализов.
`*.pdf.bin` - бинарные заглушки «как будто PDF» для проверки upload произвольных байт.

Используй в TASK_02:
- `consent_patient_1001.*` -> patient_id=1001, visibility=patient, bucket consents
- `referral_patient_1002.*` -> patient_id=1002, visibility=staff
- `lab_result_1001.*` -> patient_id=1001, visibility=clinic
