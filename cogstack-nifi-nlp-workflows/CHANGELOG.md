# Changelog

## 2026-11-25

- Flows 2 and 3 read flow 1's combined index `discharge_summaries`, tracking `indexed_at`, instead of reading `mimiciv_note.discharge` from Postgres.
- Flow 1 writes OCR and free-text notes to one index, one document per `note_id`, and stamps `indexed_at` with UpdateRecord.
- Flow 2: `document_id_field_name = note_id` on all CogStack processors, so annotations carry `note_id`, `subject_id` and `hadm_id`.
- `MedcatNoteNlpLoader` 1.3.0 takes `hadm_id` from the annotation, and falls back to looking up `note_id` or `id`.
- Flow 1 Groovy scripts and `document-mimic.avsc` write the same fields as `mimiciv_note.discharge`, plus OCR metadata.
- New `discharge_binary.sql` and sample generator notebook (multi-page PDFs, `new_sample` / `same_notes` modes).
- Setup guide: `make start-data-infra`, mounted host folders, OpenSearch certificate generation.
