-- mimiciv_note.discharge_binary
-- One binary document (e.g. PDF) per discharge note, with the same keys and
-- types as mimiciv_note.discharge, so flow 1's OCR branch can write documents
-- in the same shape as the free-text ones.
--
-- Put this in services/cogstack-db/pgsql/schemas/ so it runs when the
-- databank is first initialised. Running it by hand drops any existing data.

CREATE SCHEMA IF NOT EXISTS mimiciv_note;

DROP TABLE IF EXISTS mimiciv_note.discharge_binary;

CREATE TABLE mimiciv_note.discharge_binary (
    note_id     VARCHAR(25)  NOT NULL,
    subject_id  INTEGER      NOT NULL,
    hadm_id     INTEGER      NOT NULL,
    note_type   CHAR(2)      NOT NULL,
    note_seq    SMALLINT     NOT NULL,
    charttime   TIMESTAMP(0) NOT NULL,
    storetime   TIMESTAMP(0),
    file_mime   VARCHAR(100) NOT NULL DEFAULT 'application/pdf',
    file_data   BYTEA        NOT NULL,
    CONSTRAINT discharge_binary_pk PRIMARY KEY (note_id)
);

-- GenerateTableFetch checkpoints on charttime and partitions pages on hadm_id
CREATE INDEX IF NOT EXISTS discharge_binary_charttime_idx ON mimiciv_note.discharge_binary (charttime);
CREATE INDEX IF NOT EXISTS discharge_binary_hadm_id_idx   ON mimiciv_note.discharge_binary (hadm_id);

-- Keep the document bytes out of line and uncompressed (PDFs are already compressed)
ALTER TABLE mimiciv_note.discharge_binary ALTER COLUMN file_data SET STORAGE EXTERNAL;
