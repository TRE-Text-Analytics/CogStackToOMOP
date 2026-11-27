# OmopNoteNlpLoader.py
#
# NiFi 2.x Python processor (FlowFileTransform) that ports load_anthropometrics_v2.ipynb:
# it writes the weight/height/BMI/obesity results of WeightBmiMedspacy into the OMOP
# tables note_nlp, note_nlp_relationship and note_nlp_relationship_part in the schema set
# by the Target Schema property (default omop_temp).
#
# Input FlowFile (any of these JSON shapes):
#   * a JSON array of note documents, as emitted by WeightBmiMedspacy, or
#   * a JSON array of OpenSearch hits ({"_source": {...}}), or
#   * an OpenSearch response ({"hits": {"hits": [...]}}), or
#   * one JSON object per line.
# Each FlowFile is loaded in ONE database transaction: all of its rows are committed,
# or none are (failure relationship), so NiFi retries never leave half-written notes.
# The FlowFile content is passed through unchanged; row counts are added as attributes.
#
# Deploy: copy into the same python extensions folder as WeightBmiMedspacy.py.

import json
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators


# =====================================================================================
# Vocabulary -- copied from the notebook (verify the IDs against your CONCEPT table)
# =====================================================================================

NLP_SYSTEM_ANTHRO = "CogStack-anthropometrics"
NLP_SYSTEM_COND = "CogStack-anthropometrics"
NLP_SYSTEM_DERIVED_ANTHRO = "CogStack-derived-anthropometrics"
ALL_NLP_SYSTEMS = sorted({NLP_SYSTEM_ANTHRO, NLP_SYSTEM_COND, NLP_SYSTEM_DERIVED_ANTHRO})

RELATION_VOCAB = {
    "COMPOUND_MEASUREMENT": {"concept_id": 44818815, "source_value": "has_finding_attribute"},
    "DRUG_DRUG_INTERACTION": {"concept_id": 40767229, "source_value": "interacts_with"},
    "ROLE_MEASUREMENT_CONCEPT": {"concept_id": 4151144, "source_value": "component"},
    "ROLE_VALUE": {"concept_id": 21490848, "source_value": "has_measured_value"},
    "ROLE_UNIT": {"concept_id": 4111385, "source_value": "scale_type"},
}

# Fallback only, for documents indexed before WeightBmiMedspacy 1.0.3 (no "value" field).
# Same pattern and range as WeightBmiMedspacy's defaults (Min BMI 5.0, Max BMI 90.0).
BMI_NUMBER_RE = re.compile(r"[0-9]{2}(?:\.[0-9])?")
MIN_BMI, MAX_BMI = 5.0, 90.0
SCHEMA_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def bmi_mention_value(e: Dict[str, Any]) -> Tuple[Optional[float], bool]:
    """(value, valid) for a BMI_VALUE entity: taken from WeightBmiMedspacy's own parse when
    present, otherwise parsed the same way from the text."""
    if "value_valid" in e:
        return e.get("value"), bool(e.get("value_valid"))
    m = BMI_NUMBER_RE.search(e.get("text", ""))
    if not m:
        return None, False
    v = float(m.group())
    return v, MIN_BMI <= v <= MAX_BMI

CONCEPTS = {
    "BODY_WEIGHT": {"snomed": 27113001, "omop": 3025315},
    "BODY_HEIGHT": {"snomed": 50373000, "omop": 3036277},
    "OBESITY": {"snomed": 414916001, "omop": 4096931},
    "BODY_MASS_INDEX": {"snomed": 60621009, "omop": 3038553},
}


# =====================================================================================
# Inserts -- same SQL and argument handling as the notebook
# =====================================================================================

class Loader:
    """Wraps one cursor and counts what it writes."""

    def __init__(self, cur, schema: str = "omop_temp"):
        self.cur = cur
        self.schema = schema
        self.note_nlp_rows = 0
        self.relationships = 0
        self.relationship_parts = 0
        self.bmi_mentions_skipped = 0

    def _exec(self, query: str, params=None):
        """Run a statement with {s} replaced by the target schema as a quoted identifier."""
        from psycopg2 import sql
        self.cur.execute(sql.SQL(query).format(s=sql.Identifier(self.schema)), params)

    def insert_note_nlp(self, note_id, snippet, offset, lexical_variant, concept_key, nlp_system,
                        term_exists="Y", term_temporal="current", term_modifiers=None,
                        section_concept_id=0, value_as_number=None, value_as_string=None,
                        value_as_date=None, value_as_boolean=None, value_as_concept=None):
        self._exec(
            """
            INSERT INTO {s}.note_nlp (
                note_id, section_concept_id, snippet, "offset", lexical_variant,
                note_nlp_concept_id, note_nlp_source_concept_id, nlp_system,
                nlp_date, nlp_datetime, term_exists, term_temporal, term_modifiers,
                value_as_number, value_as_string, value_as_date, value_as_boolean, value_as_concept
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING note_nlp_id;
            """,
            (
                int(note_id),
                section_concept_id,
                snippet[:250] if snippet else None,
                str(offset) if offset else None,
                lexical_variant[:250],
                CONCEPTS[concept_key]["omop"],
                CONCEPTS[concept_key]["snomed"],
                nlp_system,
                date.today(),
                datetime.now(),
                term_exists,
                term_temporal,
                term_modifiers,
                float(value_as_number) if value_as_number is not None else None,
                str(value_as_string)[:250] if value_as_string else None,
                value_as_date,
                bool(value_as_boolean) if value_as_boolean is not None else None,
                int(value_as_concept) if value_as_concept else None,
            ),
        )
        result = self.cur.fetchone()
        if result:
            self.note_nlp_rows += 1
        return result[0] if result else None

    def insert_coded_relationship(self, relation_key, nlp_system, entity_role_pairs):
        if not entity_role_pairs:
            return None
        rel_info = RELATION_VOCAB[relation_key]
        self._exec(
            """
            INSERT INTO {s}.note_nlp_relationship (
                relationship_concept_id, relationship_source_value, nlp_system
            )
            VALUES (%s, %s, %s)
            RETURNING relationship_id;
            """,
            (rel_info["concept_id"], rel_info["source_value"], nlp_system),
        )
        relationship_id = self.cur.fetchone()[0]
        self.relationships += 1
        for note_nlp_id, role_key in entity_role_pairs:
            role_info = RELATION_VOCAB[role_key]
            self._exec(
                """
                INSERT INTO {s}.note_nlp_relationship_part (
                    relationship_id, note_nlp_id, role_concept_id, role_source_value
                )
                VALUES (%s, %s, %s, %s);
                """,
                (relationship_id, int(note_nlp_id), role_info["concept_id"], role_info["source_value"]),
            )
            self.relationship_parts += 1
        return relationship_id

    # ---------------------------------------------------------------- document steps

    def process_anthropometrics(self, src):
        note_id = src["hadm_id"]
        for a in src.get("anthro_mentions") or []:
            concept_type = "BODY_WEIGHT" if a["type"].startswith("WEIGHT") else "BODY_HEIGHT"
            parent_id = self.insert_note_nlp(note_id, a.get("context_snippet"), a.get("start_char"),
                                             a["match_text"], concept_type, NLP_SYSTEM_ANTHRO)
            if not parent_id:
                continue
            relation_parts = [(parent_id, "ROLE_MEASUREMENT_CONCEPT")]
            if a.get("value_parsed") is not None:
                val_id = self.insert_note_nlp(
                    note_id, f"Numeric value for {a['match_text']}", a.get("start_char"),
                    str(a["value_parsed"]), concept_type, NLP_SYSTEM_ANTHRO,
                    value_as_number=float(a["value_parsed"]))
                relation_parts.append((val_id, "ROLE_VALUE"))
            if a.get("unit"):
                unit_id = self.insert_note_nlp(
                    note_id, f"Unit for {a['match_text']}", a.get("start_char"),
                    a["unit"], concept_type, NLP_SYSTEM_ANTHRO,
                    value_as_string=str(a["unit"]))
                relation_parts.append((unit_id, "ROLE_UNIT"))
            if len(relation_parts) > 1:
                self.insert_coded_relationship("COMPOUND_MEASUREMENT", NLP_SYSTEM_ANTHRO, relation_parts)

    def process_conditions(self, src):
        note_id = src["hadm_id"]
        for e in src.get("entities") or []:
            if e.get("label") != "CONDITION":
                continue
            if e["text"].lower() != "obesity":
                continue
            self.insert_note_nlp(
                note_id, e.get("context_snippet"), e.get("start_char"), e["text"],
                "OBESITY", NLP_SYSTEM_COND,
                term_exists="N" if e.get("is_negated") else "Y",
                term_temporal=e.get("temporality", "unknown"),
                term_modifiers=(f"uncertain={e.get('is_uncertain')}|"
                                f"historical={e.get('is_historical')}"),
            )

    def process_bmi(self, src):
        note_id = src["hadm_id"]
        for e in src.get("entities") or []:
            if e.get("label") != "BMI_VALUE":
                continue
            # Value comes from WeightBmiMedspacy, not re-parsed here. Mentions it rejected
            # as out of range (e.g. "BMI 99") are not loaded.
            value_as_number, valid = bmi_mention_value(e)
            if not valid:
                self.bmi_mentions_skipped += 1
                continue
            self.insert_note_nlp(
                note_id, e.get("context_snippet"), e.get("start_char"), e["text"],
                "BODY_MASS_INDEX", NLP_SYSTEM_ANTHRO,
                term_exists="N" if e.get("is_negated") else "Y",
                term_temporal=e.get("temporality", "unknown"),
                value_as_number=value_as_number,
            )
        if src.get("bmi_final") is not None:
            self.insert_note_nlp(
                note_id, f"Derived BMI: {src['bmi_final']}", "1:x dummy",
                "bmi", "BODY_MASS_INDEX", NLP_SYSTEM_DERIVED_ANTHRO,
                value_as_number=float(src["bmi_final"]))

    def _derived_measurement(self, note_id, value, concept_key, label, unit):
        parent_id = self.insert_note_nlp(
            note_id, f"Extracted {label}: {value} {unit}", "1:x dummy",
            label, concept_key, NLP_SYSTEM_DERIVED_ANTHRO)
        if not parent_id:
            return
        relation_parts = [(parent_id, "ROLE_MEASUREMENT_CONCEPT")]
        val_id = self.insert_note_nlp(
            note_id, f"Numeric value for derived {label}", "1:x dummy",
            str(value), concept_key, NLP_SYSTEM_DERIVED_ANTHRO,
            value_as_number=float(value))
        relation_parts.append((val_id, "ROLE_VALUE"))
        unit_id = self.insert_note_nlp(
            note_id, f"Unit for derived {label}", "1:x dummy",
            unit, concept_key, NLP_SYSTEM_DERIVED_ANTHRO,
            value_as_string=unit)
        relation_parts.append((unit_id, "ROLE_UNIT"))
        self.insert_coded_relationship("COMPOUND_MEASUREMENT", NLP_SYSTEM_DERIVED_ANTHRO, relation_parts)

    def process_derived_values(self, src):
        note_id = src["hadm_id"]
        if src.get("weight_kg"):
            self._derived_measurement(note_id, src["weight_kg"], "BODY_WEIGHT", "weight", "kg")
        if src.get("height_m"):
            self._derived_measurement(note_id, src["height_m"], "BODY_HEIGHT", "height", "m")

    def delete_existing(self, note_ids: List[int]):
        """Remove rows this loader wrote earlier for these notes, so a reprocess replaces
        them instead of duplicating them."""
        self._exec(
            """
            SELECT DISTINCT p.relationship_id
            FROM {s}.note_nlp_relationship_part p
            JOIN {s}.note_nlp n ON n.note_nlp_id = p.note_nlp_id
            WHERE n.note_id = ANY(%s) AND n.nlp_system = ANY(%s);
            """,
            (note_ids, ALL_NLP_SYSTEMS),
        )
        rel_ids = [r[0] for r in self.cur.fetchall()]
        if rel_ids:
            self._exec("DELETE FROM {s}.note_nlp_relationship_part WHERE relationship_id = ANY(%s);",
                             (rel_ids,))
            self._exec("DELETE FROM {s}.note_nlp_relationship WHERE relationship_id = ANY(%s);",
                             (rel_ids,))
        self._exec("DELETE FROM {s}.note_nlp WHERE note_id = ANY(%s) AND nlp_system = ANY(%s);",
                         (note_ids, ALL_NLP_SYSTEMS))


def extract_sources(raw: str) -> List[Dict[str, Any]]:
    """Accept WeightBmiMedspacy output, OpenSearch hits, an OpenSearch response, or JSON lines."""
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("[") or raw.startswith("{"):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = [json.loads(line) for line in raw.splitlines() if line.strip()]
    else:
        data = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if isinstance(data, dict):
        data = data.get("hits", {}).get("hits", [data]) if "hits" in data else [data]
    return [d["_source"] if isinstance(d, dict) and "_source" in d else d for d in data]


# =====================================================================================
# NiFi processor
# =====================================================================================

class OmopNoteNlpLoader(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.3'   # 1.0.3: Target Schema property
        description = ('Loads WeightBmiMedspacy results into the OMOP note_nlp, '
                       'note_nlp_relationship and note_nlp_relationship_part tables in PostgreSQL. '
                       'One transaction per FlowFile.')
        tags = ['omop', 'note_nlp', 'postgresql', 'cogstack', 'bmi', 'weight']
        dependencies = ['psycopg2-binary']

    DB_HOST = PropertyDescriptor(name='Database Host', description='PostgreSQL host.',
                                 required=True, default_value='samples-db',
                                 validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_PORT = PropertyDescriptor(name='Database Port', description='PostgreSQL port.',
                                 required=True, default_value='5432',
                                 validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])
    DB_NAME = PropertyDescriptor(name='Database Name', description='Database name.',
                                 required=True, default_value='mimic',
                                 validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_USER = PropertyDescriptor(name='Database User', description='Database user.',
                                 required=True, default_value='test',
                                 validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_PASSWORD = PropertyDescriptor(name='Database Password', description='Database password.',
                                     required=True, sensitive=True,
                                     validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    TARGET_SCHEMA = PropertyDescriptor(
        name='Target Schema',
        description='Schema holding note_nlp, note_nlp_relationship and note_nlp_relationship_part. '
                    'Letters, digits and underscores only; case-sensitive (quoted).',
        required=True, default_value='omop_temp',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    REPLACE_EXISTING = PropertyDescriptor(
        name='Replace Existing Rows',
        description='If true, rows previously written by this loader for the same note IDs are deleted '
                    'in the same transaction before inserting, so reprocessing does not duplicate them. '
                    'false = append, as the notebook did.',
        required=True, default_value='false', allowable_values=['true', 'false'])

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = [self.DB_HOST, self.DB_PORT, self.DB_NAME, self.DB_USER,
                            self.DB_PASSWORD, self.TARGET_SCHEMA, self.REPLACE_EXISTING]
        self.conn = None
        self.conn_args = None

    def getPropertyDescriptors(self):
        return self.descriptors

    def onScheduled(self, context):
        self.conn_args = dict(
            host=context.getProperty(self.DB_HOST).getValue(),
            port=int(context.getProperty(self.DB_PORT).getValue()),
            dbname=context.getProperty(self.DB_NAME).getValue(),
            user=context.getProperty(self.DB_USER).getValue(),
            password=context.getProperty(self.DB_PASSWORD).getValue(),
        )
        self._connect()

    def onStopped(self, context):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _connect(self):
        import psycopg2
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.conn_args)

    def transform(self, context, flowfile):
        replace_existing = context.getProperty(self.REPLACE_EXISTING).getValue() == 'true'
        schema = context.getProperty(self.TARGET_SCHEMA).getValue().strip()
        if not SCHEMA_NAME_RE.fullmatch(schema):
            self.logger.error(f'Invalid Target Schema {schema!r}: use letters, digits and underscores only')
            return FlowFileTransformResult(relationship='failure')

        try:
            sources = extract_sources(flowfile.getContentsAsBytes().decode('utf-8'))
        except Exception as e:
            self.logger.error(f'Could not parse FlowFile content as JSON: {e}')
            return FlowFileTransformResult(relationship='failure')

        # note_nlp.note_id is filled from hadm_id, as in the notebook. Notes without one
        # would make int(None) fail; they are skipped and counted instead.
        usable = [s for s in sources if s.get("hadm_id") is not None]
        skipped = len(sources) - len(usable)

        try:
            self._connect()
            with self.conn:                       # commit on success, rollback on exception
                with self.conn.cursor() as cur:
                    loader = Loader(cur, schema)
                    if replace_existing and usable:
                        loader.delete_existing(sorted({int(s["hadm_id"]) for s in usable}))
                    for src in usable:
                        loader.process_anthropometrics(src)
                        loader.process_conditions(src)
                        loader.process_bmi(src)
                        loader.process_derived_values(src)
        except Exception as e:
            self.logger.error(f'OMOP load failed, transaction rolled back: {e}')
            try:
                if self.conn is not None and self.conn.closed:
                    self.conn = None
            except Exception:
                self.conn = None
            return FlowFileTransformResult(relationship='failure')

        return FlowFileTransformResult(
            relationship='success',
            attributes={
                'omop.notes.in': str(len(sources)),
                'omop.notes.loaded': str(len(usable)),
                'omop.notes.skipped_no_hadm_id': str(skipped),
                'omop.note_nlp.rows': str(loader.note_nlp_rows),
                'omop.relationships': str(loader.relationships),
                'omop.relationship_parts': str(loader.relationship_parts),
                'omop.bmi_mentions.skipped_out_of_range': str(loader.bmi_mentions_skipped),
            },
        )
