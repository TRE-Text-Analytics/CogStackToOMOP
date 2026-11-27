# MedcatNoteNlpLoader.py
#
# NiFi 2.x Python processor (FlowFileTransform) that ports load_medcat_mental_health_v2.ipynb:
# MedCAT annotations read from OpenSearch (index trext_medcat_annotations) are written to
# <Target Schema>.note_nlp, one row per annotation.
#
# Input FlowFile (any of these JSON shapes):
#   * an OpenSearch response ({"hits": {"hits": [...]}}), or
#   * a JSON array of hits ({"_source": {...}}) or of annotation sources, or
#   * one JSON object per line.
# Each annotation source needs: cui, start, source_value and meta_anns, plus either
#   * hadm_id (annotations made from discharge_summaries carry it, with note_id), or
#   * id or note_id (MIMIC note_id, e.g. "<subject_id>-DS-<seq>"), looked up in the Note Lookup Table.
#
# Per FlowFile, in ONE transaction:
#   1. take each annotation's hadm_id; for annotations without one, look it up from the
#      note id in the Note Lookup Table
#      (note_nlp.note_id = hadm_id, the same convention as OmopNoteNlpLoader);
#   2. build one note_nlp row per annotation; note_nlp_concept_id and
#      note_nlp_source_concept_id are the raw CUI cast to an integer -- an identity
#      placeholder, NOT a real OMOP concept_id (see the notebook's notes);
#   3. insert them with a single multi-row statement. With Skip Duplicates on, an
#      annotation already in note_nlp (same note, offset, CUI and nlp_system) is not
#      inserted again, so re-runs and replays are safe.
# Skipped annotations are counted in FlowFile attributes, never silently dropped.
#
# Deploy: copy into the same python extensions folder as the other processors.

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
INT32_MAX = 2_147_483_647

# How MedCAT meta-annotations map to note_nlp.term_exists / term_temporal, following the
# OMOP convention: term_exists = 'N' for negated, hypothetical or not-the-patient mentions;
# historical mentions stay present ('Y') with term_temporal = 'past'.
# Keys are meta-annotation names, then their values (compared as strings). Each rule may
# set term_exists and/or term_temporal. Any 'N' wins over 'Y'; for term_temporal, a later
# category in this JSON overrides an earlier one. Edit in the processor's Meta-annotation
# Rules property to match your model's actual labels.
DEFAULT_META_RULES = {
    "Status": {
        "Affirmed": {"term_exists": "Y"},
        "Negated": {"term_exists": "N"},
        "Hypothetical": {"term_exists": "N"},
        "Historical": {"term_exists": "Y", "term_temporal": "past"},
        "Other": {"term_exists": "N"}
    },
    "Presence": {
        "True": {"term_exists": "Y"},
        "False": {"term_exists": "N"},
        "Hypothetical": {"term_exists": "N"}
    },
    "Temporality": {
        "Past": {"term_temporal": "past"},
        "Recent": {"term_temporal": "current"},
        "Future": {"term_temporal": "future"}
    },
    "Experiencer": {
        "Patient": {},
        "Family": {"term_exists": "N"},
        "Other": {"term_exists": "N"}
    }
}


def apply_meta_rules(meta_anns: Dict[str, Any], rules: Dict[str, Dict[str, Dict[str, str]]],
                     default_term_exists: str, unmapped: Dict[str, int]):
    """(term_exists, term_temporal) for one annotation. Meta-annotation values with no rule
    are counted in `unmapped` as "Name=value" and otherwise ignored."""
    term_exists, term_temporal, saw_no = default_term_exists, "current", False
    for name, details in (meta_anns or {}).items():
        value = (details or {}).get("value")
        if name not in rules:
            continue
        rule = rules[name].get(str(value))
        if rule is None:
            key = f"{name}={value}"
            unmapped[key] = unmapped.get(key, 0) + 1
            continue
        if rule.get("term_exists") == "N":
            saw_no = True
        elif rule.get("term_exists") == "Y" and not saw_no:
            term_exists = "Y"
        if rule.get("term_temporal"):
            term_temporal = rule["term_temporal"]
    return ("N" if saw_no else term_exists), term_temporal


def extract_sources(raw: str) -> List[Dict[str, Any]]:
    """Accept an OpenSearch response, an array of hits/sources, or JSON lines."""
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if isinstance(data, dict):
        data = data.get("hits", {}).get("hits", []) if "hits" in data else [data]
    return [d["_source"] if isinstance(d, dict) and "_source" in d else d for d in data]


def resolve_cui_to_int(cui) -> Optional[int]:
    try:
        return int(cui)
    except (TypeError, ValueError):
        return None


def source_note_id(src: Dict[str, Any]) -> Optional[str]:
    """MIMIC note_id of the annotation's document: "note_id" (annotations made from
    discharge_summaries) or "id" (annotations made straight from mimiciv_note.discharge)."""
    value = src.get("note_id") or src.get("id")
    return str(value) if value else None


def source_hadm_id(src: Dict[str, Any]) -> Optional[int]:
    """hadm_id carried on the annotation itself, if any."""
    try:
        value = src.get("hadm_id")
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def resolve_hadm_id(src: Dict[str, Any], hadm_lookup: Dict[str, int]) -> Optional[int]:
    hadm_id = source_hadm_id(src)
    if hadm_id is not None:
        return hadm_id
    return hadm_lookup.get(source_note_id(src))


def build_rows(sources: List[Dict[str, Any]], hadm_lookup: Dict[str, int], nlp_system: str,
               meta_rules: Dict[str, Any], default_term_exists: str, cui_filter: Optional[Set[str]],
               concept_id_max: Optional[int], counts: Dict[str, int],
               unmapped: Dict[str, int]) -> List[tuple]:
    """One note_nlp row per annotation, same field mapping as the notebook."""
    rows, seen = [], set()
    today, now = date.today(), datetime.now()
    for src in sources:
        cui = src.get("cui")
        if cui_filter is not None and str(cui) not in cui_filter:
            counts["skipped_cui_not_in_filter"] += 1
            continue
        note_id = resolve_hadm_id(src, hadm_lookup)
        if note_id is None:
            counts["skipped_unmapped_hadm_id"] += 1
            continue
        cui_int = resolve_cui_to_int(cui)
        if cui_int is None:
            counts["skipped_non_numeric_cui"] += 1
            continue
        if concept_id_max is not None and cui_int > concept_id_max:
            # The concept_id columns are 32-bit integers and this SNOMED ID does not fit.
            counts["skipped_cui_too_large"] += 1
            continue

        meta_anns = src.get("meta_anns") or {}
        term_exists, term_temporal = apply_meta_rules(meta_anns, meta_rules, default_term_exists, unmapped)
        modifier_parts = [f"{name}={(details or {}).get('value')}" for name, details in meta_anns.items()]
        term_modifiers = "|".join(modifier_parts) if modifier_parts else None

        start = src.get("start")
        offset = str(start) if start is not None else None
        key = (int(note_id), offset, cui_int)
        if key in seen:                       # same annotation twice in one FlowFile
            counts["skipped_duplicate"] += 1
            continue
        seen.add(key)

        snippet = src.get("source_value")
        rows.append((
            int(note_id),
            0,                                          # section_concept_id
            snippet[:250] if snippet else None,         # snippet
            offset,                                     # "offset"
            snippet[:250] if snippet else None,         # lexical_variant
            cui_int,                                    # note_nlp_concept_id (placeholder)
            cui_int,                                    # note_nlp_source_concept_id (placeholder)
            nlp_system,
            today,
            now,
            term_exists,
            term_temporal,
            term_modifiers,
        ))
    return rows


class MedcatNoteNlpLoader(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.3.0'   # 1.3.0: uses the annotation's own hadm_id; looks up note_id or id otherwise
        description = ('Loads MedCAT annotations (from OpenSearch) into OMOP note_nlp in PostgreSQL, '
                       'one row per annotation, with note_id resolved to hadm_id. '
                       'One transaction per FlowFile.')
        tags = ['omop', 'note_nlp', 'medcat', 'postgresql', 'cogstack', 'snomed']
        dependencies = ['psycopg2-binary']

    DB_HOST = PropertyDescriptor(name='Database Host', description='PostgreSQL host.', required=True,
                                 default_value='samples-db', validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_PORT = PropertyDescriptor(name='Database Port', description='PostgreSQL port.', required=True,
                                 default_value='5432', validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])
    DB_NAME = PropertyDescriptor(name='Database Name', description='Database name.', required=True,
                                 default_value='mimic', validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_USER = PropertyDescriptor(name='Database User', description='Database user.', required=True,
                                 default_value='test', validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DB_PASSWORD = PropertyDescriptor(name='Database Password', description='Database password.', required=True,
                                     sensitive=True, validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    TARGET_SCHEMA = PropertyDescriptor(
        name='Target Schema', description='Schema holding note_nlp (letters, digits, underscores).',
        required=True, default_value='omop_temp', validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    NOTE_TABLE = PropertyDescriptor(
        name='Note Lookup Table',
        description='schema.table with note_id (text) and hadm_id columns, used to turn the '
                    'annotation id into hadm_id.',
        required=True, default_value='mimiciv_note.discharge',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    NLP_SYSTEM = PropertyDescriptor(
        name='NLP System', description='Value written to note_nlp.nlp_system.',
        required=True, default_value='MedCAT-v2-Snomed2025-MIMIC_IV',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    META_RULES = PropertyDescriptor(
        name='Meta-annotation Rules',
        description='JSON mapping meta-annotation name -> value -> {"term_exists": "Y"|"N", '
                    '"term_temporal": "current"|"past"|"future"}. Any N wins; later categories override '
                    'term_temporal. Values with no rule are counted in medcat.unmapped_meta_values.',
        required=True, default_value=json.dumps(DEFAULT_META_RULES),
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    DEFAULT_TERM_EXISTS = PropertyDescriptor(
        name='Default term_exists',
        description='term_exists when no rule sets it (e.g. an annotation with no meta-annotations).',
        required=True, default_value='Y', allowable_values=['Y', 'N'])
    CUI_FILTER_MODE = PropertyDescriptor(
        name='CUI Filter Mode',
        description='Which CUIs to load. None: every annotation that arrives. File: only CUIs listed in '
                    'CUI Filter File. List: only CUIs in CUI Filter List. Annotations with other CUIs are '
                    'skipped and counted in medcat.skipped_cui_not_in_filter.',
        required=True, default_value='None', allowable_values=['None', 'File', 'List'])
    CUI_FILTER_FILE = PropertyDescriptor(
        name='CUI Filter File',
        description='Used when CUI Filter Mode = File. Path inside the NiFi container to a text file '
                    'with one CUI per line (e.g. /opt/nifi/user_scripts/cui_filter.txt).',
        required=False)
    CUI_FILTER_LIST = PropertyDescriptor(
        name='CUI Filter List',
        description='Used when CUI Filter Mode = List. CUIs separated by commas (spaces and new lines '
                    'are also accepted), e.g. 35489007, 197480006, 21897009.',
        required=False)
    SKIP_DUPLICATES = PropertyDescriptor(
        name='Skip Duplicates',
        description='If true, an annotation already in note_nlp (same note_id, offset, CUI and '
                    'nlp_system) is not inserted again, so re-runs do not duplicate rows.',
        required=True, default_value='true', allowable_values=['true', 'false'])

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = [self.DB_HOST, self.DB_PORT, self.DB_NAME, self.DB_USER, self.DB_PASSWORD,
                            self.TARGET_SCHEMA, self.NOTE_TABLE, self.NLP_SYSTEM, self.META_RULES, self.DEFAULT_TERM_EXISTS,
                            self.CUI_FILTER_MODE, self.CUI_FILTER_FILE, self.CUI_FILTER_LIST,
                            self.SKIP_DUPLICATES]
        self.conn = None
        self.conn_args = None
        self.cui_filter = None
        self.concept_id_max = None

    def getPropertyDescriptors(self):
        return self.descriptors

    # ------------------------------------------------------------------ lifecycle
    def onScheduled(self, context):
        self.conn_args = dict(
            host=context.getProperty(self.DB_HOST).getValue(),
            port=int(context.getProperty(self.DB_PORT).getValue()),
            dbname=context.getProperty(self.DB_NAME).getValue(),
            user=context.getProperty(self.DB_USER).getValue(),
            password=context.getProperty(self.DB_PASSWORD).getValue(),
        )
        self.cui_filter = self._load_cui_filter(context)
        self._connect()
        self.concept_id_max = self._concept_id_limit(context.getProperty(self.TARGET_SCHEMA).getValue().strip())

    def onStopped(self, context):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _load_cui_filter(self, context) -> Optional[Set[str]]:
        """None = no filter. Raises (so the processor will not start) if the chosen
        mode has nothing usable configured -- a silent "load nothing" would be worse."""
        mode = context.getProperty(self.CUI_FILTER_MODE).getValue()
        if mode == 'File':
            path = (context.getProperty(self.CUI_FILTER_FILE).getValue() or '').strip()
            if not path:
                raise ValueError('CUI Filter Mode is File but CUI Filter File is empty')
            with open(path, 'r') as f:
                cuis = {line.strip() for line in f if line.strip()}
            source = path
        elif mode == 'List':
            raw = context.getProperty(self.CUI_FILTER_LIST).getValue() or ''
            cuis = {c.strip() for c in re.split(r'[,\s]+', raw) if c.strip()}
            source = 'CUI Filter List'
        else:
            self.logger.info('MedcatNoteNlpLoader: no CUI filter, loading every annotation received')
            return None
        if not cuis:
            raise ValueError(f'CUI Filter Mode is {mode} but no CUIs were found in {source}')
        self.logger.info(f'MedcatNoteNlpLoader: CUI filter of {len(cuis)} CUIs from {source}')
        return cuis

    def _connect(self):
        import psycopg2
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.conn_args)

    def _concept_id_limit(self, schema: str) -> Optional[int]:
        """INT32_MAX if note_nlp.note_nlp_concept_id is a 32-bit integer, else None (no limit)."""
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""SELECT data_type FROM information_schema.columns
                               WHERE table_schema = %s AND table_name = 'note_nlp'
                                 AND column_name = 'note_nlp_concept_id'""", (schema,))
                row = cur.fetchone()
        return INT32_MAX if row and row[0] == 'integer' else None

    # ------------------------------------------------------------------ transform
    def transform(self, context, flowfile):
        from psycopg2 import sql
        from psycopg2.extras import execute_values

        schema = context.getProperty(self.TARGET_SCHEMA).getValue().strip()
        note_table = context.getProperty(self.NOTE_TABLE).getValue().strip().split('.')
        nlp_system = context.getProperty(self.NLP_SYSTEM).getValue().strip()
        default_term_exists = context.getProperty(self.DEFAULT_TERM_EXISTS).getValue()
        try:
            meta_rules = json.loads(context.getProperty(self.META_RULES).getValue())
            assert isinstance(meta_rules, dict)
        except Exception as e:
            self.logger.error(f'Meta-annotation Rules is not a valid JSON object: {e}')
            return FlowFileTransformResult(relationship='failure')
        skip_dupes = context.getProperty(self.SKIP_DUPLICATES).getValue() == 'true'

        if not IDENT_RE.fullmatch(schema) or not (1 <= len(note_table) <= 2) \
                or not all(IDENT_RE.fullmatch(p) for p in note_table):
            self.logger.error('Invalid Target Schema or Note Lookup Table name')
            return FlowFileTransformResult(relationship='failure')

        try:
            sources = extract_sources(flowfile.getContentsAsBytes().decode('utf-8'))
        except Exception as e:
            self.logger.error(f'Could not parse FlowFile content as JSON: {e}')
            return FlowFileTransformResult(relationship='failure')

        counts = {k: 0 for k in ['skipped_cui_not_in_filter', 'skipped_unmapped_hadm_id',
                                 'skipped_non_numeric_cui', 'skipped_cui_too_large', 'skipped_duplicate']}
        inserted = 0
        unmapped: Dict[str, int] = {}
        try:
            self._connect()
            with self.conn:                                   # one transaction per FlowFile
                with self.conn.cursor() as cur:
                    # only annotations without their own hadm_id need the lookup
                    doc_ids = sorted({source_note_id(s) for s in sources
                                      if source_hadm_id(s) is None and source_note_id(s)})
                    hadm_lookup: Dict[str, int] = {}
                    if doc_ids:
                        cur.execute(sql.SQL("SELECT note_id, hadm_id FROM {t} "
                                            "WHERE note_id = ANY(%s) AND hadm_id IS NOT NULL")
                                    .format(t=sql.Identifier(*note_table)), (doc_ids,))
                        hadm_lookup = dict(cur.fetchall())

                    rows = build_rows(sources, hadm_lookup, nlp_system, meta_rules, default_term_exists,
                                      self.cui_filter, self.concept_id_max, counts, unmapped)
                    if rows:
                        cols = sql.SQL("""(note_id, section_concept_id, snippet, "offset", lexical_variant,
                                  note_nlp_concept_id, note_nlp_source_concept_id, nlp_system,
                                  nlp_date, nlp_datetime, term_exists, term_temporal, term_modifiers)""")
                        if skip_dupes:
                            stmt = sql.SQL("""
                                INSERT INTO {s}.note_nlp {cols}
                                SELECT v.* FROM (VALUES %s) AS v {cols}
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM {s}.note_nlp n
                                    WHERE n.note_id = v.note_id
                                      AND n."offset" IS NOT DISTINCT FROM v."offset"
                                      AND n.note_nlp_source_concept_id = v.note_nlp_source_concept_id
                                      AND n.nlp_system = v.nlp_system)
                                RETURNING 1""").format(s=sql.Identifier(schema), cols=cols)
                        else:
                            stmt = sql.SQL("INSERT INTO {s}.note_nlp {cols} VALUES %s RETURNING 1") \
                                .format(s=sql.Identifier(schema), cols=cols)
                        template = ("(%s::bigint, %s::int, %s::text, %s::text, %s::text, %s::bigint, "
                                    "%s::bigint, %s::text, %s::date, %s::timestamp, %s::text, %s::text, %s::text)")
                        result = execute_values(cur, stmt.as_string(cur), rows, template=template,
                                                page_size=1000, fetch=True)
                        inserted = len(result)
                        counts['skipped_duplicate'] += len(rows) - inserted
        except Exception as e:
            self.logger.error(f'MedCAT note_nlp load failed, transaction rolled back: {e}')
            self.conn = None if (self.conn is None or self.conn.closed) else self.conn
            return FlowFileTransformResult(relationship='failure')

        attrs = {'medcat.annotations.in': str(len(sources)), 'medcat.note_nlp.inserted': str(inserted)}
        attrs.update({f'medcat.{k}': str(v) for k, v in counts.items()})
        if unmapped:
            attrs['medcat.unmapped_meta_values'] = '; '.join(f'{k} x{v}' for k, v in sorted(unmapped.items()))
            self.logger.warn(f'Meta-annotation values with no rule: {attrs["medcat.unmapped_meta_values"]}')
        return FlowFileTransformResult(relationship='success', attributes=attrs)
