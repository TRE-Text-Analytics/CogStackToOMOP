# WeightBmiMedspacy.py
#
# NiFi 2.x Python processor (FlowFileTransform) that ports the
# weight_and_bmi_medspacy_v11 notebook's NLP stage.
#
# Input  FlowFile: JSON array of records  {note_id, subject_id, hadm_id, text}
#                  (or one JSON object per line) -- e.g. from ExecuteSQLRecord
#                  with a JsonRecordSetWriter.
# Output FlowFile: JSON array of extracted-note documents, same shape as
#                  extract_note_fields() in the notebook. By default only notes with a
#                  non-null weight_status_final are emitted (same filter as the notebook).
#
# Deploy: copy into $NIFI_HOME/python/extensions/ (or a subfolder of it).
# NiFi creates a venv for this processor and pip-installs `dependencies` on first use.
# Each concurrent task may run in its own Python process, and each process builds its
# own medspaCy pipeline once in onScheduled() -- the equivalent of _init_worker().

import json
import re
import sys

# Make this processor's own dependency folder (where NiFi pip-installed medspacy, spacy,
# thinc and numpy) win over any older numpy elsewhere on sys.path -- e.g. one installed
# into the image's system/framework Python. Must run before anything imports numpy.
_own = [p for p in sys.path if '/WeightBmiMedspacy/' in p.replace('\\', '/')]
sys.path[:] = _own + [p for p in sys.path if p not in _own]

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators


# =====================================================================================
# NLP logic -- ported from the notebook (behaviour kept identical unless noted)
# =====================================================================================

NUM_CLEAN_RE = re.compile(r"[^\d\.\-]")
RANGE_SPLIT_RE = re.compile(r"\s*[-–—]\s*")
CONTEXT_WINDOW = 40
BMI_RE = re.compile(r"(?i)\bBMI\b[:\s]*([0-9]{2}(?:\.[0-9])?)")


@dataclass
class PipelineConfig:
    prefer_numeric_bmi: bool = True
    min_bmi: float = 5.0
    max_bmi: float = 90.0
    require_present_for_text_status: bool = True
    enable_sectionizer: bool = True
    past_sections: Set[str] = field(default_factory=lambda: {
        "Past Medical History", "PMH", "Medical History", "Surgical History",
        "Family History", "Social History", "Problem List", "Prior Diagnoses", "Resolved Problems"
    })
    present_sections: Set[str] = field(default_factory=lambda: {
        "HPI", "History of Present Illness", "Assessment", "Assessment & Plan",
        "Plan", "Discharge Diagnosis", "Hospital Course", "Current Medications",
        "Vitals", "Objective", "Active Problems"
    })


_ANTHRO_MATCHER = None          # built in build_nlp(), used by anthro_matcher_component
_COMPONENTS_REGISTERED = False  # spaCy components are registered lazily, after imports


def to_float_safe(text: str):
    if text is None:
        return None
    t = text.strip()
    if RANGE_SPLIT_RE.search(t):
        parts = RANGE_SPLIT_RE.split(t)
        if parts:
            t = parts[0].strip()
    t = NUM_CLEAN_RE.sub("", t)
    if not t or t in {"-", ".", "-.", ".-"}:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _nums(span) -> List[str]:
    return [t.text for t in span if t.like_num or any(ch.isdigit() for ch in t.text)]


def _register_components():
    """Register custom spaCy components and Doc extensions (once per Python process)."""
    global _COMPONENTS_REGISTERED
    if _COMPONENTS_REGISTERED:
        return
    from spacy.language import Language
    from spacy.tokens import Doc

    if not Doc.has_extension("anthro_mentions"):
        Doc.set_extension("anthro_mentions", default=None)
    if not Doc.has_extension("anthro_numeric"):
        Doc.set_extension("anthro_numeric", default=None)

    @Language.component("bmi_value_extractor")
    def bmi_value_extractor(doc):
        spans = []
        for m in BMI_RE.finditer(doc.text):
            span = doc.char_span(m.start(), m.end(), label="BMI_VALUE", alignment_mode="expand")
            if span is not None:
                spans.append(span)
        if spans:
            doc.ents = tuple(list(doc.ents) + spans)
        return doc

    @Language.component("anthro_matcher_component")
    def anthro_matcher_component(doc):
        if _ANTHRO_MATCHER is None:
            doc._.anthro_mentions = []
            doc._.anthro_numeric = (None, None)
            return doc

        mentions: List[Dict[str, Any]] = []
        ht_m, wt_kg = None, None

        for mid, start, end in _ANTHRO_MATCHER(doc):
            label = doc.vocab.strings[mid]
            span = doc[start:end]
            start_char, end_char = span.start_char, span.end_char
            snippet = doc.text[max(0, start_char - CONTEXT_WINDOW): min(len(doc.text), end_char + CONTEXT_WINDOW)]
            values = _nums(span)
            parsed, unit = None, None

            if label == "HEIGHT_CM":
                unit = "cm"
                if values:
                    cm = to_float_safe(values[0])
                    parsed = cm if cm and 80 <= cm <= 260 else None
                    if cm and 120 <= cm <= 250:
                        ht_m = cm / 100.0
            elif label == "HEIGHT_FT_IN":
                unit = "m"
                if len(values) >= 2:
                    feet = to_float_safe(values[0])
                    inches = to_float_safe(values[1])
                    if feet is not None and inches is not None:
                        total_in = feet * 12 + inches
                        parsed = total_in * 0.0254 if 40 <= total_in <= 96 else None
                        if 48 <= total_in <= 90:
                            ht_m = total_in * 0.0254
            elif label == "WEIGHT_KG":
                unit = "kg"
                if values:
                    kg = to_float_safe(values[0])
                    parsed = kg if kg and 20 <= kg <= 400 else None
                    if kg and 25 <= kg <= 350:
                        wt_kg = kg
            elif label == "WEIGHT_LB":
                unit = "lb"
                if values:
                    lbs = to_float_safe(values[0])
                    parsed = lbs if lbs and 40 <= lbs <= 900 else None
                    if lbs and 55 <= lbs <= 770:
                        wt_kg = lbs * 0.453592

            mentions.append({
                "type": label,
                "match_text": span.text,
                "start_char": start_char,
                "end_char": end_char,
                "context_snippet": snippet,
                "value_parsed": parsed,
                "unit": unit,
            })

        doc._.anthro_mentions = mentions
        doc._.anthro_numeric = (ht_m, wt_kg)
        return doc

    _COMPONENTS_REGISTERED = True


def _add_target_rules(nlp) -> None:
    from medspacy.ner import TargetRule
    target_matcher = nlp.get_pipe("medspacy_target_matcher")
    target_matcher.add([
        TargetRule("obesity", "CONDITION"),
        TargetRule("obese", "CONDITION"),
        TargetRule("morbid obesity", "CONDITION"),
        TargetRule("morbidly obese", "CONDITION"),
        TargetRule("class 1 obesity", "WEIGHT_STATUS"),
        TargetRule("class i obesity", "WEIGHT_STATUS"),
        TargetRule("class 2 obesity", "WEIGHT_STATUS"),
        TargetRule("class ii obesity", "WEIGHT_STATUS"),
        TargetRule("class 3 obesity", "WEIGHT_STATUS"),
        TargetRule("class iii obesity", "WEIGHT_STATUS"),
        TargetRule("underweight", "WEIGHT_STATUS"),
        TargetRule("overweight", "WEIGHT_STATUS"),
        TargetRule("normal weight", "WEIGHT_STATUS"),
        TargetRule("healthy weight", "WEIGHT_STATUS"),
    ])


def _build_anthro_matcher(nlp):
    from spacy.matcher import Matcher
    matcher = Matcher(nlp.vocab)
    lead = lambda words: [
        {"LOWER": {"IN": words}},
        {"IS_PUNCT": True, "OP": "?"},
        {"IS_SPACE": True, "OP": "*"},
        {"LIKE_NUM": True},
    ]
    matcher.add("HEIGHT_CM", [lead(["height", "ht"]) + [{"LOWER": {"IN": ["cm", "centimeters"]}}]])
    matcher.add("HEIGHT_FT_IN", [lead(["height", "ht"]) + [{"TEXT": {"REGEX": r"['’]"}}, {"LIKE_NUM": True}]])
    matcher.add("WEIGHT_KG", [lead(["weight", "wt"]) + [{"LOWER": {"IN": ["kg", "kgs", "kilograms"]}}]])
    matcher.add("WEIGHT_LB", [lead(["weight", "wt"]) + [{"LOWER": {"IN": ["lb", "lbs", "pounds"]}}]])
    return matcher


def build_nlp(cfg: PipelineConfig):
    """Same pipeline as the notebook. The 'sanitize_for_multiprocessing' step is dropped:
    it only existed for nlp.pipe(n_process>1); NiFi parallelises via concurrent tasks /
    separate Python processes, and no Doc ever crosses a process boundary."""
    global _ANTHRO_MATCHER
    import medspacy

    _register_components()
    nlp = medspacy.load()

    if "medspacy_pyrush" in nlp.pipe_names:
        nlp.remove_pipe("medspacy_pyrush")
    nlp.add_pipe("sentencizer", first=True)

    if cfg.enable_sectionizer and "medspacy_sectionizer" not in nlp.pipe_names:
        nlp.add_pipe("medspacy_sectionizer")

    ctx_name, tm_name = "medspacy_context", "medspacy_target_matcher"
    names = list(nlp.pipe_names)
    if ctx_name in names and tm_name in names and names.index(ctx_name) < names.index(tm_name):
        try:
            nlp.move_pipe(ctx_name, after=tm_name)
        except AttributeError:
            nlp.remove_pipe(ctx_name)
            nlp.add_pipe(ctx_name, after=tm_name)

    _add_target_rules(nlp)

    if "bmi_value_extractor" not in nlp.pipe_names:
        nlp.add_pipe("bmi_value_extractor", after="medspacy_target_matcher")

    _ANTHRO_MATCHER = _build_anthro_matcher(nlp)
    if "anthro_matcher_component" not in nlp.pipe_names:
        nlp.add_pipe("anthro_matcher_component", after="bmi_value_extractor")
    return nlp


def _section_title_str(ent) -> Optional[str]:
    sec_val = getattr(ent._, "section_title", None)
    if sec_val is None:
        return None
    if isinstance(sec_val, str):
        return sec_val
    return getattr(sec_val, "text", str(sec_val))


def determine_temporality(ent, cfg: PipelineConfig) -> str:
    if getattr(ent._, "is_historical", False):
        return "past"
    sec = (_section_title_str(ent) or "").strip()
    if sec in cfg.past_sections:
        return "past"
    if sec in cfg.present_sections:
        return "present"
    return "unknown"


def compute_bmi(ht_m, wt_kg):
    if ht_m and wt_kg:
        return round(wt_kg / (ht_m ** 2), 1)
    return None


def categorize_bmi(bmi) -> Tuple[Optional[str], Optional[str]]:
    if bmi is None:
        return None, None
    if bmi < 18.5:
        return "Underweight", None
    if bmi < 25:
        return "Normal weight", None
    if bmi < 30:
        return "Overweight", None
    if bmi < 35:
        return "Obese", "Class 1"
    if bmi < 40:
        return "Obese", "Class 2"
    return "Obese", "Class 3"


def _clean_bmi_entity_text(text: str) -> str:
    return text.rstrip(".,;: ")


BMI_NUMBER_RE = re.compile(r"[0-9]{2}(?:\.[0-9])?")


def parse_bmi_mention(text: str, cfg: PipelineConfig) -> Tuple[Optional[float], Optional[bool]]:
    """Number in a BMI mention and whether it is within [min_bmi, max_bmi].
    Same pattern and range check that decide bmi_explicit."""
    m = BMI_NUMBER_RE.search(text)
    if not m:
        return None, False
    v = float(m.group())
    return v, cfg.min_bmi <= v <= cfg.max_bmi


def extract_note_fields(doc, cfg: PipelineConfig, note_id, subject_id, hadm_id) -> Dict[str, Any]:
    text_str = doc.text
    anthro_mentions = doc._.anthro_mentions or []
    ht_m, wt_kg = doc._.anthro_numeric or (None, None)

    ents_struct = []
    bmi_candidates: List[float] = []
    for ent in doc.ents:
        start, end = ent.start_char, ent.end_char
        snippet = text_str[max(0, start - CONTEXT_WINDOW): min(len(text_str), end + CONTEXT_WINDOW)]
        ent_text = _clean_bmi_entity_text(ent.text) if ent.label_ == "BMI_VALUE" else ent.text

        # Parse each BMI mention exactly once, here. Downstream consumers (e.g. the OMOP
        # loader) read "value"/"value_valid" instead of re-parsing the text.
        value, value_valid = None, None
        if ent.label_ == "BMI_VALUE":
            value, value_valid = parse_bmi_mention(ent.text, cfg)
            if value_valid:
                bmi_candidates.append(value)

        ents_struct.append({
            "text": ent_text,
            "label": ent.label_,
            "is_negated": bool(getattr(ent._, "is_negated", False)),
            "is_uncertain": bool(getattr(ent._, "is_uncertain", False)),
            "is_historical": bool(getattr(ent._, "is_historical", False)),
            "section": _section_title_str(ent),
            "temporality": determine_temporality(ent, cfg),
            "start_char": ent.start_char,
            "end_char": ent.end_char,
            "context_snippet": snippet,
            "value": value,              # parsed BMI number (BMI_VALUE only, else null)
            "value_valid": value_valid,  # within Min/Max BMI (BMI_VALUE only, else null)
        })

    bmi_explicit = bmi_candidates[0] if bmi_candidates else None
    bmi_derived = compute_bmi(ht_m, wt_kg)
    bmi_final = bmi_explicit if bmi_explicit is not None else bmi_derived
    weight_status_from_bmi, obesity_class = categorize_bmi(bmi_final)

    ws_text = None
    for e in ents_struct:
        if e["label"] in ("WEIGHT_STATUS", "CONDITION"):
            if cfg.require_present_for_text_status and e["temporality"] == "past":
                continue
            if e["is_negated"]:
                continue
            t = e["text"].lower()
            if "underweight" in t:
                ws_text = "Underweight"; break
            if "overweight" in t:
                ws_text = "Overweight"; break
            if "normal weight" in t or "healthy weight" in t:
                ws_text = "Normal weight"; break
            if "obes" in t:
                ws_text = "Obese"; break

    weight_status_final = (weight_status_from_bmi
                           if (cfg.prefer_numeric_bmi and weight_status_from_bmi)
                           else (weight_status_from_bmi or ws_text))
    # NOTE: kept exactly as in the notebook. categorize_bmi()/ws_text never produce
    # "Obesity" (they produce "Obese"), so this flag is always 0 or None. Change
    # "Obesity" -> "Obese" here if a real obesity flag is intended.
    obesity_flag = 1 if weight_status_final == "Obesity" else (0 if weight_status_final is not None else None)

    return {
        "subject_id": subject_id,
        "hadm_id": hadm_id,
        "note_id": note_id,
        "entities": ents_struct,
        "anthro_mentions": anthro_mentions,
        "height_m": ht_m,
        "weight_kg": wt_kg,
        "bmi_explicit": bmi_explicit,
        "bmi_derived": bmi_derived,
        "bmi_final": bmi_final,
        "weight_status_from_bmi": weight_status_from_bmi,
        "weight_status_from_text": ws_text,
        "weight_status_final": weight_status_final,
        "obesity_class": obesity_class,
        "obesity_flag": obesity_flag,
    }


def _parse_input(raw: str) -> List[Dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


# =====================================================================================
# NiFi processor
# =====================================================================================

class WeightBmiMedspacy(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        # Bump the version whenever dependencies change: NiFi keeps one dependency
        # folder per processor version, so a new version forces a clean install.
        version = '1.0.3'   # 1.0.3: BMI_VALUE entities carry "value" and "value_valid"
        description = ('Runs the medspaCy weight/BMI pipeline over a JSON array of clinical notes '
                       '(note_id, subject_id, hadm_id, text) and writes one JSON document per note '
                       'with entities, anthropometric mentions, BMI and weight status.')
        tags = ['medspacy', 'spacy', 'nlp', 'clinical', 'bmi', 'weight', 'obesity']
        # Pin these to the versions your notebook environment uses (pip freeze | grep -i spacy).
        # On Python < 3.12, medspacy 1.3.1 pins spacy < 3.8, which brings a thinc 8.2.x
        # compiled against numpy 1.x. Loading numpy 2.x with it fails with "numpy.dtype size
        # changed ... Expected 96 from C header, got 88". So keep numpy 1.x here; the
        # sys.path reordering at the top of this file makes it win over the numpy 2.x in
        # NiFi's python/framework folder.
        dependencies = ['medspacy==1.3.1', 'numpy<2']

    TEXT_FIELD = PropertyDescriptor(
        name='Text Field', description='Name of the field holding the note text.',
        required=True, default_value='text',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    BATCH_SIZE = PropertyDescriptor(
        name='nlp.pipe Batch Size', description='batch_size passed to nlp.pipe().',
        required=True, default_value='50',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])
    POSITIVE_ONLY = PropertyDescriptor(
        name='Emit Only Positive Notes',
        description='If true, only notes with a non-null weight_status_final are written '
                    '(the notebook behaviour).',
        required=True, default_value='true', allowable_values=['true', 'false'])
    MIN_BMI = PropertyDescriptor(
        name='Min BMI', description='Lowest explicit BMI value accepted.',
        required=True, default_value='5.0',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])
    MAX_BMI = PropertyDescriptor(
        name='Max BMI', description='Highest explicit BMI value accepted.',
        required=True, default_value='90.0',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR])

    def __init__(self, **kwargs):
        super().__init__()
        self.descriptors = [self.TEXT_FIELD, self.BATCH_SIZE, self.POSITIVE_ONLY,
                            self.MIN_BMI, self.MAX_BMI]
        self.nlp = None
        self.cfg = None

    def getPropertyDescriptors(self):
        return self.descriptors

    def onScheduled(self, context):
        # Expensive: build the pipeline once per Python process, reuse for every FlowFile.
        self.cfg = PipelineConfig(
            min_bmi=float(context.getProperty(self.MIN_BMI).getValue()),
            max_bmi=float(context.getProperty(self.MAX_BMI).getValue()),
        )
        import numpy
        self.logger.info(f'WeightBmiMedspacy using numpy {numpy.__version__} from {numpy.__file__}')
        self.nlp = build_nlp(self.cfg)

    def transform(self, context, flowfile):
        text_field = context.getProperty(self.TEXT_FIELD).getValue()
        batch_size = int(context.getProperty(self.BATCH_SIZE).getValue())
        positive_only = context.getProperty(self.POSITIVE_ONLY).getValue() == 'true'

        try:
            rows = _parse_input(flowfile.getContentsAsBytes().decode('utf-8'))
        except Exception as e:
            self.logger.error(f'Could not parse FlowFile content as JSON: {e}')
            return FlowFileTransformResult(relationship='failure')

        rows = [r for r in rows if r.get(text_field) is not None]  # WHERE text IS NOT NULL
        texts_with_ctx = ((r[text_field], (r.get('note_id'), r.get('subject_id'), r.get('hadm_id')))
                          for r in rows)

        out: List[Dict[str, Any]] = []
        last_note_id = None
        try:
            for doc, (note_id, subject_id, hadm_id) in self.nlp.pipe(
                    texts_with_ctx, as_tuples=True, batch_size=batch_size):
                last_note_id = note_id
                result = extract_note_fields(doc, self.cfg, note_id, subject_id, hadm_id)
                if positive_only and result['weight_status_final'] is None:
                    continue
                out.append(result)
        except Exception as e:
            self.logger.error(f'medspaCy processing failed after note_id={last_note_id}: {e}')
            return FlowFileTransformResult(relationship='failure')

        return FlowFileTransformResult(
            relationship='success',
            contents=json.dumps(out, ensure_ascii=False),
            attributes={
                'mime.type': 'application/json',
                'record.count': str(len(out)),
                'medspacy.notes.in': str(len(rows)),
                'medspacy.notes.positive': str(len(out)),
                'medspacy.last.note_id': str(last_note_id) if last_note_id is not None else '',
            },
        )
