"""Known mappings for RICU recursive concepts.

RICU calls these ``rec_cncpt``. OpenICU does not have a separate ``rec``
concept type; simple formula-like recursive concepts can be represented as
``type: derived``. Recursive concepts that need time windows, gap logic,
custom R callbacks, or multi-step clinical scoring should be emitted as
``type: complex`` stubs or reported as unsupported.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .models import RICUConcept
from .naming import concept_name

if TYPE_CHECKING:
    from .settings import ConverterSettings, SourceTarget


@dataclass(frozen=True)
class DerivedRule:
    """Declarative rule for one RICU rec_cncpt -> OpenICU derived concept."""

    ricu_key: str
    dependency_keys: tuple[str, ...]
    aliases: tuple[str, ...]
    numeric_value: str
    filters: tuple[str, ...]
    how: str = "inner"
    join_on: tuple[str, ...] = ("subject_id", "time")
    note: str | None = None


# Conservative known rules. These are intentionally limited to concepts that
# can be expressed with OpenICU's existing parse_expr DSL without custom Python.
# Anything involving rolling windows, scores, ventilation intervals, infection
# suspicion, or vasopressor duration is better treated as complex.
KNOWN_DERIVED_RULES: dict[str, DerivedRule] = {
    "bmi": DerivedRule(
        ricu_key="bmi",
        dependency_keys=("weight", "height"),
        aliases=("weight", "height"),
        numeric_value="weight / Pow(height / 100, 2)",
        filters=("weight > 0", "height > 0"),
        note="Assumes patient_height is stored in cm and patient_weight in kg.",
    ),
    "pafi": DerivedRule(
        ricu_key="pafi",
        dependency_keys=("po2", "fio2"),
        aliases=("po2", "fio2"),
        numeric_value="po2 / fio2",
        filters=("po2 > 0", "fio2 > 0"),
        note="Assumes FiO2 is already represented as a fraction. If your FiO2 concept stores percent, divide by fio2 / 100 first.",
    ),
    "safi": DerivedRule(
        ricu_key="safi",
        dependency_keys=("o2sat", "fio2"),
        aliases=("o2sat", "fio2"),
        numeric_value="o2sat / fio2",
        filters=("o2sat > 0", "fio2 > 0"),
        note="Assumes FiO2 is already represented as a fraction. If your FiO2 concept stores percent, divide by fio2 / 100 first.",
    ),
    "gcs": DerivedRule(
        ricu_key="gcs",
        dependency_keys=("egcs", "mgcs", "vgcs"),
        aliases=("gcs_eye", "gcs_motor", "gcs_verbal"),
        numeric_value="gcs_eye + gcs_motor + gcs_verbal",
        filters=("gcs_eye >= 1", "gcs_motor >= 1", "gcs_verbal >= 1"),
        note="Uses eye + motor + verbal only. RICU also references sedation/ETT helper concepts; those require custom complex logic if needed.",
    ),
}

# Good default names for RICU abbreviations that are commonly used as derived
# dependencies. User settings can still override these.
DEFAULT_RECURSIVE_NAME_OVERRIDES: dict[str, str] = {
    "bmi": "body_mass_index",
    "weight": "patient_weight",
    "height": "patient_height",
    "gcs": "Glasgow_coma_scale_non_sedated",
    "tgcs": "GCS_total",
    "egcs": "GCS_eye",
    "mgcs": "GCS_motor",
    "vgcs": "GCS_verbal",
    "po2": "O2_partial_pressure",
    "fio2": "fraction_of_inspired_oxygen",
    "o2sat": "oxygen_saturation",
    "pafi": "Horowitz_index",
    "safi": "SaO2_FiO2",
    "sbp": "systolic_blood_pressure",
    "dbp": "diastolic_blood_pressure",
    "map": "mean_arterial_pressure",
    "hr": "heart_rate",
    "resp": "respiratory_rate",
    "temp": "temperature",
    "wbc": "white_blood_cell_count",
    "plt": "platelet_count",
    "bili": "total_bilirubin",
    "crea": "creatinine",
    "urine": "urine_output",
    "urine24": "urine_output_per_24h",
    "abx": "antibiotics",
    "samp": "body_fluid_sampling",
    "vent_start": "ventilation_start",
    "vent_end": "ventilation_end",
    "mech_vent": "mechanical_ventilation_windows",
    "vent_ind": "ventilation_durations",
    "supp_o2": "supplemental_oxygen",
    "avpu": "AVPU_scale",
    "sofa": "sequential_organ_failure_assessment_score",
    "sofa_resp": "SOFA_respiratory_component",
    "sofa_coag": "SOFA_coagulation_component",
    "sofa_liver": "SOFA_liver_component",
    "sofa_cardio": "SOFA_cardiovascular_component",
    "sofa_cns": "SOFA_central_nervous_system_component",
    "sofa_renal": "SOFA_renal_component",
    "qsofa": "quick_SOFA_score",
    "sirs": "systemic_inflammatory_response_syndrome_score",
    "mews": "modified_early_warning_score",
    "news": "national_early_warning_score",
    "norepi_equiv": "norepinephrine_equivalents",
    "susp_inf": "suspected_infection",
    "vaso_ind": "vasopressor_indicator",
    "dopa60": "dopamine_administration_for_min_1h",
    "norepi60": "norepinephrine_administration_for_min_1h",
    "dobu60": "dobutamine_administration_for_min_1h",
    "epi60": "epinephrine_administration_for_min_1h",
    "dopa_rate": "dopamine_rate",
    "norepi_rate": "norepinephrine_rate",
    "dobu_rate": "dobutamine_rate",
    "epi_rate": "epinephrine_rate",
    "adh_rate": "vasopressin_rate",
    "phn_rate": "phenylephrine_rate",
    "dopa_dur": "dopamine_duration",
    "norepi_dur": "norepinephrine_duration",
    "dobu_dur": "dobutamine_duration",
    "epi_dur": "epinephrine_duration",
}


def dependency_keys(concept: RICUConcept) -> list[str]:
    """Return RICU dependency keys from the raw ``concepts`` field."""

    deps = concept.raw.get("concepts")
    if deps is None:
        return []
    if isinstance(deps, list):
        return [str(dep) for dep in deps]
    return [str(deps)]


def concept_name_for_key(
    ricu_key: str,
    concepts: dict[str, RICUConcept],
    settings: "ConverterSettings",
) -> str:
    """Resolve a RICU key to the OpenICU concept name used by this converter."""

    if ricu_key in settings.concept_names:
        return settings.concept_names[ricu_key]
    if ricu_key in DEFAULT_RECURSIVE_NAME_OVERRIDES:
        return DEFAULT_RECURSIVE_NAME_OVERRIDES[ricu_key]
    concept = concepts.get(ricu_key)
    if concept is not None:
        return concept_name(concept.key, concept.description, settings.concept_names)
    return ricu_key


def build_derived_dataset_config(
    *,
    rule: DerivedRule,
    concept: RICUConcept,
    concepts: dict[str, RICUConcept],
    settings: "ConverterSettings",
    target: "SourceTarget",
) -> dict[str, Any]:
    """Build a ``type: derived`` dataset concept YAML payload."""

    deps = [concept_name_for_key(dep, concepts, settings) for dep in rule.dependency_keys]
    aliases = list(rule.aliases)
    if len(deps) != len(aliases):
        raise ValueError(f"bad derived rule for {rule.ricu_key}: dependencies and aliases differ")

    def table_config(dep_name: str, alias: str) -> dict[str, Any]:
        return {
            "concept": dep_name,
            "columns": ["subject_id", "time", "numeric_value"],
            "callbacks": [f'col(numeric_value, output="{alias}")'],
        }

    table = table_config(deps[0], aliases[0])
    joins = []
    for dep_name, alias in zip(deps[1:], aliases[1:]):
        join_cfg = table_config(dep_name, alias)
        join_cfg.update({
            "type": "join",
            "both_on": list(rule.join_on),
            "how": rule.how,
        })
        joins.append(join_cfg)

    return {
        "type": "derived",
        "table": table,
        "join": joins,
        "event": {
            "subject_id": "col(subject_id)",
            "time": "col(time)",
            "numeric_value": rule.numeric_value,
            "text_value": "const(None)",
            "extension": {
                "dataset": f'const("{target.dataset}")',
                "table": 'const("derived")',
            },
        },
        "filters": list(rule.filters),
    }


def build_complex_dataset_config(
    *,
    concept: RICUConcept,
    concepts: dict[str, RICUConcept],
    settings: "ConverterSettings",
    transformer_path: str,
) -> dict[str, Any]:
    """Build a ``type: complex`` stub for a RICU recursive concept."""

    deps = dependency_keys(concept)
    return {
        "type": "complex",
        "concepts": [concept_name_for_key(dep, concepts, settings) for dep in deps],
        "concept_transformer": transformer_path,
        "kwargs": {
            "ricu_key": concept.key,
            "ricu_callback": concept.callback,
            "ricu_dependencies": deps,
            "note": (
                "Generated complex stub. Implement this transformer before running the OpenICU concept step "
                "or keep complex stub generation disabled."
            ),
        },
    }
