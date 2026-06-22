from open_icu.adapter.converters.ricu.loader import load_concept_dict
from open_icu.adapter.converters.ricu.mapper import RICUToOpenICUMapper
from open_icu.adapter.converters.ricu.settings import ConverterSettings


def test_mapper_generates_mimic_dbp(tmp_path):
    concept_dict = tmp_path / "concept-dict.json"
    concept_dict.write_text(
        """
        {
          "dbp": {
            "unit": ["mmHg", "mm Hg"],
            "description": "diastolic blood pressure",
            "category": "vitals",
            "sources": {
              "mimic": [
                {"ids": [220051, 220180], "table": "chartevents", "sub_var": "itemid"}
              ]
            }
          }
        }
        """,
        encoding="utf-8",
    )
    concepts = load_concept_dict(concept_dict)
    settings = ConverterSettings.with_defaults()
    settings.concept_names["dbp"] = "diastolic_blood_pressure"
    mapper = RICUToOpenICUMapper(settings)
    files = mapper.build_files(concepts, sources=["mimic"])

    paths = {file.path for file in files}
    assert "concept/vitals/diastolic_blood_pressure.yml" in paths
    assert "dataset/mimic-iv/3.1/concept/diastolic_blood_pressure.yml" in paths
