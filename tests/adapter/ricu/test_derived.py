from open_icu.adapter.converters.ricu.loader import load_concept_dict
from open_icu.adapter.converters.ricu.mapper import RICUToOpenICUMapper
from open_icu.adapter.converters.ricu.settings import ConverterSettings


def test_mapper_generates_known_derived_bmi(tmp_path):
    concept_dict = tmp_path / "concept-dict.json"
    concept_dict.write_text(
        """
        {
          "weight": {"description": "patient weight", "category": "demographics", "sources": {}},
          "height": {"description": "patient height", "category": "demographics", "sources": {}},
          "bmi": {
            "description": "patient body mass index",
            "category": "demographics",
            "class": "rec_cncpt",
            "concepts": ["weight", "height"],
            "callback": "bmi"
          }
        }
        """,
        encoding="utf-8",
    )
    concepts = load_concept_dict(concept_dict)
    mapper = RICUToOpenICUMapper(ConverterSettings.with_defaults())
    files = mapper.build_files(concepts, sources=["mimic"], include_derived=True)

    generated = {file.path: file.content for file in files}
    path = "dataset/mimic-iv/3.1/concept/body_mass_index.yml"
    assert path in generated
    assert generated[path]["type"] == "derived"
    assert generated[path]["table"]["concept"] == "patient_weight"
    assert generated[path]["join"][0]["concept"] == "patient_height"


def test_mapper_generates_complex_stub_when_requested(tmp_path):
    concept_dict = tmp_path / "concept-dict.json"
    concept_dict.write_text(
        """
        {
          "news": {
            "description": "national early warning score",
            "category": "outcome",
            "class": "rec_cncpt",
            "concepts": ["resp", "o2sat"],
            "callback": "news_score"
          }
        }
        """,
        encoding="utf-8",
    )
    concepts = load_concept_dict(concept_dict)
    mapper = RICUToOpenICUMapper(ConverterSettings.with_defaults())
    files = mapper.build_files(
        concepts,
        sources=["mimic"],
        include_derived=True,
        complex_stubs=True,
    )

    generated = {file.path: file.content for file in files}
    path = "dataset/mimic-iv/3.1/concept/national_early_warning_score.yml"
    assert path in generated
    assert generated[path]["type"] == "complex"
    assert generated[path]["concepts"] == ["respiratory_rate", "oxygen_saturation"]
