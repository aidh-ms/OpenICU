from open_icu.adapter.converters.ricu.loader import load_concept_dict
from open_icu.adapter.converters.ricu.mapper import RICUToOpenICUMapper
from open_icu.adapter.converters.ricu.settings import ConverterSettings


def _abx_files(tmp_path, *, regex_prefix_mode="none", logical_columns_mode="preserve"):
    concept_dict = tmp_path / "concept-dict.json"
    concept_dict.write_text(
        '''
        {
          "abx": {
            "class": "lgl_cncpt",
            "description": "antibiotics",
            "category": "medications",
            "sources": {
              "mimic": [
                {
                  "regex": "aztreonam|bactrim",
                  "table": "prescriptions",
                  "sub_var": "drug",
                  "callback": "transform_fun(set_val(TRUE))",
                  "class": "rgx_itm"
                },
                {
                  "ids": [225798, 225837],
                  "table": "inputevents_mv",
                  "sub_var": "itemid",
                  "callback": "transform_fun(set_val(TRUE))"
                }
              ]
            }
          }
        }
        ''',
        encoding="utf-8",
    )
    concepts = load_concept_dict(concept_dict)
    settings = ConverterSettings.with_defaults()
    settings.concept_names["abx"] = "antibiotics"
    settings.regex_prefix_mode = regex_prefix_mode
    settings.logical_columns_mode = logical_columns_mode
    mapper = RICUToOpenICUMapper(settings)
    files = mapper.build_files(concepts, sources=["mimic"])
    dataset_file = next(f for f in files if f.path.endswith("dataset/mimic-iv/3.1/concept/antibiotics.yml"))
    return dataset_file.content


def test_default_abx_preserves_values_and_does_not_prefix_regex(tmp_path):
    content = _abx_files(tmp_path)
    mappings = content["mappings"]

    assert mappings[0]["pattern"]["code"] == "(aztreonam|bactrim)"
    assert mappings[0]["columns"] == {
        "numeric_value": "col(numeric_value)",
        "text_value": "col(text_value)",
    }
    assert mappings[1]["columns"] == {
        "numeric_value": "col(numeric_value)",
        "text_value": "col(text_value)",
    }


def test_abx_can_still_generate_contains_regex_and_boolean_values(tmp_path):
    content = _abx_files(
        tmp_path,
        regex_prefix_mode="contains",
        logical_columns_mode="boolean",
    )
    mappings = content["mappings"]

    assert mappings[0]["pattern"]["code"] == ".*?(aztreonam|bactrim)"
    assert mappings[1]["columns"] == {
        "numeric_value": "const(1)",
        "text_value": 'const("true")',
    }
