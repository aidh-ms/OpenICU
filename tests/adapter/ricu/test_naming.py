from open_icu.adapter.converters.ricu.naming import snake_case


def test_snake_case_keeps_useful_acronyms():
    assert snake_case("diastolic blood pressure") == "diastolic_blood_pressure"
    assert snake_case("C-reactive protein") == "C_reactive_protein"
    assert snake_case("dextrose (as D10)") == "dextrose_as_D10"
