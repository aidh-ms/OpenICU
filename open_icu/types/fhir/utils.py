from open_icu.types.fhir.types import Reference


def to_identifiers_str(identifiers: dict[str, str] | list[Reference]) -> str:
    if isinstance(identifiers, list):
        identifiers = {ref["type"]: ref["reference"] for ref in identifiers}

    return ";".join(f"{ref_type}::{ref}" for ref_type, ref in identifiers.items())


def to_identifiers_reference(identifiers: str) -> list[Reference]:
    def _to_reference(id: str) -> Reference:
        ref_type, ref = id.split("::", maxsplit=1)
        return Reference(reference=ref.strip(), type=ref_type.strip())

    return [_to_reference(id) for id in identifiers.split(";")]
