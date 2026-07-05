# Bundled vocabulary crosswalks

## `icd9cm_to_icd10cm_gem.parquet`

CMS General Equivalence Mappings (GEM), ICD-9-CM → ICD-10-CM diagnosis
crosswalk, 2018 final release (23,912 rows). Produced by the US Centers for
Medicare & Medicaid Services / NCHS and in the public domain as a US
government work. Obtained via the MIT-licensed
[ICD-Mappings](https://github.com/snovaisg/ICD-Mappings) repository
(`icdmappings/data_files/icd9toicd10cmgem.csv`), which redistributes the CMS
file unchanged; the same crosswalk underlies the ICD harmonisation in ETHOS.

Columns: `icd9cm`, `icd10cm` (undotted, uppercase code format), `flags`, and
the unpacked flag fields `approximate`, `no_map`, `combination`, `scenario`,
`choice_list` as defined in the CMS GEM documentation. The file is the full
crosswalk; the one-to-one simplification (drop `no_map`, first row per source
code) is applied at load time in
`open_icu.steps.concept.transformers.icd.load_gem_lookup`, so alternative
mapping policies can be built on the same data.
