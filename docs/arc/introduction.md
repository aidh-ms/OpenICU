# Introduction and Goals

The use of data from Intensive Care Units (ICUs) holds significant potential for advancing both clinical decision-making and medical research. However, the practical utilization of such data is often impeded by its inherent complexity and the heterogeneity of data sources. Effective analysis requires not only domain-specific clinical knowledge but also specialized skills in data engineering — a combination that is not always readily available.

The OpenICU Project addresses this challenge by providing an open-source, Python-based framework specifically designed to streamline the extraction, preprocessing, and analysis of ICU time series data. By enabling standardized and transparent data workflows, OpenICU empowers clinicians, researchers, and developers to work collaboratively and reproducibly with diverse ICU datasets. The framework is built with openness, extensibility, and ease of use in mind, making it a valuable tool in the growing field of data-driven intensive care.

## Requirements Overview

| Id | Requirement | Explanation |
| -- | ----------- | ----------- |
| F1 | Data Sources | The system must support multiple types of data sources for ICU datasets. |
| F1.1 | Public Dataset Support | Must allow integration of publicly available ICU datasets (e.g., MIMIC, eICU, HiRID, AUCMCdb, DICdb). |
| F1.1.1 | Public Dataset Versioning | The system must support clear handling of dataset versions, ensuring reproducibility by allowing users to select and lock specific dataset versions. |
| F1.2 | Custom Dataset Support | Users must be able to load their own institutional data in tabular or SQL-based form. |
| F1.3 | Data Privacy | Data should not leave the user’s secure perimeter; the system must support fully offline usage to comply with medical data protection regulations. |
| F2 | Medical Concept Structure | Medical concepts must be clearly defined and reusable across workflows. |
| F2.1 | Unit Harmonization | Concepts must include logic to normalize units (e.g., mmHg vs kPa) to enable consistent interpretation. |
| F2.2 | Coding & Mapping | Concepts should support mappings to standard terminologies like SNOMED CT, LOINC, and ICD-11 to enhance interoperability. |
| F2.3 | Predefined Concepts | A curated set of commonly used medical concepts should be bundled with the tool to accelerate adoption and promote standardization. |
| F3 | Base Concept | A base concept represents a direct clinical observation or variable extracted from raw data (e.g., heart rate, lab values). |
| F4 | Derived Concept | A derived concept is computed from other concepts (base or derived) using deterministic logic or formulas (e.g., Acute Kidney Injury [AKI]). |
| F5 | Extraction of Medical Concepts | The system should enable bulk extraction of defined concepts from one or more data sources with minimal manual intervention. |
| F6 | Export Formats | Processed data should be exportable in both long format (event stream) and wide format (pivoted with one column per concept). |
| F7 | Python Interface | The tool should offer a native Python API to enable integration with existing Python workflows and use within IPython/Jupyter notebooks. |
| F8 | Command-Line Interface (CLI) | A CLI should support scripting and automation for advanced users and integration into data pipelines. |
| F9 | User Interface (UI) | A graphical user interface should be provided to make configuration and execution accessible to clinical or non-technical users. |

## Quality goals



## Stakeholder

The stakeholder landscape primarily comprises two closely related groups, both sharing professional backgrounds as researchers or physicians:

1. **Primary User Group**: This group focuses on leveraging the tool to extract standardized data from existing datasets, primarily for research activities. Their main interest lies in the usability, accuracy, and efficiency of data retrieval for scientific or clinical studies.
1. **Contributor Group**: In addition to using the tool for research purposes, this group actively participates in its development and enhancement. Their dual role includes providing user feedback, specifying requirements, and contributing to technical or conceptual improvements.

For more information, please refer to the [arc42](https://docs.arc42.org/section-1/) documentation.
