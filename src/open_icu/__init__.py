"""
OpenICU: A Python framework for extracting and analyzing ICU time series data.

This package provides tools for extracting, preprocessing, and analyzing ICU time
series data from diverse datasets (MIMIC, eICU, etc.). It converts heterogeneous
ICU data into the standardized MEDS (Medical Event Data Standard) format, enabling
reproducible research workflows.

Key Features:
- Support for multiple ICU data sources (MIMIC, eICU, custom datasets)
- Declarative YAML-based configuration for data extraction
- Export to MEDS format for standardized analysis
- Fully offline operation for medical data privacy compliance
- Modular and extensible architecture

Main Modules:
- callbacks: Data transformation callbacks for LazyFrames
- config: Configuration management and registry system
- storage: Project, dataset, and workspace management
- steps: Processing steps (extraction, concept)
- utils: Utility functions and helpers

Usage:
```python
from pathlib import Path

from open_icu import OpenICUProject, ExtractionStep


config_path = Path.cwd() / "config"
project_path = Path.cwd() / "output" / "project"

# Create a new OpenICU project
with OpenICUProject(project_path) as project:
    # Load and run an extraction step
    extraction_step = ExtractionStep.load(project, config_path / "extraction.yml")
    extraction_step.run()
```

Author:
- Paul Brauckmann
- Christian Porschen

License:
```
MIT License

Copyright (c) 2023 AIDH MS

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
"""

from open_icu.steps.concept.step import ConceptStep
from open_icu.steps.extraction.step import ExtractionStep
from open_icu.storage.project import OpenICUProject

__all__ = [
    "OpenICUProject",
    "ExtractionStep",
    "ConceptStep",
]
