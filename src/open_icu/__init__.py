"""
OpenICU: A Python package for analysing time series data in the ICU.

This package provides a set of tools for analysing time series data in the ICU.
It is designed to be modular and extensible, allowing users to easily add new data
sources, filters, and analyses.

Modules:
-

Usage:
```python

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

from open_icu.steps.extraction.step import ExtractionStep
from open_icu.storage.project import OpenICUProject

__all__ = [
    "OpenICUProject",
    "ExtractionStep",
]
