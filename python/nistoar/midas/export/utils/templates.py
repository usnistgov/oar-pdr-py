from __future__ import annotations
from pathlib import Path
from typing import Iterable, Union, Optional


class TemplateResolver:
    """
    Resolves template files from one or more root directories.
    Directory layout is unified: <root>/<format>/<template_filename>
    """
    def __init__(self, roots: Optional[Iterable[Union[str, Path]]] = None):
        if roots:
            self.roots = [Path(r) for r in roots]
        else:
            # default to package templates dir
            self.roots = [(Path(__file__).resolve().parents[1] / "templates")]

    def resolve(self, format_subdir: str, template_filename: str) -> Path:
        cand = [r / format_subdir / template_filename for r in self.roots]
        for p in cand:
            if p.is_file():
                return p
        # fall back to first candidate (even if missing) so callers see a clear error
        return cand[0]
