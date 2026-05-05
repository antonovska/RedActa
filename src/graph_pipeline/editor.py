from __future__ import annotations

from .editor_v2 import EditorV2


class PipelineEditor(EditorV2):
    """Production editor — inherits full edit() with drift tracking from EditorV2."""
