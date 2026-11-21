"""Local CPU embeddings via fastembed (quantised ONNX), L2-normalised."""

from __future__ import annotations

import math

from fastembed import TextEmbedding


class Embedder:
    def __init__(self, model: str) -> None:
        self._model = model
        self._engine: TextEmbedding | None = None

    def _get(self) -> TextEmbedding:
        if self._engine is None:
            self._engine = TextEmbedding(model_name=self._model)
        return self._engine

    def embed(self, text: str) -> list[float]:
        """One vector for one text. Empty text must be handled by the caller."""
        vector = next(iter(self._get().embed([text])))
        values = [float(v) for v in vector]
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]
