from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import logging
from typing import Any

import numpy as np
from sqlalchemy import bindparam, text

from app.config import settings
from app.utils.database import get_engine


logger = logging.getLogger(__name__)


try:
    from sentence_transformers import SentenceTransformer as _SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover
    _SentenceTransformer = None


try:
    from pgvector.sqlalchemy import Vector as _Vector  # type: ignore
except Exception:  # pragma: no cover
    _Vector = None


def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None

    if isinstance(obj, (np.floating, np.integer, np.bool_)):
        return obj.item()

    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]

    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}

    if isinstance(obj, float):
        # Convert nan/inf to None to keep JSON safe
        if not np.isfinite(obj):
            return None
        return float(obj)

    if isinstance(obj, (str, int, bool)):
        return obj

    return str(obj)


@dataclass(frozen=True)
class ColumnSearchResult:
    dataset_id: str
    column_name: str
    description: str | None
    score: float


class EmbeddingService:
    """Singleton wrapper around SentenceTransformer embeddings."""

    def __init__(self, model_name: str | None = None):
        self.model_name = model_name or settings.EMBEDDING_MODEL
        if _SentenceTransformer is None:
            raise RuntimeError(
                "sentence-transformers is not installed; install it to enable semantic column search"
            )
        # Loads weights (may download on first run)
        self._model = _SentenceTransformer(self.model_name)

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed texts into 384-dim vectors.

        Returns a JSON-safe list[list[float]].
        """

        if not texts:
            return []

        # Normalize embeddings so cosine distance behaves nicely.
        emb = self._model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        if emb.ndim == 1:
            emb = emb.reshape(1, -1)

        # Ensure plain Python floats
        return emb.astype(float).tolist()

    def search_columns(self, query: str, top_k: int = 10) -> list[dict[str, Any]]:
        """ANN search over column_registry embeddings.

        Runs:
          SELECT dataset_id, column_name, description,
                 1-(embedding <=> query_vec) AS score
          FROM column_registry
          ORDER BY embedding <=> query_vec
          LIMIT top_k
        """

        if not query.strip():
            return []

        engine = get_engine()
        if engine.dialect.name != "postgresql":
            logger.info("column_search_skipped_non_postgres", extra={"dialect": engine.dialect.name})
            return []

        if _Vector is None:
            raise RuntimeError("pgvector is not installed; install it to enable semantic column search")

        query_vec = self.embed([query])[0]

        stmt = (
            text(
                """
                SELECT
                    dataset_id::text AS dataset_id,
                    column_name,
                    description,
                    1 - (embedding <=> :query_vec) AS score
                FROM column_registry
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> :query_vec
                LIMIT :top_k
                """
            )
            .bindparams(
                bindparam("query_vec", value=query_vec, type_=_Vector(384)),
                bindparam("top_k", value=int(top_k)),
            )
        )

        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(
                _json_safe(
                    {
                        "dataset_id": row.get("dataset_id"),
                        "column_name": row.get("column_name"),
                        "description": row.get("description"),
                        "score": float(row.get("score")) if row.get("score") is not None else None,
                    }
                )
            )

        return results


@lru_cache
def get_embedding_service() -> EmbeddingService:
    """Returns a process-wide singleton embedding service."""

    return EmbeddingService()


def warmup_embedding_service() -> None:
    """Optional helper to load the model during app startup."""

    _ = get_embedding_service()
