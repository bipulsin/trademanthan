"""
FinBERT (ProsusAI/finbert) — lazy singleton for financial phrase sentiment.

Requires: torch, transformers (see backend/requirements-ml.txt).
Model cache: ~/.cache/huggingface/hub (override with HF_HOME).
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Sequence, Union

logger = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "ProsusAI/finbert"

_model = None
_tokenizer = None
_lock = threading.Lock()


def is_finbert_available() -> bool:
    try:
        import torch  # noqa: F401
        from transformers import AutoModelForSequenceClassification  # noqa: F401
    except ImportError:
        return False
    return True


def get_finbert(model_id: str = DEFAULT_MODEL_ID):
    """Return (tokenizer, model) tuple; loads on first call."""
    global _model, _tokenizer
    if not is_finbert_available():
        raise RuntimeError("FinBERT deps missing: pip install torch and backend/requirements-ml.txt")
    with _lock:
        if _model is None or _tokenizer is None:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            logger.info("Loading FinBERT: %s", model_id)
            _tokenizer = AutoTokenizer.from_pretrained(model_id)
            _model = AutoModelForSequenceClassification.from_pretrained(model_id)
            _model.eval()
            if torch.cuda.is_available():
                _model = _model.cuda()
            logger.info("FinBERT ready (cuda=%s)", torch.cuda.is_available())
    return _tokenizer, _model


def predict_sentiment(
    texts: Union[str, Sequence[str]],
    model_id: str = DEFAULT_MODEL_ID,
) -> List[Dict[str, Any]]:
    """
    Run FinBERT on one or more short financial texts.

    Returns list of dicts: label (str), score (float 0–1), logits (optional).
    Label names follow the model config (typically positive / negative / neutral).
    """
    import torch

    if isinstance(texts, str):
        texts = [texts]
    tokenizer, model = get_finbert(model_id)
    device = next(model.parameters()).device

    enc = tokenizer(
        list(texts),
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512,
    )
    enc = {k: v.to(device) for k, v in enc.items()}

    with torch.no_grad():
        out = model(**enc)
        logits = out.logits
        probs = torch.nn.functional.softmax(logits, dim=-1)

    id2label = getattr(model.config, "id2label", None) or {}
    results: List[Dict[str, Any]] = []
    for i in range(probs.shape[0]):
        scores = probs[i].tolist()
        best_idx = max(range(len(scores)), key=lambda j: scores[j])
        label = id2label.get(best_idx, str(best_idx))
        results.append(
            {
                "label": label,
                "score": round(float(scores[best_idx]), 4),
                "scores": {id2label.get(j, str(j)): round(float(s), 4) for j, s in enumerate(scores)},
            }
        )
    return results


def preload(model_id: str = DEFAULT_MODEL_ID) -> None:
    """Download weights into HF cache if not present; loads model once."""
    get_finbert(model_id)
