"""
Custom Keras CAPTCHA solver using pre-trained CTC (Type 1) and Classifier (Type 2) models.

Type 1 (CTC): For DC/HC ecourts securimage captchas — outputs 6-char alphanumeric text.
Type 2 (Classifier): For SC sci.gov.in math captchas — outputs answer (0–20).

Models are loaded once at startup and reused for all subsequent predictions.
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

logger = logging.getLogger("legal_scraper.captcha_model")

IMG_W, IMG_H = 200, 50

_BUNDLE_DIR = Path(__file__).resolve().parent.parent / "captcha_solver" / "bundles"

_singleton_lock = threading.Lock()
_solver_instance: CaptchaModelSolver | None = None


def get_solver() -> "CaptchaModelSolver":
    """Thread-safe singleton accessor — loads models on first call."""
    global _solver_instance
    if _solver_instance is not None:
        return _solver_instance
    with _singleton_lock:
        if _solver_instance is not None:
            return _solver_instance
        _solver_instance = CaptchaModelSolver()
        return _solver_instance


class CaptchaModelSolver:
    """Loads and runs Type 1 (CTC) and Type 2 (Classifier) Keras models."""

    def __init__(self) -> None:
        os.environ.setdefault("KERAS_BACKEND", "tensorflow")
        # Keep TensorFlow quiet and constrained in CPU-only containers.
        os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
        os.environ.setdefault("TF_NUM_INTRAOP_THREADS", "1")
        os.environ.setdefault("TF_NUM_INTEROP_THREADS", "1")

        import tensorflow as tf
        import keras
        from keras import layers

        try:
            tf.config.threading.set_intra_op_parallelism_threads(
                max(1, int(os.environ.get("TF_NUM_INTRAOP_THREADS", "1")))
            )
            tf.config.threading.set_inter_op_parallelism_threads(
                max(1, int(os.environ.get("TF_NUM_INTEROP_THREADS", "1")))
            )
        except Exception:
            # Thread config can be immutable in some TF builds; safe to ignore.
            pass

        self._tf = tf
        self._predict_semaphore = threading.BoundedSemaphore(
            max(1, int(os.environ.get("CAPTCHA_MODEL_MAX_CONCURRENCY", "2")))
        )

        # ── Type 1: CTC text recognition (DC/HC) ──
        t1_path = _BUNDLE_DIR / "type1"
        logger.info("Loading Type 1 CTC model from %s ...", t1_path)
        self._t1_model = keras.models.load_model(
            str(t1_path / "model.keras"), compile=False
        )
        with open(t1_path / "vocab.json") as f:
            t1_cfg = json.load(f)
        self._t1_vocab: list[str] = t1_cfg["vocab"]
        self._t1_maxlen: int = t1_cfg["maxlen"]
        self._t1_force_len: int = t1_cfg.get("force_len", 6)
        self._t1_num_to_char = layers.StringLookup(
            vocabulary=self._t1_vocab, mask_token=None, invert=True
        )
        logger.info(
            "  ✓ Type 1 loaded | vocab_size=%d maxlen=%d force_len=%d",
            len(self._t1_vocab), self._t1_maxlen, self._t1_force_len,
        )

        # ── Type 2: Classifier (SC math captchas) ──
        t2_path = _BUNDLE_DIR / "type2"
        logger.info("Loading Type 2 classifier model from %s ...", t2_path)
        self._t2_model = keras.models.load_model(
            str(t2_path / "model.keras"), compile=False
        )
        with open(t2_path / "vocab.json") as f:
            t2_cfg = json.load(f)
        self._t2_num_classes: int = t2_cfg["num_classes"]
        logger.info(
            "  ✓ Type 2 loaded | num_classes=%d", self._t2_num_classes,
        )

        # Warm-up once to reduce first-request latency and avoid repeated tracing.
        try:
            _ = self._t1_model(np.zeros((1, IMG_W, IMG_H, 1), dtype=np.float32), training=False)
            _ = self._t2_model(np.zeros((1, IMG_H, IMG_W, 1), dtype=np.float32), training=False)
        except Exception:
            pass

    # ─── Preprocessing ────────────────────────────────────────────

    @staticmethod
    def _preprocess_type1(image_bytes: bytes) -> np.ndarray:
        """PIL → (W=200, H=50, 1) float32 — CTC layout (transposed)."""
        img = Image.open(io.BytesIO(image_bytes)).convert("L").resize((IMG_W, IMG_H))
        arr = np.array(img, dtype=np.float32) / 255.0
        return arr.T[:, :, np.newaxis]

    @staticmethod
    def _preprocess_type2(image_bytes: bytes) -> np.ndarray:
        """PIL → (H=50, W=200, 1) float32 — standard classifier layout."""
        img = Image.open(io.BytesIO(image_bytes)).convert("L").resize((IMG_W, IMG_H))
        arr = np.array(img, dtype=np.float32) / 255.0
        return arr[:, :, np.newaxis]

    # ─── CTC Decoding ─────────────────────────────────────────────

    def _decode_ctc(self, pred_batch: np.ndarray) -> list[str]:
        """Greedy CTC decode → list of strings."""
        tf = self._tf
        logits = tf.math.log(tf.transpose(pred_batch, [1, 0, 2]) + 1e-8)
        seq_len = np.ones(pred_batch.shape[0], dtype=np.int32) * pred_batch.shape[1]
        decoded, _ = tf.nn.ctc_greedy_decoder(
            inputs=logits, sequence_length=seq_len
        )
        n, s = pred_batch.shape[0], pred_batch.shape[1]
        dense = tf.sparse.to_dense(
            tf.SparseTensor(decoded[0].indices, decoded[0].values, (n, s)),
            default_value=-1,
        ).numpy()

        results: list[str] = []
        for i in range(n):
            text = (
                tf.strings.reduce_join(
                    self._t1_num_to_char(dense[i][: self._t1_maxlen])
                )
                .numpy()
                .decode("utf-8")
                .replace("[UNK]", "")
            )
            # Pad to force_len if the model predicted fewer characters
            if self._t1_force_len and 0 < len(text) < self._t1_force_len:
                text = text + text[-1] * (self._t1_force_len - len(text))
            results.append(text)
        return results

    # ─── Public API ───────────────────────────────────────────────

    def predict_type1(self, image_bytes: bytes) -> str:
        """
        Predict DC/HC securimage text captcha.

        Returns 6-char lowercase alphanumeric string, or empty string on failure.
        """
        try:
            arr = np.array([self._preprocess_type1(image_bytes)], dtype=np.float32)
            with self._predict_semaphore:
                pred_raw = self._t1_model(arr, training=False)
            pred = pred_raw.numpy() if hasattr(pred_raw, "numpy") else pred_raw
            texts = self._decode_ctc(pred)  # type: ignore[arg-type]
            result = texts[0] if texts else ""
            logger.debug("  [Model T1] predicted: '%s'", result)
            return result
        except Exception as exc:
            logger.warning("Type 1 prediction failed: %s", exc)
            return ""

    def predict_type2(self, image_bytes: bytes) -> str:
        """
        Predict SC math captcha answer (0–20).

        Returns numeric answer string, or empty string on failure.
        """
        try:
            arr = np.array([self._preprocess_type2(image_bytes)], dtype=np.float32)
            with self._predict_semaphore:
                pred_raw = self._t2_model(arr, training=False)
            pred = pred_raw.numpy() if hasattr(pred_raw, "numpy") else pred_raw
            answer = str(int(np.argmax(pred[0])))
            logger.debug("  [Model T2] predicted: '%s'", answer)
            return answer
        except Exception as exc:
            logger.warning("Type 2 prediction failed: %s", exc)
            return ""
