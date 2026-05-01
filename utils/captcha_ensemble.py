"""
Ensemble CAPTCHA solver combining Keras model + ddddocr for higher accuracy.

Runs both solvers on preprocessed images and picks the best answer.
Tracks per-solver accuracy at runtime to auto-weight decisions.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("legal_scraper.captcha_ensemble")


@dataclass(frozen=True, slots=True)
class CaptchaPrediction:
    """A captcha answer plus the solver source used for runtime feedback."""

    answer: str
    solver: str


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# ── Lazy singleton ────────────────────────────────────────────
_singleton_lock = threading.Lock()
_solver_instance: EnsembleSolver | None = None


def get_ensemble_solver() -> "EnsembleSolver":
    """Thread-safe singleton — loads all solvers on first call."""
    global _solver_instance
    if _solver_instance is not None:
        return _solver_instance
    with _singleton_lock:
        if _solver_instance is not None:
            return _solver_instance
        _solver_instance = EnsembleSolver()
        return _solver_instance


class _AccuracyTracker:
    """Lightweight thread-safe tracker for per-solver accuracy."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.Lock()
        self._total = 0
        self._accepted = 0

    def record(self, accepted: bool) -> None:
        with self._lock:
            self._total += 1
            if accepted:
                self._accepted += 1

    @property
    def accuracy(self) -> float:
        with self._lock:
            if self._total == 0:
                return 0.5  # no data yet, neutral
            return self._accepted / self._total

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    def __repr__(self) -> str:
        return f"{self.name}: {self.accuracy:.1%} ({self._accepted}/{self._total})"


class EnsembleSolver:
    """
    Combines Keras ONNX/TF model + ddddocr for best accuracy.

    For Type 1 (DC/HC text captcha): both solvers produce text predictions.
    For Type 2 (SC math captcha): Keras classifier + ddddocr text→math eval.
    """

    def __init__(self) -> None:
        self._keras_solver: Any = None
        self._ddddocr: Any = None
        self._preprocess = _env_bool("CAPTCHA_PREPROCESS", True)
        self._mode = os.environ.get("CAPTCHA_SOLVER_MODE", "ensemble").strip().lower()
        self._predict_sem = threading.BoundedSemaphore(
            max(1, int(os.environ.get("CAPTCHA_MODEL_MAX_CONCURRENCY", "4")))
        )

        # Per-solver accuracy trackers (updated externally via record_feedback)
        self.keras_t1_tracker = _AccuracyTracker("keras_t1")
        self.keras_t2_tracker = _AccuracyTracker("keras_t2")
        self.ddddocr_t1_tracker = _AccuracyTracker("ddddocr_t1")
        self.ddddocr_t2_tracker = _AccuracyTracker("ddddocr_t2")

        self._init_keras()
        self._init_ddddocr()

    def _init_keras(self) -> None:
        """Load the existing Keras/ONNX model solver."""
        if self._mode == "ddddocr_only":
            logger.info("CAPTCHA_SOLVER_MODE=ddddocr_only — skipping Keras model")
            return
        try:
            from utils.captcha_model import get_solver
            self._keras_solver = get_solver()
            logger.info("Keras CAPTCHA solver loaded")
        except Exception as exc:
            logger.warning("Failed to load Keras solver: %s", exc)

    def _init_ddddocr(self) -> None:
        """Load ddddocr for generic OCR."""
        if self._mode == "keras_only":
            logger.info("CAPTCHA_SOLVER_MODE=keras_only — skipping ddddocr")
            return
        try:
            import ddddocr
            self._ddddocr = ddddocr.DdddOcr(show_ad=False)
            logger.info("ddddocr CAPTCHA solver loaded")
        except Exception as exc:
            logger.warning(
                "Failed to load ddddocr (pip install ddddocr): %s", exc
            )

    def _preprocess_image(self, image_bytes: bytes, captcha_type: int) -> bytes:
        """Apply image preprocessing if enabled."""
        if not self._preprocess:
            return image_bytes
        try:
            from utils.captcha_preprocess import preprocess_type1, preprocess_type2
            if captcha_type == 1:
                return preprocess_type1(image_bytes)
            return preprocess_type2(image_bytes)
        except Exception as exc:
            logger.debug("Preprocessing failed, using raw: %s", exc)
            return image_bytes

    # ── Type 1: DC/HC text captcha ────────────────────────────

    def _keras_predict_t1(self, image_bytes: bytes) -> str:
        if not self._keras_solver:
            return ""
        try:
            with self._predict_sem:
                return self._keras_solver.predict_type1(image_bytes)
        except Exception as exc:
            logger.debug("Keras T1 failed: %s", exc)
            return ""

    def _ddddocr_predict_t1(self, image_bytes: bytes) -> str:
        if not self._ddddocr:
            return ""
        try:
            result = self._ddddocr.classification(image_bytes)
            # securimage captchas are 6-char lowercase alphanumeric
            cleaned = re.sub(r"[^a-z0-9]", "", str(result).lower())
            return cleaned
        except Exception as exc:
            logger.debug("ddddocr T1 failed: %s", exc)
            return ""

    def predict_type1(self, image_bytes: bytes) -> str:
        """Predict DC/HC securimage captcha text."""
        return self.predict_type1_with_source(image_bytes).answer

    def predict_type1_with_source(self, image_bytes: bytes) -> CaptchaPrediction:
        """
        Predict DC/HC securimage captcha text.

        Runs both solvers on preprocessed + raw image, picks best.
        """
        preprocessed = self._preprocess_image(image_bytes, captcha_type=1)

        keras_result = ""
        ddddocr_result = ""

        if self._mode in {"ensemble", "keras_only"}:
            # Keras model was trained on specific preprocessing, try both
            keras_result = self._keras_predict_t1(preprocessed)
            if not keras_result:
                keras_result = self._keras_predict_t1(image_bytes)

        if self._mode in {"ensemble", "ddddocr_only"}:
            ddddocr_result = self._ddddocr_predict_t1(preprocessed)
            if not ddddocr_result:
                ddddocr_result = self._ddddocr_predict_t1(image_bytes)

        answer, solver = self._pick_best_t1(keras_result, ddddocr_result)
        return CaptchaPrediction(answer=answer, solver=solver)

    def _pick_best_t1(self, keras: str, ddddocr: str) -> tuple[str, str]:
        """Pick the best Type 1 prediction based on runtime accuracy."""
        if keras and not re.fullmatch(r"[a-z0-9]{6}", keras):
            keras = ""
        if ddddocr and not re.fullmatch(r"[a-z0-9]{6}", ddddocr):
            ddddocr = ""

        # Both empty — nothing to do
        if not keras and not ddddocr:
            return "", "none"

        # Only one produced a result
        if not keras:
            return ddddocr, "ddddocr"
        if not ddddocr:
            return keras, "keras"

        # Both agree
        if keras == ddddocr:
            return keras, "both"

        # Both disagree — pick based on runtime accuracy tracking
        keras_acc = self.keras_t1_tracker.accuracy
        ddddocr_acc = self.ddddocr_t1_tracker.accuracy

        # Need minimum 10 samples before trusting accuracy data
        if self.keras_t1_tracker.total < 10 and self.ddddocr_t1_tracker.total < 10:
            # No data yet — prefer keras (domain-trained)
            return keras, "keras"

        # Prefer the solver with higher accuracy, with a small bias toward keras
        if keras_acc >= ddddocr_acc:
            return keras, "keras"
        return ddddocr, "ddddocr"

    # ── Type 2: SC math captcha ───────────────────────────────

    def _keras_predict_t2(self, image_bytes: bytes) -> tuple[str, float]:
        """Returns (answer_str, confidence)."""
        if not self._keras_solver:
            return "", 0.0
        try:
            with self._predict_sem:
                answer = self._keras_solver.predict_type2(image_bytes)
            if not answer:
                return "", 0.0
            # Keras model returns the argmax answer — confidence unknown
            # Default to medium confidence; let ensemble comparisons decide
            return answer, 0.65
        except Exception as exc:
            logger.debug("Keras T2 failed: %s", exc)
            return "", 0.0

    def _ddddocr_predict_t2(self, image_bytes: bytes) -> str:
        """Read math expression via OCR and evaluate it."""
        if not self._ddddocr:
            return ""
        try:
            text = self._ddddocr.classification(image_bytes)
            return self._eval_math_captcha(text)
        except Exception as exc:
            logger.debug("ddddocr T2 failed: %s", exc)
            return ""

    @staticmethod
    def _eval_math_captcha(text: str) -> str:
        """
        Parse and evaluate a simple math expression like '3+5=' or '12-7='.

        SC captchas are simple addition/subtraction with answers 0–20.
        """
        # Clean up OCR output
        cleaned = text.strip().rstrip("=").strip()
        cleaned = re.sub(r"[^0-9+\-*/xX]", "", cleaned)
        # 'x' or 'X' might be multiplication
        cleaned = cleaned.replace("x", "*").replace("X", "*")

        if not cleaned:
            return ""

        try:
            # Only allow simple arithmetic (security: no eval of arbitrary code)
            if not re.match(r"^\d+[+\-*/]\d+$", cleaned):
                return ""
            result = int(eval(cleaned))  # noqa: S307 — input is validated above
            if 0 <= result <= 20:
                return str(result)
            return ""
        except Exception:
            return ""

    def predict_type2(self, image_bytes: bytes) -> str:
        """Predict SC math captcha answer (0-20)."""
        return self.predict_type2_with_source(image_bytes).answer

    def predict_type2_with_source(self, image_bytes: bytes) -> CaptchaPrediction:
        """
        Predict SC math captcha answer (0–20).

        Uses Keras classifier with confidence + ddddocr OCR→math-eval fallback.
        """
        preprocessed = self._preprocess_image(image_bytes, captcha_type=2)

        keras_answer = ""
        keras_conf = 0.0
        ddddocr_answer = ""

        if self._mode in {"ensemble", "keras_only"}:
            keras_answer, keras_conf = self._keras_predict_t2(preprocessed)
            if not keras_answer:
                keras_answer, keras_conf = self._keras_predict_t2(image_bytes)

        if self._mode in {"ensemble", "ddddocr_only"}:
            ddddocr_answer = self._ddddocr_predict_t2(preprocessed)
            if not ddddocr_answer:
                ddddocr_answer = self._ddddocr_predict_t2(image_bytes)

        answer, solver = self._pick_best_t2(keras_answer, keras_conf, ddddocr_answer)
        return CaptchaPrediction(answer=answer, solver=solver)

    def _pick_best_t2(
        self,
        keras: str,
        keras_conf: float,
        ddddocr: str,
    ) -> tuple[str, str]:
        """Pick the best Type 2 prediction."""
        if not keras and not ddddocr:
            return "", "none"
        if not keras:
            return ddddocr, "ddddocr"
        if not ddddocr:
            return keras, "keras"
        if keras == ddddocr:
            return keras, "both"

        # High-confidence Keras prediction wins
        if keras_conf >= 0.7:
            return keras, "keras"

        # Low confidence — use runtime accuracy tracking
        keras_acc = self.keras_t2_tracker.accuracy
        ddddocr_acc = self.ddddocr_t2_tracker.accuracy

        if self.keras_t2_tracker.total < 10 and self.ddddocr_t2_tracker.total < 10:
            return keras, "keras"  # no data, prefer domain-trained

        if keras_acc >= ddddocr_acc:
            return keras, "keras"
        return ddddocr, "ddddocr"

    # ── Feedback API (called by extractors on server accept/reject) ──

    def record_feedback(
        self, prefix: str, solver: str, accepted: bool
    ) -> None:
        """
        Record whether the server accepted a captcha answer.

        prefix: 'hc', 'dc', 'sci'
        solver: 'keras', 'ddddocr'
        """
        if solver == "none":
            return

        solvers = ("keras", "ddddocr") if solver == "both" else (solver,)

        if prefix in {"hc", "dc"}:
            for selected in solvers:
                if selected == "keras":
                    self.keras_t1_tracker.record(accepted)
                elif selected == "ddddocr":
                    self.ddddocr_t1_tracker.record(accepted)
        elif prefix == "sci":
            for selected in solvers:
                if selected == "keras":
                    self.keras_t2_tracker.record(accepted)
                elif selected == "ddddocr":
                    self.ddddocr_t2_tracker.record(accepted)

    def accuracy_summary(self) -> str:
        """Return human-readable accuracy summary for logging."""
        return (
            f"T1[{self.keras_t1_tracker} | {self.ddddocr_t1_tracker}] "
            f"T2[{self.keras_t2_tracker} | {self.ddddocr_t2_tracker}]"
        )
