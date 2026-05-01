"""
Image preprocessing pipeline for CAPTCHA images.

Applies noise removal, binarization, and contrast enhancement to
improve OCR/model accuracy before feeding to solvers.
"""

from __future__ import annotations

import io
import logging

import cv2
import numpy as np
from PIL import Image

logger = logging.getLogger("legal_scraper.captcha_preprocess")


def _pil_to_cv2(image_bytes: bytes) -> np.ndarray:
    """Convert raw image bytes to OpenCV grayscale array."""
    img = Image.open(io.BytesIO(image_bytes)).convert("L")
    return np.array(img, dtype=np.uint8)


def _cv2_to_bytes(arr: np.ndarray) -> bytes:
    """Convert OpenCV array back to PNG bytes."""
    success, encoded = cv2.imencode(".png", arr)
    if not success:
        raise ValueError("Failed to encode image to PNG")
    return encoded.tobytes()


def preprocess_type1(image_bytes: bytes) -> bytes:
    """
    Preprocess DC/HC securimage text captchas.

    Pipeline: grayscale → gaussian blur → Otsu binarization
    → morphological opening (remove noise dots/lines)
    → invert if needed (dark text on light background).
    """
    try:
        gray = _pil_to_cv2(image_bytes)

        # Gentle blur to smooth out high-frequency noise
        blurred = cv2.GaussianBlur(gray, (3, 3), sigmaX=0.8)

        # Otsu binarization auto-picks optimal threshold
        _, binary = cv2.threshold(
            blurred, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )

        # Morphological opening removes small noise while keeping text
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        cleaned = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)

        # Ensure dark text on white background (invert if background is dark)
        if np.mean(cleaned) < 128:
            cleaned = cv2.bitwise_not(cleaned)

        return _cv2_to_bytes(cleaned)

    except Exception as exc:
        logger.debug("Type1 preprocessing failed, using raw image: %s", exc)
        return image_bytes


def preprocess_type2(image_bytes: bytes) -> bytes:
    """
    Preprocess SC math captchas.

    Pipeline: grayscale → adaptive threshold (handles varying background)
    → dilation to thicken thin operator symbols (+, -, =).
    """
    try:
        gray = _pil_to_cv2(image_bytes)

        # Adaptive threshold handles varying background gradients
        binary = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 4,
        )

        # Slight dilation so thin characters/operators aren't lost
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        dilated = cv2.dilate(binary, kernel, iterations=1)

        # Ensure dark text on white background
        if np.mean(dilated) < 128:
            dilated = cv2.bitwise_not(dilated)

        return _cv2_to_bytes(dilated)

    except Exception as exc:
        logger.debug("Type2 preprocessing failed, using raw image: %s", exc)
        return image_bytes
