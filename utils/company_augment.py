"""Company-name search planning and matching.

Court data is inconsistent: the same entity can appear as ``PRIVATE LIMITED``,
``PVT LTD``, ``LTD.``, ``(I)``, or with punctuation between acronym letters.
This module keeps those rules in one place so sheet search and future LE
matching can reuse the same normalisation, variants, and scoring.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Pattern


_SUFFIX_EXPANSION: dict[str, tuple[str, ...]] = {
    "PRIVATE LIMITED": ("PVT LTD", "PVT LIMITED", "PRIVATE LTD", "LIMITED", "LTD"),
    "LIMITED": ("LTD", "PRIVATE LIMITED", "PVT LTD"),
    "LLP": ("LIMITED LIABILITY PARTNERSHIP",),
    "LIMITED LIABILITY PARTNERSHIP": ("LLP",),
    "INC": ("INCORPORATED",),
    "INCORPORATED": ("INC",),
    "CORPORATION": ("CORP",),
    "CORP": ("CORPORATION",),
    "COMPANY": ("CO",),
    "CO": ("COMPANY",),
}

_SUFFIX_TOKEN_SEQUENCES: tuple[tuple[str, ...], ...] = (
    ("LIMITED", "LIABILITY", "PARTNERSHIP"),
    ("PRIVATE", "LIMITED"),
    ("PVT", "LIMITED"),
    ("PVT", "LTD"),
    ("PRIVATE", "LTD"),
    ("LIMITED",),
    ("LTD",),
    ("LLP",),
    ("INCORPORATED",),
    ("INC",),
    ("CORPORATION",),
    ("CORP",),
    ("COMPANY",),
    ("CO",),
)

_TOKEN_CANONICAL: dict[str, str] = {
    "PVT": "PRIVATE",
    "PVT.": "PRIVATE",
    "PRIV": "PRIVATE",
    "PRV": "PRIVATE",
    "PRT": "PRIVATE",
    "LTD": "LIMITED",
    "LTD.": "LIMITED",
    "LIM": "LIMITED",
    "LMT": "LIMITED",
    "LMTD": "LIMITED",
    "CO": "COMPANY",
    "CO.": "COMPANY",
    "CORP": "CORPORATION",
    "CORP.": "CORPORATION",
    "IND": "INDUSTRIES",
    "IND.": "INDUSTRIES",
    "&": "AND",
}

_TOKEN_REGEX_ALIASES: dict[str, tuple[str, ...]] = {
    "PRIVATE": ("PRIVATE", "PRIV", "PVT", "PRV", "PRT"),
    "LIMITED": ("LIMITED", "LTD", "LIM", "LMT", "LMTD"),
    "COMPANY": ("COMPANY", "CO"),
    "CORPORATION": ("CORPORATION", "CORP"),
    "INDUSTRIES": ("INDUSTRIES", "INDUSTRY", "IND"),
    "AND": ("AND", "&"),
    "INDIA": ("INDIA", "I"),
}

_DESIGNATION_RE = re.compile(
    r"^(?:THE\s+)?"
    r"(?:CEO|CFO|CTO|COO|CMD|MD|MANAGING\s+DIRECTOR|DIRECTOR|CHAIRMAN"
    r"|SECRETARY|MANAGER|BRANCH\s+MANAGER|AUTHORIZED\s+OFFICER"
    r"|AUTHORISED\s+OFFICER|RECOVERY\s+OFFICER|NODAL\s+OFFICER"
    r"|REGISTRAR|PRINCIPAL|PRESIDENT|SUPERINTENDENT)\s+(?:OF\s+)?",
    re.IGNORECASE,
)
_MS_PREFIX_RE = re.compile(r"^M\s*/?\s*S\.?\s*", re.IGNORECASE)
_LEADING_NUMBER_RE = re.compile(r"^\d+[\s.\-)]+\s*")
_COUNTRY_SHORT_RE = re.compile(r"\(\s*(?:I|IND|INDIA)\s*\)", re.IGNORECASE)
_WORD_RE = re.compile(r"[A-Z0-9]+|&")


@dataclass(frozen=True)
class CompanySearchMatch:
    """A scored company match decision."""

    matched: bool
    score: int
    method: str
    candidate: str = ""
    variant: str = ""


@dataclass(frozen=True)
class CompanySearchPlan:
    """Search artefacts derived from one query name."""

    raw_name: str
    normalized_name: str
    core_name: str
    core_tokens: tuple[str, ...]
    suffix: str
    variants: tuple[str, ...]
    keyword_queries: tuple[str, ...]
    equivalent_names: tuple[str, ...]
    patterns: tuple[Pattern[str], ...]
    score_threshold: int = 92

    def matcher(self) -> "CompanySearchMatcher":
        return CompanySearchMatcher(self)


class CompanySearchMatcher:
    """Reusable matcher for one company query."""

    def __init__(self, plan: CompanySearchPlan) -> None:
        self.plan = plan
        self._variant_core_tokens = tuple(
            _strip_legal_suffix(normalize_company_name(v))[0].split()
            for v in plan.variants
        )

    def match(self, cell_value: str) -> CompanySearchMatch:
        if not cell_value:
            return CompanySearchMatch(False, 0, "empty")

        text = str(cell_value)
        for pattern, variant in zip(self.plan.patterns, self.plan.variants):
            if pattern.search(text):
                score = self._best_score(text)
                return CompanySearchMatch(True, max(score, 95), "variant_regex", text, variant)

        for candidate in _candidate_names_from_cell(text):
            normalized = normalize_company_name(candidate)
            if not normalized:
                continue

            candidate_core, _ = _strip_legal_suffix(normalized)
            candidate_tokens = tuple(candidate_core.split())
            if self._core_tokens_match(candidate_tokens):
                score = self._best_score(normalized)
                return CompanySearchMatch(
                    True,
                    max(score, 90),
                    "core_tokens",
                    normalized,
                    self.plan.core_name,
                )

            score = self._best_score(normalized)
            if score >= self.plan.score_threshold and self._token_overlap_ok(candidate_tokens):
                return CompanySearchMatch(
                    True,
                    score,
                    "jaro_winkler",
                    normalized,
                    self.plan.normalized_name,
                )

        return CompanySearchMatch(False, self._best_score(text), "no_match")

    def matches(self, cell_value: str) -> bool:
        return self.match(cell_value).matched

    def _core_tokens_match(self, candidate_tokens: tuple[str, ...]) -> bool:
        if not self.plan.core_tokens or not candidate_tokens:
            return False

        candidate_text = " ".join(candidate_tokens)
        for tokens in self._variant_core_tokens:
            tokens = tuple(t for t in tokens if t)
            if not tokens:
                continue
            ordered = " ".join(tokens)
            if ordered and ordered in candidate_text:
                return True
            if all(_token_present(t, candidate_tokens) for t in tokens):
                return True
        return False

    def _token_overlap_ok(self, candidate_tokens: tuple[str, ...]) -> bool:
        required = _required_match_tokens(self.plan.core_tokens)
        if not required:
            return True
        matched = sum(1 for token in required if _token_present(token, candidate_tokens))
        return (matched / len(required)) >= 0.75

    def _best_score(self, value: str) -> int:
        normalized = normalize_company_name(value)
        if not normalized:
            return 0
        return max(
            (jaro_winkler_score(normalized, eq) for eq in self.plan.equivalent_names),
            default=0,
        )


def normalize_company_name(raw: str) -> str:
    """Return a canonical company name for search/scoring."""
    if not raw:
        return ""

    text = str(raw).strip().upper()
    if not text:
        return ""

    text = _LEADING_NUMBER_RE.sub("", text).strip()
    text = _MS_PREFIX_RE.sub("", text).strip()
    text = _COUNTRY_SHORT_RE.sub(" INDIA ", text)
    text = text.replace("&AMP;", "&").replace("&amp;", "&")
    text = _DESIGNATION_RE.sub("", text).strip()

    tokens = [_canonical_token(t) for t in _WORD_RE.findall(text)]
    tokens = [t for t in tokens if t]
    if not tokens:
        return ""

    normalized = " ".join(tokens)
    core, suffix = _strip_legal_suffix(normalized)
    if suffix:
        normalized = f"{core} {suffix}".strip()
    return re.sub(r"\s+", " ", normalized).strip()


def build_company_search_plan(raw_name: str, score_threshold: int = 92) -> CompanySearchPlan:
    normalized = normalize_company_name(raw_name)
    if not normalized:
        normalized = normalize_company_name(str(raw_name or ""))

    core, suffix = _strip_legal_suffix(normalized)
    core_tokens = tuple(core.split())
    variants = tuple(_dedupe(_generate_name_variants(normalized, core, suffix)))
    keyword_queries = tuple(_dedupe(_generate_keyword_queries(core_tokens)))
    equivalent_names = tuple(_dedupe(_generate_equivalent_names(normalized, core, suffix)))
    patterns = tuple(_compile_variant_pattern(v) for v in variants)

    return CompanySearchPlan(
        raw_name=raw_name,
        normalized_name=normalized,
        core_name=core,
        core_tokens=core_tokens,
        suffix=suffix,
        variants=variants,
        keyword_queries=keyword_queries,
        equivalent_names=equivalent_names,
        patterns=patterns,
        score_threshold=score_threshold,
    )


def build_company_search_matcher(raw_name: str, score_threshold: int = 92) -> CompanySearchMatcher:
    return build_company_search_plan(raw_name, score_threshold).matcher()


def generate_search_variants(raw_name: str) -> list[str]:
    """Backward-compatible API used by older search code."""
    return list(build_company_search_plan(raw_name).variants)


def generate_search_patterns(raw_name: str) -> list[Pattern[str]]:
    """Backward-compatible API: regexes for all generated variants."""
    return list(build_company_search_plan(raw_name).patterns)


def cell_matches_company_query(cell_value: str, query: str, score_threshold: int = 92) -> bool:
    return build_company_search_matcher(query, score_threshold).matches(cell_value)


def jaro_winkler_score(left: str, right: str) -> int:
    """Return Jaro-Winkler similarity as an integer percentage."""
    a = _score_string(left)
    b = _score_string(right)
    if not a or not b:
        return 0
    if a == b:
        return 100

    jaro = _jaro_similarity(a, b)
    if jaro <= 0.7:
        return round(jaro * 100)

    prefix = 0
    for ac, bc in zip(a[:4], b[:4]):
        if ac != bc:
            break
        prefix += 1
    winkler = jaro + prefix * 0.1 * (1.0 - jaro)
    return round(min(winkler, 1.0) * 100)


def _jaro_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0

    len_a = len(a)
    len_b = len(b)
    if len_a == 0 or len_b == 0:
        return 0.0

    match_distance = max(len_a, len_b) // 2 - 1
    a_matches = [False] * len_a
    b_matches = [False] * len_b

    matches = 0
    for i, ac in enumerate(a):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len_b)
        for j in range(start, end):
            if b_matches[j] or ac != b[j]:
                continue
            a_matches[i] = True
            b_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    transpositions = 0
    j = 0
    for i in range(len_a):
        if not a_matches[i]:
            continue
        while not b_matches[j]:
            j += 1
        if a[i] != b[j]:
            transpositions += 1
        j += 1

    transpositions /= 2
    return (
        matches / len_a
        + matches / len_b
        + (matches - transpositions) / matches
    ) / 3.0


def _strip_legal_suffix(name: str) -> tuple[str, str]:
    tokens = name.split()
    for seq in _SUFFIX_TOKEN_SEQUENCES:
        if len(tokens) < len(seq):
            continue
        if tuple(tokens[-len(seq):]) != seq:
            continue
        core_tokens = tokens[: -len(seq)]
        suffix = _canonical_suffix(seq)
        if core_tokens and core_tokens[-1] == "P" and suffix == "LIMITED":
            core_tokens = core_tokens[:-1]
            suffix = "PRIVATE LIMITED"
        return " ".join(core_tokens).strip(), suffix
    return name.strip(), ""


def _canonical_suffix(seq: tuple[str, ...]) -> str:
    if seq in {("PVT", "LIMITED"), ("PVT", "LTD"), ("PRIVATE", "LTD")}:
        return "PRIVATE LIMITED"
    if seq == ("LTD",):
        return "LIMITED"
    if seq == ("CORP",):
        return "CORPORATION"
    if seq == ("CO",):
        return "COMPANY"
    return " ".join(seq)


def _generate_name_variants(normalized: str, core: str, suffix: str) -> Iterable[str]:
    if normalized:
        yield normalized

    token_variants = _build_token_search_queries(core.split())
    suffixes = [suffix] if suffix else []
    suffixes.extend(_SUFFIX_EXPANSION.get(suffix, ()))

    for tokens in token_variants:
        core_text = " ".join(tokens).strip()
        if not core_text:
            continue
        for suf in suffixes:
            yield f"{core_text} {suf}".strip()
        yield core_text


def _generate_equivalent_names(normalized: str, core: str, suffix: str) -> Iterable[str]:
    yield from _generate_name_variants(normalized, core, suffix)
    if suffix == "PRIVATE LIMITED":
        yield f"{core} LIMITED".strip()
        yield f"{core} LTD".strip()


def _generate_keyword_queries(core_tokens: tuple[str, ...]) -> Iterable[str]:
    for tokens in _build_token_search_queries(list(core_tokens)):
        flat = " ".join(tokens).split()
        if flat:
            yield " AND ".join(flat)


def _build_token_search_queries(tokens: list[str]) -> list[list[str]]:
    if not tokens:
        return []

    variants: list[list[str]] = [list(tokens)]
    variants.extend(_single_letter_run_variants(tokens))
    for idx, token in enumerate(tokens):
        if not _is_short_alpha(token):
            continue
        expanded = _acronym_variants(token)
        new_variants: list[list[str]] = []
        for existing in variants:
            for item in expanded:
                copy = list(existing)
                copy[idx] = item
                new_variants.append(copy)
        variants = new_variants

    return list(_dedupe_token_lists(variants))


def _single_letter_run_variants(tokens: list[str]) -> list[list[str]]:
    """Join adjacent initials, e.g. ``M K NARAYAN`` -> ``MK NARAYAN``."""
    out: list[list[str]] = []
    idx = 0
    while idx < len(tokens):
        if len(tokens[idx]) != 1 or not tokens[idx].isalpha():
            idx += 1
            continue
        end = idx + 1
        while end < len(tokens) and len(tokens[end]) == 1 and tokens[end].isalpha():
            end += 1
        run_len = end - idx
        if run_len >= 2:
            run = tokens[idx:end]
            out.append(tokens[:idx] + ["".join(run)] + tokens[end:])
            out.append(tokens[:idx] + [".".join(run) + "."] + tokens[end:])
        idx = end
    return out


def _is_short_alpha(token: str) -> bool:
    return 1 < len(token) <= 3 and token.isalpha()


def _acronym_variants(token: str) -> list[str]:
    return [token, " ".join(token), ".".join(token) + "."]


def _compile_variant_pattern(variant: str) -> Pattern[str]:
    tokens = normalize_company_name(variant).split()
    if not tokens:
        return re.compile(r"$^")
    parts = [_regex_for_token(t) for t in tokens]
    return re.compile(r"(?<![A-Z0-9])" + r"[\W_]+".join(parts) + r"(?![A-Z0-9])", re.IGNORECASE)


def _regex_for_token(token: str) -> str:
    aliases = _TOKEN_REGEX_ALIASES.get(token, (token,))
    return r"(?:" + "|".join(re.escape(a) for a in aliases) + r")"


def _candidate_names_from_cell(cell: str) -> list[str]:
    text = str(cell or "").strip()
    if not text:
        return []

    candidates = [text]
    quoted = re.findall(r'"([^"]+)"', text)
    candidates.extend(quoted)

    if not quoted and "," in text:
        candidates.extend(part.strip() for part in text.split(",") if part.strip())

    return list(_dedupe(candidates))


def _canonical_token(token: str) -> str:
    token = token.strip().upper().rstrip(".")
    return _TOKEN_CANONICAL.get(token, token)


def _token_present(query_token: str, candidate_tokens: tuple[str, ...]) -> bool:
    aliases = {_canonical_token(a) for a in _TOKEN_REGEX_ALIASES.get(query_token, (query_token,))}
    aliases.add(_canonical_token(query_token))
    for token in candidate_tokens:
        canonical = _canonical_token(token)
        if canonical in aliases:
            return True
        if len(query_token) > 4 and len(canonical) > 4:
            if query_token.rstrip("S") == canonical.rstrip("S"):
                return True
    return False


def _required_match_tokens(tokens: tuple[str, ...]) -> tuple[str, ...]:
    soft = {"AND", "OF", "THE", "INDIA"}
    hard = tuple(t for t in tokens if t not in soft)
    if len(hard) >= 2:
        return hard
    return tokens


def _score_string(value: str) -> str:
    normalized = normalize_company_name(value)
    return re.sub(r"[^A-Z0-9]+", "", normalized)


def _dedupe(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = re.sub(r"\s+", " ", str(item).strip().upper())
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _dedupe_token_lists(items: Iterable[list[str]]) -> Iterable[list[str]]:
    seen: set[str] = set()
    for item in items:
        key = " ".join(item)
        if key in seen:
            continue
        seen.add(key)
        yield item
