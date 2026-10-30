"""Claude access: structured outputs, token + cost accounting, and an on-disk response cache
(ported from P1 / P2).

The cache keys on the full request - with each document replaced by the SHA-256 of its bytes -
so re-running an unchanged step costs nothing and development runs stay comparable (the API no
longer accepts sampling parameters like temperature). `count_tokens` is free and gives exact
input sizes for cost estimates before anything is spent.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any, Literal

import anthropic
import yaml
from pydantic import BaseModel, ValidationError

from hive2snow.config import CONFIG_DIR, project_path

Effort = Literal["low", "medium", "high", "xhigh", "max"]
Content = str | list[dict[str, Any]]

CACHE_DIR = project_path("data/cache/llm")
# Server-side refusal fallback: supported on Opus 5.5 / Sonnet 5.5, not on Haiku.
FALLBACK_MODELS = {"claude-opus-5-5", "claude-sonnet-5-5"}
FALLBACK_BETA = "server-side-fallback-2026-07-01"


class LLMRefusal(RuntimeError):
    pass


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class Ledger:
    """Running token and cost totals for a process (e.g. one eval run), with a spending cap."""

    calls: int = 0
    cached_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    cap_usd: float | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def check(self) -> None:
        if self.cap_usd is not None and self.cost_usd >= self.cap_usd:
            raise BudgetExceeded(f"spent ${self.cost_usd:.2f} of the ${self.cap_usd:.2f} cap")

    def add(self, model: str, usage: dict[str, int], cached: bool) -> None:
        with self._lock:
            self.calls += 1
            if cached:
                self.cached_calls += 1
                return
            self.input_tokens += usage.get("input_tokens", 0)
            self.output_tokens += usage.get("output_tokens", 0)
            self.cost_usd += price(model, usage)

    def summary(self) -> str:
        return (
            f"{self.calls} calls ({self.cached_calls} cached), "
            f"{self.input_tokens:,} in / {self.output_tokens:,} out tokens, ${self.cost_usd:.4f}"
        )


LEDGER = Ledger()


@cache
def _prices() -> dict[str, dict[str, float]]:
    with (CONFIG_DIR / "prices.yaml").open(encoding="utf-8") as f:
        prices: dict[str, dict[str, float]] = yaml.safe_load(f)["models"]
    return prices


def price(model: str, usage: dict[str, int]) -> float:
    p = _prices().get(model)
    if p is None:
        raise KeyError(f"No price for {model} in config/prices.yaml")
    per = 1e6
    return (
        usage.get("input_tokens", 0) * p["input"] / per
        + usage.get("cache_creation_input_tokens", 0) * p["input"] * 1.25 / per
        + usage.get("cache_read_input_tokens", 0) * p.get("cache_read", p["input"] * 0.1) / per
        + usage.get("output_tokens", 0) * p["output"] / per
    )


@cache
def client() -> anthropic.Anthropic:
    return anthropic.Anthropic(max_retries=4, timeout=900.0)  # a stalled stream errors, not hangs


def _client_for(api_key: str | None) -> anthropic.Anthropic:
    """The shared client, or a short-lived one for a caller-supplied key. Deliberately not
    memoised: a cache here would keep visitors' keys in memory."""
    return client() if api_key is None else anthropic.Anthropic(api_key=api_key, max_retries=2)


def pdf_block(path: Path, title: str | None = None) -> dict[str, Any]:
    """A PDF as a document content block (the model reads its text and page images)."""
    data = base64.standard_b64encode(path.read_bytes()).decode("ascii")
    block: dict[str, Any] = {
        "type": "document",
        "source": {"type": "base64", "media_type": "application/pdf", "data": data},
    }
    if title:
        block["title"] = title
    return block


_NAMESPACE = ""


def set_cache_namespace(namespace: str) -> None:
    """Independent repeats: a non-empty namespace gives every call a fresh cache entry, so a
    repeated run gets new answers instead of replaying the first run. "" = the shared cache."""
    global _NAMESPACE
    _NAMESPACE = namespace


def _fingerprint(content: Content) -> Any:
    """Content for the cache key: document bytes replaced by their hash."""
    if isinstance(content, str):
        return content
    out = []
    for block in content:
        source = block.get("source", {})
        if source.get("type") == "base64":
            digest = hashlib.sha256(source["data"].encode("ascii")).hexdigest()
            block = {**block, "source": {**source, "data": f"sha256:{digest}"}}
        out.append(block)
    return out


def _cache_key(payload: dict[str, Any]) -> str:
    if _NAMESPACE:
        payload = {**payload, "_namespace": _NAMESPACE}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True)
class CallInfo:
    model: str
    usage: dict[str, int]
    cost_usd: float  # list price, also for cache hits
    latency_s: float  # of the original API call
    cached: bool


def count_tokens(model: str, content: Content, system: str | None = None) -> int:
    """Exact input tokens for a request (free endpoint; nothing is generated)."""
    kwargs: dict[str, Any] = {"model": model, "messages": [{"role": "user", "content": content}]}
    if system:
        kwargs["system"] = system
    return client().messages.count_tokens(**kwargs).input_tokens


def parse_full[T: BaseModel](
    model: str,
    content: Content,
    schema: type[T],
    *,
    system: str | None = None,
    effort: Effort | None = None,
    max_tokens: int = 16000,
    use_cache: bool = True,
    api_key: str | None = None,
) -> tuple[T, CallInfo]:
    """One structured-output call: a validated instance of `schema` plus usage, cost, latency.

    effort=None leaves the model's default (the naive baselines). api_key: a caller-supplied
    key (demo "bring your own key") - such calls never read or write the on-disk cache."""
    use_cache = use_cache and api_key is None
    payload = {
        "model": model,
        "system": system,
        "content": _fingerprint(content),
        "schema": schema.model_json_schema(),
        "effort": effort,
    }
    path = CACHE_DIR / f"{_cache_key(payload)}.json"
    if use_cache and path.exists():
        record = json.loads(path.read_text(encoding="utf-8"))
        served, usage = record["served_model"], record["usage"]
        LEDGER.add(served, usage, cached=True)
        info = CallInfo(served, usage, price(served, usage), record.get("latency_s", 0.0), True)
        return schema.model_validate(record["output"]), info

    LEDGER.check()
    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": content}],
        "output_format": schema,
    }
    if effort:
        kwargs["output_config"] = {"effort": effort}
    if system:
        kwargs["system"] = system
    if model in FALLBACK_MODELS:
        kwargs["betas"] = [FALLBACK_BETA]
        kwargs["fallbacks"] = "default"
    start = time.perf_counter()
    try:  # streamed: long PDF reads with a large output budget can exceed 10 minutes
        with _client_for(api_key).beta.messages.stream(**kwargs) as stream:
            response = stream.get_final_message()
    except ValidationError as e:  # the SDK validates the answer: usually a cut-off one
        raise RuntimeError(f"{model}: output did not validate (truncated?): {e}") from e
    latency = time.perf_counter() - start

    u = response.usage
    usage = {
        "input_tokens": u.input_tokens or 0,
        "output_tokens": u.output_tokens or 0,
        "cache_creation_input_tokens": u.cache_creation_input_tokens or 0,
        "cache_read_input_tokens": u.cache_read_input_tokens or 0,
    }
    served = response.model if response.model in _prices() else model
    LEDGER.add(served, usage, cached=False)  # failed calls cost money too
    if response.stop_reason == "refusal":
        raise LLMRefusal(f"{model} refused: {response.stop_details}")
    if response.stop_reason == "max_tokens" or response.parsed_output is None:
        raise RuntimeError(f"{model}: no parsed output (stop_reason={response.stop_reason})")

    output = response.parsed_output.model_dump()
    info = CallInfo(served, usage, price(served, usage), latency, False)
    if api_key is None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"served_model": served, "usage": usage, "output": output, "latency_s": latency},
                indent=1,
            ),
            encoding="utf-8",
        )
    return schema.model_validate(output), info
