from __future__ import annotations

import json
import os
import time
import urllib.request
from typing import Any, Protocol


class LLMClient(Protocol):
    def invoke(self, prompt: str, model: str) -> dict[str, Any]:
        ...


class StubLLMClient:
    def invoke(self, prompt: str, model: str) -> dict[str, Any]:
        text_prompt = str(prompt or "")
        text_model = str(model or "stub-1")
        tokens_in = max(1, (len(text_prompt) + 3) // 4)
        tokens_out = max(1, (tokens_in // 2) + (len(text_model) % 7) + 3)
        total_tokens = tokens_in + tokens_out
        latency_ms = 20 + (len(text_prompt) % 37) + (len(text_model) % 13)
        text = f"[stub:{text_model}] {text_prompt[:64]}".strip()
        return {
            "text": text,
            "tokens_in": int(tokens_in),
            "tokens_out": int(tokens_out),
            "total_tokens": int(total_tokens),
            "latency_ms": int(latency_ms),
        }


class OpenAILLMClient:
    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1") -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def invoke(self, prompt: str, model: str) -> dict[str, Any]:
        started = time.monotonic()
        payload = {
            "model": str(model),
            "input": str(prompt),
        }
        req = urllib.request.Request(
            url=f"{self.base_url}/responses",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        data = json.loads(raw.decode("utf-8"))
        usage = data.get("usage", {}) if isinstance(data, dict) else {}

        tokens_in = int(usage.get("input_tokens", 0) or 0)
        tokens_out = int(usage.get("output_tokens", 0) or 0)
        total_tokens = int(usage.get("total_tokens", tokens_in + tokens_out) or (tokens_in + tokens_out))
        if total_tokens <= 0:
            tokens_in = max(1, (len(str(prompt)) + 3) // 4)
            tokens_out = max(1, tokens_in // 2)
            total_tokens = tokens_in + tokens_out

        text = ""
        if isinstance(data, dict):
            output_text = data.get("output_text")
            if isinstance(output_text, str):
                text = output_text
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "text": text,
            "tokens_in": int(tokens_in),
            "tokens_out": int(tokens_out),
            "total_tokens": int(total_tokens),
            "latency_ms": int(max(1, latency_ms)),
        }


def build_llm_client() -> tuple[LLMClient, str]:
    provider = str(os.getenv("KRAKO_LLM_PROVIDER", "stub")).strip().lower()
    api_key = os.getenv("OPENAI_API_KEY")
    if provider == "openai" and api_key:
        return OpenAILLMClient(api_key=api_key), "openai"
    return StubLLMClient(), "stub"

