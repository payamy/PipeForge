"""Job spec loading, validation and secret resolution."""
from __future__ import annotations

import copy
import json
import os
from importlib import resources
from typing import Any, Dict

import jsonschema

from pipeforge.errors import SpecError

SPEC_ENV = "PIPEFORGE_SPEC"            # inline JSON
SPEC_PATH_ENV = "PIPEFORGE_SPEC_PATH"  # path to a JSON file


def load_schema() -> Dict[str, Any]:
    text = resources.files("pipeforge.spec").joinpath("job-spec.v1.schema.json").read_text(encoding="utf-8")
    return json.loads(text)


def read_spec(path: str | None = None, environ: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Read the spec from ``path``, or ``$PIPEFORGE_SPEC_PATH``, or inline ``$PIPEFORGE_SPEC``."""
    env = os.environ if environ is None else environ
    try:
        if path:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        if env.get(SPEC_PATH_ENV):
            with open(env[SPEC_PATH_ENV], "r", encoding="utf-8") as f:
                return json.load(f)
        if env.get(SPEC_ENV):
            return json.loads(env[SPEC_ENV])
    except (OSError, json.JSONDecodeError) as exc:
        raise SpecError(f"cannot read spec: {exc}") from exc
    raise SpecError(f"no spec given: pass --spec, or set {SPEC_PATH_ENV} or {SPEC_ENV}")


def validate_spec(spec: Dict[str, Any]) -> None:
    """Validate against the JSON Schema and a few cross-field rules."""
    validator = jsonschema.Draft202012Validator(load_schema())
    errors = sorted(validator.iter_errors(spec), key=lambda e: list(e.absolute_path))
    if errors:
        msgs = [f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}" for e in errors[:10]]
        raise SpecError("invalid job spec: " + "; ".join(msgs))
    src, tgt = spec["source"], spec["target"]
    if spec["mode"] == "incremental" and not src.get("unique_key") and tgt.get("on_duplicate_keys") == "latest":
        raise SpecError("on_duplicate_keys requires source.unique_key")
    cols = src.get("columns")
    if cols:
        needed = list(src.get("cursor_fields", [])) + list(src.get("unique_key", []))
        missing = [c for c in needed if c not in cols]
        if missing:
            raise SpecError(f"columns must include cursor_fields and unique_key; missing {missing}")


def resolve_secret(ref: Dict[str, str], environ: Dict[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    if "env" in ref:
        value = env.get(ref["env"])
        if value is None:
            raise SpecError(f"environment variable '{ref['env']}' is not set")
        return value
    path = ref["file"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().rstrip("\r\n")
    except OSError as exc:
        raise SpecError(f"cannot read secret file '{path}': {exc}") from exc


def resolve_secrets(spec: Dict[str, Any], environ: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Return a deep copy of the spec with every secret reference replaced by its value.

    The result holds secrets in memory only. Never log or serialise it.
    """
    out = copy.deepcopy(spec)
    conn = out["source"]["connection"]
    for k in ("username", "password"):
        conn[k] = resolve_secret(conn[k], environ)
    s3 = out["storage"]["s3"]
    for k in ("access_key", "secret_key"):
        s3[k] = resolve_secret(s3[k], environ)
    return out
