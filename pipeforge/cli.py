"""Command line entry point: ``pipeforge run|validate|check --spec FILE``.

Exit codes: 0 success, 1 job failure, 2 invalid spec / usage.
The very last stdout line of ``run`` is always ``PIPEFORGE_RESULT <json>``.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import List

from pipeforge import __version__
from pipeforge.errors import SpecError
from pipeforge.spec import read_spec, validate_spec

RESULT_PREFIX = "PIPEFORGE_RESULT "


def emit_result(result: dict, stream=None) -> None:
    stream = stream or sys.stdout
    stream.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    stream.flush()


def _failure(spec, exc: BaseException, start: float) -> dict:
    return {"status": "failed", "mode": (spec or {}).get("mode"), "initial_load": None,
            "rows_read": 0, "rows_written": 0, "old_watermark": None, "new_watermark": None,
            "duration_seconds": round(time.monotonic() - start, 3),
            "run_id": (spec or {}).get("run_id"), "spec_version": (spec or {}).get("spec_version"),
            "error": f"{type(exc).__name__}: {exc}"}


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeforge")
    parser.add_argument("--version", action="version", version=f"pipeforge {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_ in (("run", "run the job"), ("validate", "validate the spec only"),
                        ("check", "validate the spec and test source/checkpoint connectivity")):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--spec", help="path to the spec JSON (or set PIPEFORGE_SPEC_PATH / PIPEFORGE_SPEC)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start = time.monotonic()
    spec = None
    try:
        spec = read_spec(args.spec)
        validate_spec(spec)
        if args.command == "validate":
            print("spec ok")
            return 0
        from pipeforge import runner  # heavy imports (pyspark) only when needed
        if args.command == "check":
            runner.check_spec(spec)
            print("connections ok")
            return 0
        result = runner.run_spec(spec)
        emit_result(result)
        return 0
    except SpecError as exc:
        logging.getLogger("pipeforge").error("%s", exc)
        if args.command == "run":
            emit_result(_failure(spec, exc, start))
        return 2
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("pipeforge").exception("job failed")
        if args.command == "run":
            emit_result(_failure(spec, exc, start))
        return 1


if __name__ == "__main__":
    sys.exit(main())
