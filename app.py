import json
import os
import sys
import uuid
from pathlib import Path

from cobra_lite.config import BASE_DIR, COBRA_SESSION_ID, DATA_DIR, FINAL_RESPONSE_FILE, FINAL_RESPONSE_AUTH_TOKEN, REPORT_FILE
from cobra_lite.runner import MissionRunError, MissionRunner


def _resolve_instructions() -> str:
    instructions_path_raw = (os.getenv("COBRA_INSTRUCTIONS_FILE") or "").strip()
    if instructions_path_raw:
        instructions_file = Path(instructions_path_raw).expanduser()
        if not instructions_file.is_absolute():
            instructions_file = BASE_DIR / instructions_file
        if not instructions_file.exists():
            raise RuntimeError(f"Instructions file not found: {instructions_file}")
        text = instructions_file.read_text(encoding="utf-8").strip()
        if not text:
            raise RuntimeError(f"Instructions file is empty: {instructions_file}")
        return text

    instructions = (os.getenv("COBRA_INSTRUCTIONS") or "").strip()
    if instructions:
        return instructions

    raise RuntimeError("Set COBRA_INSTRUCTIONS or COBRA_INSTRUCTIONS_FILE in .env before running app.py.")


def _truncate(value: str, limit: int = 280) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _print_block(prefix: str, value: str) -> None:
    text = str(value or "").rstrip()
    if not text:
        return
    lines = text.splitlines() or [text]
    for line in lines:
        print(f"{prefix}{line}")


def _print_progress(event: dict) -> None:
    event_type = str(event.get("type") or "").strip()
    data = event.get("data") if isinstance(event.get("data"), dict) else {}

    if event_type == "reasoning":
        text = str(data.get("text") or "").strip()
        if text:
            _print_block("[reasoning] ", text)
        return

    if event_type == "tool_start":
        tool_name = str(data.get("tool_name") or "tool").strip()
        command = str(data.get("command") or "").strip()
        rationale = str(data.get("rationale") or "").strip()
        action_index = data.get("action_index_1based")
        step_prefix = f"[tool:start:{action_index}] " if action_index else "[tool:start] "
        if command:
            print(f"{step_prefix}{tool_name}: {command}")
        else:
            print(f"{step_prefix}{tool_name}")
        if rationale:
            _print_block("[tool:why] ", rationale)
        return

    if event_type in {"tool_execution", "tool_update"}:
        tool_name = str(data.get("tool_name") or "tool").strip()
        command = str(data.get("command") or "").strip()
        output = str(data.get("tool_output") or "").rstrip()
        action_index = data.get("action_index_1based")
        tag = "tool:error" if data.get("is_error") else ("tool:update" if event_type == "tool_update" else "tool")
        step_prefix = f"[{tag}:{action_index}] " if action_index else f"[{tag}] "
        if command:
            print(f"{step_prefix}{tool_name}: {command}")
        if output:
            _print_block(f"{step_prefix}", output)
        elif not command:
            print(f"{step_prefix}{tool_name}")
        return

    if event_type == "run_status":
        phase = str(data.get("phase") or "").strip()
        detail = str(data.get("detail") or "").strip()
        if phase:
            print(f"[run] {phase}")
        if detail:
            _print_block("[run] ", detail)
        return

    if event_type == "assistant_delta":
        text = str(data.get("text") or "").strip()
        if text:
            _print_block("[assistant] ", text)
        return

    if event_type == "error":
        text = str(data.get("message") or "").strip()
        if text:
            for line in text.splitlines():
                print(f"[error] {line}", file=sys.stderr)
        return

    if event_type:
        preview = _truncate(str(data))
        if preview:
            print(f"[event:{event_type}] {preview}")


def _write_report(path: Path, report_text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = str(report_text or "").strip()
    path.write_text((normalized + "\n") if normalized else "", encoding="utf-8")


def _write_final_response(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _build_run_session_id() -> str:
    base = str(COBRA_SESSION_ID or "").strip() or "cobra-lite"
    suffix = uuid.uuid4().hex[:8]
    return f"{base}-{suffix}"


def main() -> int:
    try:
        instructions = _resolve_instructions()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    runner = MissionRunner()
    run_session_id = _build_run_session_id()

    print(f"Session: {run_session_id}")
    print(f"Data dir: {DATA_DIR}")
    print("Instructions:")
    print(instructions)
    print("")
    print("Running mission...")

    try:
        outcome = runner.run_prompt(
            instructions,
            session_id=run_session_id,
            progress_callback=_print_progress,
        )
    except KeyboardInterrupt:
        failure_text = "Run interrupted by user."
        _write_report(REPORT_FILE, failure_text)
        _write_final_response(
            FINAL_RESPONSE_FILE,
            {
                "report_content": failure_text,
                "found_vulnerabilities": [],
                "auth_token": FINAL_RESPONSE_AUTH_TOKEN,
            },
        )
        print("\nRun interrupted by user.", file=sys.stderr)
        print(f"Failure report written to {REPORT_FILE}")
        return 1
    except MissionRunError as exc:
        failure_text = f"Run failed\n\n{exc}"
        _write_report(REPORT_FILE, failure_text)
        _write_final_response(
            FINAL_RESPONSE_FILE,
            {
                "report_content": failure_text,
                "found_vulnerabilities": [],
                "auth_token": FINAL_RESPONSE_AUTH_TOKEN,
            },
        )
        print(str(exc), file=sys.stderr)
        print(f"Failure report written to {REPORT_FILE}")
        return 1

    final_report = str(outcome.get("result", {}).get("final_observation") or "").strip()
    final_response = outcome.get("result", {}).get("final_response")
    _write_report(REPORT_FILE, final_report)
    if isinstance(final_response, dict):
        _write_final_response(FINAL_RESPONSE_FILE, final_response)
    else:
        _write_final_response(
            FINAL_RESPONSE_FILE,
            {
                "report_content": final_report,
                "found_vulnerabilities": [],
                "auth_token": FINAL_RESPONSE_AUTH_TOKEN,
            },
        )

    print(f"Final report written to {REPORT_FILE}")
    print(f"Final response written to {FINAL_RESPONSE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
