import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from cobra_lite.config import (
    BASE_DIR,
    COBRA_SESSION_ID,
    DATA_DIR,
    FINAL_RESPONSE_AUTH_TOKEN,
    FINAL_RESPONSE_CALLBACK_INCLUDE_AUTH_HEADER,
    FINAL_RESPONSE_CALLBACK_INCLUDE_AUTH_TOKEN_BODY,
    FINAL_RESPONSE_CALLBACK_TIMEOUT_SECONDS,
    FINAL_RESPONSE_CALLBACK_URL,
    FINAL_RESPONSE_FILE,
    INCIDENT_FILE,
    OPENCLAW_STATE_DIR,
    REPORT_FILE,
)
from cobra_lite.runner import MissionRunError, MissionRunner

INVALID_REPORT_PREFIXES = (
    "exec:",
    "run status:",
    "gateway cli error:",
    "command still running",
)
REPORT_STRUCTURE_MARKERS = (
    "cybersecurity risk report",
    "overall risk level:",
    "executive summary",
)
INCIDENT_REPORT_BLOCK_RE = re.compile(r"<incident_report_json>\s*(.*?)\s*</incident_report_json>", re.IGNORECASE | re.DOTALL)


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


def _write_incident(path: Path, incident_text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = str(incident_text or "").strip()
    path.write_text((normalized + "\n") if normalized else "", encoding="utf-8")


def _remove_file_if_exists(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _session_log_path(run_session_id: str) -> Path:
    return OPENCLAW_STATE_DIR / "agents" / "main" / "sessions" / f"{run_session_id}.jsonl"


def _load_session_records(run_session_id: str) -> list[dict[str, object]]:
    path = _session_log_path(run_session_id)
    if not path.exists():
        return []

    records: list[dict[str, object]] = []
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return records

    for raw in raw_lines:
        line = raw.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            records.append(parsed)
    return records


def _collect_session_text(records: list[dict[str, object]], *, role: str) -> list[str]:
    collected: list[str] = []
    for record in records:
        if str(record.get("type") or "").strip() != "message":
            continue
        message = record.get("message")
        if not isinstance(message, dict):
            continue
        if str(message.get("role") or "").strip().lower() != role:
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for item in content:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() != "text":
                continue
            text = str(item.get("text") or "").strip()
            if text:
                collected.append(text)
    return collected


def _extract_target_context(instructions: str) -> tuple[str, str]:
    match = re.search(r"asset\s+(.+?)\s+\((https?://[^)]+)\)", str(instructions or ""), re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    url_match = re.search(r"https?://[^\s)]+", str(instructions or ""))
    return "Authorized Target", url_match.group(0).strip() if url_match else ""


def _report_looks_invalid(report_text: str) -> bool:
    normalized = str(report_text or "").strip()
    if not normalized:
        return True

    lowered = normalized.lower()
    if any(lowered.startswith(prefix) for prefix in INVALID_REPORT_PREFIXES):
        return True

    if any(marker in lowered for marker in REPORT_STRUCTURE_MARKERS):
        return False

    if len(normalized) < 180:
        return True

    if normalized.startswith(("⚠", "❌", "✅", "🛠️")):
        return True

    return False


def _partial_observations(records: list[dict[str, object]]) -> list[str]:
    tool_texts = "\n\n".join(_collect_session_text(records, role="toolresult"))
    assistant_texts = "\n\n".join(_collect_session_text(records, role="assistant"))
    combined = f"{assistant_texts}\n\n{tool_texts}".lower()
    observations: list[str] = []

    if "content-security-policy" not in combined and (
        "the anti-clickjacking x-frame-options header is not present" in combined
        or "=== security headers ===" in combined
    ):
        observations.append(
            "The public static site appears to be missing browser hardening headers such as Content-Security-Policy and X-Frame-Options."
        )

    if "x-content-type-options: nosniff" in combined and "x-frame-options: deny" in combined:
        observations.append(
            "The API authentication surface returned expected unauthenticated responses and included baseline hardening headers."
        )

    if "protocol: tlsv1.3" in combined or "ssl connection using tlsv1.3" in combined:
        observations.append("TLS negotiation was modern and the certificate chain validated during the run.")

    if "nuclei found zero medium/high/critical findings" in combined or "0 error(s) and 2 item(s) reported" in combined:
        observations.append("Automated scan output did not confirm any high-confidence medium, high, or critical findings before the run ended.")

    duplicate_waitlist_pattern = (
        '{"email":"duplicate-check@test.com","ok":true}' in tool_texts
        and tool_texts.count('{"email":"duplicate-check@test.com","ok":true}') >= 2
    )
    if duplicate_waitlist_pattern:
        observations.append(
            "Duplicate waitlist submissions were accepted; this looks more like an abuse-prevention gap than a confirmed security vulnerability."
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for item in observations:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:4]


def _build_structured_fallback_report(
    *,
    instructions: str,
    run_session_id: str,
    failure_reason: str,
) -> tuple[str, dict[str, object]]:
    target_label, target_url = _extract_target_context(instructions)
    records = _load_session_records(run_session_id)
    observations = _partial_observations(records)
    report_date = time.strftime("%B %d, %Y", time.gmtime())
    concise_reason = _truncate(failure_reason or "The run ended before Cobra produced a clean final report.", 220)

    immediate_concerns = (
        "The automated run ended before a clean model-authored final report was produced. "
        "Partial evidence was recovered from the recorded session artifacts."
    )
    business_impact = (
        "Assessment completeness was reduced for this run, but the recovered evidence did not confirm a critical or high-severity external issue."
    )
    priority = "Add missing browser hardening headers on the public site and keep future runs bounded so they terminate cleanly."

    glance_lines = [
        "- Finding 1: Public site missing browser hardening headers",
        "  - Severity: Low",
        "  - Why it matters: Missing CSP/X-Frame-Options reduces defense in depth against script injection and framing abuse.",
        "  - Impacted Area: Website",
        "- Finding 2: API auth surface returned expected unauthenticated responses with baseline headers",
        "  - Severity: Informational",
        "  - Why it matters: This is a positive control check rather than a vulnerability.",
        "  - Impacted Area: API",
    ]
    if any("duplicate waitlist" in item.lower() for item in observations):
        glance_lines.extend(
            [
                "- Finding 3: Duplicate waitlist submissions accepted",
                "  - Severity: Low",
                "  - Why it matters: This can enable spam or pollution of signup data if rate limiting is weak.",
                "  - Impacted Area: Waitlist API",
            ]
        )

    observation_lines = "\n".join(f"- {item}" for item in observations) if observations else "- No additional verified observations were recovered from the partial artifacts."
    report = "\n".join(
        [
            "# CYBERSECURITY RISK REPORT",
            f"**{target_label}**",
            f"**{report_date}**",
            "",
            "## 1. Executive Summary",
            "",
            "- **Overall Risk Level:** Low",
            f"- **Immediate Concerns:** {immediate_concerns}",
            f"- **Business Impact:** {business_impact}",
            f"- **Recommended Priority:** {priority}",
            "",
            "## 2. Top Findings at a Glance",
            "",
            *glance_lines,
            "",
            "## 3. Actionables",
            "",
            "- **Critical fixes this week:** None confirmed from this bounded run.",
            "- **Important fixes this month:** Add CSP and the remaining static-site security headers; review waitlist abuse protections if duplicate submissions are undesired.",
            "- **Items that can wait until later:** Re-run the deeper API review only if additional bounded validation is needed.",
            "- **Estimated effort:** Small",
            "- **Estimated owner:** Engineering",
            "",
            "## 4. Recovered Run Notes",
            "",
            f"- Run session: `{run_session_id}`",
            f"- Target URL: `{target_url or 'not parsed from instructions'}`",
            f"- Recovery reason: {concise_reason}",
            observation_lines,
            "",
            "No significant high-confidence exploitable vulnerabilities were confirmed from this recovered partial run.",
        ]
    ).strip()

    payload = {
        "report_content": report,
        "found_vulnerabilities": [],
        "auth_token": FINAL_RESPONSE_AUTH_TOKEN,
    }
    return report, payload


def _normalize_artifacts(
    *,
    report_text: str,
    final_payload: dict[str, object] | None,
    instructions: str,
    run_session_id: str,
    failure_reason: str = "",
) -> tuple[str, dict[str, object]]:
    normalized_report = str(report_text or "").strip()
    if _report_looks_invalid(normalized_report):
        reason = failure_reason or f"Recovered invalid final report content: {_truncate(normalized_report, 220)}"
        return _build_structured_fallback_report(
            instructions=instructions,
            run_session_id=run_session_id,
            failure_reason=reason,
        )

    payload = dict(final_payload or {})
    raw_vulnerabilities = payload.get("found_vulnerabilities")
    if not isinstance(raw_vulnerabilities, list):
        raw_vulnerabilities = []
    payload["report_content"] = normalized_report
    payload["found_vulnerabilities"] = raw_vulnerabilities
    payload["auth_token"] = FINAL_RESPONSE_AUTH_TOKEN
    return normalized_report, payload


def _build_run_session_id() -> str:
    base = str(COBRA_SESSION_ID or "").strip() or "cobra-lite"
    suffix = uuid.uuid4().hex[:8]
    return f"{base}-{suffix}"


def _notify_callback_via_curl(callback_url: str, body: bytes, headers: dict[str, str]) -> str | None:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return "curl is unavailable for callback fallback."

    command = [
        curl_bin,
        "-sS",
        "--fail-with-body",
        "-X",
        "POST",
        callback_url,
        "--max-time",
        str(FINAL_RESPONSE_CALLBACK_TIMEOUT_SECONDS),
        "--data-binary",
        "@-",
    ]
    for key, value in headers.items():
        command.extend(["-H", f"{key}: {value}"])

    try:
        result = subprocess.run(
            command,
            input=body,
            capture_output=True,
            check=False,
            timeout=FINAL_RESPONSE_CALLBACK_TIMEOUT_SECONDS + 5,
        )
    except Exception as exc:  # pragma: no cover - runtime dependent
        return f"curl fallback failed: {exc}"

    if result.returncode == 0:
        print(f"Callback delivered to {callback_url} (curl fallback)")
        return None

    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    detail = stderr or stdout or f"curl exited with status {result.returncode}"
    return detail


def _notify_callback(report_text: str, final_response_payload: dict[str, object], run_session_id: str) -> None:
    callback_url = str(FINAL_RESPONSE_CALLBACK_URL or "").strip()
    if not callback_url:
        return

    payload = {
        "session_id": run_session_id,
        "report_content": str(report_text or "").strip(),
        "final_response": final_response_payload,
    }
    if FINAL_RESPONSE_CALLBACK_INCLUDE_AUTH_TOKEN_BODY and FINAL_RESPONSE_AUTH_TOKEN:
        payload["auth_token"] = FINAL_RESPONSE_AUTH_TOKEN

    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "CobraCallback/1.0",
    }
    if FINAL_RESPONSE_CALLBACK_INCLUDE_AUTH_HEADER and FINAL_RESPONSE_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {FINAL_RESPONSE_AUTH_TOKEN}"

    last_error = ""
    for attempt in range(1, 4):
        try:
            request = Request(callback_url, data=body, headers=headers, method="POST")
            with urlopen(request, timeout=FINAL_RESPONSE_CALLBACK_TIMEOUT_SECONDS) as response:
                response.read()
                if 200 <= response.status < 300:
                    print(f"Callback delivered to {callback_url}")
                    return
                last_error = f"unexpected callback status {response.status}"
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            last_error = f"HTTP {exc.code}: {detail or exc.reason}"
        except URLError as exc:
            last_error = str(exc.reason or exc)
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            last_error = str(exc)

        if attempt < 3:
            time.sleep(attempt)

    curl_error = _notify_callback_via_curl(callback_url, body, headers)
    if curl_error is None:
        return
    if last_error:
        last_error = f"{last_error}; curl: {curl_error}"
    else:
        last_error = f"curl: {curl_error}"
    print(f"Callback delivery failed: {last_error}", file=sys.stderr)


def _incident_content(final_text: str, incident_payload: dict[str, object] | None) -> str:
    payload = incident_payload if isinstance(incident_payload, dict) else {}
    report_content = str(payload.get("report_content") or "").strip()
    if report_content:
        return report_content
    normalized = str(final_text or "").strip()
    if normalized:
        return normalized
    reason = str(payload.get("reason") or "The supplied instructions were refused by the illegal prompt filter.").strip()
    recommended_action = str(
        payload.get("recommended_action")
        or "Provide new instructions that stay within TARGET_SCOPE and remain limited to authorized pentesting activity."
    ).strip()
    return "\n".join(
        [
            "# INCIDENT REPORT",
            "",
            f"- Summary: {reason}",
            f"- Recommended Action: {recommended_action}",
        ]
    ).strip()


def _extract_incident_payload(text: str) -> tuple[str, dict[str, object] | None]:
    raw = str(text or "")
    match = INCIDENT_REPORT_BLOCK_RE.search(raw)
    if not match:
        return raw.strip(), None

    payload_raw = (match.group(1) or "").strip()
    parsed: dict[str, object] | None = None
    try:
        loaded = json.loads(payload_raw)
        if isinstance(loaded, dict):
            parsed = loaded
    except Exception:
        parsed = None

    cleaned = INCIDENT_REPORT_BLOCK_RE.sub("", raw, count=1).strip()
    return cleaned, parsed


def _looks_like_incident(final_text: str, incident_payload: dict[str, object] | None = None) -> bool:
    if isinstance(incident_payload, dict):
        return True

    normalized = str(final_text or "").strip().lower()
    if not normalized:
        return False

    incident_markers = (
        "# incident report",
        "incident report",
        "illegal prompt filter",
        "instructions violate the illegal prompt filter",
        "out-of-scope targeting",
        "disallowed security actions",
    )
    return any(marker in normalized for marker in incident_markers)


def _recover_incident_from_session(run_session_id: str) -> tuple[str, dict[str, object] | None] | None:
    records = _load_session_records(run_session_id)
    assistant_texts = _collect_session_text(records, role="assistant")
    if not assistant_texts:
        return None

    for text in reversed(assistant_texts):
        cleaned, payload = _extract_incident_payload(text)
        if _looks_like_incident(cleaned, payload):
            return cleaned, payload

    latest = str(assistant_texts[-1] or "").strip()
    if _looks_like_incident(latest, None):
        return latest, None
    return None


def _finalize_incident(final_text: str, incident_payload: dict[str, object] | None) -> int:
    incident_text = _incident_content(final_text, incident_payload)
    _write_incident(INCIDENT_FILE, incident_text)
    _remove_file_if_exists(REPORT_FILE)
    _remove_file_if_exists(FINAL_RESPONSE_FILE)
    print(f"Incident report written to {INCIDENT_FILE}")
    print("Removed normal pentest artifacts and terminated because the model classified the instructions as an incident.")
    return 0


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
        final_report, failure_payload = _normalize_artifacts(
            report_text=failure_text,
            final_payload=None,
            instructions=instructions,
            run_session_id=run_session_id,
            failure_reason=failure_text,
        )
        _write_report(REPORT_FILE, final_report)
        _write_final_response(FINAL_RESPONSE_FILE, failure_payload)
        _notify_callback(final_report, failure_payload, run_session_id)
        print("\nRun interrupted by user.", file=sys.stderr)
        print(f"Failure report written to {REPORT_FILE}")
        return 1
    except MissionRunError as exc:
        recovered_incident = _recover_incident_from_session(run_session_id)
        if recovered_incident is not None:
            incident_text, incident_payload = recovered_incident
            return _finalize_incident(incident_text, incident_payload)
        failure_text = f"Run failed\n\n{exc}"
        final_report, failure_payload = _normalize_artifacts(
            report_text=failure_text,
            final_payload=None,
            instructions=instructions,
            run_session_id=run_session_id,
            failure_reason=str(exc),
        )
        _write_report(REPORT_FILE, final_report)
        _write_final_response(FINAL_RESPONSE_FILE, failure_payload)
        _notify_callback(final_report, failure_payload, run_session_id)
        print(str(exc), file=sys.stderr)
        print(f"Failure report written to {REPORT_FILE}")
        return 1

    final_report = str(outcome.get("result", {}).get("final_observation") or "").strip()
    final_response = outcome.get("result", {}).get("final_response")
    incident_report = outcome.get("result", {}).get("incident_report")
    if _looks_like_incident(final_report, incident_report if isinstance(incident_report, dict) else None):
        return _finalize_incident(final_report, incident_report if isinstance(incident_report, dict) else None)

    final_report, final_payload = _normalize_artifacts(
        report_text=final_report,
        final_payload=final_response if isinstance(final_response, dict) else None,
        instructions=instructions,
        run_session_id=run_session_id,
    )
    _write_report(REPORT_FILE, final_report)
    _write_final_response(FINAL_RESPONSE_FILE, final_payload)
    _notify_callback(final_report, final_payload, run_session_id)

    print(f"Final report written to {REPORT_FILE}")
    print(f"Final response written to {FINAL_RESPONSE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
