import os
import re
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


def env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    candidate = Path(raw.strip()).expanduser()
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


REQUEST_TIMEOUT_SECONDS = 12
CHAT_HISTORY_MAX_MESSAGES = int(os.getenv("CHAT_HISTORY_MAX_MESSAGES", "2000"))
CHAT_HISTORY_MAX_CHARS = int(os.getenv("CHAT_HISTORY_MAX_CHARS", "240000"))
COBRA_EXECUTION_MODE = (os.getenv("COBRA_EXECUTION_MODE", "cli_only").strip().lower() or "cli_only")
COBRA_REQUIRE_LIVE_TELEMETRY = env_flag("COBRA_REQUIRE_LIVE_TELEMETRY", True)
COBRA_ALLOW_NONSTREAM_FALLBACK = env_flag("COBRA_ALLOW_NONSTREAM_FALLBACK", False)
COBRA_REQUIRE_TERMINAL_ACTIONS = env_flag("COBRA_REQUIRE_TERMINAL_ACTIONS", True)
COBRA_AUTO_INSTALL_TOOLS = env_flag("COBRA_AUTO_INSTALL_TOOLS", True)
DATA_DIR = env_path("DATA_DIR", BASE_DIR / "data")
PROMPTS_DIR = env_path("PROMPTS_DIR", DATA_DIR / "prompts")
REPORT_FILE = env_path("REPORT_FILE", DATA_DIR / "final_report.md")
FINAL_RESPONSE_FILE = env_path("FINAL_RESPONSE_FILE", DATA_DIR / "final_response.json")

STATE_FILE = env_path("STATE_FILE", DATA_DIR / "state.json")
SESSIONS_FILE = env_path("SESSIONS_FILE", DATA_DIR / "sessions.json")
GRAPH_FILE = env_path("GRAPH_FILE", DATA_DIR / "graph.json")


def default_openclaw_gateway_url() -> str:
    configured_url = (os.getenv("OPENCLAW_GATEWAY_URL") or "").strip()
    if configured_url:
        return configured_url
    return "http://127.0.0.1:18789"


OPENCLAW_GATEWAY_URL = default_openclaw_gateway_url()
OPENCLAW_SESSION_KEY = os.getenv("OPENCLAW_SESSION_KEY", "")
OPENCLAW_SESSION_ID = os.getenv("OPENCLAW_SESSION_ID", OPENCLAW_SESSION_KEY or "cobra-lite")
COBRA_SESSION_ID = (os.getenv("COBRA_SESSION_ID", OPENCLAW_SESSION_ID).strip() or OPENCLAW_SESSION_ID)
# Set to 0 (or a negative value) for no local timeout cap.
OPENCLAW_AGENT_TIMEOUT_SECONDS = int(os.getenv("OPENCLAW_AGENT_TIMEOUT_SECONDS", "0"))
OPENCLAW_WS_ACCEPTED_IDLE_SECONDS = int(os.getenv("OPENCLAW_WS_ACCEPTED_IDLE_SECONDS", "30"))
OPENCLAW_VERBOSE_LEVEL = os.getenv("OPENCLAW_VERBOSE_LEVEL", "full").strip() or "full"
FINAL_RESPONSE_AUTH_TOKEN = os.getenv("FINAL_RESPONSE_AUTH_TOKEN", "").strip()
OPENCLAW_PROTOCOL_VERSION = 3
GATEWAY_SCOPES = ["operator.admin", "operator.approvals", "operator.pairing"]
OPENCLAW_STATE_DIR = Path(os.getenv("OPENCLAW_STATE_DIR", str(Path.home() / ".openclaw")))
OPENCLAW_MAIN_AGENT_DIR = Path(os.getenv("OPENCLAW_MAIN_AGENT_DIR", str(OPENCLAW_STATE_DIR / "agents" / "main" / "agent")))
OPENCLAW_IDENTITY_PATH = Path(
    os.getenv("OPENCLAW_DEVICE_IDENTITY_PATH", str(OPENCLAW_STATE_DIR / "identity" / "device.json"))
)
OPENCLAW_DEVICE_AUTH_PATH = Path(
    os.getenv("OPENCLAW_DEVICE_AUTH_PATH", str(OPENCLAW_STATE_DIR / "identity" / "device-auth.json"))
)

DIAGNOSTIC_EXEC_LINE_RE = re.compile(r"^\s*[⚠️❌✅]?\s*🛠️\s*Exec:", re.IGNORECASE)

if COBRA_AUTO_INSTALL_TOOLS:
    MISSING_TOOL_RUNTIME_POLICY = """- If a command is missing (e.g. "command not found" or exit code 127), do not stop there.
- Attempt to install the missing command with the system package manager when possible.
- Detect package manager in this order: apt-get, dnf, yum, apk, pacman, brew.
- Use non-interactive installs and avoid hanging prompts (e.g. sudo -n when sudo is needed).
- For DNS binaries, map command to package names:
  - Debian/Ubuntu: host/dig/nslookup -> dnsutils
  - Fedora/RHEL: host/dig/nslookup -> bind-utils
  - Alpine: host/dig/nslookup -> bind-tools
- After install, verify with `command -v <cmd>` and re-run the original command.
- If installation fails (permissions/network/package unavailable), report the exact failure and continue with available tools.
- Do not use multi-target `which` checks that fail when any single binary is missing; use per-command `command -v` checks."""
    MISSING_TOOL_SECURITY_POLICY = (
        "If a command is unavailable, attempt package-manager installation, verify availability, and re-run it. "
        "If install fails, report the exact reason and continue with available tools."
    )
else:
    MISSING_TOOL_RUNTIME_POLICY = "- If a command is missing, report it clearly and continue with available CLI commands."
    MISSING_TOOL_SECURITY_POLICY = "If a command is unavailable, say it is missing and continue with available CLI tooling."
