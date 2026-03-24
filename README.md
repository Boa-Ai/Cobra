# Cobra

Cobra is now a headless batch runner for OpenClaw.

Flow:

1. Configure `.env`
2. Run `python app.py`
3. Cobra reads the allowed target scope and mission instructions from `.env`
4. OpenClaw executes the mission
5. The final report is written to `./data/final_report.md`, or `./data/incident.md` if the model refuses the instructions as an incident

## Requirements

- Python 3.11+
- An OpenClaw gateway reachable at `OPENCLAW_GATEWAY_URL`
- A valid `ANTHROPIC_API_KEY`

## Setup

```bash
python -m pip install -r requirements.txt
cp .env.example .env
```

Update `.env`:

```env
ANTHROPIC_API_KEY=your-anthropic-api-key
OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789
TARGET_SCOPE=scanme.nmap.org
COBRA_INSTRUCTIONS=Scan scanme.nmap.org for open ports and services and produce a concise final report.
```

## Run

```bash
python app.py
```

On success, the report is written to:

```text
./data/final_report.md
```

If the model determines the supplied instructions violate the illegal prompt filter, Cobra writes:

```text
./data/incident.md
```

## Optional Settings

`COBRA_INSTRUCTIONS_FILE`
- Path to a text file containing the mission. If set, it is used instead of `COBRA_INSTRUCTIONS`.

`TARGET_SCOPE`
- Allowed target scope provided to the pentesting model. Use a hostname, URL host, IP, CIDR, comma-separated list, or a fuller human-readable scope contract.

`COBRA_SESSION_ID`
- Base prefix for the OpenClaw session ID. Each `python app.py` run gets a fresh session with a random suffix to avoid transcript carryover between missions.

`DATA_DIR`
- Output directory for runtime state and reports. Default: `./data`

`REPORT_FILE`
- Final report path. Default: `./data/final_report.md`

`FINAL_RESPONSE_FILE`
- Machine-readable JSON payload for your upstream server. Default: `./data/final_response.json`

`INCIDENT_FILE`
- Incident report path used when the model refuses the supplied instructions. Default: `./data/incident.md`

`FINAL_RESPONSE_AUTH_TOKEN`
- Auth token injected into `final_response.json` at write time.

## Runtime Files

By default Cobra stores runtime artifacts under `./data`:

- `final_report.md`
- `final_response.json`
- `incident.md`
- `state.json`
- `sessions.json`
- `graph.json`
