# Quick Start

```bash
npm install -g openclaw
openclaw gateway start
python -m pip install -r requirements.txt
cp .env.example .env
python app.py
```

Minimum `.env`:

```env
ANTHROPIC_API_KEY=your-anthropic-api-key
OPENCLAW_GATEWAY_URL=http://127.0.0.1:18789
TARGET_SCOPE=scanme.nmap.org
COBRA_INSTRUCTIONS=Scan scanme.nmap.org for open ports and services and produce a concise final report.
```

Output:

```text
./data/final_report.md
```

If the model refuses the instructions as an incident, Cobra writes `./data/incident.md` instead of the normal pentest artifacts.
