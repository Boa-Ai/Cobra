const hasGateway = window.CLAW_HAS_GATEWAY === true;
const hasAnthropicKey = window.CLAW_HAS_ANTHROPIC_KEY === true;
const defaultGatewayUrl = window.CLAW_DEFAULT_GATEWAY_URL || "http://127.0.0.1:18789";
const desktopMediaQuery = window.matchMedia("(max-width: 1160px)");

const workspace = document.querySelector(".workspace");
const keyGate = document.getElementById("key-gate");
const mainApp = document.getElementById("main-app");
const composerDock = document.getElementById("composer-dock");
const chatColumn = document.querySelector(".chat-column");
const missionTabOverview = document.getElementById("mission-tab-overview");
const missionTabReport = document.getElementById("mission-tab-report");
const missionActiveTitle = document.getElementById("mission-active-title");
const missionOverviewStatus = document.getElementById("mission-overview-status");
const missionOverviewContent = document.getElementById("mission-overview-content");
const missionOverviewView = document.getElementById("mission-overview-view");
const missionReportView = document.getElementById("mission-report-view");
const missionLoadingState = document.getElementById("mission-loading-state");
const missionReportSummary = document.getElementById("mission-report-summary");
const reportSeverityTableBody = document.getElementById("report-severity-table-body");
const reportFindingsList = document.getElementById("report-findings-list");
const downloadReportBtn = document.getElementById("download-report-btn");
const chatThread = document.getElementById("chat-thread");
const sessionPane = document.getElementById("session-pane");
const sessionList = document.getElementById("session-list");
const sessionStatus = document.getElementById("session-status");
const newSessionBtn = document.getElementById("new-session-btn");
const sessionFoldBtn = document.getElementById("session-fold-btn");
const chatExecutionResizer = document.getElementById("chat-execution-resizer");
const executionPane = document.getElementById("execution-pane");
const executionFeed = document.getElementById("execution-feed");
const executionTerminalView = document.getElementById("execution-terminal-view");
const executionGraphView = document.getElementById("execution-graph-view");
const executionFilesView = document.getElementById("execution-files-view");
const executionTabTerminal = document.getElementById("execution-tab-terminal");
const executionTabGraph = document.getElementById("execution-tab-graph");
const executionTabFiles = document.getElementById("execution-tab-files");
const filesUpBtn = document.getElementById("files-up-btn");
const filesRefreshBtn = document.getElementById("files-refresh-btn");
const filesBreadcrumb = document.getElementById("files-breadcrumb");
const filesList = document.getElementById("files-list");
const filesPreviewMeta = document.getElementById("files-preview-meta");
const filesPreviewContent = document.getElementById("files-preview-content");
const filesView = executionFilesView?.querySelector(".files-view") || null;
const filesSelector = executionFilesView?.querySelector(".files-selector") || null;
const filesResizer = document.getElementById("files-resizer");
const graphCanvas = document.getElementById("graph-canvas");
const graphEdgesLayer = document.getElementById("graph-edges-layer");
const graphNodesLayer = document.getElementById("graph-nodes-layer");
const graphStatus = document.getElementById("graph-status");
const graphResetBtn = document.getElementById("graph-reset-btn");
const graphRefreshBtn = document.getElementById("graph-refresh-btn");
const graphAddNoteBtn = document.getElementById("graph-add-note-btn");
const graphNodeId = document.getElementById("graph-node-id");
const graphNodeLabelInput = document.getElementById("graph-node-label");
const graphNodeTypeInput = document.getElementById("graph-node-type");
const graphNodeStatusInput = document.getElementById("graph-node-status");
const graphNodeSeverityInput = document.getElementById("graph-node-severity");
const graphNodeConfidenceInput = document.getElementById("graph-node-confidence");
const graphNodeDescriptionInput = document.getElementById("graph-node-description");
const graphNodeSaveBtn = document.getElementById("graph-node-save-btn");
const graphNodeLinks = document.getElementById("graph-node-links");

const keyForm = document.getElementById("key-form");
const keyInput = document.getElementById("api-key-input");
const keyStatus = document.getElementById("key-status");

const promptForm = document.getElementById("prompt-form");
const targetUrlInput = document.getElementById("target-url-input");
const instructionsInput = document.getElementById("instructions-input");
const promptStatus = document.getElementById("prompt-status");
const promptSubmit = promptForm ? promptForm.querySelector(".prompt-submit") : null;

const settingsBtn = document.getElementById("settings-btn");
const settingsModal = document.getElementById("settings-modal");
const closeSettingsBtn = document.getElementById("close-settings");
const settingsForm = document.getElementById("settings-form");
const settingsKeyInput = document.getElementById("settings-key-input");
const settingsStatus = document.getElementById("settings-status");

const authModal = document.getElementById("auth-modal");
const authForm = document.getElementById("auth-form");
const authKeyInput = document.getElementById("auth-key-input");
const authStatus = document.getElementById("auth-status");

let isRunning = false;
let autoScrollEnabled = true;
let unseenEventCount = 0;
let jumpLatestBtn = null;
let activeRunAbortController = null;
let activeRunStopRequested = false;
let activeRunMeta = null;
const ACTIVE_SESSION_STORAGE_KEY = "cobraLite.activeSessionId.v1";
const SESSION_PANE_COLLAPSED_KEY = "cobraLite.sessionPaneCollapsed.v1";
const EXECUTION_WIDTH_STORAGE_KEY = "cobraLite.executionPaneWidth.v2";
const FILE_SELECTOR_HEIGHT_STORAGE_KEY = "cobraLite.filesSelectorHeight.v1";

let activeSessionId = null;
let activeSessionMessages = [];
let activeSessionOverview = "";
let activeSessionOverviewUpdatedAt = null;
let hasSubmittedPrompt = false;
let sessionSummaries = [];
let anthropicConfigured = hasAnthropicKey;
let sessionPaneCollapsed = false;
let paneResizeHandlersBound = false;
let activeMissionCenterTab = "overview";
const executionHistoryBySession = new Map();
let executionPersistTimer = null;
let executionPersistInFlight = false;
let executionPersistPending = null;
const GRAPH_MIN_SCALE = 0.08;
const GRAPH_MAX_SCALE = 3.2;
const GRAPH_AUTOFIT_MIN_SCALE = 0.2;
const fileViewerState = {
  initialized: false,
  loadingDirectory: false,
  workspaceRoot: "",
  currentPath: "",
  parentPath: null,
  selectedFilePath: "",
};
const graphState = {
  loadedSessionId: null,
  loading: false,
  nodes: [],
  edges: [],
  selectedNodeId: null,
  viewport: { x: 0, y: 0, scale: 1 },
  pan: null,
  draggingNodeId: null,
  dragStart: null,
  needsSpreadPass: false,
  autoFitPending: false,
};

function decodeEscapedSequences(text) {
  if (!text || !text.includes("\\")) {
    return text;
  }

  const decodeOnce = (input) =>
    input
      .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
      .replace(/\\([nrt"\\/])/g, (_, token) => {
        if (token === "n") return "\n";
        if (token === "r") return "\r";
        if (token === "t") return "\t";
        if (token === '"') return '"';
        if (token === "/") return "/";
        return "\\";
      });

  let decoded = text;
  for (let i = 0; i < 3; i += 1) {
    const next = decodeOnce(decoded);
    if (next === decoded) {
      break;
    }
    decoded = next;
  }
  return decoded;
}

function normalizeText(value, fallback = "", options = {}) {
  const decodeEscapes = options.decodeEscapes === true;
  if (value === null || value === undefined) {
    return fallback;
  }
  const raw = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  let normalized = String(raw)
    .replace(/\r\n?/g, "\n")
    .replace(/\u0000/g, "")
    .replace(/\x1b\[[0-9;]*[A-Za-z]/g, "")
    .trim();

  if (decodeEscapes) {
    normalized = decodeEscapedSequences(normalized);
  }

  return normalized;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function sanitizeMarkdownUrl(rawUrl) {
  const source = String(rawUrl || "").trim().replace(/&amp;/g, "&");
  if (!source) return "";
  try {
    const parsed = new URL(source, window.location.origin);
    const protocol = parsed.protocol.toLowerCase();
    if (protocol === "http:" || protocol === "https:" || protocol === "mailto:") {
      return parsed.href;
    }
  } catch (_err) {
    return "";
  }
  return "";
}

function renderInlineMarkdown(sourceText) {
  if (!sourceText) return "";
  let working = escapeHtml(sourceText);
  const codeTokens = [];

  working = working.replace(/`([^`]+)`/g, (_match, codeInner) => {
    const token = `@@CODE_${codeTokens.length}@@`;
    codeTokens.push(`<code>${codeInner}</code>`);
    return token;
  });

  working = working.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_match, label, url) => {
    const safeHref = sanitizeMarkdownUrl(url);
    if (!safeHref) {
      return label;
    }
    return `<a href="${escapeHtml(safeHref)}" target="_blank" rel="noopener noreferrer">${label}</a>`;
  });

  working = working.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  working = working.replace(/(^|[\s(])\*([^*]+)\*(?=[\s).,!?:;]|$)/g, "$1<em>$2</em>");

  working = working.replace(/@@CODE_(\d+)@@/g, (_match, idx) => codeTokens[Number(idx)] || "");
  return working;
}

function renderMarkdown(markdownText) {
  const text = normalizeText(markdownText, "");
  if (!text) return "";

  const lines = text.split("\n");
  const htmlParts = [];
  let paragraphLines = [];
  let inUl = false;
  let inOl = false;
  let inCode = false;
  let codeLang = "";
  let codeLines = [];

  const closeLists = () => {
    if (inUl) {
      htmlParts.push("</ul>");
      inUl = false;
    }
    if (inOl) {
      htmlParts.push("</ol>");
      inOl = false;
    }
  };

  const flushParagraph = () => {
    if (!paragraphLines.length) return;
    const paragraphHtml = paragraphLines.map((line) => renderInlineMarkdown(line)).join("<br />");
    htmlParts.push(`<p>${paragraphHtml}</p>`);
    paragraphLines = [];
  };

  const flushCode = () => {
    const escapedCode = escapeHtml(codeLines.join("\n"));
    const langClass = codeLang ? ` class="language-${escapeHtml(codeLang)}"` : "";
    htmlParts.push(`<pre><code${langClass}>${escapedCode}</code></pre>`);
    inCode = false;
    codeLang = "";
    codeLines = [];
  };

  for (const line of lines) {
    if (inCode) {
      if (/^```/.test(line.trim())) {
        flushCode();
      } else {
        codeLines.push(line);
      }
      continue;
    }

    const fenceMatch = line.match(/^```+\s*([^`\s]*)\s*$/);
    if (fenceMatch) {
      flushParagraph();
      closeLists();
      inCode = true;
      codeLang = String(fenceMatch[1] || "").trim();
      continue;
    }

    if (!line.trim()) {
      flushParagraph();
      closeLists();
      continue;
    }

    const headingMatch = line.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      flushParagraph();
      closeLists();
      const level = headingMatch[1].length;
      const headingBody = renderInlineMarkdown(headingMatch[2]);
      htmlParts.push(`<h${level}>${headingBody}</h${level}>`);
      continue;
    }

    const ulMatch = line.match(/^\s*[-*+]\s+(.*)$/);
    if (ulMatch) {
      flushParagraph();
      if (inOl) {
        htmlParts.push("</ol>");
        inOl = false;
      }
      if (!inUl) {
        htmlParts.push("<ul>");
        inUl = true;
      }
      htmlParts.push(`<li>${renderInlineMarkdown(ulMatch[1])}</li>`);
      continue;
    }

    const olMatch = line.match(/^\s*\d+\.\s+(.*)$/);
    if (olMatch) {
      flushParagraph();
      if (inUl) {
        htmlParts.push("</ul>");
        inUl = false;
      }
      if (!inOl) {
        htmlParts.push("<ol>");
        inOl = true;
      }
      htmlParts.push(`<li>${renderInlineMarkdown(olMatch[1])}</li>`);
      continue;
    }

    if (inUl || inOl) {
      closeLists();
    }
    paragraphLines.push(line.trimEnd());
  }

  if (inCode) {
    flushCode();
  }
  flushParagraph();
  closeLists();

  return htmlParts.join("\n");
}

function getMarkdownSource(node) {
  if (!node) return "";
  const stored = node.dataset.rawMarkdown;
  if (typeof stored === "string") {
    return stored;
  }
  return normalizeText(node.textContent, "");
}

function setMarkdownContent(node, value) {
  if (!node) return;
  const normalized = normalizeText(value, "");
  node.dataset.rawMarkdown = normalized;
  node.innerHTML = normalized ? renderMarkdown(normalized) : "";
}

function parseMissionOverviewSections(markdownText) {
  const text = normalizeText(markdownText, "");
  if (!text) return [];
  const sections = [];
  let current = null;
  for (const rawLine of text.split("\n")) {
    const headingMatch = rawLine.match(/^##\s+(.+?)\s*$/);
    if (headingMatch) {
      if (current) {
        sections.push(current);
      }
      current = { title: normalizeText(headingMatch[1], "").trim(), lines: [] };
      continue;
    }
    if (!current) {
      return [];
    }
    current.lines.push(rawLine);
  }
  if (current) {
    sections.push(current);
  }
  return sections.filter((section) => section.title);
}

function setMissionOverviewContent(node, value) {
  if (!node) return;
  const normalized = normalizeText(value, "");
  node.dataset.rawMarkdown = normalized;
  node.innerHTML = "";
  if (!normalized) {
    return;
  }

  const sections = parseMissionOverviewSections(normalized);
  if (!sections.length) {
    node.innerHTML = renderMarkdown(normalized);
    return;
  }

  for (const [index, section] of sections.entries()) {
    const card = document.createElement("section");
    card.className = `mission-overview-section ${index === 0 ? "lead" : ""}`.trim();

    const title = document.createElement("h4");
    title.className = "mission-overview-section-title";
    title.textContent = section.title;
    card.appendChild(title);

    const body = document.createElement("div");
    body.className = "mission-overview-section-body markdown-content";
    setMarkdownContent(body, normalizeText(section.lines.join("\n"), "_Not captured yet._"));
    card.appendChild(body);

    node.appendChild(card);
  }
}

function stripMarkdownToText(value) {
  return normalizeText(value, "")
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

function buildDetailedReportModel() {
  const levels = ["P1", "P2", "P3", "P4", "P5"];
  const sections = parseMissionOverviewSections(activeSessionOverview);
  const summarySection = sections[0] || null;
  const scopeSection = sections.find((section) => section.title.toLowerCase() === "scope") || null;
  const stateSection = sections.find((section) => section.title.toLowerCase() === "current state") || null;
  const findings = [];

  if (stateSection) {
    const items = normalizeText(stateSection.lines.join("\n"), "")
      .split("\n")
      .map((line) => line.replace(/^\s*[-*+]\s+/, "").trim())
      .filter(Boolean)
      .filter((line) => !/^no\s+(high-confidence\s+)?findings/i.test(line));

    for (const item of items.slice(0, 6)) {
      findings.push({
        priority: "P3",
        title: item.length > 72 ? `${item.slice(0, 69)}...` : item,
        summary: item,
        howFound:
          "Placeholder report entry generated from the Mission Summary current-state section. Replace this logic with your preferred reporting pipeline.",
      });
    }
  }

  const counts = Object.fromEntries(levels.map((level) => [level, 0]));
  for (const finding of findings) {
    counts[finding.priority] = (counts[finding.priority] || 0) + 1;
  }

  return {
    title: `${formatMissionTitle(missionActiveTitle?.textContent || "Mission")} Detailed Report`,
    summary: summarySection ? stripMarkdownToText(summarySection.lines.join("\n")) : "No executive summary available yet.",
    scope: scopeSection ? stripMarkdownToText(scopeSection.lines.join("\n")) : "Scope not captured yet.",
    counts,
    findings,
    generatedAt: new Date().toLocaleString(),
  };
}

function renderDetailedReport() {
  const model = buildDetailedReportModel();
  if (missionReportSummary) {
    missionReportSummary.innerHTML = `
      <div class="report-meta-row">
        <div>
          <div class="report-meta-label">Report Title</div>
          <div class="report-meta-value">${escapeHtml(model.title)}</div>
        </div>
        <div>
          <div class="report-meta-label">Generated</div>
          <div class="report-meta-value">${escapeHtml(model.generatedAt)}</div>
        </div>
      </div>
      <div class="report-summary-block">
        <h4>Executive Summary</h4>
        <p>${escapeHtml(model.summary)}</p>
      </div>
      <div class="report-summary-block">
        <h4>Scope</h4>
        <p>${escapeHtml(model.scope)}</p>
      </div>
    `;
  }

  if (reportSeverityTableBody) {
    reportSeverityTableBody.innerHTML = ["P1", "P2", "P3", "P4", "P5"]
      .map(
        (level) => `
          <tr>
            <td>${level}</td>
            <td>${Number(model.counts[level] || 0)}</td>
          </tr>
        `
      )
      .join("");
  }

  if (reportFindingsList) {
    if (!model.findings.length) {
      reportFindingsList.innerHTML = `
        <div class="report-empty-state">
          <h4>No vulnerabilities captured yet</h4>
          <p>
            This placeholder report is active. Edit the report-generation logic to map confirmed findings into P1-P5 entries.
          </p>
        </div>
      `;
      return;
    }

    reportFindingsList.innerHTML = model.findings
      .map(
        (finding, index) => `
          <article class="report-finding-card">
            <div class="report-finding-head">
              <span class="report-priority">${escapeHtml(finding.priority)}</span>
              <h4>${index + 1}. ${escapeHtml(finding.title)}</h4>
            </div>
            <div class="report-finding-grid">
              <div>
                <div class="report-meta-label">Summary</div>
                <p>${escapeHtml(finding.summary)}</p>
              </div>
              <div>
                <div class="report-meta-label">How We Found It</div>
                <p>${escapeHtml(finding.howFound)}</p>
              </div>
            </div>
          </article>
        `
      )
      .join("");
  }
}

function buildDetailedReportPrintHtml() {
  const model = buildDetailedReportModel();
  const rows = ["P1", "P2", "P3", "P4", "P5"]
    .map(
      (level) => `
        <tr>
          <td>${level}</td>
          <td>${Number(model.counts[level] || 0)}</td>
        </tr>
      `
    )
    .join("");
  const findingsHtml = model.findings.length
    ? model.findings
        .map(
          (finding) => `
            <section class="finding">
              <h3>${escapeHtml(finding.priority)} · ${escapeHtml(finding.title)}</h3>
              <p><strong>Summary:</strong> ${escapeHtml(finding.summary)}</p>
              <p><strong>How we found it:</strong> ${escapeHtml(finding.howFound)}</p>
            </section>
          `
        )
        .join("")
    : `<section class="finding"><h3>No vulnerabilities captured yet</h3><p>Edit the report-generation instructions to populate this template with confirmed findings.</p></section>`;

  return `<!doctype html>
  <html lang="en">
    <head>
      <meta charset="utf-8" />
      <title>${escapeHtml(model.title)}</title>
      <style>
        @page { margin: 18mm; size: A4; }
        body { font-family: Arial, sans-serif; color: #111; margin: 0; }
        h1, h2, h3, h4 { margin: 0 0 10px; }
        .meta { display: flex; justify-content: space-between; gap: 16px; margin-bottom: 24px; }
        .section { margin-bottom: 24px; }
        table { width: 100%; border-collapse: collapse; margin-top: 8px; }
        th, td { border: 1px solid #bbb; padding: 8px 10px; text-align: left; }
        th { background: #f3f3f3; }
        .finding { border: 1px solid #bbb; padding: 14px; margin-bottom: 14px; break-inside: avoid; }
        .muted { color: #555; font-size: 12px; }
      </style>
    </head>
    <body>
      <h1>${escapeHtml(model.title)}</h1>
      <div class="meta">
        <div><div class="muted">Generated</div><div>${escapeHtml(model.generatedAt)}</div></div>
        <div><div class="muted">Scope</div><div>${escapeHtml(model.scope)}</div></div>
      </div>
      <section class="section">
        <h2>Executive Summary</h2>
        <p>${escapeHtml(model.summary)}</p>
      </section>
      <section class="section">
        <h2>Priority Summary</h2>
        <table>
          <thead><tr><th>P1 - P5</th><th># of instances</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </section>
      <section class="section">
        <h2>Findings</h2>
        ${findingsHtml}
      </section>
    </body>
  </html>`;
}

function downloadDetailedReportPdf() {
  const printWindow = window.open("", "_blank", "noopener,noreferrer,width=1024,height=768");
  if (!printWindow) return;
  printWindow.document.open();
  printWindow.document.write(buildDetailedReportPrintHtml());
  printWindow.document.close();
  printWindow.focus();
  printWindow.print();
}

function resizeInstructionsInput() {
  if (!instructionsInput) return;
  instructionsInput.style.height = "0px";
  const maxHeight = 256;
  const nextHeight = Math.min(instructionsInput.scrollHeight, maxHeight);
  instructionsInput.style.height = `${Math.max(nextHeight, 44)}px`;
  instructionsInput.style.overflowY = instructionsInput.scrollHeight > maxHeight ? "auto" : "hidden";
}

function buildMissionPrompt(targetUrl, instructions) {
  const url = normalizeText(targetUrl, "");
  const extra = normalizeText(instructions, "");
  if (!url) return "";
  if (!extra) return `Assess the target at ${url}.`;
  return `Assess the target at ${url}.\n\nAdditional instructions:\n${extra}`;
}

function setStatus(node, message, ok) {
  if (!node) return;
  node.textContent = message || "";
  node.classList.remove("ok", "bad");
  if (!message) return;
  node.classList.add(ok ? "ok" : "bad");
}

function formatMissionTitle(value) {
  const normalized = normalizeText(value, "New Mission") || "New Mission";
  return normalized === "New Chat" ? "New Mission" : normalized;
}

function syncActiveMissionHeader(title) {
  if (!missionActiveTitle) return;
  missionActiveTitle.textContent = formatMissionTitle(title);
  missionActiveTitle.title = formatMissionTitle(title);
}

function setMissionOverview(overview, updatedAt = null) {
  activeSessionOverview = normalizeText(overview, "");
  const numericUpdatedAt = Number(updatedAt);
  activeSessionOverviewUpdatedAt = Number.isFinite(numericUpdatedAt) && numericUpdatedAt > 0 ? numericUpdatedAt : null;
  renderMissionOverview();
}

function syncMissionWorkspaceState() {
  const hasOverview = activeSessionOverview.trim().length > 0;
  const hasMissionActivity = activeSessionMessages.length > 0 || hasSubmittedPrompt;
  const isPreSubmit = !hasOverview && !hasMissionActivity && !isRunning;
  const waitingForOverview = !hasOverview && !isPreSubmit;
  document.body.classList.toggle("mission-pre-submit", isPreSubmit);
  document.body.classList.toggle("overview-ready", hasOverview);
  document.body.classList.toggle("overview-pending", waitingForOverview);
  if (!hasOverview) {
    activeMissionCenterTab = "overview";
  }
  missionOverviewView?.classList.toggle("hidden", !hasOverview);
  missionReportView?.classList.toggle("hidden", !hasOverview || activeMissionCenterTab !== "report");
  missionLoadingState?.classList.toggle("hidden", !waitingForOverview);
}

function renderMissionOverview() {
  const hasOverview = activeSessionOverview.trim().length > 0;
  if (missionOverviewContent) {
    setMissionOverviewContent(missionOverviewContent, hasOverview ? activeSessionOverview : "");
  }
  renderDetailedReport();
  syncMissionWorkspaceState();
  if (!missionOverviewStatus) return;
  if (!hasOverview) {
    missionOverviewStatus.textContent = isRunning ? "Generating overview..." : "Awaiting first run";
    return;
  }
  const relative = activeSessionOverviewUpdatedAt ? formatRelativeTime(activeSessionOverviewUpdatedAt) : "";
  missionOverviewStatus.textContent = relative ? `Updated ${relative}` : "Tracked summary";
}

function setUnlocked(unlocked) {
  keyGate?.classList.toggle("hidden", unlocked);
  mainApp?.classList.toggle("hidden", !unlocked);
  composerDock?.classList.toggle("hidden", !unlocked);
  sessionPane?.classList.toggle("hidden", !unlocked);
  chatExecutionResizer?.classList.toggle("hidden", !unlocked);
  executionPane?.classList.toggle("hidden", !unlocked);
  document.body.classList.toggle("sidebar-enabled", unlocked);
  if (unlocked) {
    requestAnimationFrame(() => {
      restoreExecutionWidth();
    });
  }
}

function readStoredNumber(key) {
  try {
    const raw = localStorage.getItem(key);
    if (typeof raw !== "string") return null;
    const parsed = Number.parseFloat(raw.trim());
    return Number.isFinite(parsed) ? parsed : null;
  } catch (_err) {
    return null;
  }
}

function writeStoredNumber(key, value) {
  try {
    localStorage.setItem(key, String(value));
  } catch (_err) {
    // Ignore storage errors
  }
}

function clampNumber(value, min, max) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return min;
  return Math.min(max, Math.max(min, numeric));
}

function getExecutionWidthBounds() {
  const min = 260;
  const currentExecution = executionPane?.getBoundingClientRect().width || min;
  const currentChat = chatColumn?.getBoundingClientRect().width || 0;
  const max = Math.max(min, currentExecution + Math.max(0, currentChat - 520));
  return { min, max };
}

function applyExecutionWidth(widthPx, persist = false) {
  if (!executionPane || desktopMediaQuery.matches) return;
  const { min, max } = getExecutionWidthBounds();
  const next = clampNumber(widthPx, min, max);
  document.documentElement.style.setProperty("--execution-width", `${next}px`);
  chatExecutionResizer?.setAttribute("aria-valuemin", String(Math.round(min)));
  chatExecutionResizer?.setAttribute("aria-valuemax", String(Math.round(max)));
  chatExecutionResizer?.setAttribute("aria-valuenow", String(Math.round(next)));
  if (persist) {
    writeStoredNumber(EXECUTION_WIDTH_STORAGE_KEY, Math.round(next));
  }
}

function restoreExecutionWidth() {
  if (desktopMediaQuery.matches) return;
  const stored = readStoredNumber(EXECUTION_WIDTH_STORAGE_KEY);
  if (stored === null) return;
  applyExecutionWidth(stored, false);
}

function getFilesSelectorHeightBounds() {
  const min = 120;
  const filesRect = filesView?.getBoundingClientRect();
  const resizerRect = filesResizer?.getBoundingClientRect();
  if (!filesRect) {
    return { min, max: min };
  }
  const max = Math.max(min, filesRect.height - 160 - (resizerRect?.height || 0));
  return { min, max };
}

function applyFilesSelectorHeight(heightPx, persist = false) {
  if (!filesView) return;
  const { min, max } = getFilesSelectorHeightBounds();
  const next = clampNumber(heightPx, min, max);
  filesView.style.setProperty("--files-selector-height", `${next}px`);
  filesResizer?.setAttribute("aria-valuemin", String(Math.round(min)));
  filesResizer?.setAttribute("aria-valuemax", String(Math.round(max)));
  filesResizer?.setAttribute("aria-valuenow", String(Math.round(next)));
  if (persist) {
    writeStoredNumber(FILE_SELECTOR_HEIGHT_STORAGE_KEY, Math.round(next));
  }
}

function restoreFilesSelectorHeight() {
  const stored = readStoredNumber(FILE_SELECTOR_HEIGHT_STORAGE_KEY);
  if (stored === null) return;
  applyFilesSelectorHeight(stored, false);
}

function setupResizablePanes() {
  if (paneResizeHandlersBound) return;
  paneResizeHandlersBound = true;

  chatExecutionResizer?.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 || desktopMediaQuery.matches || !executionPane) {
      return;
    }
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = executionPane.getBoundingClientRect().width;
    document.body.classList.add("dragging-col");

    const onPointerMove = (moveEvent) => {
      const deltaX = startX - moveEvent.clientX;
      applyExecutionWidth(startWidth + deltaX, false);
    };

    const onPointerUp = () => {
      document.body.classList.remove("dragging-col");
      const currentWidth = executionPane.getBoundingClientRect().width;
      applyExecutionWidth(currentWidth, true);
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
      window.removeEventListener("pointercancel", onPointerUp);
    };

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    window.addEventListener("pointercancel", onPointerUp);
  });

  chatExecutionResizer?.addEventListener("keydown", (event) => {
    if (desktopMediaQuery.matches || !executionPane) return;
    const step = event.shiftKey ? 42 : 16;
    const current = executionPane.getBoundingClientRect().width;
    const { min, max } = getExecutionWidthBounds();

    if (event.key === "ArrowLeft") {
      event.preventDefault();
      applyExecutionWidth(current + step, true);
    } else if (event.key === "ArrowRight") {
      event.preventDefault();
      applyExecutionWidth(current - step, true);
    } else if (event.key === "Home") {
      event.preventDefault();
      applyExecutionWidth(min, true);
    } else if (event.key === "End") {
      event.preventDefault();
      applyExecutionWidth(max, true);
    }
  });

  filesResizer?.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 || !filesSelector) {
      return;
    }
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = filesSelector.getBoundingClientRect().height;
    document.body.classList.add("dragging-row");

    const onPointerMove = (moveEvent) => {
      const deltaY = moveEvent.clientY - startY;
      applyFilesSelectorHeight(startHeight + deltaY, false);
    };

    const onPointerUp = () => {
      document.body.classList.remove("dragging-row");
      const currentHeight = filesSelector.getBoundingClientRect().height;
      applyFilesSelectorHeight(currentHeight, true);
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
      window.removeEventListener("pointercancel", onPointerUp);
    };

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    window.addEventListener("pointercancel", onPointerUp);
  });

  filesResizer?.addEventListener("keydown", (event) => {
    if (!filesSelector) return;
    const step = event.shiftKey ? 32 : 12;
    const current = filesSelector.getBoundingClientRect().height;
    const { min, max } = getFilesSelectorHeightBounds();

    if (event.key === "ArrowUp") {
      event.preventDefault();
      applyFilesSelectorHeight(current - step, true);
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      applyFilesSelectorHeight(current + step, true);
    } else if (event.key === "Home") {
      event.preventDefault();
      applyFilesSelectorHeight(min, true);
    } else if (event.key === "End") {
      event.preventDefault();
      applyFilesSelectorHeight(max, true);
    }
  });

  window.addEventListener("resize", () => {
    restoreExecutionWidth();
    restoreFilesSelectorHeight();
  });
}

function setRunningState(running) {
  isRunning = running;
  document.body.classList.toggle("mission-running", running);
  if (targetUrlInput) targetUrlInput.disabled = running;
  if (instructionsInput) instructionsInput.disabled = running;
  if (promptSubmit) {
    promptSubmit.disabled = false;
    promptSubmit.classList.toggle("stop-mode", running);
    if (running) {
      promptSubmit.textContent = activeRunStopRequested ? "Stopping..." : "Stop";
      promptSubmit.setAttribute("aria-label", "Stop current run");
      promptSubmit.title = "Stop current run";
    } else {
      promptSubmit.textContent = "Send";
      promptSubmit.removeAttribute("aria-label");
      promptSubmit.removeAttribute("title");
    }
  }
  if (!running) {
    activeRunAbortController = null;
    activeRunStopRequested = false;
    activeRunMeta = null;
  }
  renderMissionOverview();
}

async function requestStopActiveRun() {
  if (!isRunning || activeRunStopRequested) return;
  activeRunStopRequested = true;
  if (promptSubmit) {
    promptSubmit.textContent = "Stopping...";
  }
  setStatus(promptStatus, "Stopping run...", false);

  try {
    const payload = {};
    if (activeRunMeta?.sessionId) payload.session_id = activeRunMeta.sessionId;
    if (activeRunMeta?.runId) payload.run_id = activeRunMeta.runId;
    await fetchJson("/api/prompt/stop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (_error) {
    // Ignore stop endpoint failures; we still abort local stream below.
  }

  if (activeRunAbortController) {
    activeRunAbortController.abort();
  }
}

function normalizeMessages(messages) {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && typeof m === "object")
    .map((m) => ({
      role: m.role === "assistant" ? "assistant" : "user",
      content: typeof m.content === "string" ? m.content : "",
      ts: typeof m.ts === "number" ? m.ts : Date.now(),
    }))
    .filter((m) => m.content.trim().length > 0);
}

function readStoredSessionId() {
  try {
    const raw = localStorage.getItem(ACTIVE_SESSION_STORAGE_KEY);
    const id = typeof raw === "string" ? raw.trim() : "";
    return id || null;
  } catch (_err) {
    return null;
  }
}

function writeStoredSessionId(sessionId) {
  try {
    if (sessionId) {
      localStorage.setItem(ACTIVE_SESSION_STORAGE_KEY, sessionId);
    } else {
      localStorage.removeItem(ACTIVE_SESSION_STORAGE_KEY);
    }
  } catch (_err) {
    // Ignore storage errors
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let payload = {};
  try {
    payload = text ? JSON.parse(text) : {};
  } catch (_err) {
    payload = {};
  }
  if (!response.ok) {
    const err = new Error(payload.message || text || "Request failed.");
    if (payload && typeof payload === "object") {
      err.code = payload.code;
      err.provider = payload.provider;
    }
    throw err;
  }
  return payload;
}

function isMissingAnthropicKeyError(error) {
  if (!error) return false;
  if (error.code === "missing_provider_key") {
    return (error.provider || "").toLowerCase() === "anthropic";
  }
  const message = normalizeText(error.message, "").toLowerCase();
  return message.includes('no api key found for provider "anthropic"');
}

function isAnthropicAuthInvalidError(error) {
  if (!error) return false;
  if (error.code === "provider_auth_invalid") {
    return (error.provider || "").toLowerCase() === "anthropic";
  }
  const message = normalizeText(error.message, "").toLowerCase();
  return (
    message.includes("invalid x-api-key") ||
    message.includes("authentication failed") ||
    message.includes("unauthorized")
  );
}

function isGatewayConnectivityError(error) {
  if (!error) return false;
  if (error.code === "gateway_connectivity") return true;
  const message = normalizeText(error.message, "").toLowerCase();
  return (
    message.includes("websocket transport is unavailable") ||
    message.includes("gateway connect failed") ||
    message.includes("cannot reach gateway") ||
    message.includes("connection refused") ||
    message.includes("timed out")
  );
}

function openSettingsModal(message = "") {
  if (!settingsModal) return;
  settingsModal.classList.remove("hidden");
  setStatus(settingsStatus, message || "", Boolean(!message));
  if (settingsKeyInput && !settingsKeyInput.value.trim()) {
    settingsKeyInput.value = defaultGatewayUrl;
  }
  settingsKeyInput?.focus();
}

function openAuthModal(message = "") {
  if (!authModal) return;
  authModal.classList.remove("hidden");
  setStatus(authStatus, message || "", false);
  if (authKeyInput) {
    authKeyInput.focus();
  }
}

function closeAuthModal() {
  authModal?.classList.add("hidden");
  setStatus(authStatus, "", true);
}

async function refreshAuthStatus() {
  const payload = await fetchJson("/api/auth-status");
  const configured = payload?.providers?.anthropic?.configured === true;
  anthropicConfigured = configured;
  return configured;
}

async function ensureAnthropicKeyConfigured({ showModal = false } = {}) {
  if (anthropicConfigured) {
    return true;
  }
  const configured = await refreshAuthStatus();
  if (!configured && showModal) {
    openAuthModal("Add your Anthropic key to continue.");
  }
  return configured;
}

function showExecutionTab(tab) {
  const normalized = tab === "graph" || tab === "files" ? tab : "terminal";
  const showTerminal = normalized === "terminal";
  const showGraph = normalized === "graph";
  const showFiles = normalized === "files";

  executionTerminalView?.classList.toggle("hidden", !showTerminal);
  executionGraphView?.classList.toggle("hidden", !showGraph);
  executionFilesView?.classList.toggle("hidden", !showFiles);

  executionTabTerminal?.classList.toggle("active", showTerminal);
  executionTabGraph?.classList.toggle("active", showGraph);
  executionTabFiles?.classList.toggle("active", showFiles);

  executionTabTerminal?.setAttribute("aria-selected", showTerminal ? "true" : "false");
  executionTabGraph?.setAttribute("aria-selected", showGraph ? "true" : "false");
  executionTabFiles?.setAttribute("aria-selected", showFiles ? "true" : "false");

  if (showFiles) {
    requestAnimationFrame(() => {
      restoreFilesSelectorHeight();
    });
    ensureFilesViewReady();
  }

  if (showGraph) {
    ensureGraphViewReady();
  }
}

function showMissionCenterTab(tab) {
  const normalized = tab === "report" ? "report" : "overview";
  activeMissionCenterTab = normalized;
  const showOverview = normalized === "overview";
  const showReport = normalized === "report";
  missionOverviewView?.classList.toggle("hidden", !showOverview);
  missionReportView?.classList.toggle("hidden", !showReport);
  missionTabOverview?.classList.toggle("active", showOverview);
  missionTabReport?.classList.toggle("active", showReport);
  missionTabOverview?.setAttribute("aria-selected", showOverview ? "true" : "false");
  missionTabReport?.setAttribute("aria-selected", showReport ? "true" : "false");
}

function syncMissionViewForActiveSession() {
  showMissionCenterTab(activeMissionCenterTab);
}

function isGraphTabActive() {
  return executionTabGraph?.classList.contains("active") === true;
}

function formatFileSize(sizeBytes) {
  const size = Number(sizeBytes);
  if (!Number.isFinite(size) || size < 0) return "0 B";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function formatFileDate(epochSeconds) {
  const value = Number(epochSeconds);
  if (!Number.isFinite(value) || value <= 0) return "";
  try {
    return new Date(value * 1000).toLocaleString();
  } catch (_err) {
    return "";
  }
}

function setFilesPreviewMeta(message, bad = false) {
  if (!filesPreviewMeta) return;
  filesPreviewMeta.textContent = message || "";
  filesPreviewMeta.classList.toggle("bad", !!bad);
}

function clearFilesPreview(message = "Select a file to preview.") {
  setFilesPreviewMeta(message, false);
  if (filesPreviewContent) {
    filesPreviewContent.value = "";
  }
}

function renderFilesBreadcrumb() {
  if (!filesBreadcrumb) return;
  const suffix = fileViewerState.currentPath ? `/${fileViewerState.currentPath}` : "";
  filesBreadcrumb.textContent = `Workspace${suffix}`;
}

function renderFilesList(entries) {
  if (!filesList) return;
  filesList.innerHTML = "";

  if (!Array.isArray(entries) || entries.length === 0) {
    const empty = document.createElement("div");
    empty.className = "files-empty";
    empty.textContent = "This folder is empty.";
    filesList.appendChild(empty);
    return;
  }

  for (const entry of entries) {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `files-entry ${entry.type === "dir" ? "dir" : "file"}`;
    item.dataset.entryPath = normalizeText(entry.path, "");
    if (entry.path === fileViewerState.selectedFilePath) {
      item.classList.add("active");
    }

    const name = document.createElement("div");
    name.className = "files-entry-name";
    name.textContent = `${entry.type === "dir" ? "▸" : "•"} ${entry.name}`;
    item.appendChild(name);

    const meta = document.createElement("div");
    meta.className = "files-entry-meta";
    if (entry.type === "dir") {
      const updated = formatFileDate(entry.modified_at);
      meta.textContent = updated ? `Directory · ${updated}` : "Directory";
    } else {
      const updated = formatFileDate(entry.modified_at);
      const size = formatFileSize(entry.size);
      meta.textContent = updated ? `${size} · ${updated}` : size;
    }
    item.appendChild(meta);

    item.addEventListener("click", async () => {
      if (entry.type === "dir") {
        await loadFilesDirectory(entry.path);
      } else {
        await loadFilePreview(entry.path);
      }
    });

    filesList.appendChild(item);
  }
}

function updateFilesSelectionHighlight() {
  if (!filesList) return;
  const selectedPath = fileViewerState.selectedFilePath;
  for (const child of filesList.children) {
    if (!(child instanceof HTMLElement)) continue;
    const path = child.dataset.entryPath || "";
    child.classList.toggle("active", !!selectedPath && path === selectedPath);
  }
}

async function loadFilesDirectory(path = "") {
  if (fileViewerState.loadingDirectory) return;
  fileViewerState.loadingDirectory = true;
  if (filesList) {
    filesList.innerHTML = '<div class="files-empty">Loading...</div>';
  }

  try {
    const params = new URLSearchParams();
    if (path) {
      params.set("path", path);
    }
    const query = params.toString();
    const payload = await fetchJson(`/api/files/list${query ? `?${query}` : ""}`);
    fileViewerState.initialized = true;
    fileViewerState.workspaceRoot = normalizeText(payload.workspace_root, "");
    fileViewerState.currentPath = normalizeText(payload.path, "");
    fileViewerState.parentPath = payload.parent_path === null ? null : normalizeText(payload.parent_path, "");

    renderFilesBreadcrumb();
    const entries = Array.isArray(payload.entries) ? payload.entries : [];
    renderFilesList(entries);
    const selectedStillVisible = entries.some(
      (entry) => entry.type === "file" && normalizeText(entry.path, "") === fileViewerState.selectedFilePath
    );
    if (!selectedStillVisible) {
      fileViewerState.selectedFilePath = "";
      clearFilesPreview(
        fileViewerState.workspaceRoot
          ? `Workspace root: ${fileViewerState.workspaceRoot}`
          : "Select a file to preview."
      );
    }
  } catch (error) {
    fileViewerState.initialized = false;
    if (filesList) {
      filesList.innerHTML = `<div class="files-empty bad">${escapeHtml(error.message || "Could not load files.")}</div>`;
    }
    clearFilesPreview("Could not load directory.");
  } finally {
    fileViewerState.loadingDirectory = false;
    if (filesUpBtn) {
      filesUpBtn.disabled = fileViewerState.parentPath === null;
    }
  }
}

async function loadFilePreview(path) {
  const normalizedPath = normalizeText(path, "");
  if (!normalizedPath) return;
  fileViewerState.selectedFilePath = normalizedPath;
  renderFilesBreadcrumb();

  setFilesPreviewMeta("Loading file...");
  if (filesPreviewContent) {
    filesPreviewContent.value = "";
  }

  try {
    const params = new URLSearchParams();
    params.set("path", normalizedPath);
    const payload = await fetchJson(`/api/files/read?${params.toString()}`);

    const sizeLabel = formatFileSize(payload.size);
    const mimeType = normalizeText(payload.mime_type, "application/octet-stream");
    const isBinary = payload.is_binary === true;
    const truncated = payload.truncated === true;
    const summaryParts = [`${payload.name || normalizedPath}`, `${sizeLabel}`, mimeType];
    if (isBinary) summaryParts.push("binary");
    if (truncated) summaryParts.push("preview truncated");
    setFilesPreviewMeta(summaryParts.join(" · "));

    if (filesPreviewContent) {
      if (isBinary) {
        filesPreviewContent.value = "Binary file preview is not supported.";
      } else {
        filesPreviewContent.value = normalizeText(payload.content, "");
      }
    }
    updateFilesSelectionHighlight();
  } catch (error) {
    setFilesPreviewMeta(error.message || "Could not read file.", true);
    if (filesPreviewContent) {
      filesPreviewContent.value = "";
    }
  }
}

async function ensureFilesViewReady() {
  if (fileViewerState.initialized) {
    return;
  }
  clearFilesPreview();
  try {
    const rootPayload = await fetchJson("/api/files/root");
    fileViewerState.workspaceRoot = normalizeText(rootPayload.workspace_root, "");
    if (rootPayload.exists !== true) {
      if (filesList) {
        filesList.innerHTML = '<div class="files-empty bad">Workspace path does not exist yet.</div>';
      }
      setFilesPreviewMeta(fileViewerState.workspaceRoot || "Workspace not found.", true);
      return;
    }
    clearFilesPreview(`Workspace root: ${fileViewerState.workspaceRoot}`);
    await loadFilesDirectory("");
  } catch (error) {
    if (filesList) {
      filesList.innerHTML = `<div class="files-empty bad">${escapeHtml(error.message || "Could not load workspace root.")}</div>`;
    }
    setFilesPreviewMeta("Workspace unavailable.", true);
  }
}

function setGraphStatus(message, bad = false) {
  if (!graphStatus) return;
  graphStatus.textContent = message || "";
  graphStatus.classList.toggle("bad", !!bad);
}

function graphTypeClass(nodeType) {
  return `type-${String(nodeType || "note").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
}

function normalizeGraphNode(rawNode) {
  if (!rawNode || typeof rawNode !== "object") return null;
  const id = normalizeText(rawNode.id, "");
  if (!id) return null;
  const xRaw = Number(rawNode.x);
  const yRaw = Number(rawNode.y);
  return {
    id,
    type: normalizeText(rawNode.type, "Note") || "Note",
    label: normalizeText(rawNode.label, "Untitled node") || "Untitled node",
    description: normalizeText(rawNode.description, ""),
    created_by: normalizeText(rawNode.created_by, ""),
    status: normalizeText(rawNode.status, "new") || "new",
    severity: normalizeText(rawNode.severity, "info") || "info",
    confidence:
      rawNode.confidence === null || rawNode.confidence === undefined || rawNode.confidence === ""
        ? null
        : Number.isFinite(Number(rawNode.confidence))
          ? Math.max(0, Math.min(1, Number(rawNode.confidence)))
          : null,
    source: rawNode.source && typeof rawNode.source === "object" ? rawNode.source : {},
    refs: Array.isArray(rawNode.refs) ? rawNode.refs.filter((item) => item && typeof item === "object") : [],
    data: rawNode.data && typeof rawNode.data === "object" ? rawNode.data : {},
    x: Number.isFinite(xRaw) ? xRaw : null,
    y: Number.isFinite(yRaw) ? yRaw : null,
    updated_at: Number(rawNode.updated_at) || 0,
  };
}

function normalizeGraphEdge(rawEdge) {
  if (!rawEdge || typeof rawEdge !== "object") return null;
  const id = normalizeText(rawEdge.id, "");
  const from = normalizeText(rawEdge.from, "");
  const to = normalizeText(rawEdge.to, "");
  if (!id || !from || !to) return null;
  return {
    id,
    from,
    to,
    type: normalizeText(rawEdge.type, "related") || "related",
    label: normalizeText(rawEdge.label, ""),
  };
}

function graphNodeById(nodeId) {
  if (!nodeId) return null;
  return graphState.nodes.find((node) => node.id === nodeId) || null;
}

function resetGraphViewport() {
  graphState.viewport = { x: 0, y: 0, scale: 1 };
}

function fitGraphViewportToNodes() {
  if (!graphCanvas || !graphState.nodes.length) {
    resetGraphViewport();
    return;
  }
  const positioned = graphState.nodes.filter((node) => Number.isFinite(node.x) && Number.isFinite(node.y));
  if (!positioned.length) {
    resetGraphViewport();
    return;
  }

  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const node of positioned) {
    const radius = graphNodeCollisionRadius(node);
    const x = Number(node.x) || 0;
    const y = Number(node.y) || 0;
    minX = Math.min(minX, x - radius);
    maxX = Math.max(maxX, x + radius);
    minY = Math.min(minY, y - radius);
    maxY = Math.max(maxY, y + radius);
  }

  const width = Math.max(1, maxX - minX);
  const height = Math.max(1, maxY - minY);
  const canvasWidth = Math.max(1, graphCanvas.clientWidth || 1);
  const canvasHeight = Math.max(1, graphCanvas.clientHeight || 1);
  const padding = 64;
  const fitScale = Math.min((canvasWidth - padding) / width, (canvasHeight - padding) / height);
  const scale = Math.max(GRAPH_AUTOFIT_MIN_SCALE, Math.min(1.9, Number.isFinite(fitScale) ? fitScale : 1));
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;

  graphState.viewport.scale = scale;
  graphState.viewport.x = -centerX * scale;
  graphState.viewport.y = -centerY * scale;
}

function graphCenterOffset() {
  if (!graphCanvas) {
    return { cx: 0, cy: 0 };
  }
  return { cx: graphCanvas.clientWidth / 2, cy: graphCanvas.clientHeight / 2 };
}

function worldToScreen(node) {
  const { cx, cy } = graphCenterOffset();
  const scale = graphState.viewport.scale;
  return {
    x: cx + graphState.viewport.x + (Number(node.x) || 0) * scale,
    y: cy + graphState.viewport.y + (Number(node.y) || 0) * scale,
  };
}

function screenToWorld(clientX, clientY) {
  if (!graphCanvas) return { x: 0, y: 0 };
  const rect = graphCanvas.getBoundingClientRect();
  const localX = clientX - rect.left;
  const localY = clientY - rect.top;
  const { cx, cy } = graphCenterOffset();
  const scale = graphState.viewport.scale;
  return {
    x: (localX - cx - graphState.viewport.x) / scale,
    y: (localY - cy - graphState.viewport.y) / scale,
  };
}

function ensureGraphLayout() {
  const ringOrder = [
    "Objective",
    "Scope",
    "Asset",
    "Surface",
    "Hypothesis",
    "Finding",
    "Risk",
    "Recommendation",
    "Action",
    "Evidence",
    "Artifact",
    "Agent",
    "Note",
    "Run",
  ];
  const byType = new Map();
  for (const node of graphState.nodes) {
    if (Number.isFinite(node.x) && Number.isFinite(node.y)) {
      continue;
    }
    const type = normalizeText(node.type, "Note") || "Note";
    if (!byType.has(type)) {
      byType.set(type, []);
    }
    byType.get(type).push(node);
  }
  const types = Array.from(byType.keys()).sort((a, b) => {
    const aIdx = ringOrder.indexOf(a);
    const bIdx = ringOrder.indexOf(b);
    const aNorm = aIdx >= 0 ? aIdx : ringOrder.length + 1;
    const bNorm = bIdx >= 0 ? bIdx : ringOrder.length + 1;
    return aNorm - bNorm;
  });

  types.forEach((type, typeIdx) => {
    const nodes = byType.get(type) || [];
    if (!nodes.length) return;
    const angle = (typeIdx / Math.max(1, types.length)) * Math.PI * 2;
    const anchorRadius = 260 + typeIdx * 38;
    const anchorX = Math.cos(angle) * anchorRadius;
    const anchorY = Math.sin(angle) * anchorRadius * 0.72;

    const cols = Math.max(1, Math.ceil(Math.sqrt(nodes.length)));
    const rows = Math.max(1, Math.ceil(nodes.length / cols));
    nodes.forEach((node, idx) => {
      const row = Math.floor(idx / cols);
      const col = idx % cols;
      const offsetX = (col - (cols - 1) / 2) * 196;
      const offsetY = (row - (rows - 1) / 2) * 104;
      node.x = anchorX + offsetX;
      node.y = anchorY + offsetY;
    });
  });
}

function graphNodeCollisionRadius(node) {
  const label = normalizeText(node?.label || node?.type || "", "");
  const compact = label.replace(/\s+/g, " ").trim();
  const extra = Math.min(84, Math.max(0, compact.length - 12) * 1.8);
  return 98 + extra;
}

function spreadGraphNodes() {
  const movable = graphState.nodes.filter((node) => Number.isFinite(node.x) && Number.isFinite(node.y));
  if (movable.length <= 1) return;

  const pairAngle = (idA, idB) => {
    const seedInput = `${idA || ""}|${idB || ""}`;
    let seed = 0;
    for (let i = 0; i < seedInput.length; i += 1) {
      seed = (seed * 131 + seedInput.charCodeAt(i)) % 1000003;
    }
    return ((seed % 6283) / 1000) % (Math.PI * 2);
  };

  const iterations = 14;
  for (let iter = 0; iter < iterations; iter += 1) {
    for (let i = 0; i < movable.length; i += 1) {
      for (let j = i + 1; j < movable.length; j += 1) {
        const a = movable[i];
        const b = movable[j];
        let dx = (Number(b.x) || 0) - (Number(a.x) || 0);
        let dy = (Number(b.y) || 0) - (Number(a.y) || 0);
        let dist = Math.sqrt(dx * dx + dy * dy);

        if (dist < 0.0001) {
          const angle = pairAngle(a.id, b.id);
          dx = Math.cos(angle);
          dy = Math.sin(angle);
          dist = 1;
        }
        const minDist = graphNodeCollisionRadius(a) + graphNodeCollisionRadius(b);
        if (dist >= minDist) continue;
        const push = (minDist - dist) * 0.54;
        const nx = dx / dist;
        const ny = dy / dist;
        a.x -= nx * push;
        a.y -= ny * push;
        b.x += nx * push;
        b.y += ny * push;
      }
    }

    // Keep clusters from drifting too far while resolving overlaps.
    for (const node of movable) {
      node.x *= 0.998;
      node.y *= 0.998;
    }
  }

  // Final deterministic pass for any lingering near-overlaps.
  for (let i = 0; i < movable.length; i += 1) {
    for (let j = i + 1; j < movable.length; j += 1) {
      const a = movable[i];
      const b = movable[j];
      const dx = (Number(b.x) || 0) - (Number(a.x) || 0);
      const dy = (Number(b.y) || 0) - (Number(a.y) || 0);
      const dist = Math.sqrt(dx * dx + dy * dy);
      const minDist = graphNodeCollisionRadius(a) + graphNodeCollisionRadius(b);
      if (dist >= minDist) continue;
      const angle = pairAngle(a.id, b.id);
      const correction = minDist - dist + 8;
      b.x += Math.cos(angle) * correction;
      b.y += Math.sin(angle) * correction;
    }
  }
}

function setGraphInspectorEnabled(enabled) {
  const disabled = !enabled;
  if (graphNodeLabelInput) graphNodeLabelInput.disabled = disabled;
  if (graphNodeTypeInput) graphNodeTypeInput.disabled = disabled;
  if (graphNodeStatusInput) graphNodeStatusInput.disabled = disabled;
  if (graphNodeSeverityInput) graphNodeSeverityInput.disabled = disabled;
  if (graphNodeConfidenceInput) graphNodeConfidenceInput.disabled = disabled;
  if (graphNodeDescriptionInput) graphNodeDescriptionInput.disabled = disabled;
  if (graphNodeSaveBtn) graphNodeSaveBtn.disabled = disabled;
}

function renderGraphInspector() {
  const node = graphNodeById(graphState.selectedNodeId);
  if (!node) {
    if (graphNodeId) graphNodeId.textContent = "None selected";
    if (graphNodeLabelInput) graphNodeLabelInput.value = "";
    if (graphNodeTypeInput) graphNodeTypeInput.value = "";
    if (graphNodeStatusInput) graphNodeStatusInput.value = "";
    if (graphNodeSeverityInput) graphNodeSeverityInput.value = "";
    if (graphNodeConfidenceInput) graphNodeConfidenceInput.value = "";
    if (graphNodeDescriptionInput) graphNodeDescriptionInput.value = "";
    if (graphNodeLinks) graphNodeLinks.textContent = "Select a node to inspect and edit details.";
    setGraphInspectorEnabled(false);
    return;
  }

  if (graphNodeId) graphNodeId.textContent = node.id;
  if (graphNodeLabelInput) graphNodeLabelInput.value = node.label || "";
  if (graphNodeTypeInput) graphNodeTypeInput.value = node.type || "";
  if (graphNodeStatusInput) graphNodeStatusInput.value = node.status || "";
  if (graphNodeSeverityInput) graphNodeSeverityInput.value = node.severity || "";
  if (graphNodeConfidenceInput) graphNodeConfidenceInput.value = node.confidence === null ? "" : String(node.confidence);
  if (graphNodeDescriptionInput) graphNodeDescriptionInput.value = node.description || "";

  const refs = Array.isArray(node.refs) ? node.refs : [];
  const sourceRun = normalizeText(node.source?.run_id, "");
  const sourceExecution = normalizeText(node.source?.execution_id, "");
  const bits = [];
  if (sourceRun) bits.push(`run ${sourceRun}`);
  if (sourceExecution) bits.push(`execution ${sourceExecution}`);
  if (refs.length > 0) bits.push(`${refs.length} refs`);
  if (graphNodeLinks) {
    graphNodeLinks.textContent = bits.length ? bits.join(" · ") : "No linked refs.";
  }
  setGraphInspectorEnabled(true);
}

function renderGraph() {
  if (!graphNodesLayer || !graphEdgesLayer) return;

  ensureGraphLayout();
  if (graphState.needsSpreadPass) {
    spreadGraphNodes();
    graphState.needsSpreadPass = false;
  }
  if (graphState.autoFitPending) {
    fitGraphViewportToNodes();
    graphState.autoFitPending = false;
  }
  graphNodesLayer.innerHTML = "";
  graphEdgesLayer.innerHTML = "";

  if (!graphState.nodes.length) {
    const empty = document.createElement("div");
    empty.className = "graph-empty";
    empty.textContent = "Graph is empty for this mission";
    graphNodesLayer.appendChild(empty);
    renderGraphInspector();
    return;
  }

  const nodeScreenMap = new Map();
  const visualScale = Math.max(0.06, Math.min(2.2, graphState.viewport.scale));
  for (const node of graphState.nodes) {
    const screen = worldToScreen(node);
    nodeScreenMap.set(node.id, screen);

    const nodeBtn = document.createElement("button");
    nodeBtn.type = "button";
    nodeBtn.className = `graph-node ${graphTypeClass(node.type)}`;
    if (node.id === graphState.selectedNodeId) {
      nodeBtn.classList.add("active");
    }
    nodeBtn.dataset.nodeId = node.id;
    nodeBtn.style.left = `${screen.x}px`;
    nodeBtn.style.top = `${screen.y}px`;
    nodeBtn.style.transform = `translate(-50%, -50%) scale(${visualScale})`;

    const title = document.createElement("div");
    title.className = "graph-node-title";
    title.textContent = node.label || node.type || node.id;
    nodeBtn.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "graph-node-meta";
    const confidence = node.confidence === null ? "n/a" : Number(node.confidence).toFixed(2);
    meta.textContent = `${node.type} · ${node.status} · ${confidence}`;
    nodeBtn.appendChild(meta);

    nodeBtn.addEventListener("click", () => {
      graphState.selectedNodeId = node.id;
      renderGraph();
    });

    nodeBtn.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      graphState.selectedNodeId = node.id;
      graphState.draggingNodeId = node.id;
      graphState.dragStart = { x: Number(node.x) || 0, y: Number(node.y) || 0, moved: false };
      graphState.pan = null;
      nodeBtn.classList.add("dragging");
      renderGraphInspector();
    });

    graphNodesLayer.appendChild(nodeBtn);
  }

  if (graphCanvas) {
    graphEdgesLayer.setAttribute("width", String(graphCanvas.clientWidth));
    graphEdgesLayer.setAttribute("height", String(graphCanvas.clientHeight));
  }

  for (const edge of graphState.edges) {
    const from = nodeScreenMap.get(edge.from);
    const to = nodeScreenMap.get(edge.to);
    if (!from || !to) continue;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(from.x));
    line.setAttribute("y1", String(from.y));
    line.setAttribute("x2", String(to.x));
    line.setAttribute("y2", String(to.y));
    line.classList.add("graph-edge");
    const edgeWidth = Math.max(0.35, Math.min(2.5, 1.2 * visualScale));
    line.style.strokeWidth = String(edgeWidth);
    if (graphState.selectedNodeId && (edge.from === graphState.selectedNodeId || edge.to === graphState.selectedNodeId)) {
      line.classList.add("selected");
      line.style.strokeWidth = String(Math.max(0.5, Math.min(3.4, 2.1 * visualScale)));
    }
    graphEdgesLayer.appendChild(line);
  }

  renderGraphInspector();
}

function updateGraphLocalNode(nodeId, patch) {
  const node = graphNodeById(nodeId);
  if (!node) return;
  Object.assign(node, patch || {});
}

async function patchGraphNode(nodeId, patch) {
  if (!activeSessionId || !nodeId) return;
  const payload = patch && typeof patch === "object" ? patch : {};
  const response = await fetchJson(`/api/graph/${encodeURIComponent(activeSessionId)}/nodes/${encodeURIComponent(nodeId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const normalized = normalizeGraphNode(response.node);
  if (!normalized) return;
  const idx = graphState.nodes.findIndex((item) => item.id === normalized.id);
  if (idx >= 0) {
    graphState.nodes[idx] = normalized;
  } else {
    graphState.nodes.push(normalized);
  }
  renderGraph();
}

async function createGraphNoteNode() {
  if (!activeSessionId) return;
  const response = await fetchJson(`/api/graph/${encodeURIComponent(activeSessionId)}/nodes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "Note",
      label: "New note",
      description: "Describe why this node matters to the mission.",
      status: "new",
      severity: "info",
      confidence: 0.3,
    }),
  });
  const normalized = normalizeGraphNode(response.node);
  if (!normalized) return;
  graphState.nodes.push(normalized);
  graphState.selectedNodeId = normalized.id;
  setGraphStatus("Created note node.", false);
  renderGraph();
}

async function loadGraphForSession(sessionId, { force = false } = {}) {
  if (!sessionId) {
    graphState.nodes = [];
    graphState.edges = [];
    graphState.selectedNodeId = null;
    graphState.loadedSessionId = null;
    graphState.needsSpreadPass = false;
    graphState.autoFitPending = false;
    renderGraph();
    return;
  }
  if (!force && graphState.loadedSessionId === sessionId && graphState.nodes.length > 0) {
    renderGraph();
    return;
  }
  if (graphState.loading) return;
  graphState.loading = true;
  setGraphStatus("Loading graph...", false);
  try {
    const payload = await fetchJson(`/api/graph/${encodeURIComponent(sessionId)}`);
    graphState.loadedSessionId = sessionId;
    const incomingNodes = Array.isArray(payload.nodes) ? payload.nodes.map(normalizeGraphNode).filter(Boolean) : [];
    const incomingEdges = Array.isArray(payload.edges) ? payload.edges.map(normalizeGraphEdge).filter(Boolean) : [];
    const hasUnpositionedNodes = incomingNodes.some(
      (node) => !Number.isFinite(node.x) || !Number.isFinite(node.y)
    );

    graphState.nodes = incomingNodes;
    graphState.edges = incomingEdges;
    graphState.needsSpreadPass = hasUnpositionedNodes;
    graphState.autoFitPending = true;
    if (graphState.selectedNodeId && !graphNodeById(graphState.selectedNodeId)) {
      graphState.selectedNodeId = null;
    }
    setGraphStatus(`${graphState.nodes.length} nodes · ${graphState.edges.length} edges`, false);
    renderGraph();
  } catch (error) {
    setGraphStatus(error.message || "Could not load graph.", true);
  } finally {
    graphState.loading = false;
  }
}

async function ensureGraphViewReady(force = false) {
  if (!activeSessionId) {
    graphState.nodes = [];
    graphState.edges = [];
    graphState.selectedNodeId = null;
    graphState.needsSpreadPass = false;
    graphState.autoFitPending = false;
    renderGraph();
    return;
  }
  await loadGraphForSession(activeSessionId, { force });
}

let graphInteractionsBound = false;
function setupGraphInteractions() {
  if (graphInteractionsBound) return;
  graphInteractionsBound = true;

  graphCanvas?.addEventListener("pointerdown", (event) => {
    if (event.button !== 0 || graphState.draggingNodeId) return;
    event.preventDefault();
    graphState.pan = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      originX: graphState.viewport.x,
      originY: graphState.viewport.y,
    };
    graphCanvas.classList.add("panning");
  });

  window.addEventListener("pointermove", (event) => {
    if (graphState.draggingNodeId) {
      const node = graphNodeById(graphState.draggingNodeId);
      if (!node) return;
      const beforeX = Number(node.x) || 0;
      const beforeY = Number(node.y) || 0;
      const world = screenToWorld(event.clientX, event.clientY);
      node.x = world.x;
      node.y = world.y;
      if (
        graphState.dragStart &&
        (Math.abs((Number(node.x) || 0) - graphState.dragStart.x) > 1 ||
          Math.abs((Number(node.y) || 0) - graphState.dragStart.y) > 1 ||
          Math.abs((Number(node.x) || 0) - beforeX) > 0.1 ||
          Math.abs((Number(node.y) || 0) - beforeY) > 0.1)
      ) {
        graphState.dragStart.moved = true;
      }
      renderGraph();
      return;
    }
    if (!graphState.pan) return;
    const dx = event.clientX - graphState.pan.startX;
    const dy = event.clientY - graphState.pan.startY;
    graphState.viewport.x = graphState.pan.originX + dx;
    graphState.viewport.y = graphState.pan.originY + dy;
    renderGraph();
  });

  window.addEventListener("pointerup", async () => {
    if (graphState.pan) {
      graphState.pan = null;
      graphCanvas?.classList.remove("panning");
    }
    if (graphState.draggingNodeId) {
      const nodeId = graphState.draggingNodeId;
      const dragMoved = graphState.dragStart?.moved === true;
      graphState.draggingNodeId = null;
      graphState.dragStart = null;
      const node = graphNodeById(nodeId);
      if (node && dragMoved) {
        try {
          await patchGraphNode(node.id, { x: node.x, y: node.y });
        } catch (_err) {
          // Ignore transient drag-save errors.
        }
      }
      renderGraph();
    }
  });

  graphCanvas?.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      if (!graphCanvas) return;
      const rect = graphCanvas.getBoundingClientRect();
      const cursorX = event.clientX - rect.left;
      const cursorY = event.clientY - rect.top;
      const { cx, cy } = graphCenterOffset();
      const scale = graphState.viewport.scale;
      const worldX = (cursorX - cx - graphState.viewport.x) / scale;
      const worldY = (cursorY - cy - graphState.viewport.y) / scale;
      const delta = event.deltaY < 0 ? 1.08 : 0.92;
      const nextScale = Math.max(GRAPH_MIN_SCALE, Math.min(GRAPH_MAX_SCALE, graphState.viewport.scale * delta));
      graphState.viewport.scale = nextScale;
      graphState.viewport.x = cursorX - cx - worldX * nextScale;
      graphState.viewport.y = cursorY - cy - worldY * nextScale;
      renderGraph();
    },
    { passive: false }
  );

  graphResetBtn?.addEventListener("click", () => {
    graphState.autoFitPending = true;
    renderGraph();
  });

  graphRefreshBtn?.addEventListener("click", async () => {
    await ensureGraphViewReady(true);
  });

  graphAddNoteBtn?.addEventListener("click", async () => {
    try {
      await createGraphNoteNode();
    } catch (error) {
      setGraphStatus(error.message || "Could not create node.", true);
    }
  });

  graphNodeSaveBtn?.addEventListener("click", async () => {
    const selected = graphNodeById(graphState.selectedNodeId);
    if (!selected || !activeSessionId) return;
    const confidenceRaw = graphNodeConfidenceInput?.value;
    const confidenceValue =
      confidenceRaw === "" || confidenceRaw === undefined || confidenceRaw === null
        ? null
        : Number.isFinite(Number(confidenceRaw))
          ? Math.max(0, Math.min(1, Number(confidenceRaw)))
          : null;
    const patch = {
      label: normalizeText(graphNodeLabelInput?.value, selected.label),
      type: normalizeText(graphNodeTypeInput?.value, selected.type),
      status: normalizeText(graphNodeStatusInput?.value, selected.status),
      severity: normalizeText(graphNodeSeverityInput?.value, selected.severity),
      description: normalizeText(graphNodeDescriptionInput?.value, selected.description),
      confidence: confidenceValue,
    };
    try {
      setGraphStatus("Saving node...", false);
      updateGraphLocalNode(selected.id, patch);
      renderGraph();
      await patchGraphNode(selected.id, patch);
      setGraphStatus("Node saved.", false);
    } catch (error) {
      setGraphStatus(error.message || "Could not save node.", true);
    }
  });
}

function readSessionPaneCollapsed() {
  try {
    return localStorage.getItem(SESSION_PANE_COLLAPSED_KEY) === "1";
  } catch (_err) {
    return false;
  }
}

function writeSessionPaneCollapsed(collapsed) {
  try {
    if (collapsed) {
      localStorage.setItem(SESSION_PANE_COLLAPSED_KEY, "1");
    } else {
      localStorage.removeItem(SESSION_PANE_COLLAPSED_KEY);
    }
  } catch (_err) {
    // Ignore storage errors
  }
}

function applySessionPaneCollapsed(collapsed) {
  sessionPaneCollapsed = !!collapsed;
  document.body.classList.toggle("sessions-collapsed", sessionPaneCollapsed);
  if (sessionFoldBtn) {
    sessionFoldBtn.textContent = sessionPaneCollapsed ? "»" : "«";
    sessionFoldBtn.title = sessionPaneCollapsed ? "Expand missions" : "Collapse missions";
    sessionFoldBtn.setAttribute("aria-expanded", sessionPaneCollapsed ? "false" : "true");
  }
}

function scrollExecutionToBottom() {
  if (!executionFeed) return;
  executionFeed.scrollTop = executionFeed.scrollHeight;
  persistExecutionHistory();
}

function persistExecutionHistory(sessionId = activeSessionId) {
  if (!executionFeed) return;
  const sid = normalizeText(sessionId, "");
  if (!sid) return;
  const html = executionFeed.innerHTML || "";
  executionHistoryBySession.set(sid, html);
  executionPersistPending = { sid, html };
  if (executionPersistTimer) {
    clearTimeout(executionPersistTimer);
  }
  executionPersistTimer = setTimeout(() => {
    flushExecutionHistoryPersist().catch(() => {});
  }, 250);
}

function restoreExecutionHistory(sessionId) {
  if (!executionFeed) return false;
  const sid = normalizeText(sessionId, "");
  if (!sid) return false;
  const html = executionHistoryBySession.get(sid);
  if (typeof html !== "string" || !html.trim()) {
    return false;
  }
  executionFeed.innerHTML = html;
  return true;
}

async function flushExecutionHistoryPersist() {
  if (executionPersistInFlight || !executionPersistPending) return;
  const pending = executionPersistPending;
  executionPersistPending = null;
  executionPersistInFlight = true;
  try {
    await fetchJson(`/api/sessions/${encodeURIComponent(pending.sid)}/execution-html`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ html: pending.html }),
    });
  } catch (_error) {
    // Keep UI responsive; persistence will retry on next write.
  } finally {
    executionPersistInFlight = false;
    if (executionPersistPending) {
      flushExecutionHistoryPersist().catch(() => {});
    }
  }
}

function createExecutionRunShell(promptText) {
  if (!executionFeed) {
    return { streamEvents: null };
  }
  const emptyNodes = executionFeed.querySelectorAll(".execution-empty");
  emptyNodes.forEach((node) => node.remove());

  const runShell = document.createElement("section");
  runShell.className = "terminal-run";

  const runMeta = document.createElement("div");
  runMeta.className = "terminal-run-meta";
  runMeta.textContent = `Activity started · ${new Date().toLocaleTimeString()}`;
  runShell.appendChild(runMeta);

  const runPrompt = document.createElement("div");
  runPrompt.className = "terminal-run-prompt markdown-content";
  setMarkdownContent(runPrompt, `**Prompt**\n${normalizeText(promptText, "")}`);
  runShell.appendChild(runPrompt);

  const streamEvents = document.createElement("div");
  streamEvents.className = "stream-events terminal-stream";
  runShell.appendChild(streamEvents);

  executionFeed.appendChild(runShell);
  showExecutionTab("terminal");
  scrollExecutionToBottom();

  return { streamEvents };
}

function renderExecutionEmpty(message = "Run a prompt to stream mission activity.") {
  if (!executionFeed) return;
  executionFeed.innerHTML = "";
  const empty = document.createElement("div");
  empty.className = "execution-empty";
  empty.textContent = message;
  executionFeed.appendChild(empty);
  persistExecutionHistory();
}

function setActiveSession(session) {
  const sessionId = normalizeText(session?.id, "");
  const messages = normalizeMessages(session?.messages);
  const missionTitle = formatMissionTitle(session?.title);
  const previousSessionId = activeSessionId;
  if (previousSessionId && previousSessionId !== sessionId) {
    persistExecutionHistory(previousSessionId);
  }
  activeSessionId = sessionId || null;
  activeSessionMessages = messages;
  hasSubmittedPrompt = messages.length > 0;
  writeStoredSessionId(activeSessionId);
  if (activeSessionId) {
    const existingIndex = sessionSummaries.findIndex((item) => item.id === activeSessionId);
    const nextSummary = {
      id: activeSessionId,
      title: missionTitle,
      created_at: Number(session?.created_at) || Date.now() / 1000,
      updated_at: Number(session?.updated_at) || Date.now() / 1000,
      message_count: messages.length,
    };
    if (existingIndex >= 0) {
      sessionSummaries[existingIndex] = nextSummary;
    } else {
      sessionSummaries.unshift(nextSummary);
    }
    const persistedExecutionHtml = typeof session?.execution_html === "string" ? session.execution_html : "";
    if (persistedExecutionHtml.trim()) {
      executionHistoryBySession.set(activeSessionId, persistedExecutionHtml);
    }
  }
  syncActiveMissionHeader(missionTitle);
  setMissionOverview(session?.overview, session?.overview_updated_at);
  if (sessionId && messages.length > 0 && !normalizeText(session?.overview, "")) {
    refreshMissionOverviewFromServer(sessionId).catch(() => {});
  }
  renderSessionList();
  renderChatHistory(activeSessionMessages);
  syncMissionViewForActiveSession();
  if (!restoreExecutionHistory(activeSessionId)) {
    renderExecutionEmpty("Run a prompt to stream mission activity.");
  } else {
    scrollExecutionToBottom();
  }

  graphState.loadedSessionId = null;
  graphState.nodes = [];
  graphState.edges = [];
  graphState.selectedNodeId = null;
  graphState.needsSpreadPass = false;
  if (isGraphTabActive()) {
    ensureGraphViewReady(true).catch(() => {
      setGraphStatus("Could not load graph.", true);
    });
  } else {
    setGraphStatus("");
    renderGraph();
  }
}

function normalizeSessionSummaries(summaries) {
  if (!Array.isArray(summaries)) return [];
  return summaries
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      id: normalizeText(item.id, ""),
      title: formatMissionTitle(item.title),
      created_at: Number(item.created_at) || 0,
      updated_at: Number(item.updated_at) || 0,
      message_count: Number(item.message_count) || 0,
    }))
    .filter((item) => item.id);
}

function formatRelativeTime(secondsEpoch) {
  const ts = Number(secondsEpoch);
  if (!Number.isFinite(ts) || ts <= 0) return "";
  const diff = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (diff < 30) return "just now";
  if (diff < 3600) return `${Math.max(1, Math.floor(diff / 60))}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 86400 * 7) return `${Math.floor(diff / 86400)}d ago`;
  return `${Math.floor(diff / (86400 * 7))}w ago`;
}

function setSessionStatus(message, ok) {
  setStatus(sessionStatus, message, ok);
}

function renderSessionList() {
  if (!sessionList) return;
  sessionList.innerHTML = "";

  if (!sessionSummaries.length) {
    const emptyNode = document.createElement("div");
    emptyNode.className = "session-empty";
    emptyNode.textContent = "No missions yet. Start a new one.";
    sessionList.appendChild(emptyNode);
    return;
  }

  for (const summary of sessionSummaries) {
    const row = document.createElement("div");
    row.className = `session-item ${summary.id === activeSessionId ? "active" : ""}`.trim();

    const selectBtn = document.createElement("button");
    selectBtn.type = "button";
    selectBtn.className = "session-select";
    selectBtn.title = summary.title;

    const titleNode = document.createElement("span");
    titleNode.className = "session-title";
    titleNode.textContent = summary.title;
    selectBtn.appendChild(titleNode);

    const metaNode = document.createElement("span");
    metaNode.className = "session-meta";
    const labelCount = summary.message_count === 1 ? "1 msg" : `${summary.message_count} msgs`;
    const relativeUpdated = formatRelativeTime(summary.updated_at);
    metaNode.textContent = relativeUpdated ? `${labelCount} · ${relativeUpdated}` : labelCount;
    selectBtn.appendChild(metaNode);

    const activateMission = async () => {
      if (summary.id === activeSessionId) return;
      if (isRunning) {
        setSessionStatus("Wait for the current run to finish before switching missions.", false);
        return;
      }
      try {
        await getSession(summary.id);
        setSessionStatus("", true);
        setStatus(promptStatus, "", true);
        scrollChatToBottom({ behavior: "auto", force: true });
      } catch (error) {
        setSessionStatus(error.message || "Could not switch missions.", false);
      }
    };

    row.addEventListener("click", async (event) => {
      if (event.target instanceof Element && event.target.closest(".session-delete")) {
        return;
      }
      await activateMission();
    });

    selectBtn.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      await activateMission();
    });

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "session-delete";
    deleteBtn.textContent = "×";
    deleteBtn.title = "Delete mission";
    deleteBtn.setAttribute("aria-label", `Delete mission ${summary.title}`);
    deleteBtn.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (isRunning && summary.id === activeSessionId) {
        setSessionStatus("Wait for the current run to finish before deleting this mission.", false);
        return;
      }
      const confirmed = window.confirm(`Delete mission "${summary.title}"? This cannot be undone.`);
      if (!confirmed) return;

      try {
        await fetchJson(`/api/sessions/${encodeURIComponent(summary.id)}`, { method: "DELETE" });
        executionHistoryBySession.delete(summary.id);
        sessionSummaries = sessionSummaries.filter((item) => item.id !== summary.id);

        if (summary.id === activeSessionId) {
          if (sessionSummaries.length > 0) {
            await getSession(sessionSummaries[0].id);
          } else {
            await createSession();
          }
        }
        await refreshSessionSummaries();
        setSessionStatus("Mission deleted.", true);
      } catch (error) {
        setSessionStatus(error.message || "Could not delete mission.", false);
      }
    });

    row.appendChild(selectBtn);
    row.appendChild(deleteBtn);
    sessionList.appendChild(row);
  }
}

async function refreshSessionSummaries() {
  const { sessions } = await listSessions();
  sessionSummaries = normalizeSessionSummaries(sessions);
  if (activeSessionId && !sessionSummaries.some((item) => item.id === activeSessionId)) {
    activeSessionId = null;
    writeStoredSessionId(null);
  }
  renderSessionList();
}

function touchActiveSessionSummary(role, content) {
  if (!activeSessionId) return;
  const nowSeconds = Date.now() / 1000;
  const idx = sessionSummaries.findIndex((item) => item.id === activeSessionId);
  if (idx < 0) return;
  const next = { ...sessionSummaries[idx] };
  next.updated_at = nowSeconds;
  next.message_count = Math.max(0, Number(next.message_count) || 0) + 1;
  if ((next.title === "New Chat" || next.title === "New Mission" || !next.title) && role === "user") {
    const firstLine = normalizeText(content, "New Mission").split("\n")[0];
    next.title = firstLine.slice(0, 72) || "New Mission";
  }
  sessionSummaries[idx] = next;
  sessionSummaries.sort((a, b) => Number(b.updated_at || 0) - Number(a.updated_at || 0));
  renderSessionList();
}

async function listSessions() {
  const payload = await fetchJson("/api/sessions");
  const sessions = Array.isArray(payload.sessions) ? payload.sessions : [];
  const lastSessionId = normalizeText(payload.last_session_id, "");
  return { sessions, lastSessionId: lastSessionId || null };
}

async function createSession(title = "") {
  const payload = await fetchJson("/api/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title }),
  });
  if (!payload.session || typeof payload.session !== "object") {
    throw new Error("Invalid create mission response.");
  }
  setActiveSession(payload.session);
  return payload.session;
}

async function getSession(sessionId) {
  const payload = await fetchJson(`/api/sessions/${encodeURIComponent(sessionId)}`);
  if (!payload.session || typeof payload.session !== "object") {
    throw new Error("Invalid mission response.");
  }
  setActiveSession(payload.session);
  return payload.session;
}

async function refreshMissionOverviewFromServer(sessionId) {
  const missionId = normalizeText(sessionId, "");
  if (!missionId) return null;
  const payload = await fetchJson(`/api/sessions/${encodeURIComponent(missionId)}`);
  if (!payload.session || typeof payload.session !== "object") {
    throw new Error("Invalid mission response.");
  }
  if (normalizeText(payload.session.id, "") !== activeSessionId) {
    return payload.session;
  }
  setMissionOverview(payload.session.overview, payload.session.overview_updated_at);
  return payload.session;
}

async function ensureActiveSession() {
  const preferredId = readStoredSessionId();
  if (preferredId) {
    try {
      return await getSession(preferredId);
    } catch (_err) {
      // Fall through to fallback strategy
    }
  }

  const { sessions, lastSessionId } = await listSessions();
  sessionSummaries = normalizeSessionSummaries(sessions);
  renderSessionList();
  if (lastSessionId) {
    try {
      return await getSession(lastSessionId);
    } catch (_err) {
      // Fall through to first available session
    }
  }
  if (sessions.length > 0) {
    return getSession(sessions[0].id);
  }
  return createSession();
}

function isNearBottom(thresholdPx = 220) {
  if (!chatThread) return true;
  const distance = chatThread.scrollHeight - (chatThread.scrollTop + chatThread.clientHeight);
  return distance <= thresholdPx;
}

function updateAutoScrollEnabled() {
  autoScrollEnabled = isNearBottom();
  if (autoScrollEnabled && jumpLatestBtn) {
    unseenEventCount = 0;
    jumpLatestBtn.classList.add("hidden");
    jumpLatestBtn.textContent = "Jump to latest";
  }
}

function scrollChatToBottom(options = {}) {
  if (!options.force && !autoScrollEnabled) {
    return;
  }
  if (!chatThread) return;
  if ((options.behavior || "auto") === "smooth" && typeof chatThread.scrollTo === "function") {
    chatThread.scrollTo({ top: chatThread.scrollHeight, behavior: "smooth" });
    return;
  }
  chatThread.scrollTop = chatThread.scrollHeight;
}

function ensureJumpLatestButton() {
  if (jumpLatestBtn) return;
  const btn = document.createElement("button");
  btn.type = "button";
  btn.id = "jump-latest-btn";
  btn.className = "jump-latest hidden";
  btn.textContent = "Jump to latest";
  document.body.appendChild(btn);
  btn.addEventListener("click", () => {
    unseenEventCount = 0;
    btn.classList.add("hidden");
    btn.textContent = "Jump to latest";
    scrollChatToBottom({ behavior: "smooth", force: true });
    updateAutoScrollEnabled();
  });
  jumpLatestBtn = btn;
}

function createChatMessageNode({ role, content }) {
  const wrapper = document.createElement("div");
  wrapper.className = `chat-message ${role === "assistant" ? "assistant" : "user"}`;

  const meta = document.createElement("div");
  meta.className = "chat-meta";
  meta.textContent = role === "assistant" ? "Cobra Lite" : "You";
  wrapper.appendChild(meta);

  const bubble = document.createElement("div");
  bubble.className = "chat-bubble markdown-content";
  setMarkdownContent(bubble, content);
  wrapper.appendChild(bubble);

  return { wrapper, bubble };
}

function renderChatHistory(history) {
  if (!chatThread) return;
  chatThread.innerHTML = "";
  for (const msg of history || []) {
    const role = msg.role === "assistant" ? "assistant" : "user";
    const node = createChatMessageNode({ role, content: msg.content || "" });
    chatThread.appendChild(node.wrapper);
  }
}

function createRunUI(promptText) {
  const userMsg = createChatMessageNode({ role: "user", content: promptText });
  const assistantMsg = createChatMessageNode({ role: "assistant", content: "" });
  const terminalRun = createExecutionRunShell(promptText);

  return {
    userMsg,
    assistantMsg,
    assistantInserted: false,
    streamEvents: terminalRun.streamEvents,
    runningExecutionBlocks: new Map(),
    reasoningLiveNode: null,
    committedAssistant: false,
    commitAssistant: null,
  };
}

function ensureAssistantNode(run) {
  if (!run || !run.assistantMsg || run.assistantInserted) return;
  chatThread?.appendChild(run.assistantMsg.wrapper);
  run.assistantInserted = true;
}

function setAssistantBubble(run, content) {
  if (!run || !run.assistantMsg || !run.assistantMsg.bubble) return;
  ensureAssistantNode(run);
  setMarkdownContent(run.assistantMsg.bubble, content);
}

function appendStreamEvent(run, content, kind) {
  if (!run || !run.streamEvents) return;
  const block = document.createElement("div");
  block.className = `stream-event ${kind || ""}`.trim();

  const safeContent = normalizeText(content, "");
  const body = document.createElement("div");
  body.className = "stream-event-body markdown-content";
  setMarkdownContent(body, safeContent);
  block.appendChild(body);

  run.streamEvents.appendChild(block);
  scrollExecutionToBottom();
}

function createCommandBlock({ stepIndex, toolName, command, rationale, isRunning }) {
  const details = document.createElement("details");
  details.className = `stream-event command-block ${isRunning ? "running" : "completed"}`;
  if (isRunning) {
    details.open = true;
  }

  const summary = document.createElement("summary");
  summary.textContent = isRunning
    ? `Action ${stepIndex} · ${toolName} · running`
    : `Action ${stepIndex} · ${toolName}`;
  details.appendChild(summary);

  const summaryLine = document.createElement("div");
  summaryLine.className = "stream-event-summary";
  summaryLine.textContent = command;
  details.appendChild(summaryLine);

  let rationaleLine = null;
  if (rationale) {
    rationaleLine = document.createElement("div");
    rationaleLine.className = "stream-event-summary";
    rationaleLine.textContent = `Rationale: ${rationale}`;
    details.appendChild(rationaleLine);
  }

  const outputNode = document.createElement("div");
  outputNode.className = "command-output markdown-content";
  setMarkdownContent(outputNode, isRunning ? "(running...)" : "");
  details.appendChild(outputNode);

  return {
    details,
    summary,
    summaryLine,
    rationaleLine,
    outputNode,
  };
}

function appendCommandStart(run, data) {
  const stepIndex = data.action_index_1based || data.step_index_1based || "?";
  const toolName = data.tool_name || "unknown tool";
  const command = normalizeText(data.command, "(no command)");
  const rationale = normalizeText(data.rationale, "");
  const executionId = normalizeText(data.execution_id, "");

  const blockRef = createCommandBlock({
    stepIndex,
    toolName,
    command,
    rationale,
    isRunning: true,
  });

  if (executionId) {
    blockRef.details.dataset.executionId = executionId;
    run.runningExecutionBlocks.set(executionId, blockRef);
  }

  run.streamEvents.appendChild(blockRef.details);
  scrollExecutionToBottom();
}

function appendCommandExecution(run, data) {
  const stepIndex = data.action_index_1based || data.step_index_1based || "?";
  const toolName = data.tool_name || "unknown tool";
  const command = normalizeText(data.command, "(no command)");
  const output = normalizeText(data.tool_output, "(no output)", { decodeEscapes: true });
  const rationale = normalizeText(data.rationale, "");
  const executionId = normalizeText(data.execution_id, "");
  const runningBlock = executionId ? run.runningExecutionBlocks.get(executionId) : null;

  if (runningBlock) {
    runningBlock.details.classList.remove("running");
    runningBlock.details.classList.add("completed");
    runningBlock.summary.textContent = `Action ${stepIndex} · ${toolName}`;
    runningBlock.summaryLine.textContent = command;
    if (rationale) {
      if (!runningBlock.rationaleLine) {
        const rationaleLine = document.createElement("div");
        rationaleLine.className = "stream-event-summary";
        runningBlock.details.insertBefore(rationaleLine, runningBlock.outputNode);
        runningBlock.rationaleLine = rationaleLine;
      }
      runningBlock.rationaleLine.textContent = `Rationale: ${rationale}`;
    }
    setMarkdownContent(
      runningBlock.outputNode,
      mergeCommandOutput(getMarkdownSource(runningBlock.outputNode), output)
    );
    run.runningExecutionBlocks.delete(executionId);
    scrollExecutionToBottom();
    return;
  }

  const blockRef = createCommandBlock({
    stepIndex,
    toolName,
    command,
    rationale,
    isRunning: false,
  });
  setMarkdownContent(blockRef.outputNode, output);
  run.streamEvents.appendChild(blockRef.details);
  scrollExecutionToBottom();
}

function mergeCommandOutput(existingText, incomingText) {
  const existing = normalizeText(existingText, "");
  const incoming = normalizeText(incomingText, "");
  const base = existing === "(running...)" ? "" : existing;

  if (!incoming) {
    return base || "(running...)";
  }
  if (!base) {
    return incoming;
  }
  if (incoming.startsWith(base)) {
    return incoming;
  }
  if (base.endsWith(incoming)) {
    return base;
  }
  return `${base}\n${incoming}`;
}

function mergeIncrementalText(existingText, incomingText) {
  const existing = existingText || "";
  const incoming = normalizeText(incomingText, "", { decodeEscapes: true });
  if (!incoming) return existing;
  if (!existing) return incoming;
  if (incoming.startsWith(existing)) return incoming;
  if (existing.endsWith(incoming)) return existing;
  if (/^[,.;:!?)]/.test(incoming)) return `${existing}${incoming}`;
  if (/^\s/.test(incoming) || /\s$/.test(existing)) return `${existing}${incoming}`;
  return `${existing}\n${incoming}`;
}

function ensureReasoningLiveNode(run) {
  if (run.reasoningLiveNode) {
    return run.reasoningLiveNode;
  }
  const details = document.createElement("details");
  details.className = "stream-event note";
  details.open = true;

  const summary = document.createElement("summary");
  summary.textContent = "Agent notes (live)";
  details.appendChild(summary);

  const outputNode = document.createElement("div");
  outputNode.className = "stream-event-multi markdown-content";
  setMarkdownContent(outputNode, "");
  details.appendChild(outputNode);

  run.streamEvents.appendChild(details);
  scrollExecutionToBottom();
  run.reasoningLiveNode = { details, outputNode };
  return run.reasoningLiveNode;
}

function appendCommandUpdate(run, data) {
  const executionId = normalizeText(data.execution_id, "");
  const runningBlock = executionId ? run.runningExecutionBlocks.get(executionId) : null;
  const output = normalizeText(data.tool_output, "", { decodeEscapes: true });
  if (!output) return;

  if (runningBlock) {
    setMarkdownContent(
      runningBlock.outputNode,
      mergeCommandOutput(getMarkdownSource(runningBlock.outputNode), output)
    );
    scrollExecutionToBottom();
    return;
  }

  const stepIndex = data.action_index_1based || data.step_index_1based || "?";
  const toolName = data.tool_name || "unknown tool";
  const command = normalizeText(data.command, "(no command)");
  const rationale = normalizeText(data.rationale, "");
  const blockRef = createCommandBlock({
    stepIndex,
    toolName,
    command,
    rationale,
    isRunning: true,
  });
  setMarkdownContent(
    blockRef.outputNode,
    mergeCommandOutput(getMarkdownSource(blockRef.outputNode), output)
  );
  if (executionId) {
    blockRef.details.dataset.executionId = executionId;
    run.runningExecutionBlocks.set(executionId, blockRef);
  }
  run.streamEvents.appendChild(blockRef.details);
  scrollExecutionToBottom();
}

function handleStreamEvent(type, data, run) {
  if (!data || !run) return;

  switch (type) {
    case "llm_decide_start":
    case "llm_decide_end":
      // Internal debug events
      break;
    case "tool_start":
      appendCommandStart(run, data);
      break;
    case "tool_update":
      appendCommandUpdate(run, data);
      break;
    case "tool_execution":
      appendCommandExecution(run, data);
      break;
    case "reasoning": {
      const text = normalizeText(data.text, "");
      if (text) {
        const ref = ensureReasoningLiveNode(run);
        setMarkdownContent(
          ref.outputNode,
          mergeIncrementalText(getMarkdownSource(ref.outputNode), text)
        );
      }
      break;
    }
    case "assistant_delta": {
      // Keep center chat focused on final assistant output only.
      break;
    }
    case "run_status":
      if (data.phase === "start") {
        appendStreamEvent(run, "Run started.", "note");
      } else if (data.phase === "end") {
        appendStreamEvent(run, "Run finished.", "note");
      }
      break;
    case "final_observation": {
      const text = normalizeText(data.final_observation, "(no final observation)");
      setAssistantBubble(run, text);
      run.commitAssistant?.(text);
      break;
    }
    case "final_result":
      if (data.result && typeof data.result === "object" && typeof data.result.final_observation === "string") {
        const text = normalizeText(data.result.final_observation, "(no final observation)");
        setAssistantBubble(run, text);
        run.commitAssistant?.(text);
      }
      if (data.result && typeof data.result === "object") {
        setMissionOverview(data.result.mission_overview, data.result.mission_overview_updated_at);
        if (!normalizeText(data.result.mission_overview, "")) {
          refreshMissionOverviewFromServer(data.session_id || activeSessionId).catch(() => {});
        }
      }
      break;
    case "cancelled": {
      const message = normalizeText(data.message, "Run stopped by user.");
      appendStreamEvent(run, message, "note");
      setStatus(promptStatus, message, false);
      break;
    }
    case "error": {
      const message = normalizeText(data.message, "Backend error while running prompt.");
      const code = normalizeText(data.code, "");
      const provider = normalizeText(data.provider, "").toLowerCase();
      if (code === "missing_provider_key" && provider === "anthropic") {
        anthropicConfigured = false;
        openAuthModal("Add your Anthropic key, then resend your prompt.");
      } else if (code === "provider_auth_invalid" && provider === "anthropic") {
        anthropicConfigured = false;
        openAuthModal("Anthropic authentication failed. Update your API key and retry.");
      } else if (code === "gateway_connectivity") {
        openSettingsModal("Gateway connectivity issue detected. Check URL/auth and retry.");
      }
      setStatus(promptStatus, message, false);
      appendStreamEvent(run, `ERROR: ${message}`, "note");
      if (!run.committedAssistant) {
        setAssistantBubble(run, `Error: ${message}`);
        run.commitAssistant?.(`Error: ${message}`);
      }
      break;
    }
    case "graph_updated":
      ensureGraphViewReady(true).catch(() => {});
      break;
    case "done":
      if (data.ok === false) {
        const doneMessage = activeRunStopRequested
          ? "Run stopped by user."
          : normalizeText(data.message, "Run ended with errors.");
        setStatus(promptStatus, doneMessage, false);
      } else {
        setStatus(promptStatus, "Run complete.", true);
      }
      break;
    default:
      break;
  }

  // Graph updates are pushed explicitly via graph_updated events.

  ensureJumpLatestButton();
  if (!autoScrollEnabled && jumpLatestBtn) {
    unseenEventCount += 1;
    jumpLatestBtn.classList.remove("hidden");
    jumpLatestBtn.textContent = unseenEventCount > 1 ? `Jump to latest (${unseenEventCount})` : "Jump to latest";
  }

  scrollChatToBottom({ behavior: "auto" });
}

function parseSseFrame(rawFrame) {
  const lines = rawFrame.split("\n");
  let eventName = "message";
  const dataParts = [];
  let payload = null;

  for (const line of lines) {
    if (line.startsWith("event:")) {
      eventName = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      dataParts.push(line.slice(5).trimStart());
    }
  }

  const dataText = dataParts.join("\n").trim();
  if (!dataText) return null;

  try {
    payload = JSON.parse(dataText);
  } catch (_err) {
    return null;
  }

  return { type: eventName, payload: payload };
}

async function runPromptStream({ prompt, sessionId, run, signal, onEvent }) {
  const response = await fetch("/api/prompt/stream", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt, session_id: sessionId }),
    signal,
  });

  if (!response.ok) {
    const text = await response.text();
    let message = "Prompt submission failed.";
    let code = "";
    let provider = "";
    try {
      const data = JSON.parse(text);
      message = data.message || message;
      code = data.code || "";
      provider = data.provider || "";
    } catch (_err) {
      message = text || message;
    }
    const err = new Error(message);
    err.code = code;
    err.provider = provider;
    throw err;
  }

  if (!response.body) {
    throw new Error("Streaming not supported by this browser.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let doneReceived = false;
  let doneStatus = false;

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      if (buffer.trim()) {
        const parsed = parseSseFrame(buffer.trim());
        if (parsed) {
          onEvent?.(parsed);
          handleStreamEvent(parsed.type, parsed.payload, run);
          if (parsed.type === "done") {
            doneReceived = true;
            doneStatus = parsed.payload.ok !== false;
          }
        }
      }
      break;
    }

    buffer += decoder.decode(value, { stream: true });
    let idx = buffer.indexOf("\n\n");
    while (idx !== -1) {
      const frame = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      const parsed = parseSseFrame(frame.trim());
      if (parsed) {
        onEvent?.(parsed);
        handleStreamEvent(parsed.type, parsed.payload, run);
        if (parsed.type === "done") {
          doneReceived = true;
          doneStatus = parsed.payload.ok !== false;
        }
      }
      idx = buffer.indexOf("\n\n");
    }
  }

  if (!doneReceived) {
    appendStreamEvent(run, "Stream ended unexpectedly without a done event.", "note");
    doneStatus = false;
  }

  return doneStatus;
}

async function verifyGateway(gatewayUrl, statusNode) {
  const response = await fetch("/api/verify-gateway", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ gateway_url: gatewayUrl }),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.message || "Gateway connection failed.");
  }
  setStatus(statusNode, data.message || "Gateway connected.", true);
  return data;
}

keyForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const value = keyInput.value.trim() || defaultGatewayUrl;

  setStatus(keyStatus, "Connecting to gateway...", true);
  try {
    await verifyGateway(value, keyStatus);
    await ensureActiveSession();
    await refreshSessionSummaries();
    await ensureAnthropicKeyConfigured({ showModal: true });
    renderChatHistory(activeSessionMessages);
    setUnlocked(true);
    showMissionCenterTab(activeSessionMessages.length > 0 ? "chat" : "overview");
    resizeInstructionsInput();
    targetUrlInput?.focus();
    scrollChatToBottom({ behavior: "smooth", force: true });
  } catch (error) {
    setStatus(keyStatus, error.message, false);
  }
});

authForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const value = authKeyInput?.value?.trim() || "";
  if (!value) {
    setStatus(authStatus, "Please enter your Anthropic API key.", false);
    return;
  }
  setStatus(authStatus, "Saving Anthropic key...", true);
  try {
    await fetchJson("/api/auth/anthropic", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: value }),
    });
    anthropicConfigured = true;
    if (authKeyInput) {
      authKeyInput.value = "";
    }
    closeAuthModal();
    setStatus(promptStatus, "Anthropic key saved. You can run prompts now.", true);
    targetUrlInput?.focus();
  } catch (error) {
    setStatus(authStatus, error.message || "Could not save API key.", false);
  }
});

newSessionBtn?.addEventListener("click", async () => {
  if (isRunning) {
    setSessionStatus("Wait for the current run to finish before creating a new mission.", false);
    return;
  }
  try {
    await createSession();
    hasSubmittedPrompt = false;
    await refreshSessionSummaries();
    renderChatHistory(activeSessionMessages);
    showMissionCenterTab("overview");
    setStatus(promptStatus, "Started a new mission.", true);
    setSessionStatus("", true);
    targetUrlInput?.focus();
    scrollChatToBottom({ behavior: "auto", force: true });
  } catch (error) {
    setSessionStatus(error.message || "Could not create a new mission.", false);
  }
});

promptForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isRunning) {
    await requestStopActiveRun();
    return;
  }
  const targetUrl = normalizeText(targetUrlInput?.value, "");
  const instructions = normalizeText(instructionsInput?.value, "");
  const prompt = buildMissionPrompt(targetUrl, instructions);
  if (!targetUrl) {
    setStatus(promptStatus, "Target URL is required.", false);
    return;
  }

  if (!activeSessionId) {
    try {
      await ensureActiveSession();
    } catch (error) {
      setStatus(promptStatus, error.message || "Could not initialize mission.", false);
      return;
    }
  }
  try {
    const configured = await ensureAnthropicKeyConfigured({ showModal: true });
    if (!configured) {
      setStatus(promptStatus, "Anthropic API key required before running prompts.", false);
      return;
    }
  } catch (error) {
    setStatus(promptStatus, error.message || "Could not verify provider auth.", false);
    return;
  }
  hasSubmittedPrompt = true;
  syncMissionWorkspaceState();
  showMissionCenterTab("chat");
  const run = createRunUI(prompt);

  run.commitAssistant = (text) => {
    if (run.committedAssistant) return;
    const normalized = normalizeText(text, "");
    activeSessionMessages.push({ role: "assistant", content: normalized, ts: Date.now() });
    touchActiveSessionSummary("assistant", normalized);
    run.committedAssistant = true;
  };

  chatThread?.appendChild(run.userMsg.wrapper);
  scrollChatToBottom({ behavior: "smooth", force: true });

  activeSessionMessages.push({ role: "user", content: prompt, ts: Date.now() });
  touchActiveSessionSummary("user", prompt);

  if (targetUrlInput) {
    targetUrlInput.value = "";
  }
  if (instructionsInput) {
    instructionsInput.value = "";
  }
  resizeInstructionsInput();

  activeRunAbortController = new AbortController();
  activeRunStopRequested = false;
  activeRunMeta = { sessionId: activeSessionId, runId: "" };
  setRunningState(true);
  setStatus(promptStatus, "Starting Cobra Lite run...", true);

  try {
    const ok = await runPromptStream({
      prompt,
      sessionId: activeSessionId,
      run,
      signal: activeRunAbortController?.signal,
      onEvent: (evt) => {
        const payload = evt && typeof evt.payload === "object" ? evt.payload : {};
        const runId = normalizeText(payload.run_id, "");
        if (runId && activeRunMeta) {
          activeRunMeta.runId = runId;
        }
      },
    });
    if (ok) {
      setStatus(promptStatus, "Run complete.", true);
      if (!run.committedAssistant) {
        const fallback = run.assistantMsg.bubble.textContent || "(no response)";
        setAssistantBubble(run, fallback);
        run.commitAssistant(fallback);
      }
    }
  } catch (error) {
    if (error && error.name === "AbortError") {
      const stopMessage = "Run stopped by user.";
      setStatus(promptStatus, stopMessage, false);
      appendStreamEvent(run, stopMessage, "note");
      if (!run.committedAssistant) {
        setAssistantBubble(run, stopMessage);
        run.commitAssistant(stopMessage);
      }
    } else if (isMissingAnthropicKeyError(error)) {
      anthropicConfigured = false;
      openAuthModal("Add your Anthropic key, then resend your prompt.");
      setStatus(promptStatus, "Anthropic API key required.", false);
      appendStreamEvent(run, "Error: Anthropic API key required.", "note");
      if (!run.committedAssistant) {
        const message = "Error: Anthropic API key required.";
        setAssistantBubble(run, message);
        run.commitAssistant(message);
      }
    } else if (isAnthropicAuthInvalidError(error)) {
      anthropicConfigured = false;
      openAuthModal("Anthropic authentication failed. Update your API key and retry.");
      setStatus(promptStatus, "Anthropic authentication failed.", false);
      appendStreamEvent(run, "Error: Anthropic authentication failed.", "note");
      if (!run.committedAssistant) {
        const message = "Error: Anthropic authentication failed.";
        setAssistantBubble(run, message);
        run.commitAssistant(message);
      }
    } else if (isGatewayConnectivityError(error)) {
      openSettingsModal("Gateway connectivity issue detected. Check URL/auth and retry.");
      setStatus(promptStatus, error.message, false);
      appendStreamEvent(run, `Error: ${error.message}`, "note");
      if (!run.committedAssistant) {
        const message = `Error: ${error.message}`;
        setAssistantBubble(run, message);
        run.commitAssistant(message);
      }
    } else {
      setStatus(promptStatus, error.message, false);
      appendStreamEvent(run, `Error: ${error.message}`, "note");
      if (!run.committedAssistant) {
        const message = `Error: ${error.message}`;
        setAssistantBubble(run, message);
        run.commitAssistant(message);
      }
    }
  } finally {
    try {
      await refreshSessionSummaries();
    } catch (_err) {
      // Ignore sidebar refresh errors after prompt completion.
    }
    setRunningState(false);
    targetUrlInput?.focus();
  }
});

targetUrlInput?.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    promptForm?.requestSubmit();
  }
});
instructionsInput?.addEventListener("input", resizeInstructionsInput);

missionTabOverview?.addEventListener("click", () => {
  showMissionCenterTab("overview");
});

missionTabReport?.addEventListener("click", () => {
  showMissionCenterTab("report");
});

downloadReportBtn?.addEventListener("click", () => {
  downloadDetailedReportPdf();
});

executionTabTerminal?.addEventListener("click", () => {
  showExecutionTab("terminal");
});

executionTabGraph?.addEventListener("click", () => {
  showExecutionTab("graph");
});

executionTabFiles?.addEventListener("click", () => {
  showExecutionTab("files");
});

filesUpBtn?.addEventListener("click", async () => {
  if (fileViewerState.parentPath === null) return;
  await loadFilesDirectory(fileViewerState.parentPath);
});

filesRefreshBtn?.addEventListener("click", async () => {
  await loadFilesDirectory(fileViewerState.currentPath);
});

sessionFoldBtn?.addEventListener("click", () => {
  const next = !sessionPaneCollapsed;
  applySessionPaneCollapsed(next);
  writeSessionPaneCollapsed(next);
});

settingsBtn?.addEventListener("click", () => {
  openSettingsModal("");
});

closeSettingsBtn?.addEventListener("click", () => {
  settingsModal.classList.add("hidden");
});

settingsModal?.addEventListener("click", (event) => {
  if (event.target === settingsModal) {
    settingsModal.classList.add("hidden");
  }
});

settingsForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const value = settingsKeyInput.value.trim();
  if (!value) {
    setStatus(settingsStatus, "Please enter a gateway URL.", false);
    return;
  }

  setStatus(settingsStatus, "Updating gateway...", true);
  try {
    await verifyGateway(value, settingsStatus);
    settingsKeyInput.value = "";
    setUnlocked(true);
    setTimeout(() => settingsModal.classList.add("hidden"), 400);
  } catch (error) {
    setStatus(settingsStatus, error.message, false);
  }
});

async function bootstrap() {
  setupResizablePanes();
  setupGraphInteractions();
  applySessionPaneCollapsed(readSessionPaneCollapsed());
  showMissionCenterTab("overview");
  setUnlocked(hasGateway);
  restoreExecutionWidth();
  showExecutionTab("terminal");
  renderExecutionEmpty();
  renderMissionOverview();
  renderGraph();
  if (hasGateway) {
    try {
      await ensureActiveSession();
      await refreshSessionSummaries();
      await ensureAnthropicKeyConfigured({ showModal: true });
      await ensureGraphViewReady();
    } catch (error) {
      setStatus(promptStatus, error.message || "Could not load mission.", false);
    }
  }
  renderSessionList();
  renderChatHistory(activeSessionMessages);
  resizeInstructionsInput();
  scrollChatToBottom({ behavior: "auto", force: true });
  ensureJumpLatestButton();
  updateAutoScrollEnabled();
  chatThread?.addEventListener("scroll", updateAutoScrollEnabled, { passive: true });
  const authModalVisible = !!authModal && !authModal.classList.contains("hidden");
  if (hasGateway && !authModalVisible) {
    targetUrlInput?.focus();
  }
}

window.addEventListener("beforeunload", () => {
  if (!executionFeed || !activeSessionId) return;
  const html = executionFeed.innerHTML || "";
  executionHistoryBySession.set(activeSessionId, html);
  try {
    const url = `/api/sessions/${encodeURIComponent(activeSessionId)}/execution-html`;
    const payload = JSON.stringify({ html });
    navigator.sendBeacon(url, new Blob([payload], { type: "application/json" }));
  } catch (_err) {
    // Ignore unload persistence errors.
  }
});

bootstrap();
