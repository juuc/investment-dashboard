const $ = (id) => document.getElementById(id);

const state = {
  jobRunning: false,
  pollHandle: null,
};

const TERM_DEFINITIONS = {
  NPS: "National Pension Service의 약자입니다. 한국의 국민연금공단을 의미합니다.",
  SEC: "U.S. Securities and Exchange Commission의 약자입니다. 미국 증권거래위원회입니다.",
  "13F": "미국 기관투자자가 분기마다 제출하는 보유종목 공시 양식(Form 13F)입니다.",
  CIK: "Central Index Key의 약자입니다. SEC에서 공시 주체를 식별하는 고유 번호입니다.",
  AUM: "Assets Under Management의 약자입니다. 운용자산 총액을 뜻합니다.",
  KST: "Korea Standard Time의 약자입니다. 한국 표준시(UTC+9)입니다.",
  KOSPI: "Korea Composite Stock Price Index. 한국거래소 유가증권시장(코스피)입니다.",
  KOSDAQ: "Korean Securities Dealers Automated Quotations. 한국거래소 코스닥시장입니다.",
  OPENDART: "금융감독원 전자공시시스템(DART) Open API 서비스입니다.",
  KIS: "한국투자증권 Open API를 제공하는 Korea Investment & Securities를 뜻합니다.",
  API: "Application Programming Interface의 약자입니다. 시스템 간 데이터 연동 규격입니다.",
  CUSIP: "북미 증권 식별 코드 체계(Committee on Uniform Securities Identification Procedures)입니다.",
  USD: "United States Dollar의 약자입니다. 미국 달러화입니다.",
};

function asText(value, fallback = "-") {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function fmtNumber(value, digits = 1) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function fmtSigned(value, digits = 3) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  const prefix = num > 0 ? "+" : "";
  return `${prefix}${num.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })}`;
}

function fmtUSDbn(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return (num / 1_000_000_000).toLocaleString(undefined, {
    maximumFractionDigits: 3,
    minimumFractionDigits: 3,
  });
}

function fmtEok(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return (num / 100_000_000).toLocaleString(undefined, {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  });
}

function fmtKST(isoString) {
  if (!isoString) return "-";
  try {
    const date = new Date(isoString);
    if (isNaN(date.getTime())) return String(isoString);
    const kst = new Date(date.getTime() + 9 * 60 * 60 * 1000);
    const y = kst.getUTCFullYear();
    const m = String(kst.getUTCMonth() + 1).padStart(2, "0");
    const d = String(kst.getUTCDate()).padStart(2, "0");
    const h = String(kst.getUTCHours()).padStart(2, "0");
    const min = String(kst.getUTCMinutes()).padStart(2, "0");
    const s = String(kst.getUTCSeconds()).padStart(2, "0");
    return `${y}-${m}-${d} ${h}:${min}:${s}`;
  } catch {
    return String(isoString);
  }
}

function formatKoreaWeightBasis(rawBasis) {
  const basis = asText(rawBasis, "");
  if (basis === "estimated_value_krw") return "평가금액 기준(원화)";
  if (basis === "stake_pct_normalized") return "공시 지분율 정규화 기준";
  return asText(rawBasis, "정보 없음");
}

function fmtJoWon(krw) {
  const num = Number(krw);
  if (!Number.isFinite(num)) return "-";
  return (num / 1_000_000_000_000).toLocaleString(undefined, {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  }) + "조 원";
}

function initTabs() {
  const buttons = document.querySelectorAll(".tab-btn");
  const panels = document.querySelectorAll(".tab-panel");

  for (const btn of buttons) {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-tab");

      for (const b of buttons) b.classList.remove("active");
      btn.classList.add("active");

      for (const p of panels) {
        p.classList.toggle("active", p.id === `tab-${target}`);
      }
    });
  }
}

function renderMarketIndices(indicesPayload) {
  const indices = indicesPayload?.indices || {};

  const mapping = [
    { key: "KOSPI", valueId: "kpi-kospi-value", changeId: "kpi-kospi-change" },
    { key: "KOSDAQ", valueId: "kpi-kosdaq-value", changeId: "kpi-kosdaq-change" },
    { key: "NASDAQ", valueId: "kpi-nasdaq-value", changeId: "kpi-nasdaq-change" },
    { key: "SP500", valueId: "kpi-sp500-value", changeId: "kpi-sp500-change" },
    { key: "GOLD", valueId: "kpi-gold-value", changeId: "kpi-gold-change" },
    { key: "BTC", valueId: "kpi-btc-value", changeId: "kpi-btc-change" },
  ];

  for (const { key, valueId, changeId } of mapping) {
    const valueEl = $(valueId);
    const changeEl = $(changeId);
    if (!valueEl || !changeEl) continue;

    const data = indices[key];
    if (!data || !Number.isFinite(data.value)) {
      valueEl.textContent = "-";
      changeEl.textContent = "";
      changeEl.className = "kpi-change";
      continue;
    }

    valueEl.textContent = fmtNumber(data.value, key === "BTC" ? 0 : 2);
    const pct = Number(data.change_pct || 0);
    const prefix = pct > 0 ? "+" : "";
    changeEl.textContent = `${prefix}${fmtNumber(pct, 2)}%`;
    changeEl.className = `kpi-change ${pct > 0 ? "up" : pct < 0 ? "down" : "flat"}`;
  }

  // Bitcoin KRW sub-line
  const btcKrw = $("kpi-btc-krw");
  if (btcKrw) {
    const btcData = indices["BTC"];
    if (btcData && Number.isFinite(btcData.value_krw) && btcData.value_krw > 0) {
      btcKrw.textContent = `₩${fmtNumber(btcData.value_krw, 0)}`;
    } else {
      btcKrw.textContent = "";
    }
  }
}

function renderSentiment(sentimentPayload) {
  const badge = $("sentiment-signal-badge");
  const fill = $("gauge-fill");
  const creditEl = $("sentiment-credit");
  const depositsEl = $("sentiment-deposits");
  const ratioEl = $("sentiment-ratio");

  if (!badge) return;

  const status = sentimentPayload?.status;
  if (status !== "ok") {
    badge.textContent = status === "skipped" ? "API 키 없음" : "데이터 없음";
    badge.className = "signal-badge";
    if (fill) fill.style.width = "0%";
    if (creditEl) creditEl.textContent = "-";
    if (depositsEl) depositsEl.textContent = "-";
    if (ratioEl) ratioEl.textContent = "-";
    return;
  }

  const ratio = Number(sentimentPayload.credit_ratio_pct || 0);
  const signal = sentimentPayload.signal || "normal";

  const SIGNAL_LABELS = { normal: "정상", watch: "관찰", warning: "주의", danger: "위험" };
  badge.textContent = SIGNAL_LABELS[signal] || signal;
  badge.className = `signal-badge ${signal}`;

  // Gauge fill: map 0-50% ratio to 0-100% width
  if (fill) {
    const fillPct = Math.max(0, Math.min(100, (ratio / 50) * 100));
    fill.style.width = `${fillPct}%`;
  }

  if (creditEl) creditEl.textContent = fmtJoWon(sentimentPayload.credit_balance_krw);
  if (depositsEl) depositsEl.textContent = fmtJoWon(sentimentPayload.investor_deposits_krw);
  if (ratioEl) ratioEl.textContent = `${fmtNumber(ratio, 2)}%`;
}

function initTermTooltips() {
  const tooltip = document.createElement("div");
  tooltip.className = "term-tooltip";
  tooltip.hidden = true;
  document.body.appendChild(tooltip);

  let activeEl = null;

  const closeTooltip = () => {
    tooltip.hidden = true;
    activeEl = null;
  };

  const terms = document.querySelectorAll("[data-term-key]");
  for (const el of terms) {
    const key = String(el.getAttribute("data-term-key") || "").trim();
    const description = TERM_DEFINITIONS[key];
    if (!description) continue;
    el.classList.add("term");
    el.setAttribute("aria-label", `${asText(el.textContent, key)}: ${description}`);

    el.addEventListener("click", (event) => {
      event.stopPropagation();
      if (activeEl === el) {
        closeTooltip();
        return;
      }
      activeEl = el;
      tooltip.innerHTML = `<div class="term-tooltip-key">${key}</div><div>${description}</div>`;
      tooltip.hidden = false;

      const rect = el.getBoundingClientRect();
      let top = rect.bottom + 6;
      let left = rect.left;
      if (left + 320 > window.innerWidth) left = window.innerWidth - 330;
      if (left < 8) left = 8;
      if (top + tooltip.offsetHeight > window.innerHeight) top = rect.top - tooltip.offsetHeight - 6;
      tooltip.style.top = `${top}px`;
      tooltip.style.left = `${left}px`;
    });
  }

  document.addEventListener("click", closeTooltip);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeTooltip();
  });
}

async function getJSON(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function formatEta(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "-";
  const minutes = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  if (minutes > 0) return `약 ${minutes}분 ${secs}초 남음`;
  return `약 ${secs}초 남음`;
}

function renderProgress(job) {
  const container = $("job-progress");
  if (!container) return;

  const progress = job?.progress;
  if (!job?.running || !progress) {
    container.hidden = true;
    return;
  }

  container.hidden = false;
  $("progress-label").textContent = progress.label || "처리 중...";
  $("progress-step").textContent = `${progress.step}/${progress.total}`;

  const pct = progress.total > 0 ? (progress.step / progress.total) * 100 : 0;
  $("progress-fill").style.width = `${Math.min(100, pct)}%`;

  const elapsed = progress.elapsed_sec || 0;
  const minutes = Math.floor(elapsed / 60);
  const secs = elapsed % 60;
  $("progress-elapsed").textContent = minutes > 0 ? `경과: ${minutes}분 ${secs}초` : `경과: ${secs}초`;

  if (progress.step > 0 && progress.step < progress.total) {
    const etaSec = (elapsed / progress.step) * (progress.total - progress.step);
    $("progress-eta").textContent = formatEta(etaSec);
  } else if (progress.step >= progress.total) {
    $("progress-eta").textContent = "마무리 중...";
  } else {
    $("progress-eta").textContent = "예상 남은 시간: -";
  }
}

function renderJob(job) {
  const pill = $("job-pill");
  const log = $("job-log");

  if (!job) {
    pill.textContent = "대기 중";
    log.style.display = "none";
    renderProgress(null);
    return;
  }

  if (job.running) {
    pill.textContent = "갱신 중...";
    pill.style.background = "#fff2da";
    pill.style.borderColor = "#ffd89c";
    pill.style.color = "#6d3d00";
    const text = [job.started_at_utc ? `시작 시각: ${fmtKST(job.started_at_utc)}` : "", job.stdout || "", job.stderr || ""]
      .filter(Boolean)
      .join("\n");
    log.style.display = text ? "block" : "none";
    log.textContent = text;
    renderProgress(job);
    state.jobRunning = true;
    return;
  }

  state.jobRunning = false;
  renderProgress(null);

  if (job.exit_code === 0) {
    pill.textContent = "마지막 갱신 성공";
    pill.style.background = "#e8fff2";
    pill.style.borderColor = "#bde8cd";
    pill.style.color = "#116235";
  } else if (job.exit_code !== null) {
    pill.textContent = "마지막 갱신 실패";
    pill.style.background = "#ffecec";
    pill.style.borderColor = "#ffc6c6";
    pill.style.color = "#8f1b1b";
  } else {
    pill.textContent = "대기 중";
    pill.style.background = "#edf4ff";
    pill.style.borderColor = "#bad0ff";
    pill.style.color = "#123a88";
  }

  const logText = [job.stdout || "", job.stderr || ""].filter(Boolean).join("\n");
  log.style.display = logText ? "block" : "none";
  log.textContent = logText;
}

function renderNps(nps) {
  $("kpi-nps-month").textContent = asText(nps?.as_of_month);
  $("kpi-aum").textContent = Number.isFinite(Number(nps?.total_aum_trillion_krw))
    ? `${fmtNumber(nps.total_aum_trillion_krw, 1)}조 원`
    : "-";
  $("nps-asof").textContent = `기준월: ${asText(nps?.as_of_month)}`;

  const bars = $("nps-bars");
  bars.innerHTML = "";

  const rows = Array.isArray(nps?.rows) ? nps.rows : [];
  if (rows.length === 0) {
    bars.innerHTML = "<p>NPS 자산배분 데이터가 없습니다. 상단의 전체 데이터 새로고침을 눌러주세요.</p>";
    return;
  }

  for (const row of rows) {
    const pct = Number(row.weight_pct || 0);
    const width = Math.max(0, Math.min(100, Math.abs(pct)));
    const bar = document.createElement("div");
    bar.className = "bar-row";
    bar.innerHTML = `
      <div class="bar-label">${asText(row.asset_name)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      <div class="bar-value">${fmtNumber(row.weight_pct, 1)}%</div>
    `;
    bars.appendChild(bar);
  }
}

function renderSecTop(topHoldings) {
  const tbody = $("sec-top-table").querySelector("tbody");
  tbody.innerHTML = "";
  const rows = Array.isArray(topHoldings) ? topHoldings : [];
  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5">SEC 보유 데이터가 없습니다.</td>`;
    tbody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${asText(row.issuer_name)}</td>
      <td>${asText(row.title_of_class)}</td>
      <td>${asText(row.cusip)}</td>
      <td>${fmtUSDbn(row.value_usd)}</td>
      <td>${fmtNumber(row.weight_pct_of_13f, 3)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderKoreaChart(koreaPayload) {
  const container = $("korea-weight-bars");
  const basisTag = $("korea-weight-basis");
  if (!container || !basisTag) return;
  container.innerHTML = "";

  const rows = Array.isArray(koreaPayload?.holdings) ? koreaPayload.holdings : [];
  const basis = formatKoreaWeightBasis(koreaPayload?.weight_basis);
  basisTag.textContent = `가중치 기준: ${basis}`;

  if (rows.length === 0) {
    container.innerHTML = "<p>국내 공시 보유 데이터가 없습니다. 상단의 전체 데이터 새로고침을 눌러주세요.</p>";
    return;
  }

  const maxWeight = rows.reduce((max, row) => Math.max(max, Number(row.weight_pct || 0)), 0) || 1;
  for (const [index, row] of rows.slice(0, 30).entries()) {
    const weight = Number(row.weight_pct || 0);
    const width = Math.max(2, Math.min(100, (weight / maxWeight) * 100));
    const bar = document.createElement("div");
    bar.className = "bar-row korea-row";
    bar.innerHTML = `
      <div class="bar-label korea-label" title="${asText(row.corp_name_ko)}">${index + 1}. ${asText(row.corp_name_ko)}</div>
      <div class="bar-track korea-track"><div class="bar-fill korea-fill" style="width:${width}%"></div></div>
      <div class="bar-value">${fmtNumber(weight, 3)}%</div>
    `;
    container.appendChild(bar);
  }
}

function renderKoreaTable(koreaPayload) {
  const tbody = $("korea-table")?.querySelector("tbody");
  const count = $("korea-count");
  if (!tbody || !count) return;

  tbody.innerHTML = "";
  const rows = Array.isArray(koreaPayload?.holdings) ? koreaPayload.holdings : [];
  count.textContent = `${rows.length} 종목`;

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7">국내 공시 보유 데이터가 없습니다.</td>`;
    tbody.appendChild(tr);
    return;
  }

  for (const row of rows.slice(0, 100)) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${asText(row.corp_name_ko)}</td>
      <td>${asText(row.market)}</td>
      <td>${asText(row.stock_code)}</td>
      <td>${fmtNumber(row.weight_pct, 3)}</td>
      <td>${row.stake_pct === null || row.stake_pct === undefined ? "-" : fmtNumber(row.stake_pct, 3)}</td>
      <td>${row.estimated_value_krw === null || row.estimated_value_krw === undefined ? "-" : fmtEok(row.estimated_value_krw)}</td>
      <td>${asText(row.latest_disclosure_date)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderKoreaEmerging(emergingPayload) {
  const barContainer = $("korea-emerging-bars");
  const metaTag = $("korea-emerging-meta");
  const tbody = $("korea-emerging-table")?.querySelector("tbody");
  if (!barContainer || !metaTag || !tbody) return;

  barContainer.innerHTML = "";
  tbody.innerHTML = "";

  const rows = Array.isArray(emergingPayload?.ranked) ? emergingPayload.ranked : [];
  const shortRuns = Number(emergingPayload?.short_window_runs || 0);
  const longRuns = Number(emergingPayload?.long_window_runs || 0);
  if (shortRuns > 0 && longRuns > 0) {
    metaTag.textContent = `최근 ${shortRuns}회 / ${longRuns}회 비교`;
  } else {
    metaTag.textContent = "히스토리 비교 정보 없음";
  }

  if (rows.length === 0) {
    barContainer.innerHTML = "<p>주목 종목을 계산할 히스토리 데이터가 부족합니다.</p>";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7">주목 종목 데이터가 없습니다.</td>`;
    tbody.appendChild(tr);
    return;
  }

  const maxAbsScore = rows.reduce((max, row) => Math.max(max, Math.abs(Number(row.score || 0))), 0) || 1;
  for (const [index, row] of rows.slice(0, 20).entries()) {
    const score = Number(row.score || 0);
    const width = Math.max(2, Math.min(100, (Math.abs(score) / maxAbsScore) * 100));
    const fillClass = score < 0 ? "bar-fill emerging-fill caution" : "bar-fill emerging-fill";
    const bar = document.createElement("div");
    bar.className = "bar-row emerging-row";
    bar.innerHTML = `
      <div class="bar-label emerging-label" title="${asText(row.corp_name_ko)}">${index + 1}. ${asText(row.corp_name_ko)} (${asText(row.market)})</div>
      <div class="bar-track emerging-track"><div class="${fillClass}" style="width:${width}%"></div></div>
      <div class="bar-value">${fmtSigned(score, 3)}</div>
    `;
    barContainer.appendChild(bar);
  }

  for (const row of rows.slice(0, 40)) {
    const currentWeight = Number(row.current_weight_pct || 0);
    const deltaShort = Number(row.delta_short_pctp || 0);
    const deltaLong = Number(row.delta_long_pctp || 0);
    const score = Number(row.score || 0);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${asText(row.corp_name_ko)}</td>
      <td>${asText(row.market)}</td>
      <td>${fmtNumber(currentWeight, 3)}</td>
      <td class="${deltaShort >= 0 ? "status-ok" : "status-error"}">${fmtSigned(deltaShort, 3)}</td>
      <td class="${deltaLong >= 0 ? "status-ok" : "status-error"}">${fmtSigned(deltaLong, 3)}</td>
      <td>${fmtSigned(score, 3)}${row.new_flag ? " (신규)" : ""}</td>
      <td>${asText(row.latest_disclosure_date)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderSecWeightChart(topHoldings) {
  const container = $("sec-weight-bars");
  if (!container) return;
  container.innerHTML = "";

  const rows = Array.isArray(topHoldings) ? topHoldings : [];
  if (rows.length === 0) {
    container.innerHTML = "<p>SEC 보유 데이터가 없습니다. 상단의 전체 데이터 새로고침을 눌러주세요.</p>";
    return;
  }

  const maxWeight = rows.reduce((max, row) => Math.max(max, Number(row.weight_pct_of_13f || 0)), 0) || 1;

  for (const [index, row] of rows.entries()) {
    const weight = Number(row.weight_pct_of_13f || 0);
    const width = Math.max(2, Math.min(100, (weight / maxWeight) * 100));
    const bar = document.createElement("div");
    bar.className = "bar-row holdings-row";
    bar.innerHTML = `
      <div class="bar-label holdings-label" title="${asText(row.issuer_name)}">${index + 1}. ${asText(row.issuer_name)}</div>
      <div class="bar-track holdings-track"><div class="bar-fill holdings-fill" style="width:${width}%"></div></div>
      <div class="bar-value">${fmtNumber(weight, 3)}%</div>
    `;
    container.appendChild(bar);
  }
}

function renderSecHistory(historyRows) {
  const tbody = $("sec-history-table").querySelector("tbody");
  tbody.innerHTML = "";

  const rows = Array.isArray(historyRows) ? historyRows : [];
  $("sec-history-count").textContent = `${rows.length}건`;

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6">SEC 13F 제출 이력이 없습니다.</td>`;
    tbody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    const statusRaw = asText(row.status).toLowerCase();
    const isOk = statusRaw === "ok";
    const statusClass = isOk ? "status-ok" : "status-error";
    const statusText = isOk ? "성공" : "실패";
    tr.innerHTML = `
      <td>${asText(row.filing_date)}</td>
      <td>${asText(row.report_date)}</td>
      <td>${asText(row.form)}</td>
      <td>${asText(row.accession_number)}</td>
      <td class="${statusClass}">${statusText}</td>
      <td>${isOk ? fmtUSDbn(row.total_value_usd) : "-"}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderSnapshots(rows) {
  const list = $("snapshot-list");
  list.innerHTML = "";
  const snapshots = Array.isArray(rows) ? rows : [];
  if (snapshots.length === 0) {
    const li = document.createElement("li");
    li.textContent = "스냅샷 이력이 없습니다.";
    list.appendChild(li);
    return;
  }
  for (const row of snapshots.slice(0, 20)) {
    const li = document.createElement("li");
    li.innerHTML = `
      <span><strong>${asText(row.run_id)}</strong></span>
      <span>${fmtKST(row.created_at_utc)}</span>
    `;
    list.appendChild(li);
  }
}

function renderKpis(latest) {
  const secMeta = latest?.sec_meta || {};
  const latestFiling = secMeta?.latest_filing || {};
  $("kpi-filing-date").textContent = asText(latestFiling.filing_date);
  const processed = secMeta?.filings_processed_count;
  const success = secMeta?.filings_success_count;
  $("kpi-sec-processed").textContent =
    Number.isFinite(Number(processed)) && Number.isFinite(Number(success))
      ? `${processed}건 (${success}건 성공)`
      : "-";
}

async function refreshDashboardData() {
  const payload = await getJSON("/api/dashboard");
  $("generated-at").textContent = fmtKST(payload.generated_at_utc);

  const latest = payload.latest || {};
  const runManifest = latest.run_manifest || {};
  $("latest-run").textContent = asText(runManifest.run_id);

  renderMarketIndices(latest.market_indices || {});
  renderSentiment(latest.market_sentiment || {});
  renderNps(latest.nps || {});
  renderKoreaChart(latest.korea || {});
  renderKoreaTable(latest.korea || {});
  renderKoreaEmerging(payload.korea_emerging || {});
  renderKpis(latest);
  renderSecTop(latest.sec_top_holdings || []);
  renderSecWeightChart(latest.sec_top_holdings || []);
  renderSecHistory(latest.sec_history || []);
  renderSnapshots(payload.snapshots || []);
  renderJob(payload.job || {});
}

async function refreshJobState() {
  const payload = await getJSON("/api/job");
  const previous = state.jobRunning;
  renderJob(payload.job || {});
  if (previous && !state.jobRunning) {
    await refreshDashboardData();
  }
}

async function triggerRefresh() {
  const button = $("refresh-btn");
  button.disabled = true;
  try {
    const body = {
      sec_history: $("sec-history").value,
      sec_max_filings: Number($("sec-max-filings").value || 0),
      top_holdings: Number($("top-holdings").value || 30),
      korea_lookback_days: Number($("korea-lookback-days")?.value || 365),
      korea_request_delay: Number($("korea-request-delay")?.value || 0.1),
      snapshot_retain: Number($("snapshot-retain")?.value || 120),
      skip_korea: Boolean($("skip-korea")?.checked),
      output_dir: "data",
    };
    await getJSON("/api/refresh", {
      method: "POST",
      body: JSON.stringify(body),
    });
    await refreshJobState();
  } catch (error) {
    $("job-pill").textContent = `갱신 요청 실패: ${error}`;
  } finally {
    button.disabled = false;
  }
}

function startPolling() {
  if (state.pollHandle) return;
  state.pollHandle = setInterval(() => {
    refreshJobState().catch(() => {});
  }, 2500);
}

function initSourceTooltip() {
  const button = $("source-tooltip-btn");
  const panel = $("source-tooltip-panel");
  if (!button || !panel) return;

  let opened = false;

  const closePanel = () => {
    opened = false;
    panel.hidden = true;
    button.setAttribute("aria-expanded", "false");
  };

  const openPanel = () => {
    opened = true;
    panel.hidden = false;
    button.setAttribute("aria-expanded", "true");
  };

  button.addEventListener("click", (event) => {
    event.stopPropagation();
    if (opened) {
      closePanel();
    } else {
      openPanel();
    }
  });

  panel.addEventListener("click", (event) => {
    event.stopPropagation();
  });

  document.addEventListener("click", () => {
    if (opened) closePanel();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && opened) {
      closePanel();
    }
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  $("refresh-btn").addEventListener("click", () => {
    triggerRefresh().catch(() => {});
  });
  initTabs();
  initTermTooltips();
  initSourceTooltip();
  await refreshDashboardData();
  startPolling();
});
