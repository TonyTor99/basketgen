function toggleFeatures(checked) {
  document.querySelectorAll('input[name="features"]').forEach((el) => {
    el.checked = checked;
  });
}

function parseMetric(value) {
  const num = parseFloat(String(value ?? "").replace(",", "."));
  return Number.isFinite(num) ? num : null;
}

function getCellValue(row, index) {
  return row.children[index]?.innerText?.trim() ?? "";
}

function compareValues(a, b, type, asc) {
  if (type === "num") {
    const numA = parseMetric(a);
    const numB = parseMetric(b);
    const safeA = numA === null ? -Infinity : numA;
    const safeB = numB === null ? -Infinity : numB;
    return asc ? safeA - safeB : safeB - safeA;
  }
  return asc ? String(a).localeCompare(String(b), "ru") : String(b).localeCompare(String(a), "ru");
}

function initSortableTable() {
  const table = document.getElementById("sortableTable");
  if (!table) return;

  const headers = Array.from(table.querySelectorAll("th"));
  const tbody = table.querySelector("tbody");
  const search = document.getElementById("tableSearch");
  const rows = () => Array.from(tbody.querySelectorAll("tr"));

  const minRoiInput = document.getElementById("qualityMinRoi");
  const minRoiTestInput = document.getElementById("qualityMinRoiTest");
  const maxDdInput = document.getElementById("qualityMaxDdPct");
  const minProfitDdInput = document.getElementById("qualityMinProfitDd");
  const minMatchesInput = document.getElementById("qualityMinMatches");
  const applyBtn = document.getElementById("qualityApplyBtn");
  const resetBtn = document.getElementById("qualityResetBtn");
  const visibleRowsCounter = document.getElementById("visibleRowsCounter");

  let currentSort = { index: 0, asc: false };
  const hasQualityFilters = Boolean(minRoiInput && minRoiTestInput && maxDdInput && minProfitDdInput && minMatchesInput);

  const collectFilters = () => ({
    minRoi: parseMetric(minRoiInput?.value),
    minRoiTest: parseMetric(minRoiTestInput?.value),
    maxDdPct: parseMetric(maxDdInput?.value),
    minProfitDd: parseMetric(minProfitDdInput?.value),
    minMatches: parseMetric(minMatchesInput?.value),
  });

  const rowMatchesQuality = (row, filters) => {
    if (!hasQualityFilters) return true;

    const roi = parseMetric(row.dataset.roi);
    const roiTest = parseMetric(row.dataset.roiTest);
    const ddPct = parseMetric(row.dataset.ddPct);
    const profitDd = parseMetric(row.dataset.profitDd);
    const matches = parseMetric(row.dataset.matches);

    if (filters.minRoi !== null && (roi === null || roi < filters.minRoi)) return false;
    if (filters.minRoiTest !== null && (roiTest === null || roiTest < filters.minRoiTest)) return false;
    if (filters.maxDdPct !== null && (ddPct === null || ddPct > filters.maxDdPct)) return false;
    if (filters.minProfitDd !== null && (profitDd === null || profitDd < filters.minProfitDd)) return false;
    if (filters.minMatches !== null && (matches === null || matches < filters.minMatches)) return false;
    return true;
  };

  const filterRows = () => {
    const term = (search?.value || "").toLowerCase().trim();
    const filters = collectFilters();
    let visible = 0;

    rows().forEach((row) => {
      const text = row.innerText.toLowerCase();
      const textOk = !term || text.includes(term);
      const qualityOk = rowMatchesQuality(row, filters);
      const show = textOk && qualityOk;
      row.style.display = show ? "" : "none";
      if (show) visible += 1;
    });

    if (visibleRowsCounter) {
      visibleRowsCounter.textContent = `Показано: ${visible}`;
    }
  };

  headers.forEach((header, index) => {
    header.addEventListener("click", () => {
      const asc = currentSort.index === index ? !currentSort.asc : true;
      currentSort = { index, asc };
      const type = header.dataset.type || "text";
      const sorted = rows().sort((rowA, rowB) => compareValues(getCellValue(rowA, index), getCellValue(rowB, index), type, asc));
      sorted.forEach((row) => tbody.appendChild(row));
      filterRows();
    });
  });

  if (search) {
    search.addEventListener("input", filterRows);
  }

  if (applyBtn) {
    applyBtn.addEventListener("click", filterRows);
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      [minRoiInput, minRoiTestInput, maxDdInput, minProfitDdInput, minMatchesInput].forEach((input) => {
        if (input) input.value = "";
      });
      filterRows();
    });
  }

  [minRoiInput, minRoiTestInput, maxDdInput, minProfitDdInput, minMatchesInput].forEach((input) => {
    if (!input) return;
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        filterRows();
      }
    });
  });

  filterRows();
}

function initResultsTableUX() {
  const wrap = document.getElementById("resultsTableWrap");
  const table = document.getElementById("sortableTable");
  if (!wrap || !table) return;

  const topScroller = document.getElementById("resultsTopScroller");
  const topScrollerInner = document.getElementById("resultsTopScrollerInner");
  const scrollHint = document.getElementById("resultsScrollHint");
  const fullscreenBtn = document.getElementById("toggleTableFullscreen");
  const tableCard = document.getElementById("resultsTableCard");
  let syncLock = false;

  const updateOverflow = () => {
    const hasOverflow = table.scrollWidth > wrap.clientWidth + 2;
    if (topScroller) topScroller.style.display = hasOverflow ? "block" : "none";
    if (scrollHint) scrollHint.style.display = hasOverflow ? "block" : "none";
    if (topScrollerInner) topScrollerInner.style.width = `${table.scrollWidth}px`;
  };

  if (topScroller && topScrollerInner) {
    topScroller.addEventListener("scroll", () => {
      if (syncLock) return;
      syncLock = true;
      wrap.scrollLeft = topScroller.scrollLeft;
      syncLock = false;
    });

    wrap.addEventListener("scroll", () => {
      if (syncLock) return;
      syncLock = true;
      topScroller.scrollLeft = wrap.scrollLeft;
      syncLock = false;
    });
  }

  wrap.addEventListener(
    "wheel",
    (event) => {
      const shouldHorizontal = event.shiftKey || Math.abs(event.deltaX) < Math.abs(event.deltaY);
      if (!shouldHorizontal) return;
      wrap.scrollLeft += event.deltaX + event.deltaY;
      event.preventDefault();
    },
    { passive: false },
  );

  let isDragging = false;
  let dragStartX = 0;
  let dragStartScroll = 0;

  wrap.addEventListener("mousedown", (event) => {
    if (event.button !== 0) return;
    if (event.target.closest("a, button, input, textarea, select, label")) return;
    isDragging = true;
    dragStartX = event.pageX;
    dragStartScroll = wrap.scrollLeft;
    wrap.classList.add("is-dragging");
    event.preventDefault();
  });

  window.addEventListener("mousemove", (event) => {
    if (!isDragging) return;
    const delta = event.pageX - dragStartX;
    wrap.scrollLeft = dragStartScroll - delta;
  });

  window.addEventListener("mouseup", () => {
    if (!isDragging) return;
    isDragging = false;
    wrap.classList.remove("is-dragging");
  });

  document.querySelectorAll(".rule-cell").forEach((cell) => {
    cell.addEventListener("click", () => {
      cell.classList.toggle("expanded");
    });
  });

  const setFullscreenLabel = () => {
    if (!fullscreenBtn) return;
    const inNativeFullscreen = document.fullscreenElement === tableCard;
    const inManualFullscreen = tableCard?.classList.contains("manual-fullscreen");
    fullscreenBtn.textContent = inNativeFullscreen || inManualFullscreen ? "Свернуть таблицу" : "Развернуть таблицу";
  };

  if (fullscreenBtn && tableCard) {
    fullscreenBtn.addEventListener("click", async () => {
      const inNativeFullscreen = document.fullscreenElement === tableCard;
      if (inNativeFullscreen) {
        await document.exitFullscreen();
        setFullscreenLabel();
        updateOverflow();
        return;
      }

      if (tableCard.requestFullscreen) {
        await tableCard.requestFullscreen();
      } else {
        tableCard.classList.toggle("manual-fullscreen");
      }
      setFullscreenLabel();
      window.setTimeout(updateOverflow, 100);
    });
  }

  document.addEventListener("fullscreenchange", () => {
    setFullscreenLabel();
    updateOverflow();
  });

  if ("ResizeObserver" in window) {
    const observer = new ResizeObserver(() => updateOverflow());
    observer.observe(wrap);
    observer.observe(table);
  }

  window.addEventListener("resize", updateOverflow);
  setFullscreenLabel();
  updateOverflow();
}

function initEquityChart() {
  const canvas = document.getElementById("equityChart");
  const payloadNode = document.getElementById("equityPayload");
  if (!canvas || !payloadNode) return;

  let payload = { points: [] };
  try {
    payload = JSON.parse(payloadNode.textContent || "{}");
  } catch (error) {
    payload = { points: [] };
  }

  const points = Array.isArray(payload.points) ? payload.points : [];
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const render = () => {
    const parentWidth = canvas.parentElement?.clientWidth || 900;
    const width = Math.max(320, parentWidth);
    const height = 320;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (!points.length) {
      ctx.fillStyle = "#9ca3af";
      ctx.font = "14px Inter, Arial, sans-serif";
      ctx.fillText("Недостаточно данных для графика.", 20, 30);
      return;
    }

    const values = points.map((item) => parseMetric(item.value) ?? 0);
    const minVal = Math.min(...values, 0);
    const maxVal = Math.max(...values, 0);
    const safeMin = minVal === maxVal ? minVal - 1 : minVal;
    const safeMax = minVal === maxVal ? maxVal + 1 : maxVal;

    const padding = { top: 18, right: 22, bottom: 32, left: 56 };
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;

    const xOf = (idx) => {
      if (points.length <= 1) return padding.left;
      return padding.left + (idx / (points.length - 1)) * innerW;
    };
    const yOf = (val) => {
      const ratio = (val - safeMin) / (safeMax - safeMin);
      return padding.top + innerH - ratio * innerH;
    };

    ctx.strokeStyle = "rgba(148, 163, 184, 0.25)";
    ctx.lineWidth = 1;
    for (let step = 0; step <= 4; step += 1) {
      const y = padding.top + (innerH / 4) * step;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(width - padding.right, y);
      ctx.stroke();
    }

    const zeroY = yOf(0);
    ctx.strokeStyle = "rgba(59, 130, 246, 0.45)";
    ctx.beginPath();
    ctx.moveTo(padding.left, zeroY);
    ctx.lineTo(width - padding.right, zeroY);
    ctx.stroke();

    ctx.strokeStyle = "#22c55e";
    ctx.lineWidth = 2;
    ctx.beginPath();
    values.forEach((value, index) => {
      const x = xOf(index);
      const y = yOf(value);
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    const lastIndex = values.length - 1;
    const lastX = xOf(lastIndex);
    const lastY = yOf(values[lastIndex]);
    ctx.fillStyle = "#22c55e";
    ctx.beginPath();
    ctx.arc(lastX, lastY, 3.5, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = "#9ca3af";
    ctx.font = "12px Inter, Arial, sans-serif";
    ctx.fillText(String(points[0].label || ""), padding.left, height - 10);
    const lastLabel = String(points[lastIndex].label || "");
    const labelWidth = ctx.measureText(lastLabel).width;
    ctx.fillText(lastLabel, width - padding.right - labelWidth, height - 10);

    ctx.fillStyle = "#e5e7eb";
    ctx.font = "12px Inter, Arial, sans-serif";
    ctx.fillText(`Баланс: ${values[lastIndex].toFixed(2)} units`, padding.left, 14);
  };

  render();
  window.addEventListener("resize", render);
}

function initJobWaitPage() {
  const root = document.querySelector('[data-job-wait="true"]');
  if (!root) return;

  const statusUrl = root.dataset.statusUrl;
  const fallbackResultUrl = root.dataset.fallbackResultUrl;
  const stageText = document.getElementById("jobStageText");
  const progressText = document.getElementById("jobProgressText");
  const progressTrack = document.getElementById("jobProgressTrack");
  const progressFill = document.getElementById("jobProgressFill");
  const messageText = document.getElementById("jobMessageText");
  const errorBox = document.getElementById("jobErrorBox");
  const steps = Array.from(document.querySelectorAll(".wait-step"));
  let stopped = false;

  const setSteps = (stageId, done, error) => {
    const currentIndex = steps.findIndex((step) => step.dataset.stepId === stageId);
    steps.forEach((step, index) => {
      step.classList.remove("is-active", "is-done", "is-error");
      if (error && index === currentIndex) {
        step.classList.add("is-error");
      } else if (currentIndex >= 0 && (index < currentIndex || (done && index <= currentIndex))) {
        step.classList.add("is-done");
      }
      if (!done && !error && index === currentIndex) {
        step.classList.add("is-active");
      }
    });
  };

  const applyStatus = (status) => {
    const progress = Math.max(0, Math.min(100, Number(status.progress || 0)));
    const stageId = String(status.stage || "");
    const done = Boolean(status.done);
    const error = String(status.error || "");

    setSteps(stageId, done, error);
    const activeStep = steps.find((step) => step.dataset.stepId === stageId);
    stageText.textContent = activeStep?.querySelector(".wait-step-title")?.textContent || stageId || "Выполняется";
    progressText.textContent = `${Math.round(progress)}%`;
    progressFill.style.width = `${progress}%`;
    progressTrack?.setAttribute("aria-valuenow", String(Math.round(progress)));
    messageText.textContent = status.message || "";

    if (error) {
      errorBox.classList.remove("hidden");
      errorBox.textContent = error;
      stopped = true;
      return;
    }

    if (done) {
      stopped = true;
      const resultUrl = status.result_url || fallbackResultUrl;
      if (resultUrl) {
        window.setTimeout(() => {
          window.location.assign(resultUrl);
        }, 600);
      }
    }
  };

  const poll = async () => {
    if (stopped) return;
    try {
      const response = await fetch(statusUrl, { cache: "no-store" });
      const status = await response.json();
      applyStatus(status);
    } catch (error) {
      messageText.textContent = "Не удалось получить статус. Повторяю запрос...";
    }
    if (!stopped) {
      window.setTimeout(poll, 1000);
    }
  };

  poll();
}

function initHistoryTableSearch() {
  const table = document.getElementById("historyTable");
  if (!table) return;

  const search = document.getElementById("historySearch");
  const counter = document.getElementById("historyVisibleCounter");
  const tbody = table.querySelector("tbody");
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll("tr"));

  const filterRows = () => {
    const term = (search?.value || "").toLowerCase().trim();
    let visible = 0;

    rows.forEach((row) => {
      const text = row.innerText.toLowerCase();
      const show = !term || text.includes(term);
      row.style.display = show ? "" : "none";
      if (show) visible += 1;
    });

    if (counter) {
      counter.textContent = `Показано: ${visible} из ${rows.length}`;
    }
  };

  if (search) {
    search.addEventListener("input", filterRows);
  }
  filterRows();
}

document.addEventListener("DOMContentLoaded", () => {
  initSortableTable();
  initResultsTableUX();
  initEquityChart();
  initJobWaitPage();
  initHistoryTableSearch();
});
