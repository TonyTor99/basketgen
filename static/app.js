function toggleFeatures(checked) {
  document.querySelectorAll('input[name="features"]').forEach((el) => {
    el.checked = checked;
  });
}

function getCellValue(row, index) {
  return row.children[index]?.innerText?.trim() ?? '';
}

function compareValues(a, b, type, asc) {
  if (type === 'num') {
    const numA = parseFloat(String(a).replace(',', '.'));
    const numB = parseFloat(String(b).replace(',', '.'));
    const safeA = Number.isNaN(numA) ? -Infinity : numA;
    const safeB = Number.isNaN(numB) ? -Infinity : numB;
    return asc ? safeA - safeB : safeB - safeA;
  }
  return asc ? String(a).localeCompare(String(b), 'ru') : String(b).localeCompare(String(a), 'ru');
}

function initSortableTable() {
  const table = document.getElementById('sortableTable');
  if (!table) return;
  const headers = Array.from(table.querySelectorAll('th'));
  const tbody = table.querySelector('tbody');
  const search = document.getElementById('tableSearch');
  let currentSort = { index: 0, asc: false };

  const filterRows = () => {
    const term = (search?.value || '').toLowerCase().trim();
    Array.from(tbody.querySelectorAll('tr')).forEach((row) => {
      const text = row.innerText.toLowerCase();
      row.style.display = !term || text.includes(term) ? '' : 'none';
    });
  };

  headers.forEach((header, index) => {
    header.addEventListener('click', () => {
      const asc = currentSort.index === index ? !currentSort.asc : true;
      currentSort = { index, asc };
      const type = header.dataset.type || 'text';
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort((rowA, rowB) => compareValues(getCellValue(rowA, index), getCellValue(rowB, index), type, asc));
      rows.forEach((row) => tbody.appendChild(row));
      filterRows();
    });
  });

  if (search) {
    search.addEventListener('input', filterRows);
  }
}

function initJobWaitPage() {
  const root = document.querySelector('[data-job-wait="true"]');
  if (!root) return;

  const statusUrl = root.dataset.statusUrl;
  const fallbackResultUrl = root.dataset.fallbackResultUrl;
  const stageText = document.getElementById('jobStageText');
  const progressText = document.getElementById('jobProgressText');
  const progressTrack = document.getElementById('jobProgressTrack');
  const progressFill = document.getElementById('jobProgressFill');
  const messageText = document.getElementById('jobMessageText');
  const errorBox = document.getElementById('jobErrorBox');
  const steps = Array.from(document.querySelectorAll('.wait-step'));

  let stopped = false;

  const setSteps = (stageId, done, error) => {
    const currentIndex = steps.findIndex((step) => step.dataset.stepId === stageId);
    steps.forEach((step, index) => {
      step.classList.remove('is-active', 'is-done', 'is-error');
      if (error && index === currentIndex) {
        step.classList.add('is-error');
      } else if (currentIndex >= 0 && (index < currentIndex || (done && index <= currentIndex))) {
        step.classList.add('is-done');
      }
      if (!done && !error && index === currentIndex) {
        step.classList.add('is-active');
      }
    });
  };

  const applyStatus = (status) => {
    const progress = Math.max(0, Math.min(100, Number(status.progress || 0)));
    const stageId = String(status.stage || '');
    const done = Boolean(status.done);
    const error = String(status.error || '');

    setSteps(stageId, done, error);

    const activeStep = steps.find((step) => step.dataset.stepId === stageId);
    stageText.textContent = activeStep?.querySelector('.wait-step-title')?.textContent || stageId || 'Выполняется';
    progressText.textContent = `${Math.round(progress)}%`;
    progressFill.style.width = `${progress}%`;
    progressTrack?.setAttribute('aria-valuenow', String(Math.round(progress)));
    messageText.textContent = status.message || '';

    if (error) {
      errorBox.classList.remove('hidden');
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
      const response = await fetch(statusUrl, { cache: 'no-store' });
      const status = await response.json();
      applyStatus(status);
    } catch (err) {
      messageText.textContent = 'Не удалось получить статус. Повторяю запрос...';
    }
    if (!stopped) {
      window.setTimeout(poll, 1000);
    }
  };

  poll();
}

document.addEventListener('DOMContentLoaded', () => {
  initSortableTable();
  initJobWaitPage();
});
