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

document.addEventListener('DOMContentLoaded', initSortableTable);
