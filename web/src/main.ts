import './style.css';
import { CONFIG, COLUMN_CONFIG } from './config.js';
import { parseCSV } from './csvParser.js';
import type { StudentData, ColumnVisibility, SortState, Metadata, School } from './types.js';

// 状态管理
let allData: StudentData[] = [];
let filteredData: StudentData[] = [];
let currentSort: SortState = { column: null, direction: 'asc' };
let columnVisibility: ColumnVisibility = {} as ColumnVisibility;
let schoolsMap: Map<number, School> = new Map();

// DOM 元素引用
const elements = {
  searchInput: document.getElementById('searchInput') as HTMLInputElement,
  schoolFilter: document.getElementById('schoolFilter') as HTMLSelectElement,
  tableContainer: document.getElementById('tableContainer') as HTMLDivElement,
  columnDropdown: document.getElementById('columnDropdown') as HTMLDivElement,
  totalCount: document.getElementById('totalCount') as HTMLDivElement,
  studentCount: document.getElementById('studentCount') as HTMLDivElement,
  updateTime: document.getElementById('updateTime') as HTMLDivElement,
  dataFileLink: document.getElementById('dataFileLink') as HTMLAnchorElement,
};

// 初始化列显示状态
function initColumnVisibility(): void {
  COLUMN_CONFIG.forEach(col => {
    columnVisibility[col.key] = col.defaultVisible;
  });
}

// 生成表头 HTML
function generateHeaderHTML(): string {
  return COLUMN_CONFIG.map(col => {
    if (!columnVisibility[col.key]) return '';
    const sortClass = currentSort.column === col.key ? `sort-${currentSort.direction}` : '';
    const arrow = currentSort.column === col.key
      ? (currentSort.direction === 'asc' ? '▲' : '▼')
      : '⇅';
    return `
      <th data-col="${col.key}">
        <div class="th-content ${sortClass}" data-sort="${col.key}">
          ${col.label}
          <span class="sort-arrow">${arrow}</span>
        </div>
      </th>
    `;
  }).join('');
}

// 生成数据行 HTML
function generateRowHTML(row: StudentData): string {
  return COLUMN_CONFIG.map(col => {
    if (!columnVisibility[col.key]) return '';
    const value = row[col.key] || '';

    // 特殊处理某些列
    if (col.key === 'file_id') {
      return `<td data-col="${col.key}"><code>${value}</code></td>`;
    } else if (col.key === 'student_id') {
      return `<td data-col="${col.key}">${value}</td>`;
    } else if (col.key === 'page_id') {
      const url = `https://kivo.wiki/data/character/${value}?mode=appreciation`;
      return `<td data-col="${col.key}"><a href="${url}" target="_blank" rel="noopener">${value}</a></td>`;
    } else if (col.key === 'name') {
      return `<td data-col="${col.key}"><strong>${value}</strong></td>`;
    } else if (col.key === 'skin_name') {
      return `<td data-col="${col.key}">${value || '-'}</td>`;
    } else if (col.key === 'school_name') {
      // 渲染学校 logo + 名称
      const schoolId = parseInt(row.school_id);
      const school = schoolsMap.get(schoolId);
      if (school && school.logo) {
        return `<td data-col="${col.key}"><span class="school-tag"><img src="https:${school.logo}" class="school-logo" alt="">${value}</span></td>`;
      }
      return `<td data-col="${col.key}"><span class="school-tag">${value}</span></td>`;
    } else {
      return `<td data-col="${col.key}">${value}</td>`;
    }
  }).join('');
}

// 渲染表格
function renderTable(data: StudentData[]): void {
  if (data.length === 0) {
    elements.tableContainer.innerHTML = '<div class="empty"><p>没有找到匹配的数据</p></div>';
    return;
  }

  const html = `
    <table>
      <thead>
        <tr>
          ${generateHeaderHTML()}
        </tr>
      </thead>
      <tbody>
        ${data.map(row => `<tr>${generateRowHTML(row)}</tr>`).join('')}
      </tbody>
    </table>
  `;

  elements.tableContainer.innerHTML = html;

  // 绑定排序事件
  document.querySelectorAll('.th-content').forEach(th => {
    th.addEventListener('click', () => {
      const column = th.getAttribute('data-sort') as keyof StudentData;
      sortBy(column);
    });
  });
}

// 排序功能
function sortBy(column: keyof StudentData): void {
  if (currentSort.column === column) {
    // 切换排序方向
    currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
  } else {
    currentSort.column = column;
    currentSort.direction = 'asc';
  }

  // 排序数据
  filteredData.sort((a, b) => {
    let valA = a[column] || '';
    let valB = b[column] || '';

    // 尝试数字排序
    const numA = parseFloat(valA);
    const numB = parseFloat(valB);

    if (!isNaN(numA) && !isNaN(numB) && valA !== '' && valB !== '') {
      return currentSort.direction === 'asc' ? numA - numB : numB - numA;
    }

    // 字符串排序
    valA = valA.toString().toLowerCase();
    valB = valB.toString().toLowerCase();

    if (valA < valB) return currentSort.direction === 'asc' ? -1 : 1;
    if (valA > valB) return currentSort.direction === 'asc' ? 1 : -1;
    return 0;
  });

  renderTable(filteredData);
}

// 应用所有筛选条件
function applyFilters(): void {
  const searchTerm = elements.searchInput.value.toLowerCase();
  const schoolFilter = elements.schoolFilter.value;

  filteredData = allData.filter(row => {
    // 全局搜索
    const matchSearch = !searchTerm ||
      row.name?.toLowerCase().includes(searchTerm) ||
      row.full_name?.toLowerCase().includes(searchTerm) ||
      row.name_cn?.toLowerCase().includes(searchTerm) ||
      row.name_jp?.toLowerCase().includes(searchTerm) ||
      row.name_tw?.toLowerCase().includes(searchTerm) ||
      row.name_en?.toLowerCase().includes(searchTerm) ||
      row.name_kr?.toLowerCase().includes(searchTerm) ||
      row.file_id?.toLowerCase().includes(searchTerm);

    // 学校筛选
    const matchSchool = !schoolFilter || row.school_name === schoolFilter;

    return matchSearch && matchSchool;
  });

  // 如果有排序，重新应用
  if (currentSort.column) {
    sortBy(currentSort.column);
  } else {
    renderTable(filteredData);
  }
  updateStats(filteredData);
}

// 生成列控制下拉菜单
function generateColumnDropdown(): void {
  elements.columnDropdown.innerHTML = COLUMN_CONFIG.map(col => `
    <label class="column-dropdown-item">
      <input type="checkbox" 
             ${columnVisibility[col.key] ? 'checked' : ''} 
             data-column="${col.key}">
      ${col.label}
    </label>
  `).join('');

  // 绑定复选框事件
  elements.columnDropdown.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
    checkbox.addEventListener('change', (e) => {
      const target = e.target as HTMLInputElement;
      const column = target.getAttribute('data-column') as keyof StudentData;
      toggleColumn(column, target.checked);
    });
  });
}

// 切换列显示/隐藏
function toggleColumn(column: keyof StudentData, visible: boolean): void {
  columnVisibility[column] = visible;
  renderTable(filteredData);
}

// 显示/隐藏下拉菜单
function toggleColumnDropdown(): void {
  elements.columnDropdown.classList.toggle('show');
}

// 点击外部关闭下拉菜单
document.addEventListener('click', (event) => {
  const target = event.target as HTMLElement;
  const btn = document.querySelector('.column-toggle-btn');
  if (!elements.columnDropdown.contains(target) && !btn?.contains(target)) {
    elements.columnDropdown.classList.remove('show');
  }
});

// 更新统计
function updateStats(data: StudentData[]): void {
  elements.totalCount.textContent = data.length.toString();
  // 使用 student_id 统计唯一学生数
  const uniqueStudents = new Set(data.map(d => d.student_id)).size;
  elements.studentCount.textContent = uniqueStudents.toString();
}

// 填充学校筛选器
function populateSchoolFilter(data: StudentData[]): void {
  const schools = [...new Set(data.map(d => d.school_name).filter(Boolean))].sort();

  schools.forEach(school => {
    const option = document.createElement('option');
    option.value = school;
    option.textContent = school;
    elements.schoolFilter.appendChild(option);
  });
}

// 获取元数据
async function fetchMetadata(): Promise<Metadata | null> {
  try {
    const response = await fetch(CONFIG.metadataUrl);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('获取元数据失败:', error);
    return null;
  }
}

// 获取学校数据
async function fetchSchools(): Promise<void> {
  try {
    const response = await fetch(CONFIG.schoolsUrl);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const schools: School[] = await response.json();
    schoolsMap = new Map(schools.map(s => [s.id, s]));
  } catch (error) {
    console.error('获取学校数据失败:', error);
  }
}

// 更新页面底部的 Release 链接
function updateReleaseLink(metadata: Metadata | null): void {
  if (elements.dataFileLink && metadata) {
    elements.dataFileLink.href = metadata.releaseUrl;
    elements.dataFileLink.textContent = '下载数据';
  }
}

// 加载数据
async function loadData(): Promise<void> {
  try {
    initColumnVisibility();
    generateColumnDropdown();

    const [csvResponse, metadata] = await Promise.all([
      fetch(CONFIG.csvUrl),
      fetchMetadata()
    ]);

    // 并行加载学校数据
    await fetchSchools();

    if (!csvResponse.ok) {
      throw new Error(`HTTP error! status: ${csvResponse.status}`);
    }

    const text = await csvResponse.text();
    allData = parseCSV<StudentData>(text);
    filteredData = allData;

    updateStats(allData);
    populateSchoolFilter(allData);
    renderTable(allData);

    if (metadata && metadata.updateDate) {
      elements.updateTime.textContent = metadata.updateDate;
      updateReleaseLink(metadata);
    } else {
      elements.updateTime.textContent = new Date().toLocaleDateString('zh-CN');
    }
  } catch (error) {
    console.error('加载数据失败:', error);
    elements.tableContainer.innerHTML = `
      <div class="error">
        <p>❌ 加载数据失败</p>
        <p style="font-size: 12px; margin-top: 10px;">${error instanceof Error ? error.message : '未知错误'}</p>
        <p style="font-size: 12px; margin-top: 10px;">
          数据文件可能尚未生成，请稍后再试
        </p>
      </div>
    `;
  }
}

// 事件监听
elements.searchInput.addEventListener('input', applyFilters);
elements.schoolFilter.addEventListener('change', applyFilters);

// 绑定列切换按钮
document.querySelector('.column-toggle-btn')?.addEventListener('click', toggleColumnDropdown);

// 初始化
loadData();
