import { CONFIG, COLUMN_CONFIG } from './config.js';
import { parseCSV } from './csvParser.js';
import { t } from './i18n.js';
import { getUiSettings } from './settings.js';
import { getSchoolColor, getSchoolColorOrNull } from './schoolColors.js';
import { createSchoolFilter } from './schoolFilter.js';
import type { SchoolFilter, SchoolOption } from './schoolFilter.js';
import { getSchoolDisplayName } from './schoolNames.js';
import type { StudentData, ColumnVisibility, SortState, Metadata, School } from './types.js';

// 状态管理
let allData: StudentData[] = [];
let filteredData: StudentData[] = [];
let currentSort: SortState = { column: null, direction: 'asc' };
let columnVisibility: ColumnVisibility = {} as ColumnVisibility;
let schoolsMap: Map<number, School> = new Map();
let schoolFilter: SchoolFilter | null = null;  // 学校多选筛选组件

// DOM 元素引用
const elements = {
  searchInput: document.getElementById('searchInput') as HTMLInputElement,
  schoolFilterBtn: document.getElementById('schoolFilterBtn') as HTMLButtonElement,
  schoolDropdown: document.getElementById('schoolDropdown') as HTMLDivElement,
  schoolFilterCount: document.getElementById('schoolFilterCount') as HTMLSpanElement,
  tableContainer: document.getElementById('tableContainer') as HTMLDivElement,
  columnDropdown: document.getElementById('columnDropdown') as HTMLDivElement,
  totalCount: document.getElementById('totalCount') as HTMLDivElement,
  studentCount: document.getElementById('studentCount') as HTMLDivElement,
  updateTime: document.getElementById('updateTime') as HTMLDivElement,
  dataFileLink: document.getElementById('dataFileLink') as HTMLAnchorElement,
};

// 列显示配置的 localStorage key
const COLUMN_VISIBILITY_KEY = 'ba-column-visibility';

// 从 localStorage 读取已保存的列显示配置（含废弃 key，由调用方过滤）
function loadSavedColumnVisibility(): Partial<ColumnVisibility> | null {
  try {
    const raw = localStorage.getItem(COLUMN_VISIBILITY_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return null;
    return parsed as Partial<ColumnVisibility>;
  } catch {
    return null; // 解析失败时静默退回默认配置
  }
}

// 保存列显示配置到 localStorage（不可用时静默跳过）
function saveColumnVisibility(): void {
  try {
    localStorage.setItem(COLUMN_VISIBILITY_KEY, JSON.stringify(columnVisibility));
  } catch {
    // localStorage 不可用（如隐私模式），忽略
  }
}

// 初始化列显示状态：默认值为基底，再应用保存值（仅限仍存在的列 key）
function initColumnVisibility(): void {
  COLUMN_CONFIG.forEach(col => {
    columnVisibility[col.key] = col.defaultVisible;
  });
  const saved = loadSavedColumnVisibility();
  if (!saved) return;
  COLUMN_CONFIG.forEach(col => {
    if (typeof saved[col.key] === 'boolean') {
      columnVisibility[col.key] = saved[col.key] as boolean;
    }
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
          ${t(col.label)}
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
    } else if (col.key === 'spine_id' && getUiSettings().spineLink) {
      const url = `https://api.kivo.wiki/api/v1/data/spines/${value}`;
      return `<td data-col="${col.key}"><a href="${url}" target="_blank" rel="noopener">${value}</a></td>`;
    } else if (col.key === 'name') {
      return `<td data-col="${col.key}"><strong>${value}</strong></td>`;
    } else if (col.key === 'skin_kivo') {
      return `<td data-col="${col.key}">${value || '-'}</td>`;
    } else if (col.key === 'school_name') {
      // 渲染学校 logo + 名称（名称按当前界面语言取名，见 schoolNames.ts），
      // 标签底色/文字色按学校 id 着色（见 schoolColors.ts）
      const schoolId = parseInt(row.school_id);
      const school = schoolsMap.get(schoolId);
      const displayName = getSchoolDisplayName(school);
      const color = getSchoolColor(schoolId);
      const colorStyle = ` style="background:${color.bg};color:${color.text}"`;
      if (school && school.logo) {
        return `<td data-col="${col.key}"><span class="school-tag"${colorStyle}><img src="https:${school.logo}" class="school-logo" alt="">${displayName}</span></td>`;
      }
      return `<td data-col="${col.key}"><span class="school-tag"${colorStyle}>${displayName}</span></td>`;
    } else {
      return `<td data-col="${col.key}">${value}</td>`;
    }
  }).join('');
}

// 渲染表格
function renderTable(data: StudentData[]): void {
  if (data.length === 0) {
    elements.tableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">🔍</div><p>${t('common.noResults')}</p><p class="empty-state-hint">${t('common.noResultsHint')}</p></div>`;
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

    // 学校筛选（多选，来自共享组件）
    const selected = schoolFilter?.selected;
    const matchSchool = !selected || selected.size === 0 || selected.has(row.school_id);

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
      ${t(col.label)}
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
  saveColumnVisibility();
  renderTable(filteredData);
}

// 显示/隐藏下拉菜单
function toggleColumnDropdown(): void {
  const btn = document.querySelector('.column-toggle-btn') as HTMLElement;
  const dropdown = elements.columnDropdown;
  const isShowing = dropdown.classList.contains('show');

  if (!isShowing) {
    // 计算按钮位置
    const rect = btn.getBoundingClientRect();
    dropdown.style.top = `${rect.bottom + 5}px`;
    dropdown.style.right = `${window.innerWidth - rect.right}px`;
  }

  dropdown.classList.toggle('show');
}

// 点击外部关闭列下拉菜单
function onDocumentClick(event: Event): void {
  const target = event.target as HTMLElement;
  const columnBtn = document.querySelector('.column-toggle-btn');

  if (!elements.columnDropdown.contains(target) && !columnBtn?.contains(target)) {
    elements.columnDropdown.classList.remove('show');
  }
}

// 更新统计
function updateStats(data: StudentData[]): void {
  elements.totalCount.textContent = data.length.toString();
  // 使用 student_id 统计唯一学生数
  const uniqueStudents = new Set(data.map(d => d.student_id)).size;
  elements.studentCount.textContent = uniqueStudents.toString();
}

// 构建学校过滤器选项：以 schools.json 为准按 id 排序（与 kivonavi 页一致）
function buildSchoolOptions(): SchoolOption[] {
  return Array.from(schoolsMap.values())
    .sort((a, b) => a.id - b.id)
    .map(s => ({ school: s, color: getSchoolColorOrNull(s.id) }));
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

// 更新页面底部的数据文件链接
function updateReleaseLink(): void {
  if (elements.dataFileLink) {
    elements.dataFileLink.href = './data/';
    elements.dataFileLink.textContent = t('footer.downloadData');
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
    // 创建学校多选筛选组件
    schoolFilter = createSchoolFilter(
      { btn: elements.schoolFilterBtn, dropdown: elements.schoolDropdown, count: elements.schoolFilterCount },
      buildSchoolOptions(),
      applyFilters
    );
    renderTable(allData);

    if (metadata && metadata.updateDate) {
      elements.updateTime.textContent = metadata.updateDate;
    } else {
      // debug 为伪语言，日期回落 zh-CN 格式
      const dateLocale = getUiSettings().lang === 'debug' ? 'zh-CN' : getUiSettings().lang;
      elements.updateTime.textContent = new Date().toLocaleDateString(dateLocale);
    }
    updateReleaseLink();
  } catch (error) {
    console.error('加载数据失败:', error);
    elements.tableContainer.innerHTML = `
      <div class="error">
        <p>${t('common.loadFailed')}</p>
        <p style="font-size: 12px; margin-top: 10px;">${error instanceof Error ? error.message : t('common.unknownError')}</p>
        <p style="font-size: 12px; margin-top: 10px;">
          ${t('common.loadFailedHint')}
        </p>
      </div>
    `;
  }
}

// 初始化文件ID表格视图（懒加载：首次切换到该视图时调用）
export function initTableView(): void {
  // 事件监听
  elements.searchInput.addEventListener('input', applyFilters);

  // 界面设置变更（如 spine 链接开关）时重渲染表格
  window.addEventListener('ui-settings-changed', () => {
    renderTable(filteredData);
  });

  // 语言切换时重渲染表格与列下拉（列标题、空态、错误文案等）
  window.addEventListener('lang-changed', () => {
    renderTable(filteredData);
    generateColumnDropdown();
  });

  // 绑定列切换按钮
  document.querySelector('.column-toggle-btn')?.addEventListener('click', toggleColumnDropdown);

  // 点击外部关闭下拉菜单（学校下拉由共享组件自行处理）
  document.addEventListener('click', onDocumentClick);

  // 加载数据
  loadData();
}
