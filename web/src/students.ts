import './style.css';
import { CONFIG } from './config.js';
import type { Student, KivoPage, School, ExpandState, Metadata } from './types.js';

// 状态管理
let allStudents: Student[] = [];
let filteredStudents: Student[] = [];
let schoolsMap: Map<number, School> = new Map();
let expandState: ExpandState = {};

// DOM 元素引用
const elements = {
  searchInput: document.getElementById('searchInput') as HTMLInputElement,
  schoolFilter: document.getElementById('schoolFilter') as HTMLSelectElement,
  installFilter: document.getElementById('installFilter') as HTMLSelectElement,
  studentsContainer: document.getElementById('studentsContainer') as HTMLDivElement,
  expandAllBtn: document.getElementById('expandAllBtn') as HTMLButtonElement,
  collapseAllBtn: document.getElementById('collapseAllBtn') as HTMLButtonElement,
  totalCount: document.getElementById('totalCount') as HTMLDivElement,
  pageCount: document.getElementById('pageCount') as HTMLDivElement,
  updateTime: document.getElementById('updateTime') as HTMLDivElement,
};

// 获取学校名称
function getSchoolName(schoolId: number): string {
  const school = schoolsMap.get(schoolId);
  return school?.name || school?.name_cn || '未知';
}

// 获取学校Logo
function getSchoolLogo(schoolId: number): string | null {
  const school = schoolsMap.get(schoolId);
  return school?.logo || null;
}

// 获取学生头像（使用第一个page的avatar）
function getStudentAvatar(student: Student): string | null {
  const firstPage = student.pages[0];
  if (firstPage?.avatar) {
    return firstPage.avatar.startsWith('//') ? `https:${firstPage.avatar}` : firstPage.avatar;
  }
  return null;
}

// 获取page头像
function getPageAvatar(page: KivoPage): string | null {
  if (page.avatar) {
    return page.avatar.startsWith('//') ? `https:${page.avatar}` : page.avatar;
  }
  return null;
}

// 生成星级字符串
function getRarityStars(rarity: number): string {
  if (rarity <= 0) return '-';
  return '⭐'.repeat(Math.min(rarity, 3));
}

// 生成page名称
function getPageName(page: KivoPage): string {
  if (page.skin_name) return page.skin_name;
  return '默认';
}

// 生成Page HTML
function generatePageHTML(page: KivoPage): string {
  const avatar = getPageAvatar(page);
  const pageName = getPageName(page);
  const kivoUrl = `https://kivo.wiki/data/character/${page.page_id}?mode=appreciation`;
  const rarityStars = getRarityStars(page.rarity);

  const installJpClass = page.is_install ? '' : 'not-installed';
  const installCnClass = page.is_install_cn ? '' : 'not-installed';
  const installGlobalClass = page.is_install_global ? '' : 'not-installed';
  const installJpIcon = page.is_install ? '🇯🇵' : '❌';
  const installCnIcon = page.is_install_cn ? '🇨🇳' : '❌';
  const installGlobalIcon = page.is_install_global ? '🌐' : '❌';

  const spinesHTML = page.spines.map(spineId =>
    `<span class="spine-id">${spineId}</span>`
  ).join('');

  return `
    <div class="page-item">
      <div class="page-header">
        ${avatar ? `<img src="${avatar}" class="page-avatar" alt="" loading="lazy">` : '<div class="page-avatar" style="background:#e2e8f0;"></div>'}
        <div class="page-info">
          <div class="page-name">${pageName}</div>
          <div class="page-id">
            page_id: <a href="${kivoUrl}" target="_blank" rel="noopener" class="page-link">${page.page_id}</a>
          </div>
        </div>
      </div>
      <div class="page-tags">
        ${page.is_npc ? '<span class="tag tag-npc">🚫 NPC</span>' : `
          <span class="tag tag-install-jp ${installJpClass}">${installJpIcon} 日服</span>
          <span class="tag tag-install-global ${installGlobalClass}">${installGlobalIcon} 国际服</span>
          <span class="tag tag-install-cn ${installCnClass}">${installCnIcon} 国服</span>
        `}
        ${page.rarity > 0 ? `<span class="tag tag-rarity">${rarityStars}</span>` : ''}
        ${page.limited ? '<span class="tag tag-limited">🌟 限定</span>' : ''}
      </div>
      <div class="spines-section">
        <div class="spines-label">🎬 Spine (${page.spines.length}个)</div>
        <div class="spines-list">
          ${spinesHTML || '<span style="color:#94a3b8;font-size:0.75rem;">无</span>'}
        </div>
      </div>
    </div>
  `;
}

// 生成学生卡片 HTML
function generateStudentCard(student: Student): string {
  const avatar = getStudentAvatar(student);
  const schoolName = getSchoolName(student.school_id);
  const schoolLogo = getSchoolLogo(student.school_id);
  const isExpanded = expandState[student.id] || false;

  const pagesHTML = student.pages.map(page => generatePageHTML(page)).join('');

  return `
    <div class="student-card ${isExpanded ? 'expanded' : ''}" data-student-id="${student.id}">
      <div class="student-header">
        ${avatar ? `<img src="${avatar}" class="student-avatar" alt="" loading="lazy">` : '<div class="student-avatar" style="background:#e2e8f0;"></div>'}
        <div class="student-info">
          <div class="student-name">${student.name}</div>
          <div class="student-name-sub">${student.name_jp || ''}</div>
          <div class="student-meta">ID: ${student.id} | ${student.pages.length} 个页面</div>
        </div>
        <div class="student-school">
          ${schoolLogo ? `<img src="https:${schoolLogo}" class="student-school-logo" alt="" title="${schoolName}">` : ''}
        </div>
        <span class="expand-icon">▼</span>
      </div>
      <div class="student-content">
        <div class="pages-container">
          <div class="pages-title">📄 Pages (${student.pages.length}个页面)</div>
          ${pagesHTML}
        </div>
      </div>
    </div>
  `;
}

// 渲染学生列表
function renderStudents(students: Student[]): void {
  if (students.length === 0) {
    elements.studentsContainer.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">🔍</div>
        <p>没有找到匹配的学生</p>
        <p style="font-size: 0.85rem; margin-top: 8px; color: #94a3b8;">请尝试调整搜索条件</p>
      </div>
    `;
    return;
  }

  const gridHTML = `
    <div class="students-grid">
      ${students.map(student => generateStudentCard(student)).join('')}
    </div>
  `;

  elements.studentsContainer.innerHTML = gridHTML;

  // 绑定卡片点击事件
  document.querySelectorAll('.student-header').forEach(header => {
    header.addEventListener('click', () => {
      const card = header.closest('.student-card') as HTMLElement;
      const studentId = parseInt(card.dataset.studentId || '0');
      toggleExpand(studentId);
    });
  });
}

// 切换展开状态
function toggleExpand(studentId: number): void {
  expandState[studentId] = !expandState[studentId];
  const card = document.querySelector(`.student-card[data-student-id="${studentId}"]`);
  if (card) {
    card.classList.toggle('expanded', expandState[studentId]);
  }
}

// 展开全部
function expandAll(): void {
  filteredStudents.forEach(student => {
    expandState[student.id] = true;
  });
  renderStudents(filteredStudents);
}

// 折叠全部
function collapseAll(): void {
  expandState = {};
  renderStudents(filteredStudents);
}

// 应用筛选
function applyFilters(): void {
  const searchTerm = elements.searchInput.value.toLowerCase().trim();
  const schoolFilter = elements.schoolFilter.value;
  const installFilter = elements.installFilter.value;

  filteredStudents = allStudents.filter(student => {
    // 搜索过滤
    const matchSearch = !searchTerm ||
      student.name?.toLowerCase().includes(searchTerm) ||
      student.name_cn?.toLowerCase().includes(searchTerm) ||
      student.name_jp?.toLowerCase().includes(searchTerm) ||
      student.name_en?.toLowerCase().includes(searchTerm) ||
      student.name_kr?.toLowerCase().includes(searchTerm) ||
      student.name_tw?.toLowerCase().includes(searchTerm) ||
      student.id.toString().includes(searchTerm);

    // 学校过滤
    const matchSchool = !schoolFilter || student.school_id.toString() === schoolFilter;

    // 实装状态过滤
    let matchInstall = true;
    if (installFilter === 'jp') {
      matchInstall = student.pages.some(p => p.is_install);
    } else if (installFilter === 'cn') {
      matchInstall = student.pages.some(p => p.is_install_cn);
    } else if (installFilter === 'global') {
      matchInstall = student.pages.some(p => p.is_install_global);
    } else if (installFilter === 'npc') {
      matchInstall = student.pages.some(p => p.is_npc);
    }

    return matchSearch && matchSchool && matchInstall;
  });

  renderStudents(filteredStudents);
  updateStats(filteredStudents);
}

// 更新统计
function updateStats(students: Student[]): void {
  const totalPages = students.reduce((sum, s) => sum + s.pages.length, 0);
  elements.totalCount.textContent = students.length.toString();
  elements.pageCount.textContent = totalPages.toString();
}

// 填充学校筛选器
function populateSchoolFilter(): void {
  const schools = Array.from(schoolsMap.values()).sort((a, b) => a.id - b.id);

  schools.forEach(school => {
    const option = document.createElement('option');
    option.value = school.id.toString();
    option.textContent = school.name || school.name_cn || '未知';
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

// 获取学生数据
async function fetchStudents(): Promise<Student[]> {
  try {
    const response = await fetch(CONFIG.studentsUrl);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('获取学生数据失败:', error);
    throw error;
  }
}

// 加载数据
async function loadData(): Promise<void> {
  try {
    const [students, metadata] = await Promise.all([
      fetchStudents(),
      fetchMetadata()
    ]);

    // 加载学校数据
    await fetchSchools();

    allStudents = students;
    filteredStudents = students;

    updateStats(allStudents);
    populateSchoolFilter();
    renderStudents(allStudents);

    if (metadata?.updateDate) {
      elements.updateTime.textContent = metadata.updateDate;
    } else {
      elements.updateTime.textContent = new Date().toLocaleDateString('zh-CN');
    }
  } catch (error) {
    console.error('加载数据失败:', error);
    elements.studentsContainer.innerHTML = `
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
elements.installFilter.addEventListener('change', applyFilters);
elements.expandAllBtn.addEventListener('click', expandAll);
elements.collapseAllBtn.addEventListener('click', collapseAll);

// 初始化
loadData();
