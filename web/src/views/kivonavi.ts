import { CONFIG } from '../core/config.js';
import { t } from '../core/i18n.js';
import { matchesSearch } from '../utils/search.js';
import { getUiSettings } from '../core/settings.js';
import { getSchoolColor, getSchoolColorOrNull } from '../utils/schoolColors.js';
import { createSchoolFilter } from '../ui/schoolFilter.js';
import type { SchoolFilter } from '../ui/schoolFilter.js';
import { getSchoolDisplayName } from '../utils/schoolNames.js';
import { joinName } from '../utils/nameFormat.js';
import type { Student, KivoPage, School, ExpandState, Metadata } from '../core/types.js';

// 状态管理
let allStudents: Student[] = [];
let filteredStudents: Student[] = [];
let schoolsMap: Map<number, School> = new Map();
let expandState: ExpandState = {};
let schoolFilter: SchoolFilter | null = null;  // 学校多选筛选组件

// DOM 元素引用
const elements = {
  searchInput: document.getElementById('kivoSearchInput') as HTMLInputElement,
  schoolFilterBtn: document.getElementById('kivoSchoolFilterBtn') as HTMLButtonElement,
  schoolDropdown: document.getElementById('kivoSchoolDropdown') as HTMLDivElement,
  schoolFilterCount: document.getElementById('kivoSchoolFilterCount') as HTMLSpanElement,
  installFilter: document.getElementById('installFilter') as HTMLSelectElement,
  studentsContainer: document.getElementById('studentsContainer') as HTMLDivElement,
  expandAllBtn: document.getElementById('expandAllBtn') as HTMLButtonElement,
  collapseAllBtn: document.getElementById('collapseAllBtn') as HTMLButtonElement,
  totalCount: document.getElementById('kivoTotalCount') as HTMLDivElement,
  pageCount: document.getElementById('kivoPageCount') as HTMLDivElement,
  updateTime: document.getElementById('kivoUpdateTime') as HTMLDivElement,
};

// 获取学校名称（按当前界面语言取名，见 schoolNames.ts）
function getSchoolName(schoolId: number): string {
  return getSchoolDisplayName(schoolsMap.get(schoolId));
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
  if (page.skin) return page.skin;
  return t('kivo.defaultPage');
}

// 生成单个 page 行（皮肤头像 + 皮肤名胶囊 + 实装状态 + 星级 + spine 芯片）
function generatePageHTML(page: KivoPage): string {
  const avatar = getPageAvatar(page);
  const pageName = getPageName(page);
  const kivoUrl = `https://kivo.wiki/data/character/${page.page_id}?mode=appreciation`;
  const spineUrl = (spineId: number) => `https://api.kivo.wiki/api/v1/data/spines/${spineId}`;
  const rarityStars = getRarityStars(page.rarity);

  // 实装状态：未实装的服统一置灰（.not-installed）；胶囊内文字包一层 span 便于反向斜切
  const installTags = page.is_npc
    ? `<span class="tag tag-npc"><span>🚫 ${t('kivo.installNpc')}</span></span>`
    : `
      <span class="tag tag-install-jp ${page.is_install ? '' : 'not-installed'}"><span>${page.is_install ? '🇯🇵' : '❌'} ${t('kivo.installJp')}</span></span>
      <span class="tag tag-install-global ${page.is_install_global ? '' : 'not-installed'}"><span>${page.is_install_global ? '🌐' : '❌'} ${t('kivo.installGlobal')}</span></span>
      <span class="tag tag-install-cn ${page.is_install_cn ? '' : 'not-installed'}"><span>${page.is_install_cn ? '🇨🇳' : '❌'} ${t('kivo.installCn')}</span></span>
    `;

  const spinesHTML = page.spines.map(spineId =>
    getUiSettings().spineLink
      ? `<a href="${spineUrl(spineId)}" target="_blank" rel="noopener" class="spine-id">${spineId}</a>`
      : `<span class="spine-id">${spineId}</span>`
  ).join('');

  return `
    <div class="page-row">
      ${avatar ? `<img src="${avatar}" class="page-avatar" alt="" loading="lazy">` : '<div class="page-avatar page-avatar-empty"></div>'}
      <a href="${kivoUrl}" target="_blank" rel="noopener" class="page-chip" data-pid="${page.page_id}" title="page_id: ${page.page_id}"><span>${pageName}</span></a>
      <div class="page-tags">
        ${installTags}
        ${page.rarity > 0 ? `<span class="tag tag-rarity"><span>${rarityStars}</span></span>` : ''}
        ${page.limited ? `<span class="tag tag-limited"><span>${t('kivo.limited')}</span></span>` : ''}
      </div>
      <div class="spines-list">
        ${spinesHTML || `<span class="spine-empty">${t('kivo.none')}</span>`}
      </div>
    </div>
  `;
}

// 生成语言姓名胶囊（仅显示已填写的语言，姓与名按该语言规则拼接）
function generateLangPills(student: Student): string {
  const langs: { code: string; lang: string; family: string; given: string }[] = [
    { code: 'JP', lang: 'jp', family: student.family_name_jp, given: student.given_name_jp },
    { code: 'EN', lang: 'en', family: student.family_name_en, given: student.given_name_en },
    { code: 'KR', lang: 'kr', family: student.family_name_kr, given: student.given_name_kr },
    { code: 'TW', lang: 'tw', family: student.family_name_tw, given: student.given_name_tw },
    { code: 'CN', lang: 'cn', family: student.family_name_cn, given: student.given_name_cn },
  ];

  return langs
    .filter(l => l.family || l.given)
    .map(l => `<span class="lang-pill"><span><b>${l.code}</b>${joinName(l.family, l.given, l.lang, true)}</span></span>`)
    .join('');
}

// 生成学生卡片 HTML：折叠态为头像瓦片，展开态在瓦片后追加整行详情面板
function generateStudentCard(student: Student): string {
  const avatar = getStudentAvatar(student);
  const schoolName = getSchoolName(student.school_id);
  const schoolLogo = getSchoolLogo(student.school_id);
  // 学校配色：校徽底座用 bg（配色与图片适配已由配置保证，见 schoolColors.ts）
  const schoolColor = getSchoolColor(student.school_id);
  const isExpanded = expandState[student.id] || false;
  // 完整名称是否显示姓氏由界面设置控制（见 nameFormat.ts 与设置面板）
  const showFamily = getUiSettings().showFamilyName;
  // 角标：任一时装为限定则显示限定，全部页面为 NPC 时显示 NPC
  const isLimited = student.pages.some(p => p.limited);
  const isNpc = student.pages.length > 0 && student.pages.every(p => p.is_npc);
  const badge = isLimited
    ? '<span class="tile-badge">★限定</span>'
    : isNpc ? '<span class="tile-badge">🚫NPC</span>' : '';

  const tile = `
    <div class="student-tile ${isExpanded ? 'selected' : ''}" data-student-id="${student.id}">
      ${avatar ? `<img src="${avatar}" class="tile-avatar" alt="" loading="lazy">` : '<div class="tile-avatar tile-avatar-empty"></div>'}
      <div class="tile-name">${joinName(student.family_name, student.given_name, 'cn', showFamily)}</div>
      ${badge}
    </div>
  `;

  if (!isExpanded) return tile;

  const schoolChip = `
    <span class="school-chip" ${schoolLogo ? `style="background:${schoolColor.bg}"` : ''}>
      ${schoolLogo ? `<img src="https:${schoolLogo}" class="school-chip-logo" alt="" title="${schoolName}">` : ''}
      <span>${schoolName}</span>
    </span>
  `;

  return tile + `
    <div class="student-detail">
      <div class="detail-left">
        <div class="detail-avatar">
          ${avatar ? `<img src="${avatar}" class="tile-avatar" alt="" loading="lazy">` : '<div class="tile-avatar tile-avatar-empty"></div>'}
        </div>
        ${schoolChip}
      </div>
      <div class="detail-right">
        <div class="detail-name">
          <span>${joinName(student.family_name, student.given_name, 'cn', showFamily)}</span>
          <span class="detail-sid">ID ${student.id}</span>
        </div>
        <div class="lang-row">${generateLangPills(student)}</div>
        <div class="pages-title">${t('kivo.pagesLabel', { n: student.pages.length })}</div>
        ${student.pages.map(page => generatePageHTML(page)).join('')}
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
        <p>${t('common.noResults')}</p>
        <p class="empty-state-hint">${t('common.noResultsHint')}</p>
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

  // 绑定瓦片点击事件
  document.querySelectorAll<HTMLElement>('.student-tile').forEach(tile => {
    tile.addEventListener('click', () => {
      toggleExpand(parseInt(tile.dataset.studentId || '0'));
    });
  });
}

// 切换展开状态（展开态由整行详情面板承载，需重渲染列表）
function toggleExpand(studentId: number): void {
  expandState[studentId] = !expandState[studentId];
  renderStudents(filteredStudents);
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
  const installFilter = elements.installFilter.value;

  filteredStudents = allStudents.filter(student => {
    // 搜索过滤（覆盖各语言名字与 ID），分词 AND 规则见 search.ts，
    // en 做词前缀匹配：搜 Mika 命中 Misono Mika，不意外命中 Umika / Sumika
    const showFamily = getUiSettings().showFamilyName;
    const matchSearch = matchesSearch(
      [
        joinName(student.family_name, student.given_name, 'cn', showFamily),
        joinName(student.family_name_cn, student.given_name_cn, 'cn', true),
        joinName(student.family_name_jp, student.given_name_jp, 'jp', true),
        joinName(student.family_name_kr, student.given_name_kr, 'kr', true),
        joinName(student.family_name_tw, student.given_name_tw, 'tw', true),
        student.id.toString(),
      ],
      [joinName(student.family_name_en, student.given_name_en, 'en', true)],
      elements.searchInput.value
    );

    // 学校过滤（多选，来自共享组件）
    const selected = schoolFilter?.selected;
    const matchSchool = !selected || selected.size === 0 ||
      selected.has(String(student.school_id));

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
    } else if (installFilter === 'not_npc') {
      matchInstall = student.pages.some(p => !p.is_npc);
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

// 创建学校多选筛选组件
function setupSchoolFilter(): void {
  const options = Array.from(schoolsMap.values())
    .sort((a, b) => a.id - b.id)
    .map(s => ({ school: s, color: getSchoolColorOrNull(s.id) }));

  schoolFilter = createSchoolFilter(
    { btn: elements.schoolFilterBtn, dropdown: elements.schoolDropdown, count: elements.schoolFilterCount },
    options,
    applyFilters
  );
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
    setupSchoolFilter();
    renderStudents(allStudents);

    if (metadata?.updateDate) {
      elements.updateTime.textContent = metadata.updateDate;
    } else {
      // debug 为伪语言，日期回落 zh-CN 格式
      const dateLocale = getUiSettings().lang === 'debug' ? 'zh-CN' : getUiSettings().lang;
      elements.updateTime.textContent = new Date().toLocaleDateString(dateLocale);
    }
  } catch (error) {
    console.error('加载数据失败:', error);
    elements.studentsContainer.innerHTML = `
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

// 初始化 KivoWiki 导航视图（懒加载：首次切换到该视图时调用）
export function initKivoView(): void {
  // 事件监听（学校筛选按钮由共享组件绑定）
  elements.searchInput.addEventListener('input', applyFilters);
  elements.installFilter.addEventListener('change', applyFilters);
  elements.expandAllBtn.addEventListener('click', expandAll);
  elements.collapseAllBtn.addEventListener('click', collapseAll);

  // 界面设置变更（如 spine 链接开关）时重渲染学生列表
  window.addEventListener('ui-settings-changed', () => {
    renderStudents(filteredStudents);
  });

  // 语言切换时重渲染学生列表
  window.addEventListener('lang-changed', () => {
    renderStudents(filteredStudents);
  });

  // 加载数据
  loadData();
}
