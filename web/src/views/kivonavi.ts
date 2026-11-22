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

// 生成星级字符串（页面级属性，用于头像卡底部色条）
function getRarityStars(rarity: number): string {
  if (rarity <= 0) return '';
  return '★'.repeat(Math.min(rarity, 3));
}

// 当前界面语言对应的名称来源：中文绑定 KivoWiki 原始数据，debug 不隐藏任何语言
function getDisplayLang(): 'kivo' | 'en' | null {
  const lang = getUiSettings().lang;
  if (lang === 'en-US') return 'en';
  if (lang === 'zh-CN') return 'kivo';
  return null;
}

// 学生名称：按界面语言取名，英文缺失时回落 KivoWiki 原始名称
function getStudentName(student: Student, showFamily: boolean): string {
  if (getDisplayLang() === 'en') {
    return joinName(
      student.family_name_en || student.family_name,
      student.given_name_en || student.given_name,
      'en', showFamily
    );
  }
  return joinName(student.family_name, student.given_name, 'cn', showFamily);
}

// 生成page名称：按界面语言取皮肤名，缺失时回落原始名称，再缺失显示默认页
function getPageName(page: KivoPage): string {
  const localized = getDisplayLang() === 'en' ? page.skin_en : page.skin;
  return localized || t('kivo.defaultPage');
}

// 生成单个 page 行：斜切头像卡（限定标记 + 星级条）+ 皮肤名 + 三服实装 + spine
function generatePageHTML(page: KivoPage, studentIsNpc: boolean): string {
  const avatar = getPageAvatar(page);
  const pageName = getPageName(page);
  const kivoUrl = `https://kivo.wiki/data/character/${page.page_id}?mode=appreciation`;
  const spineUrl = (spineId: number) => `https://api.kivo.wiki/api/v1/data/spines/${spineId}`;
  // 底部深色条：有星级时放星级，无星级（未实装）时放 🔒
  const stars = getRarityStars(page.rarity);
  const bandText = stars || '🔒';
  // 限定是页面级属性；NPC 学生不展示
  const limitedBadge = page.limited && !studentIsNpc
    ? `<span class="corner-badge">${t('kivo.limited')}</span>`
    : '';

  // 单服实装状态小卡：上半部分深灰底放图标，下半部分放 ✓/✕
  const server = (installed: boolean, icon: string, label: string) =>
    `<span class="server ${installed ? 'on' : 'off'}" title="${label}">
      <span class="server-head"><span class="ico">${icon}</span></span>
      <span class="mark">${installed ? '✓' : '✕'}</span>
    </span>`;

  // NPC 页面不展示实装状态（NPC 属于学生级属性）
  const servers = studentIsNpc ? '' : `
    <div class="servers">
      ${server(page.is_install, '⛩️', t('kivo.installJp'))}
      ${server(page.is_install_global, '🌐', t('kivo.installGlobal'))}
      ${server(page.is_install_cn, '🀄', t('kivo.installCn'))}
    </div>
  `;

  const spinesHTML = page.spines.map(spineId =>
    getUiSettings().spineLink
      ? `<a href="${spineUrl(spineId)}" target="_blank" rel="noopener" class="spine-id"><span>${spineId}</span></a>`
      : `<span class="spine-id"><span>${spineId}</span></span>`
  ).join('');

  return `
    <div class="page-row">
      <div class="page-card">
        ${limitedBadge}
        <a class="page-card-inner" href="${kivoUrl}" target="_blank" rel="noopener" title="page_id: ${page.page_id}">
          ${avatar
            ? `<img src="${avatar}" alt="" loading="lazy">`
            : '<div class="page-card-empty"></div>'}
          <div class="card-band">${bandText}</div>
        </a>
      </div>
      <div class="page-main">
        <div class="page-meta">
          <div class="facts">
            <div class="fact">
              <span class="fact-key"><span>${t('kivo.skinLabel')}</span></span>
              <span class="fact-val"><span>${pageName}</span></span>
            </div>
            <div class="fact">
              <span class="fact-key"><span>${t('kivo.pageLabel')}</span></span>
              <span class="fact-val"><span>${page.page_id}</span></span>
            </div>
          </div>
          ${servers}
        </div>
        <div class="spine-line">
          <div class="spines-list">${spinesHTML || `<span class="spine-empty">${t('kivo.none')}</span>`}</div>
        </div>
      </div>
    </div>
  `;
}

// 生成语言姓名胶囊（仅显示已填写的语言，姓与名按该语言规则拼接）
function generateLangPills(student: Student): string {
  // 当前界面语言已显示在卡片与标题中，胶囊里隐藏对应来源
  const hidden = getDisplayLang();
  // 是否显示姓氏同样受界面设置控制
  const showFamily = getUiSettings().showFamilyName;
  const langs: { code: string; source: 'kivo' | 'jp' | 'en' | 'kr' | 'tw' | 'cn'; family: string; given: string }[] = [
    { code: 'KivoWiki', source: 'kivo', family: student.family_name, given: student.given_name },
    { code: 'JP', source: 'jp', family: student.family_name_jp, given: student.given_name_jp },
    { code: 'EN', source: 'en', family: student.family_name_en, given: student.given_name_en },
    { code: 'KR', source: 'kr', family: student.family_name_kr, given: student.given_name_kr },
    { code: 'TW', source: 'tw', family: student.family_name_tw, given: student.given_name_tw },
    { code: 'CN', source: 'cn', family: student.family_name_cn, given: student.given_name_cn },
  ];

  return langs
    .filter(l => (l.family || l.given) && l.source !== hidden)
    .map(l => `<span class="lang-pill"><span><b>${l.code}</b>${joinName(l.family, l.given, l.source === 'en' ? 'en' : 'cn', showFamily)}</span></span>`)
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
  // 角标：NPC 学生显示 NPC（限定标志不再展示在卡片上）
  const isNpc = student.is_npc;
  const badge = isNpc ? '<span class="tile-badge">🚫NPC</span>' : '';

  const tile = `
    <div class="student-tile ${isExpanded ? 'selected' : ''}" data-student-id="${student.id}">
      ${avatar ? `<img src="${avatar}" class="tile-avatar" alt="" loading="lazy">` : '<div class="tile-avatar tile-avatar-empty"></div>'}
      <div class="tile-name">${getStudentName(student, showFamily)}</div>
      ${badge}
    </div>
  `;

  if (!isExpanded) return tile;

  const schoolChip = `
    <span class="school-chip" style="background:${schoolColor.bg};color:${schoolColor.text}">
      ${schoolLogo ? `<img src="https:${schoolLogo}" class="school-chip-logo" alt="" title="${schoolName}">` : ''}
      <span>${schoolName}</span>
    </span>
  `;
  // NPC 是学生级属性，显示在信息头中
  const npcChip = isNpc ? `<span class="tag tag-npc"><span>🚫NPC</span></span>` : '';

  return tile + `
    <div class="student-detail">
      <div class="detail-head">
        <span class="detail-name">${getStudentName(student, showFamily)}</span>
        <span class="detail-sid">ID ${student.id}</span>
        ${npcChip}
        ${schoolChip}
      </div>
      <div class="lang-row">${generateLangPills(student)}</div>
      <div class="section-band">${t('kivo.pagesLabel', { n: student.pages.length })}</div>
      ${student.pages.map(page => generatePageHTML(page, isNpc)).join('')}
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

  fitTileNames();
}

// 瓦片名牌保持单行：按可用宽度逐级缩小字号（最小 9px），仍超出则由 CSS 省略号截断
function fitTileNames(): void {
  const MIN_FONT_SIZE = 9;
  const DEFAULT_FONT_SIZE = 12;
  document.querySelectorAll<HTMLElement>('.tile-name').forEach(el => {
    let size = DEFAULT_FONT_SIZE;
    el.style.fontSize = `${size}px`;
    while (el.scrollWidth > el.clientWidth && size > MIN_FONT_SIZE) {
      size -= 1;
      el.style.fontSize = `${size}px`;
    }
  });
}

// 切换展开状态（展开态由整行详情面板承载，需重渲染列表）
function toggleExpand(studentId: number): void {
  expandState[studentId] = !expandState[studentId];
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
      matchInstall = student.is_npc;
    } else if (installFilter === 'not_npc') {
      matchInstall = !student.is_npc;
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
