import './style.css';
import { initClickFX } from './clickFx.js';
import { initSettingsPanel, getUiSettings } from './settings.js';
import { CONFIG, saveUiSettings } from './config.js';
import { initI18n, t } from './i18n.js';
import { countView } from './goatcounter.js';
import { initTableView } from './table.js';
import { initKivoView } from './kivonavi.js';
import type { Metadata } from './types.js';

// 容器宽度拖拽的常量（需在下方 initContainerResize() 调用前声明，避免 const TDZ 异常）
const CONTAINER_MIN_WIDTH = 800;
const CONTAINER_BODY_PADDING = 30; // body 左右 padding 各 15px

// 初始化 i18n（需先于其他模块发起；内部 await init 完成后自动应用静态文案），随后初始化点击特效与设置面板
void initI18n(getUiSettings().lang);
initClickFX();
initSettingsPanel();
initContainerResize();

// 容器宽度拖拽：容器 margin auto 居中（中心恒为视口中心），
// 拖左右缘时按 2*(鼠标x-中心x) 计算宽度，使拖拽侧边缘精确跟随鼠标；
// 宽度持久化到 UiSettings.containerWidth（0 = 使用 CSS 默认），恢复默认设置时自动还原

// 宽度夹取：下限防过窄，上限不超过视口
function clampContainerWidth(width: number): number {
  const max = document.documentElement.clientWidth - CONTAINER_BODY_PADDING;
  return Math.min(max, Math.max(CONTAINER_MIN_WIDTH, width));
}

// 应用容器宽度（width <= 0 时清除内联样式，回落 CSS 默认）
function applyContainerWidth(width: number): void {
  const container = document.querySelector('.container') as HTMLElement | null;
  if (!container) return;
  container.style.width = width > 0 ? `${clampContainerWidth(width)}px` : '';
}

function initContainerResize(): void {
  const resizers = document.querySelectorAll('.container-resizer');
  if (resizers.length === 0) return;

  // 启动时恢复持久化的宽度；设置变更（含「恢复默认」）后重新应用
  applyContainerWidth(getUiSettings().containerWidth);
  window.addEventListener('ui-settings-changed', () => {
    applyContainerWidth(getUiSettings().containerWidth);
  });

  let dragging = false;
  let fromLeft = false;
  const isMobile = window.matchMedia('(max-width: 768px)');

  resizers.forEach(resizer => {
    resizer.addEventListener('mousedown', (e) => {
      if (isMobile.matches) return;
      e.preventDefault();
      dragging = true;
      fromLeft = resizer.classList.contains('left');
      document.body.style.cursor = 'ew-resize';
      document.body.style.userSelect = 'none';
    });
  });

  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const offset = fromLeft
      ? document.documentElement.clientWidth / 2 - e.clientX
      : e.clientX - document.documentElement.clientWidth / 2;
    applyContainerWidth(2 * offset);
  });

  window.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    // 拖拽结束持久化当前宽度（移动端不触发 dragging，无需额外判断）
    const container = document.querySelector('.container') as HTMLElement | null;
    if (container) {
      const width = clampContainerWidth(parseFloat(container.style.width) || 0);
      const settings = getUiSettings();
      settings.containerWidth = width;
      saveUiSettings(settings);
    }
  });
}

// 更新页脚右侧的 web / 数据 提交 hash（截取前6位）
async function loadFooterHashes(): Promise<void> {
  const el = document.getElementById('footerHashes');
  if (!el) return;
  try {
    const response = await fetch(CONFIG.metadataUrl);
    if (!response.ok) return;
    const metadata: Metadata = await response.json();
    const short = (hash: string): string =>
      hash && hash.length > 7 ? hash.slice(0, 6) : (hash || 'unknown');
    el.textContent = `web ${short(metadata.webCommitHash)} · data ${short(metadata.dataCommitHash)}`;
    // 悬浮时显示完整 commit hash（而非截断的短 hash）
    const orUnknown = (value: string): string => value || 'unknown';
    const lines = [
      `web: ${orUnknown(metadata.webCommitHash)}`,
      `data: ${orUnknown(metadata.dataCommitHash)}`,
      `crawler: ${orUnknown(metadata.codeVersion)}`,
    ];
    if (metadata.repoUrl) lines.push(metadata.repoUrl);
    el.title = lines.join('\n');
  } catch {
    // 元数据加载失败时静默处理，页脚 hash 留空
  }
}
loadFooterHashes();

// 视图注册表：首次切换到某视图时才初始化并加载数据
const viewInits: Record<string, () => void> = {
  index: initTableView,
  kivonavi: initKivoView,
};
const initializedViews = new Set<string>();

// 从 hash 解析当前视图（默认 index），hash 保证 URL query（如 spine_link）不丢失
function currentView(): string {
  return location.hash === '#kivonavi' ? 'kivonavi' : 'index';
}

// 切换视图可见性，并懒初始化目标视图
function switchView(): void {
  const view = currentView();
  const indexEl = document.getElementById('view-index') as HTMLElement;
  const kivoEl = document.getElementById('view-kivonavi') as HTMLElement;
  indexEl.hidden = view !== 'index';
  kivoEl.hidden = view !== 'kivonavi';
  document.title = view === 'kivonavi' ? t('nav.docTitleKivo') : t('nav.docTitleIndex');
  // 按视图上报访问统计（初始加载与 hash 切换都会经过这里）
  countView(view);
  if (!initializedViews.has(view)) {
    initializedViews.add(view);
    viewInits[view]();
  }
}

// 绑定导航链接（两个视图的 header 中各有一份，均拦截为 hash 切换）
document.querySelectorAll<HTMLAnchorElement>('.nav-links a').forEach(a => {
  a.addEventListener('click', (e) => {
    e.preventDefault();
    if (location.hash !== a.hash) {
      location.hash = a.hash;
    }
  });
});

window.addEventListener('hashchange', switchView);

// 语言切换时同步更新标题
window.addEventListener('lang-changed', () => {
  document.title = currentView() === 'kivonavi' ? t('nav.docTitleKivo') : t('nav.docTitleIndex');
});

// 初始视图
switchView();
