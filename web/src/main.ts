import './style.css';
import { initClickFX } from './clickFx.js';
import { initTableView } from './table.js';
import { initKivoView } from './kivonavi.js';

// 初始化蔚蓝档案点击特效（全局一次）
initClickFX();

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
  document.title = view === 'kivonavi' ? 'KivoWiki 导航' : 'BA Characters Internal ID';
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

// 初始视图
switchView();
