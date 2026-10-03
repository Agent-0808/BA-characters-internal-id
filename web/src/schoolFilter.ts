// 学校多选筛选下拉组件（table 与 kivonavi 共用）
// 封装：下拉渲染、多选状态、按钮计数、清除全部、按钮定位、点击外部关闭

import { t } from './i18n.js';
import type { SchoolColor } from './schoolColors.js';

// 学校选项（名称 + logo + 可选配色）
export interface SchoolOption {
  name: string;
  logo: string | null;
  color?: SchoolColor | null;  // 学校配色；未配置时不应用（见 schoolColors.ts）
}

// 组件所需的 DOM 元素
export interface SchoolFilterElements {
  btn: HTMLButtonElement;      // 触发下拉的按钮
  dropdown: HTMLDivElement;    // 下拉内容容器
  count: HTMLSpanElement;      // 已选数量徽标
}

// 组件对外暴露的接口
export interface SchoolFilter {
  readonly selected: Set<string>;
}

// 创建学校多选筛选组件；选择变化时调用 onChange
export function createSchoolFilter(
  elements: SchoolFilterElements,
  schools: SchoolOption[],
  onChange: () => void
): SchoolFilter {
  const selected: Set<string> = new Set();

  // 更新按钮计数与选项选中态
  function updateUI(): void {
    elements.count.textContent = selected.size > 0 ? selected.size.toString() : '';
    elements.dropdown.querySelectorAll('.school-filter-item').forEach(item => {
      const name = item.getAttribute('data-school') as string;
      const checkbox = item.querySelector('input[type="checkbox"]') as HTMLInputElement;
      const isSelected = selected.has(name);
      checkbox.checked = isSelected;
      item.classList.toggle('selected', isSelected);
    });
  }

  // 渲染下拉内容并绑定选项事件（语言切换时整体重渲染，选中态由 selected 集合恢复）
  function render(): void {
    let html = `
      <div class="school-filter-header">
        <span style="font-size: 12px; color: #64748b;">${t('schoolFilter.title')}</span>
        <span class="school-filter-clear">${t('schoolFilter.clearAll')}</span>
      </div>
      <div class="school-filter-list">
    `;
    schools.forEach(({ name, logo, color }) => {
      const logoHtml = logo ? `<img src="https:${logo}" class="school-filter-logo" alt="">` : '';
      // 配置了配色的学校：选项行平铺底色 + 文字色；未配置则保持默认样式
      const colorStyle = color ? ` style="background:${color.bg};color:${color.text}"` : '';
      html += `
        <label class="school-filter-item" data-school="${name}"${colorStyle}>
          <input type="checkbox" data-school="${name}">
          ${logoHtml}
          <span class="school-filter-name">${name}</span>
        </label>
      `;
    });
    html += '</div>';
    elements.dropdown.innerHTML = html;

    elements.dropdown.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
      checkbox.addEventListener('change', (e) => {
        const target = e.target as HTMLInputElement;
        toggleSchool(target.getAttribute('data-school') as string, target.checked);
      });
    });
    elements.dropdown.querySelector('.school-filter-clear')?.addEventListener('click', clearAll);

    updateUI();
  }

  // 切换学校选中状态
  function toggleSchool(name: string, checked: boolean): void {
    if (checked) {
      selected.add(name);
    } else {
      selected.delete(name);
    }
    updateUI();
    onChange();
  }

  // 清除全部
  function clearAll(): void {
    selected.clear();
    updateUI();
    onChange();
  }

  // 显示/隐藏下拉菜单
  function toggleDropdown(): void {
    if (!elements.dropdown.classList.contains('show')) {
      // 计算按钮位置
      const rect = elements.btn.getBoundingClientRect();
      elements.dropdown.style.top = `${rect.bottom + 5}px`;
      elements.dropdown.style.left = `${rect.left}px`;
    }
    elements.dropdown.classList.toggle('show');
  }

  // 点击外部关闭下拉菜单
  function onDocumentClick(event: Event): void {
    const target = event.target as HTMLElement;
    if (!elements.dropdown.contains(target) && !elements.btn.contains(target)) {
      elements.dropdown.classList.remove('show');
    }
  }

  // 绑定事件（下拉选项事件在 render() 内绑定）
  elements.btn.addEventListener('click', toggleDropdown);
  document.addEventListener('click', onDocumentClick);

  // 语言切换时重渲染下拉文案
  window.addEventListener('lang-changed', render);

  // 首次渲染
  render();

  return { selected };
}
