// 学校多选筛选下拉组件（table 与 kivonavi 共用）
// 封装：下拉渲染、多选状态、按钮计数、清除全部、按钮定位、点击外部关闭

// 学校选项（名称 + logo）
export interface SchoolOption {
  name: string;
  logo: string | null;
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

  // 生成下拉内容
  let html = `
    <div class="school-filter-header">
      <span style="font-size: 12px; color: #64748b;">选择学校</span>
      <span class="school-filter-clear">清除全部</span>
    </div>
  `;
  schools.forEach(({ name, logo }) => {
    const logoHtml = logo ? `<img src="https:${logo}" class="school-filter-logo" alt="">` : '';
    html += `
      <label class="school-filter-item" data-school="${name}">
        <input type="checkbox" data-school="${name}">
        ${logoHtml}
        <span class="school-filter-name">${name}</span>
      </label>
    `;
  });
  elements.dropdown.innerHTML = html;

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

  // 绑定事件
  elements.btn.addEventListener('click', toggleDropdown);
  elements.dropdown.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
    checkbox.addEventListener('change', (e) => {
      const target = e.target as HTMLInputElement;
      toggleSchool(target.getAttribute('data-school') as string, target.checked);
    });
  });
  elements.dropdown.querySelector('.school-filter-clear')?.addEventListener('click', clearAll);
  document.addEventListener('click', onDocumentClick);

  return { selected };
}
