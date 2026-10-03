import type { AppConfig, ColumnConfig, ClickFXConfig, UiSettings } from './types.js';

// 配置 - 使用本地嵌入的数据
export const CONFIG: AppConfig = {
  csvUrl: './data/students_data.csv',
  metadataUrl: './data/metadata.json',
  schoolsUrl: './data/schools.json',
  studentsUrl: './data/students.json',
  repoOwner: 'Agent-0808',
  repoName: 'BA-characters-internal-id'
};

// 蔚蓝档案点击特效配置 (ba-click-fx)
export const CLICK_FX_CONFIG: ClickFXConfig = {
  enabled: true,
  themeColor: '#4ca7ff', // 默认蓝色主题
  clickEnabled: true,
  trailEnabled: true,
  trailAlways: false, // 默认只在按下时显示拖尾
  opacity: 1,
  scale: 1,
};

// Click FX 用户配置的 localStorage key
const CLICK_FX_STORAGE_KEY = 'ba-click-fx-config';

// 校验单个 Click FX 字段值是否合法（类型 + 取值范围）
function isValidClickFxValue(key: keyof ClickFXConfig, value: unknown): boolean {
  switch (key) {
    case 'enabled':
    case 'clickEnabled':
    case 'trailEnabled':
    case 'trailAlways':
      return typeof value === 'boolean';
    case 'opacity':
      return typeof value === 'number' && value >= 0 && value <= 1;
    case 'scale':
      return typeof value === 'number' && value >= 0.1 && value <= 3;
    case 'themeColor':
      return typeof value === 'string' && /^#[0-9a-fA-F]{6}$/.test(value);
  }
}

// 从 localStorage 读取用户 Click FX 配置并与默认值合并（非法字段/值回落默认；不可用时返回默认）
export function loadClickFxConfig(): ClickFXConfig {
  const cfg: ClickFXConfig = { ...CLICK_FX_CONFIG };
  try {
    const raw = localStorage.getItem(CLICK_FX_STORAGE_KEY);
    if (!raw) return cfg;
    const saved = JSON.parse(raw) as Record<string, unknown>;
    if (typeof saved !== 'object' || saved === null) return cfg;
    (Object.keys(cfg) as (keyof ClickFXConfig)[]).forEach(key => {
      const value = saved[key];
      if (isValidClickFxValue(key, value)) {
        // 已通过 isValidClickFxValue 按字段校验类型
        (cfg as unknown as Record<string, unknown>)[key] = value;
      }
    });
  } catch {
    // localStorage 不可用或数据损坏时使用默认配置
  }
  return cfg;
}

// 保存用户 Click FX 配置到 localStorage（不可用时静默跳过）
export function saveClickFxConfig(cfg: ClickFXConfig): void {
  try {
    localStorage.setItem(CLICK_FX_STORAGE_KEY, JSON.stringify(cfg));
  } catch {
    // localStorage 不可用（如隐私模式），忽略
  }
}

// 清除用户 Click FX 配置（恢复默认）
export function clearClickFxConfig(): void {
  try {
    localStorage.removeItem(CLICK_FX_STORAGE_KEY);
  } catch {
    // 忽略
  }
}

// 支持的界面语言（设置面板下拉选项，单一来源；debug / en-US 在后续阶段加入）
export const UI_LANGS: { code: string; label: string }[] = [
  { code: 'zh-CN', label: '简体中文' },
];

// 界面设置默认值
export const UI_SETTINGS_DEFAULT: UiSettings = {
  spineLink: false,
  lang: 'zh-CN',
};

// 界面设置的 localStorage key
const UI_SETTINGS_STORAGE_KEY = 'ba-ui-settings';

// 读取界面设置（缺失/非法字段回落默认；不可用时返回默认）
export function loadUiSettings(): UiSettings {
  const settings: UiSettings = { ...UI_SETTINGS_DEFAULT };
  try {
    const raw = localStorage.getItem(UI_SETTINGS_STORAGE_KEY);
    if (!raw) return settings;
    const saved = JSON.parse(raw) as Record<string, unknown>;
    if (typeof saved !== 'object' || saved === null) return settings;
    if (typeof saved.spineLink === 'boolean') {
      settings.spineLink = saved.spineLink;
    }
    if (typeof saved.lang === 'string' && UI_LANGS.some(l => l.code === saved.lang)) {
      settings.lang = saved.lang;
    }
  } catch {
    // localStorage 不可用或数据损坏时使用默认设置
  }
  return settings;
}

// 保存界面设置到 localStorage（不可用时静默跳过）
export function saveUiSettings(settings: UiSettings): void {
  try {
    localStorage.setItem(UI_SETTINGS_STORAGE_KEY, JSON.stringify(settings));
  } catch {
    // localStorage 不可用（如隐私模式），忽略
  }
}

// 列配置 - 定义所有列的信息
export const COLUMN_CONFIG: ColumnConfig[] = [
  { key: 'file_id', label: '文件ID', defaultVisible: true },
  { key: 'student_id', label: 'ID', defaultVisible: true },
  { key: 'page_id', label: 'Page', defaultVisible: true },
  { key: 'spine_id', label: 'Spine', defaultVisible: true },
  { key: 'full_name', label: '完整名称', defaultVisible: true },
  { key: 'name', label: '角色名', defaultVisible: false },
  { key: 'skin_name', label: '皮肤名', defaultVisible: false },
  { key: 'spine_remark', label: '备注', defaultVisible: false },
  { key: 'name_cn', label: '国服名称', defaultVisible: false },
  { key: 'name_jp', label: '日本語', defaultVisible: true },
  { key: 'name_tw', label: '繁體中文', defaultVisible: false },
  { key: 'name_en', label: 'English', defaultVisible: true },
  { key: 'name_kr', label: '한국어', defaultVisible: false },
  { key: 'school_name', label: '学校', defaultVisible: true }
];
