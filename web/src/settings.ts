// 设置面板：BA Click FX 运行时配置与持久化
// 变更即时生效（无确认按钮），同时写入 localStorage；「恢复默认」清除存储并还原控件与实例

import {
  CLICK_FX_CONFIG,
  UI_SETTINGS_DEFAULT,
  UI_LANGS,
  loadUiSettings,
  saveUiSettings,
  saveClickFxConfig,
  clearClickFxConfig,
} from './config.js';
import {
  setClickFxEnabled,
  updateClickFxConfig,
  setClickFxThemeColor,
  getActiveClickFxConfig,
} from './clickFx.js';
import { setLang } from './i18n.js';
import type { ClickFXConfig, UiSettings } from './types.js';

// 界面设置状态（单一来源，供其他模块查询）
let uiSettings: UiSettings = loadUiSettings();

// 获取当前界面设置（spine 链接等）
export function getUiSettings(): UiSettings {
  return uiSettings;
}

// 界面设置变更后广播，视图监听并重渲染
function notifyUiSettingsChanged(): void {
  window.dispatchEvent(new Event('ui-settings-changed'));
}

// DOM 元素引用
const elements = {
  settingsBtn: document.getElementById('settingsBtn') as HTMLButtonElement,
  panel: document.getElementById('settingsPanel') as HTMLDivElement,
  closeBtn: document.getElementById('settingsClose') as HTMLButtonElement,
  resetBtn: document.getElementById('settingsReset') as HTMLButtonElement,
  langSelect: document.getElementById('uiLang') as HTMLSelectElement,
  spineLink: document.getElementById('uiSpineLink') as HTMLInputElement,
  enabled: document.getElementById('fxEnabled') as HTMLInputElement,
  clickEnabled: document.getElementById('fxClickEnabled') as HTMLInputElement,
  trailEnabled: document.getElementById('fxTrailEnabled') as HTMLInputElement,
  trailAlways: document.getElementById('fxTrailAlways') as HTMLInputElement,
  opacity: document.getElementById('fxOpacity') as HTMLInputElement,
  opacityValue: document.getElementById('fxOpacityValue') as HTMLSpanElement,
  scale: document.getElementById('fxScale') as HTMLInputElement,
  scaleValue: document.getElementById('fxScaleValue') as HTMLSpanElement,
  themeColor: document.getElementById('fxThemeColor') as HTMLInputElement,
};

// 除「启用特效」外的所有控件（enabled 关闭时统一置灰）
const fxControls: HTMLInputElement[] = [
  elements.clickEnabled, elements.trailEnabled, elements.trailAlways,
  elements.opacity, elements.scale, elements.themeColor,
];

// 将控件显示同步为当前生效配置
function syncControls(cfg: ClickFXConfig): void {
  elements.enabled.checked = cfg.enabled;
  elements.clickEnabled.checked = cfg.clickEnabled;
  elements.trailEnabled.checked = cfg.trailEnabled;
  elements.trailAlways.checked = cfg.trailAlways;
  elements.opacity.value = cfg.opacity.toString();
  elements.opacityValue.textContent = cfg.opacity.toFixed(2);
  elements.scale.value = cfg.scale.toString();
  elements.scaleValue.textContent = cfg.scale.toFixed(2);
  elements.themeColor.value = cfg.themeColor;
  updateDisabledState();
}

// enabled 关闭时其余控件禁用置灰
function updateDisabledState(): void {
  const disabled = !elements.enabled.checked;
  fxControls.forEach(el => { el.disabled = disabled; });
}

// 保存当前生效配置到 localStorage
function persist(): void {
  saveClickFxConfig(getActiveClickFxConfig());
}

// 显示/隐藏面板
function togglePanel(show?: boolean): void {
  const visible = show ?? elements.panel.hidden;
  elements.panel.hidden = !visible;
}

// 点击面板外部关闭
function onDocumentClick(event: Event): void {
  const target = event.target as HTMLElement;
  if (!elements.panel.hidden &&
      !elements.panel.contains(target) && !elements.settingsBtn.contains(target)) {
    elements.panel.hidden = true;
  }
}

// 恢复默认：清除存储，实例与控件还原为硬编码默认值
function resetToDefaults(): void {
  clearClickFxConfig();
  setClickFxEnabled(CLICK_FX_CONFIG.enabled);
  updateClickFxConfig({
    clickEnabled: CLICK_FX_CONFIG.clickEnabled,
    trailEnabled: CLICK_FX_CONFIG.trailEnabled,
    trailAlways: CLICK_FX_CONFIG.trailAlways,
    opacity: CLICK_FX_CONFIG.opacity,
    scale: CLICK_FX_CONFIG.scale,
  });
  setClickFxThemeColor(CLICK_FX_CONFIG.themeColor);
  syncControls(CLICK_FX_CONFIG);

  uiSettings = { ...UI_SETTINGS_DEFAULT };
  saveUiSettings(uiSettings);
  elements.spineLink.checked = uiSettings.spineLink;
  elements.langSelect.value = uiSettings.lang;
  void setLang(uiSettings.lang);
  notifyUiSettingsChanged();
}

// 初始化语言下拉：选项来自 UI_LANGS 单一来源，选中当前设置
function initLangSelect(): void {
  UI_LANGS.forEach(({ code, label }) => {
    const option = document.createElement('option');
    option.value = code;
    option.textContent = label;
    elements.langSelect.appendChild(option);
  });
  elements.langSelect.value = uiSettings.lang;
}

// 初始化设置面板（全局调用一次）
export function initSettingsPanel(): void {
  syncControls(getActiveClickFxConfig());
  initLangSelect();
  elements.spineLink.checked = uiSettings.spineLink;

  // 齿轮按钮 / 关闭按钮 / 恢复默认
  elements.settingsBtn.addEventListener('click', () => togglePanel());
  elements.closeBtn.addEventListener('click', () => togglePanel(false));
  elements.resetBtn.addEventListener('click', resetToDefaults);

  // 界面设置：语言切换（持久化 + i18next 切换 + lang-changed 广播）
  elements.langSelect.addEventListener('change', () => {
    uiSettings.lang = elements.langSelect.value;
    saveUiSettings(uiSettings);
    void setLang(uiSettings.lang);
  });

  // 界面设置：spine 链接开关（保存 + 广播重渲染）
  elements.spineLink.addEventListener('change', () => {
    uiSettings.spineLink = elements.spineLink.checked;
    saveUiSettings(uiSettings);
    notifyUiSettingsChanged();
  });

  // 控件变更：应用运行时 + 持久化
  elements.enabled.addEventListener('change', () => {
    setClickFxEnabled(elements.enabled.checked);
    updateDisabledState();
    persist();
  });
  elements.clickEnabled.addEventListener('change', () => {
    updateClickFxConfig({ clickEnabled: elements.clickEnabled.checked });
    persist();
  });
  elements.trailEnabled.addEventListener('change', () => {
    updateClickFxConfig({ trailEnabled: elements.trailEnabled.checked });
    persist();
  });
  elements.trailAlways.addEventListener('change', () => {
    updateClickFxConfig({ trailAlways: elements.trailAlways.checked });
    persist();
  });
  elements.opacity.addEventListener('input', () => {
    const opacity = parseFloat(elements.opacity.value);
    elements.opacityValue.textContent = opacity.toFixed(2);
    updateClickFxConfig({ opacity });
    persist();
  });
  elements.scale.addEventListener('input', () => {
    const scale = parseFloat(elements.scale.value);
    elements.scaleValue.textContent = scale.toFixed(2);
    updateClickFxConfig({ scale });
    persist();
  });
  elements.themeColor.addEventListener('input', () => {
    setClickFxThemeColor(elements.themeColor.value);
    persist();
  });

  // 点击外部关闭面板
  document.addEventListener('click', onDocumentClick);
}
