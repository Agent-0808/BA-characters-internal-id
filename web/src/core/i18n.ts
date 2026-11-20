// i18n 初始化封装：i18next + 静态词典（Vite 构建期内联，无 fetch、无异步初始化）
// 词典位于 ./locales/，key 采用嵌套风格（如 settings.language）
import i18next, { type TOptions } from 'i18next';
import zhCN from './locales/zh-CN.json';
import enUS from './locales/en-US.json';

// 初始化（main.ts 在其他模块之前调用一次），lang 来自界面设置
// init 是异步的：await 完成后再执行首次 applyI18n，否则 t() 会回落中文占位（刷新恢复语言时的加载顺序问题）
export async function initI18n(lang: string): Promise<void> {
  await i18next.init({
    lng: lang,
    // debug 伪语言不回落 zh-CN：缺失 key 原样返回键名，实现「显示键」调试模式
    fallbackLng: (code) => (code === 'debug' ? [] : ['zh-CN']),
    resources: {
      'zh-CN': { translation: zhCN },
      'en-US': { translation: enUS },
    },
    parseMissingKeyHandler: (key) => {
      // 缺失词条时回落显示 key 本身并汇总警告；debug 语言下缺失是预期行为，不警告
      if (i18next.language !== 'debug') {
        console.warn(`[i18n] 缺失词条: ${key}`);
      }
      return key;
    },
  });
  document.documentElement.lang = lang;
  // 首次应用静态标注文案；此后语言切换时由 languageChanged 再次触发
  i18next.on('languageChanged', () => applyI18n());
  applyI18n();
}

// 翻译函数透传（params 为插值参数，如 t('kivo.spineCount', { n: 3 })）
export function t(key: string, params?: TOptions): string {
  return i18next.t(key, params);
}

// 当前界面语言（用于数据侧多语言取名，如学校名）
export function getCurrentLang(): string {
  return i18next.language;
}

// 切换语言：changeLanguage + 同步 <html lang> + 广播 lang-changed（视图监听后重渲染动态文案）
export async function setLang(lang: string): Promise<void> {
  await i18next.changeLanguage(lang);
  document.documentElement.lang = lang;
  window.dispatchEvent(new Event('lang-changed'));
}

// 应用静态标注文案：data-i18n 替换文本内容；data-i18n-attrs 以 "attr:key" 逗号列表替换属性
// 示例：<input data-i18n-attrs="placeholder:table.search">
export function applyI18n(root: ParentNode = document): void {
  root.querySelectorAll<HTMLElement>('[data-i18n]').forEach(el => {
    el.textContent = t(el.dataset.i18n as string);
  });
  root.querySelectorAll<HTMLElement>('[data-i18n-attrs]').forEach(el => {
    el.dataset.i18nAttrs?.split(',').forEach(pair => {
      const [attr, key] = pair.split(':').map(s => s.trim());
      if (attr && key) el.setAttribute(attr, t(key));
    });
  });
}
