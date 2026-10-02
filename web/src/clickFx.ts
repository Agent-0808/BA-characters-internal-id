import { BAClickFX } from 'ba-click-fx';
import { loadClickFxConfig } from './config.js';
import type { ClickFXConfig } from './types.js';

// 全局单例：避免在两个页面分别创建多份
let fxInstance: BAClickFX | null = null;
// 生效配置（用户保存值优先于硬编码默认），供懒创建实例与设置面板使用
let activeConfig: ClickFXConfig = loadClickFxConfig();

// 按生效配置创建特效实例
function createInstance(): void {
  fxInstance = new BAClickFX({
    themeColor: activeConfig.themeColor,
    clickEnabled: activeConfig.clickEnabled,
    trailEnabled: activeConfig.trailEnabled,
    trailAlways: activeConfig.trailAlways,
    opacity: activeConfig.opacity,
    scale: activeConfig.scale,
  });
}

/**
 * 初始化蔚蓝档案点击特效
 * 根据 activeConfig.enabled 决定是否启用，关闭时该函数为 no-op
 */
export function initClickFX(): void {
  if (fxInstance) return; // 防止重复初始化
  if (activeConfig.enabled) createInstance();
}

// 启用/禁用特效：禁用时销毁实例，重新启用时懒创建
export function setClickFxEnabled(enabled: boolean): void {
  activeConfig.enabled = enabled;
  if (!enabled) {
    fxInstance?.destroy();
    fxInstance = null;
  } else if (!fxInstance) {
    createInstance();
  }
}

// 运行时更新特效参数（enabled/themeColor 除外，二者有专用接口）
export function updateClickFxConfig(
  patch: Partial<Omit<ClickFXConfig, 'enabled' | 'themeColor'>>
): void {
  Object.assign(activeConfig, patch);
  if (!fxInstance && activeConfig.enabled) {
    createInstance();
  } else {
    fxInstance?.updateConfig(patch);
  }
}

// 运行时更新主题色（6 位十六进制，非法值由库回落默认蓝）
export function setClickFxThemeColor(color: string): void {
  activeConfig.themeColor = color;
  if (!fxInstance && activeConfig.enabled) {
    createInstance();
  } else {
    fxInstance?.setThemeColor(color);
  }
}

// 读取当前生效配置（设置面板初始化控件用）
export function getActiveClickFxConfig(): ClickFXConfig {
  return activeConfig;
}
