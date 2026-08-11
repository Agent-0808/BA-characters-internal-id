import { BAClickFX } from 'ba-click-fx';
import { CLICK_FX_CONFIG } from './config.js';

// 全局单例：避免在两个页面分别创建多份
let fxInstance: BAClickFX | null = null;

/**
 * 初始化蔚蓝档案点击特效
 * 根据 CLICK_FX_CONFIG.enabled 决定是否启用，关闭时该函数为 no-op
 */
export function initClickFX(): void {
  if (!CLICK_FX_CONFIG.enabled) return;
  if (fxInstance) return; // 防止重复初始化

  fxInstance = new BAClickFX({
    themeColor: CLICK_FX_CONFIG.themeColor,
    clickEnabled: CLICK_FX_CONFIG.clickEnabled,
    trailEnabled: CLICK_FX_CONFIG.trailEnabled,
    trailAlways: CLICK_FX_CONFIG.trailAlways,
    opacity: CLICK_FX_CONFIG.opacity,
    scale: CLICK_FX_CONFIG.scale,
  });
}
