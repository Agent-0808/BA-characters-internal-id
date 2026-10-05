// 学校显示名：按当前界面语言取 schools.json 的多语言字段（name_{lang}，由爬虫从 school_names.json 合并）
// 与 schoolColors.ts 同为两页共享的学校数据模块

import type { School } from './types.js';
import { t, getCurrentLang } from './i18n.js';

// 界面语言 -> School 多语言字段映射；新增界面语言时在此登记（如 'ja-JP': 'name_jp'）
const LANG_FIELD: Record<string, 'name_en' | undefined> = {
  'en-US': 'name_en',
};

// 取学校显示名：当前语言有非空译文则用之，否则回落 Kivo 源数据 name
export function getSchoolDisplayName(school: School | undefined): string {
  if (!school) return t('common.unknown');
  const field = LANG_FIELD[getCurrentLang()];
  const translated = field ? school[field] : undefined;
  return translated || school.name;
}
