// 学生数据结构
export interface StudentData {
  file_id: string;
  student_id: string;
  page_id: string;
  spine_id: string;
  full_name: string;
  name: string;
  skin_kivo: string;
  spine_remark: string;
  name_cn: string;
  name_jp: string;
  name_tw: string;
  name_en: string;
  name_kr: string;
  school_id: string;
  school_name: string;
  [key: string]: string;  // 允许其他字符串属性
}

// 列配置
export interface ColumnConfig {
  key: keyof StudentData;
  label: string;  // i18n key（如 col.file_id），渲染时经 t() 翻译
  defaultVisible: boolean;
}

// 排序状态
export interface SortState {
  column: keyof StudentData | null;
  direction: 'asc' | 'desc';
}

// 列可见性映射
export type ColumnVisibility = Record<keyof StudentData, boolean>;

// GitHub Release 信息
export interface ReleaseInfo {
  published_at: string;
  html_url: string;
}

// 元数据
export interface Metadata {
  repoUrl: string;
  updateDate: string;
  codeVersion: string;  // 爬虫版本（crawler/VERSION）
  releaseUrl: string;
  webCommitHash: string;  // 页面部署对应的 main 提交，展示时截取前6位
  dataCommitHash: string;  // data 分支最新提交，展示时截取前6位
}

// 应用配置
export interface AppConfig {
  csvUrl: string;
  metadataUrl: string;
  schoolsUrl: string;
  studentsUrl: string;
  repoOwner: string;
  repoName: string;
}

// 界面设置（localStorage 持久化，与特效无关的通用开关）
export interface UiSettings {
  spineLink: boolean;  // spine_id 是否渲染为 KivoWiki API 链接
  lang: string;        // 界面语言代码（zh-CN / en-US；debug 为显示键名的调试伪语言）
}

// 蔚蓝档案点击特效配置 (ba-click-fx)
// 关闭特效时把 enabled 置为 false 即可，无需改其它代码
export interface ClickFXConfig {
  enabled: boolean;
  themeColor: string; // 主题色，6位十六进制，例如 '#4ca7ff'
  clickEnabled: boolean; // 是否启用点击特效
  trailEnabled: boolean; // 是否启用鼠标拖尾
  trailAlways: boolean; // true：仅移动鼠标也会显示拖尾（无需按下）
  opacity: number; // 整体不透明度 0~1
  scale: number; // 全局缩放
}

// 学校数据
export interface School {
  id: number;
  name: string;
  name_cn: string;
  name_en?: string;  // 多语言字段由爬虫从 school_names.json 合并，后续语言按需扩展（name_jp 等）
  logo: string;
}

// Page (页面) 数据结构
export interface KivoPage {
  page_id: number;
  skin: string;
  skin_cn: string;
  skin_jp: string;
  skin_tw: string;
  skin_en: string;
  skin_kr: string;
  avatar: string;
  spines: number[];
  is_install: boolean;
  is_install_cn: boolean;
  is_install_global: boolean;
  is_npc: boolean;
  rarity: number;
  limited: boolean;
}

// 学生数据结构 (来自 students.json)
export interface Student {
  id: number;
  name: string;
  name_cn: string;
  name_jp: string;
  name_en: string;
  name_kr: string;
  name_tw: string;
  school_id: number;
  pages: KivoPage[];
}

// 展开状态映射
export type ExpandState = Record<number, boolean>;
