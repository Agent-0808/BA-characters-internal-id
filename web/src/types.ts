// 学生数据结构（CSV 行，25 列；语言列按数据可得性递减：kivo → jp → kr → en → tw → cn，
// 每种语言一组 family/given/skin 三元组，姓与名分离存储（不做拼接），
// 完整名称由前端经 nameFormat.ts 按各语言的括号/空格规则现算，仅 kivo 拼接时额外计入 spine_remark）
export interface StudentData {
  file_id: string;
  student_id: string;
  page_id: string;
  spine_id: string;
  spine_remark: string;
  family_kivo: string;
  given_kivo: string;
  skin_kivo: string;
  family_jp: string;
  given_jp: string;
  skin_jp: string;
  family_kr: string;
  given_kr: string;
  skin_kr: string;
  family_en: string;
  given_en: string;
  skin_en: string;
  family_tw: string;
  given_tw: string;
  skin_tw: string;
  family_cn: string;
  given_cn: string;
  skin_cn: string;
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
  showFamilyName: boolean;  // 完整名称中是否显示姓氏（关闭时仅显示名，为游戏内常态）
  boldFullName: boolean;  // 表格中 full_* 完整名称列是否加粗
  lang: string;        // 界面语言代码（zh-CN / en-US；debug 为显示键名的调试伪语言）
  containerWidth: number; // 主容器宽度 px（0 表示未自定义，使用 CSS 默认）
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
  sd_model_image: string;
  recollection_lobby_image: string;  // 回忆大厅背景 URL
  game_id: number;  // API character_datas.character_id，游戏机制侧 ID
  dev_name: string;  // API character_datas.dev_name，资源命名
}

// 学生数据结构 (来自 students.json，姓与名分离存储，后缀语言与 skin_x 一致：
// 无后缀/jp/kr/en/tw/cn；tw 取自 kivo 源字段 *_zh_tw)
export interface Student {
  id: number;
  family_name: string;
  given_name: string;
  family_name_jp: string;
  given_name_jp: string;
  family_name_kr: string;
  given_name_kr: string;
  family_name_en: string;
  given_name_en: string;
  family_name_tw: string;
  given_name_tw: string;
  family_name_cn: string;
  given_name_cn: string;
  school_id: number;
  pages: KivoPage[];
}

// 展开状态映射
export type ExpandState = Record<number, boolean>;
