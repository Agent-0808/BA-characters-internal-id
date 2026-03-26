// 学生数据结构
export interface StudentData {
  file_id: string;
  student_id: string;
  page_id: string;
  spine_id: string;
  full_name: string;
  name: string;
  skin_name: string;
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
  label: string;
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
  updateDate: string;
  version: string;
  releaseUrl: string;
}

// 应用配置
export interface AppConfig {
  csvUrl: string;
  metadataUrl: string;
  schoolsUrl: string;
  repoOwner: string;
  repoName: string;
}

// 学校数据
export interface School {
  id: number;
  name: string;
  name_cn: string;
  logo: string;
}
