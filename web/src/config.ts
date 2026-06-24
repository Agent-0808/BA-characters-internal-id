import type { AppConfig, ColumnConfig } from './types.js';

// 配置 - 使用本地嵌入的数据
export const CONFIG: AppConfig = {
  csvUrl: './data/students_data.csv',
  metadataUrl: './data/metadata.json',
  schoolsUrl: './data/schools.json',
  studentsUrl: './data/students.json',
  repoOwner: 'Agent-0808',
  repoName: 'BA-characters-internal-id'
};

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
