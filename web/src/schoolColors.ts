// 学校标签配色配置：按学校 id（对应 schools.json 中的 id）指定 school-tag 的底色与文字色
// 颜色由使用者自行填写；未列出的学校使用 SCHOOL_COLOR_FALLBACK（即原有黄色样式）
// 建议底色用浅色、文字用同色系深色，保证可读性

// 单个学校的标签配色
export interface SchoolColor {
  bg: string;    // 标签底色
  text: string;  // 标签文字（与图标）颜色
}

// 未配置学校的兜底配色（原有黄色药丸样式）
export const SCHOOL_COLOR_FALLBACK: SchoolColor = {
  bg: '#fef3c7',
  text: '#92400e',
};

// 学校 id → 标签配色
export const SCHOOL_COLORS: Record<number, SchoolColor> = {
  1: { bg: '#f5ecd7', text: '#8a6d1f' },  // 阿比多斯高中
  2: { bg: '#f6eab9', text: '#826d00' },  // 圣三一综合学园
  3: { bg: '#ffcbc7', text: '#7d160f' },  // 格赫娜学园
  4: { bg: '#d3e4ff', text: '#003ca4' },  // 千禧科学学院
  5: { bg: '#fde3e3', text: '#b03434' },  // 红冬联邦学园
  6: { bg: '#e2acff', text: '#1486ff' },  // 夏莱
  7: { bg: '#e2f2e0', text: '#2f7a34' },  // 山海经高级中学
  8: { bg: '#dce8f8', text: '#2c5282' },  // 瓦尔基里警察学校
  9: { bg: '#ebd6ec', text: '#9b185c' },  // 百鬼夜行联合学园
  10: { bg: '#e0e0e6', text: '#4a4a55' }, // 阿里乌斯分校
  11: { bg: '#eee6dc', text: '#7a5a34' }, // SRT特殊学园
  12: { bg: '#e8e8f8', text: '#4a4a9e' }, // 联邦学生会
  13: { bg: '#fdeee0', text: '#a05e20' }, // 克罗诺斯学校
  14: { bg: '#bda96a', text: '#112134' }, // 狂猎艺术学院
  15: { bg: '#ddf0ee', text: '#1e7a72' }, // 奥德赛海洋学校
  16: { bg: '#fff2d9', text: '#9a7418' }, // 天使24便利店
  17: { bg: '#e3e3ef', text: '#55557a' }, // 不良少女们
  18: { bg: '#eef0e8', text: '#5f6b4f' }, // 地方组织与居民
  19: { bg: '#e8e0d4', text: '#6e5c40' }, // 凯撒集团
  20: { bg: '#8994b0', text: '#f7f9ff' }, // 数秘术
  21: { bg: '#180a0a', text: '#feb8b7' }, // 神名十文字
  22: { bg: '#172d52', text: '#bdeefe' }, // 史林皮亚
  23: { bg: '#192d6d', text: '#9192e4' }, // 怪谈的无限图书馆
  24: { bg: '#b7d2cf', text: '#232f46' }, // 海兰德铁道学园
  25: { bg: '#ececec', text: '#616161' }, // 所属存疑
  26: { bg: '#ffe9f0', text: '#b04a6e' }, // 联动作品
  27: { bg: '#1c222f', text: '#faf6cd' }, // 诸圣相通
  28: { bg: '#dd5674', text: '#2f2a29' }, // 幻魉百物语
  29: { bg: '#111c3b', text: '#9ae1f6' }, // 星界诸神
  30: { bg: '#f0f0f0', text: '#666666' }, // 制作组相关
};

// 查询学校标签配色：未配置或 id 无效时返回兜底配色
export function getSchoolColor(schoolId: number): SchoolColor {
  if (!Number.isInteger(schoolId)) return SCHOOL_COLOR_FALLBACK;
  return SCHOOL_COLORS[schoolId] ?? SCHOOL_COLOR_FALLBACK;
}
