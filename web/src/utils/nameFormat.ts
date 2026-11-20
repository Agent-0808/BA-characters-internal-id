// 姓名拼接工具：数据中姓与名分离存储（爬虫不做拼接），
// 完整名称由前端按各语言的括号/空格规则自行拼接（与游戏内文案一致）
// 国服（cn）括号规则暂按繁中，待国服数据到手后仅需在 NAME_FORMAT 单点修正

// 各语言拼接规则：姓+名连接符 / 括号 / 括号前空格
// 示例: 聖園ミカ（応援団） / 桃井(女僕) / 모모이(메이드) / Momoi (Maid)
interface NameFormat {
  separator: string;        // 姓与名之间的连接符
  left: string;             // 左括号
  right: string;            // 右括号
  spaceBeforeBracket: boolean; // 括号前是否有空格（en 的空格同时是断行点）
}

const NAME_FORMAT: Record<string, NameFormat> = {
  jp: { separator: '', left: '（', right: '）', spaceBeforeBracket: false },
  tw: { separator: '', left: '(', right: ')', spaceBeforeBracket: false },
  cn: { separator: '', left: '(', right: ')', spaceBeforeBracket: false },
  kr: { separator: ' ', left: '(', right: ')', spaceBeforeBracket: false },
  en: { separator: ' ', left: '(', right: ')', spaceBeforeBracket: true },
};

// 获取语言对应的拼接规则（kivo 为源数据即 cn；未知语言回落 cn）
export function getNameFormat(lang: string): NameFormat {
  return NAME_FORMAT[lang === 'kivo' ? 'cn' : lang] ?? NAME_FORMAT.cn;
}

// 姓+名拼接：showFamily=false 时仅显示名（游戏内常态）；缺姓或缺名时直接返回另一个
export function joinName(family: string, given: string, lang: string, showFamily: boolean): string {
  if (!showFamily || !family) return given;
  if (!given) return family;
  return family + getNameFormat(lang).separator + given;
}

// 完整名称：joinName 结果 + 括号包裹 [skin, remark]（逗号连接，均空则不加括号）
export function fullNameOf(
  family: string, given: string, skin: string, remark: string,
  lang: string, showFamily: boolean
): string {
  const base = joinName(family, given, lang, showFamily);
  if (!base) return '';
  const parts = [skin, remark].filter(s => s);
  if (parts.length === 0) return base;
  const fmt = getNameFormat(lang);
  const prefix = fmt.spaceBeforeBracket ? ' ' : '';
  return `${base}${prefix}${fmt.left}${parts.join(',')}${fmt.right}`;
}
