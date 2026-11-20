// 全局搜索的通用匹配逻辑，供 table.ts 与 kivonavi.ts 复用：
// 查询按空白分词，各词都需命中（AND，可实现 "mika swim" 多词查询）。
// 拉丁文本按非字母数字分词后做词前缀匹配（搜 Mika 命中 Misono Mika，
// 不意外命中 Umika / Sumika 等词中子串）；其余文本与数字保留子串匹配。

// 单个查询词是否命中：任一子串文本包含该词，或任一拉丁文本的任一 token 以该词开头
function matchTerm(substringTexts: (string | undefined)[], latinTexts: (string | undefined)[], term: string): boolean {
  if (substringTexts.some(v => v?.toLowerCase().includes(term))) return true;
  return latinTexts.some(v =>
    (v ?? '').toLowerCase().split(/[^a-z0-9]+/).some(token => token.startsWith(term))
  );
}

// 查询整体是否命中一行数据
export function matchesSearch(
  substringTexts: (string | undefined)[],
  latinTexts: (string | undefined)[],
  query: string
): boolean {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return true;
  return terms.every(term => matchTerm(substringTexts, latinTexts, term));
}
