// 解析单行 CSV
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current);
  return result;
}

// 解析 CSV
export function parseCSV<T extends Record<string, string>>(text: string): T[] {
  const lines = text.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim());
  const result: T[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row = {} as Record<string, string>;
    headers.forEach((header, index) => {
      row[header] = values[index]?.trim() || '';
    });
    result.push(row as T);
  }

  return result;
}
