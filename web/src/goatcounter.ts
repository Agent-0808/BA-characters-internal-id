// GoatCounter 访问统计封装：统计脚本在 index.html 中以 async 引入，并通过
// data-goatcounter-settings 的 no_onload 关闭脚本默认的整站上报。
// 改为在视图切换时调用 countView() 手动上报，使 #index / #kivonavi
// 两个子页面在后台以不同路径区分；加载失败静默放弃，不影响页面功能
interface GoatCounter {
  count: (vars?: { path?: string; title?: string; event?: boolean }) => void;
}

declare global {
  interface Window {
    goatcounter?: GoatCounter;
  }
}

const POLL_INTERVAL_MS = 100;
const POLL_TIMEOUT_MS = 10000;

// 等待统计脚本就绪（结果缓存，加载失败返回 null）
let counterPromise: Promise<GoatCounter['count'] | null> | null = null;

function getCounter(): Promise<GoatCounter['count'] | null> {
  counterPromise ??= new Promise((resolve) => {
    const start = Date.now();
    const timer = window.setInterval(() => {
      if (window.goatcounter?.count) {
        window.clearInterval(timer);
        resolve(window.goatcounter.count.bind(window.goatcounter));
      } else if (Date.now() - start > POLL_TIMEOUT_MS) {
        window.clearInterval(timer);
        resolve(null);
      }
    }, POLL_INTERVAL_MS);
  });
  return counterPromise;
}

// 每个页面加载（session）内，每个视图只上报一次（a → b → a 只计 a、b 各一次）
const countedViews = new Set<string>();

// 规范化基础路径：去掉结尾的 index.html 与尾部斜杠，
// 避免 /、/index.html、双斜杠等不同访问形态把同一视图的统计拆散
const BASE_PATH = location.pathname.replace(/\/index\.html?$/i, '').replace(/\/+$/, '');

// 上报一次视图访问：index 使用规范化路径，其余视图追加 /view/视图名 后缀以示区分
export function countView(view: string): void {
  if (countedViews.has(view)) return;
  countedViews.add(view);
  void getCounter().then((count) => {
    if (!count) return;
    count({ path: view === 'index' ? BASE_PATH : `${BASE_PATH}/view/${view}` });
  });
}
