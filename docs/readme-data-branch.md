# BA-characters-internal-id 数据分支

> 本分支由 GitHub Actions 自动生成，请勿直接修改。任何手工提交都会在下一次数据更新时被整体覆盖。

整理了《碧蓝档案》/《蔚蓝档案》/Blue Archive 中角色对应的 Spine 文件 ID，方便解包时确定对应文件名。

- 仓库地址：<https://github.com/Agent-0808/BA-characters-internal-id>
- 在线预览：<https://agent-0808.github.io/BA-characters-internal-id/>
- 历史版本：<https://github.com/Agent-0808/BA-characters-internal-id/releases>

## 目录结构

```
output/             已发布的数据产物
  students_data.csv   扁平化的角色信息列表，将 Spine 信息合并（主要使用）
  students.json       角色信息列表，字段化，作为中间格式，引用 Spine 文件 ID
  schools.json        学校信息，包含名称与徽标 URL
  spines.json         所有 Spine 文件 ID 的列表，包含变体的编号与名称
  skipped_ids.csv     因各种因素跳过不获取信息的列表，仅做参考，不受支持
cache/              爬取缓存（增量更新用，不属于数据产物）
LICENSE-DATA        数据许可协议（CC BY-SA 4.0）
README.md           本文件
```

数据来源：[基沃托斯古书馆](https://kivo.wiki)（[数据许可协议](https://kivo.wiki/license)）。

## students_data.csv

扁平化的角色信息列表，一行对应一个 Spine 文件，共 25 列。语言列按数据可得性递减排列（kivo → jp → kr → en → tw → cn），每种语言一组 `full_*` / `name_*` / `skin_*` 三元组。

| 列              | 说明                                 |
| -------------- | ---------------------------------- |
| `file_id`      | 文件 ID，与 Spine 文件一一对应             |
| `student_id`   | 角色 ID，取该角色所有页面 ID 的最小值             |
| `page_id`      | KivoWiki 页面 ID，一个角色可能有多个页面（对应不同皮肤） |
| `spine_id`     | Spine 资源 ID，一个页面可能有多个 Spine        |
| `spine_remark` | Spine 备注，数据源提供的中文备注经规则清洗后的结果（仅拼入 `full_kivo`） |
| `full_kivo`    | 完整名称（kivo 源语言），角色名 + 皮肤名 + 备注      |
| `name_kivo`    | 角色名（kivo 源数据，不含皮肤名）                |
| `skin_kivo`    | 皮肤名（kivo 源数据）                      |
| `full_jp`      | 日文完整名，角色名 + 皮肤名（不含备注）              |
| `name_jp`      | 日文角色名                              |
| `skin_jp`      | 日文皮肤名                              |
| `full_kr`      | 韩文完整名，角色名 + 皮肤名（不含备注）              |
| `name_kr`      | 韩文角色名                              |
| `skin_kr`      | 韩文皮肤名                              |
| `full_en`      | 英文完整名，角色名 + 皮肤名（不含备注）              |
| `name_en`      | 英文角色名                              |
| `skin_en`      | 英文皮肤名（由词表映射）                       |
| `full_tw`      | 繁体中文完整名，角色名 + 皮肤名（不含备注）            |
| `name_tw`      | 繁体中文角色名                            |
| `skin_tw`      | 繁体中文皮肤名                            |
| `full_cn`      | 简体中文（国服）完整名，角色名 + 皮肤名（不含备注）        |
| `name_cn`      | 简体中文（国服）角色名                        |
| `skin_cn`      | 简体中文（国服）皮肤名                        |
| `school_id`    | 学校 ID                              |
| `school_name`  | 学校名称                               |

拼接规则：

- `full_kivo` = `name_kivo` + `skin_kivo` + `spine_remark`，格式为「角色名（皮肤名,备注）」；
- 其余 `full_x` = `name_x` + `skin_x`，格式同上但不拼备注（备注为数据源人工标注的中文，难以对应翻译）；
- 皮肤名或备注为空时自动省略对应部分，皮肤名为空时 `full_x` 即纯角色名；
- 某语言角色名缺失时，该语言的 `name_x` / `full_x` 输出空串（`name_kivo` 必有值）。


## students.json

角色信息列表，字段化组织，页面的 `spines` 数组引用 `spines.json` 中的 ID。名字段按数据可得性递减排列（jp → kr → en → tw → cn）。以下为一个角色的示例：

```json
{
  "id": 6,
  "name": "月雪 宫子",
  "name_jp": "月雪 ミヤコ",
  "name_kr": "츠키유키 미야코",
  "name_en": "Tsukiyuki Miyako",
  "name_tw": "月雪 都子",
  "name_cn": "月雪 宫子",
  "school_id": 11,
  "pages": [
    {
      "page_id": 6,
      "skin": "",
      "skin_jp": "",
      "skin_kr": "",
      "skin_en": "",
      "skin_tw": "",
      "skin_cn": "",
      "avatar": "//static.kivo.wiki/images/students/%E6%9C%88%E9%9B%AA%E5%AE%AB%E5%AD%90/avatar.png",
      "spines": [506, 345, 189, 598],
      "is_install": true,
      "is_install_cn": true,
      "is_install_global": true,
      "is_npc": false,
      "rarity": 3,
      "limited": false
    },
    {
      "page_id": 280,
      "skin": "泳装",
      "skin_jp": "水着",
      "skin_kr": "수영복",
      "skin_en": "Swimsuit",
      "skin_tw": "泳裝",
      "skin_cn": "泳装",
      "avatar": "//static.kivo.wiki/images/students/%E6%9C%88%E9%9B%AA%20%E5%AE%AB%E5%AD%90/%E6%B3%B3%E8%A3%85/avatar.png",
      "spines": [1473, 526],
      "is_install": true,
      "is_install_cn": true,
      "is_install_global": true,
      "is_npc": false,
      "rarity": 3,
      "limited": false
    }
  ]
}
```

- 角色级字段：`id`（角色 ID）、`name`（kivo 源数据角色名）与 `name_jp` / `name_kr` / `name_en` / `name_tw` / `name_cn`（各语言角色名）、`school_id`、`pages`。
- 页面级字段：`page_id`、`skin` 与各语言皮肤名、`avatar`（头像 URL）、`spines`（Spine ID 列表）、`is_install` / `is_install_cn` / `is_install_global`（各服实装状态）、`is_npc`、`rarity`（星级）、`limited`（是否限定）。
- 页面级皮肤名：`skin` 为数据源原始命名（中文），`skin_jp` / `skin_kr` / `skin_en` / `skin_tw` / `skin_cn` 为各语言皮肤名（顺序同为数据可得性递减；en/kr 由维护词表映射，其余语言网站数据优先、词表兜底）；角色默认形态或未覆盖时为空字符串。

## 其他文件

- `schools.json`：学校 ID、名称与徽标 URL 的对应表，用于展示学校 logo。
- `spines.json`：全部 Spine ID 与变体编号、名称的对应表。
- `skipped_ids.csv`：爬取过程中被跳过的 ID 及原因，仅供参考。

## 更新机制

- 每天北京时间 3:00（UTC 19:00）自动检查一次数据源更新。
- 仅当 `students.json` 内容发生变化时，才会向本分支提交，并同步发布一个 Release（包含变更报告与数据文件）。
- 因此本分支的每一次提交都对应一个 Release，可以直接从提交历史回溯版本。

## 获取方式

- GitHub Pages：<https://agent-0808.github.io/BA-characters-internal-id/data/>（推荐，绕过 GitHub API 限制）
- Release 附件：<https://github.com/Agent-0808/BA-characters-internal-id/releases>
- 本分支的 `output/` 目录

## 许可协议

- **数据**：采用 [CC BY-SA 4.0](LICENSE-DATA) 协议，与数据源 [基沃托斯古书馆](https://kivo.wiki)（[数据许可协议](https://kivo.wiki/license)）保持一致。
- **代码**：采用 [MIT](https://github.com/Agent-0808/BA-characters-internal-id/blob/main/LICENSE) 协议，详见主仓库。

本项目仅缓存和处理文本数据，不包含任何游戏内的图像、模型、音频等二进制资源。所有《蔚蓝档案》游戏素材版权归 Nexon 和 Yostar 所有。
