# BA-characters-internal-id 数据分支

> [回到 main 分支](../../tree/main/)

> [!NOTE]
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
  skin_map.json       皮肤译名词表，原始皮肤名到各语言译名的映射（手动维护）
cache/              爬取缓存（增量更新用，不属于数据产物）
LICENSE-DATA        数据许可协议（CC BY-SA 4.0）
README.md           本文件
```

## 数据源信息说明

数据来源：[基沃托斯古书馆](https://kivo.wiki)（[数据许可协议](https://kivo.wiki/license)）。

- **`family_kivo`** **/** **`given_kivo`** **必有值**：数据源为百科网站，kivo 姓名拆分自页面名称，因此能够保证所有条目都拥有该名称。
- **`spine_remark`** **字段**：`spine_remark` 是数据源为每个 Spine 文件人工标注的中文备注，使得 `file_id` 能够真正被文本描述，是区分同一角色多个 Spine 文件时最有价值的字段。但它同样由百科编辑人工命名，存在随意性，且部分属于差分的变体也没有被标注。
- **信息完整度**：可玩学生角色的信息基本完整。NPC 类角色（各类市民、杂鱼酱等）的其他语言名称缺失较大，因为它们并没有官方名称。部分 NPC 角色的官方名称见于官方出版的设定集。
  - 部分早期页面信息填写较随意，例如存在许多将 `file_id` 填入日文姓名的情况，本仓库难以逐一处理，只能等待数据源更新。
- **各语言姓名的出现顺序**：新角色登场后，通常日文姓名（`family_jp` / `given_jp`）最先确定（日服进度最快）；随后是韩文与英文姓名（游戏开发团队为韩国团队，且日服游戏内含完整韩语脚本，因此可获得韩文名；英文名通常可由日语罗马字转写得到，故也较快）；繁中姓名需等国际服更新后才能确定；简中（国服）姓名来自进度最慢的国服，最为滞后。
  - 这也是各文件中语言列按「数据可得性递减」排列的依据。
- **手动维护的译名**：数据源仅提供 `skin`、`skin_jp`、`skin_tw`、`skin_cn` 四个皮肤名字段，学校名也只有 `name` 与 `name_cn`。因此 `skin_kr` / `skin_en` 由本项目手动维护的翻译词表直接映射，`skin_jp` / `skin_tw` / `skin_cn` 以数据源为准、缺失时由该词表兜底，学校英文名（`schools.json` 的 `name_en` 等）同样由手动维护的词表合并生成。翻译词表存放在 main 分支。

## students\_data.csv

扁平化的角色信息列表，一行对应一个 Spine 文件，共 31 列。语言列按数据可得性递减排列（kivo → jp → kr → en → tw → cn），每种语言一组 `full_*` / `family_*` / `given_*` / `skin_*` 四元组。姓与名分离存储；`full_*` 为由分量列按拼接规则生成的便利派生列，供直接使用（也可用分量列自行拼接，规则见下文）。

`kivo` 列为基沃托斯古书馆（KivoWiki）的主要使用名称，为简体中文的民间名称。由于其含有独有的`spine_remark`，因此其信息最充足，推荐将 `family_kivo` / `given_kivo`（配合 `skin_kivo` / `spine_remark`）作为简体中文名称使用。

| 列              | 说明                                            |
| -------------- | --------------------------------------------- |
| `file_id`      | 文件 ID，与 Spine 文件一一对应                          |
| `student_id`   | 角色 ID，取该角色所有页面 ID 的最小值                        |
| `page_id`      | KivoWiki 页面 ID，一个角色可能有多个页面（对应不同皮肤）            |
| `spine_id`     | Spine 资源 ID，一个页面可能有多个 Spine                   |
| `spine_remark` | Spine 备注，数据源提供的中文备注经规则清洗后的结果（拼接完整名称时与皮肤名一同使用） |
| `full_kivo`    | 完整名称（KivoWiki 源语言），派生列：姓名 + 皮肤名 + 备注          |
| `family_kivo`  | 姓氏（KivoWiki 源数据）                              |
| `given_kivo`   | 名字（KivoWiki 源数据）                              |
| `skin_kivo`    | 皮肤名（KivoWiki 源数据）                             |
| `full_jp`      | 日文完整名，派生列：姓名 + 皮肤名                            |
| `family_jp`    | 日文姓氏                                          |
| `given_jp`     | 日文名字                                          |
| `skin_jp`      | 日文皮肤名                                         |
| `full_kr`      | 韩文完整名，派生列：姓名 + 皮肤名                            |
| `family_kr`    | 韩文姓氏                                          |
| `given_kr`     | 韩文名字                                          |
| `skin_kr`      | 韩文皮肤名（手动维护）                                   |
| `full_en`      | 英文完整名，派生列：姓名 + 皮肤名                            |
| `family_en`    | 英文姓氏                                          |
| `given_en`     | 英文名字                                          |
| `skin_en`      | 英文皮肤名（手动维护）                                   |
| `full_tw`      | 繁体中文完整名，派生列：姓名 + 皮肤名                          |
| `family_tw`    | 繁体中文姓氏                                        |
| `given_tw`     | 繁体中文名字                                        |
| `skin_tw`      | 繁体中文皮肤名                                       |
| `full_cn`      | 简体中文（国服）完整名，派生列：姓名 + 皮肤名                      |
| `family_cn`    | 简体中文（国服）姓氏                                    |
| `given_cn`     | 简体中文（国服）名字                                    |
| `skin_cn`      | 简体中文（国服）皮肤名                                   |
| `school_id`    | 学校 ID                                         |
| `school_name`  | 学校名称                                          |

完整名称拼接规则（`full_*` 派生列的生成规则，消费端自行拼接时亦可参考）：

- 姓与名的连接：kivo / jp / tw / cn 直接连接不分隔（橘望 / 橘ノゾミ / 橘希望）；kr 与 en 以空格连接且 en 姓在前（타치바나 히카리、Tachibana Hikari）；缺姓时仅使用名；
- 括号规则（与游戏内文案一致）：jp 使用全角括号（ユウカ（体操服））；kivo / tw / kr / cn 使用半角括号且括号前无空格（愛麗絲(女僕)、아리스(메이드)、爱丽丝(女仆)）；en 使用半角括号且括号前有空格（Miyako (Swimsuit)）；
- 括号内为皮肤名与备注（备注仅 kivo 组的 `spine_remark` 有），两者以逗号连接；皮肤名与备注均为空时省略括号；
- 某语言姓名缺失时，该语言的 `family_x` / `given_x` 输出空串（`family_kivo` / `given_kivo` 必有值）。

> [!TIP]
> 名称拼接规则详见主分支中的 [nameFormat.ts](../../tree/main/web/src/nameFormat.ts)

## students.json

角色信息列表，字段化组织，页面的 `spines` 数组引用 `spines.json` 中的 ID。姓名以姓与名分离存储，各语言字段后缀按数据可得性递减排列（jp → kr → en → tw → cn），无后缀字段为 kivo 源数据。以下为一个角色的示例：

```json
{
  "id": 6,
  "family_name": "月雪",
  "given_name": "宫子",
  "family_name_jp": "月雪",
  "given_name_jp": "ミヤコ",
  "family_name_kr": "츠키유키",
  "given_name_kr": "미야코",
  "family_name_en": "Tsukiyuki",
  "given_name_en": "Miyako",
  "family_name_tw": "月雪",
  "given_name_tw": "都子",
  "family_name_cn": "月雪",
  "given_name_cn": "宫子",
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

- 角色级字段：`id`（角色 ID）、`family_name` / `given_name`（KivoWiki 源数据姓名，简体中文民间译名，无后缀）与 `family_name_jp` / `given_name_jp` 等各语言姓名（后缀 jp / kr / en / tw / cn 按数据可得性递减，`cn` 字段单独输出不做省略合并）、`school_id`、`pages`。
- 页面级字段：`page_id`、`skin` 与各语言皮肤名、`avatar`（头像 URL）、`spines`（Spine ID 列表）、`is_install` / `is_install_cn` / `is_install_global`（各服实装状态）、`is_npc`、`rarity`（星级）、`limited`（是否限定）。
- 页面级皮肤名：`skin` 为 KivoWiki 数据源原始命名（中文），`skin_jp` / `skin_kr` / `skin_en` / `skin_tw` / `skin_cn` 为各语言皮肤名（顺序同为数据可得性递减；en/kr 由本项目手动维护的翻译词表映射，其余语言网站数据优先、词表兜底）；角色默认形态或未覆盖时为空字符串。

## 其他文件

- `schools.json`：学校 ID、名称与徽标 URL 的对应表，用于展示学校 logo。多语言学校名（如 `name_en`）由本项目手动维护的翻译词表合并生成，数据源未提供译名。
- `spines.json`：全部 Spine ID 与变体编号、名称的对应表。
- `skipped_ids.csv`：爬取过程中被跳过的 ID 及原因，仅供参考。
- `skin_map.json`：手动维护的皮肤译名词表，键为数据源的原始皮肤名（`skin` 字段），值为 `jp` / `kr` / `en` / `tw` / `cn` 各语言译名的映射，维护于 main 分支的 `crawler/skin_map.json`。适用于翻译项目、游戏内显示等需要查询原始皮肤名对应译名的场景；`students.json` 与 `students_data.csv` 中的皮肤译名即由该词表兜底生成。

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
