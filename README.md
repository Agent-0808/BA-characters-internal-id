# BA-characters-internal-id

整理了《碧蓝档案》/《蔚蓝档案》/Blue Archive 中角色对应的 Spine 文件ID，方便解包时确定对应文件名。

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![License: CC BY-SA 4.0](https://img.shields.io/badge/License-CC%20BY--SA%204.0-brightgreen.svg)](LICENSE-DATA)

[![GitHub Downloads](https://img.shields.io/github/downloads/Agent-0808/BA-characters-internal-id/total)](https://github.com/Agent-0808/BA-characters-internal-id/releases)
[![GitHub Downloads (latest)](https://img.shields.io/github/downloads/Agent-0808/BA-characters-internal-id/latest/total)](https://github.com/Agent-0808/BA-characters-internal-id/releases)

![GitHub last commit (branch)](https://img.shields.io/github/last-commit/Agent-0808/BA-characters-internal-id/data)
![GitHub Release](https://img.shields.io/github/v/release/Agent-0808/BA-characters-internal-id?display_name=release)

### 数据

- 本项目的数据文件（包括缓存内容与输出文件）在 `data` 分支中保存。
- 可以从 [Release](https://github.com/Agent-0808/BA-characters-internal-id/releases) 中下载各版本的数据文件。
- 数据文件也在 [Github Pages](https://agent-0808.github.io/BA-characters-internal-id/data/) 中托管。
- 如果您需要直接从URL获取数据（例如被其他项目引用），推荐使用 Github Pages 链接以绕过 Github API 限制。

#### 在线查看

- 在 [Github Pages](https://agent-0808.github.io/BA-characters-internal-id/) 网页在线预览
- 在 [Github Gist](https://gist.github.com/Agent-0808/c487982029230a9d046e32aaf4d3beb7) 在线预览纯文本 Markdown 表格

#### 文件内容

- [students.json](https://agent-0808.github.io/BA-characters-internal-id/data/students.json)：所有角色的信息列表，字段化，作为中间格式，引用spine文件ID。
- [spines.json](https://agent-0808.github.io/BA-characters-internal-id/data/spines.json)：所有spine文件ID的列表，包含 Spine 变体的编号与名称。
- [schools.json](https://agent-0808.github.io/BA-characters-internal-id/data/schools.json)：学校信息，包含名称与徽标（URL）。
- [students_data.csv](https://agent-0808.github.io/BA-characters-internal-id/data/students_data.csv)：扁平化的角色信息列表，将 Spine 信息进行合并。（主要使用）
- [skipped_ids.csv](https://agent-0808.github.io/BA-characters-internal-id/data/skipped_ids.csv)：因各种因素跳过不获取信息的列表，仅做参考，不受支持。


### 部署

- 每天北京时间3点（UTC 19:00）自动检查，若 `students.json` 数据更新则会发布新release
- 配有缓存机制，非必要不请求，减少API请求次数
- 请求频率真的很低，3秒只发1次请求

### 使用示例

Steam 版的文件储存路径为`BlueArchive\BlueArchive_Data\StreamingAssets\PUB\Resource\GameData\Windows\`目录

- 查表得，`霞沢 ミユ`的文件ID为 `CH0145`，则可以获取其对应文件为：
  - 人物模型：`assets-_mx-characters-ch0145-_mxdependency-*_assets_all_*.bundle`
  - 人物立绘：`assets-_mx-spinecharacters-ch0145_spr-_mxdependency-*_assets_all_*.bundle`
  - 回忆大厅：`assets-_mx-spinelobbies-ch0145_home-_mxdependency-*_assets_all_*.bundle`
  - .....

手机版文件名同理。

### 致谢

- 信息来自 [基沃托斯古书馆](https://kivo.wiki)，感谢
- API用法参考：[说明](https://github.com/Agent-0808/bluearchive-api-kivowiki)（[原仓库](https://github.com/Dale233/bluearchive-api-kivowiki)）
- 代码由AI编写
- 网页也是AI搞的，我完全不懂前端orz

### 许可协议

本项目采用双许可协议：

- **代码**：采用 [MIT 协议](LICENSE)，您可以自由使用、修改和分发代码
- **数据**：采用 [CC BY-SA 4.0 协议](LICENSE-DATA)，与数据源 [基沃托斯古书馆](https://kivo.wiki)（ [数据许可协议](https://kivo.wiki/license)）保持一致

本项目仅缓存和处理文本数据，不包含任何游戏内的图像、模型、音频等二进制资源。所有《蔚蓝档案》游戏素材版权归 Nexon 和 Yostar 所有。