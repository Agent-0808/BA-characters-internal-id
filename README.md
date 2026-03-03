# BA-characters-internal-id

整理了《碧蓝档案》/《蔚蓝档案》/Blue Archive 中角色对应的文件ID，方便解包时确定对应文件名。

部署页面：[https://agent-0808.github.io/BA-characters-internal-id/](Github Pages)

可以从Release中下载最新版本的`students_data.csv`文件。

### 部署

- 每天北京时间3点（UTC 19:00）自动检查，如有更新则会发布新release
- 配有缓存机制，减少API请求次数
- 请求频率真的很低

### 使用示例

Steam版的文件储存路径为`BlueArchive\BlueArchive_Data\StreamingAssets\PUB\Resource\GameData\Windows\`目录

- 查表得，`霞沢 ミユ`的文件ID为 `CH0145`，则可以获取其对应文件为：
  - 人物模型：`assets-_mx-characters-ch0145-_mxdependency-*_assets_all_*.bundle`
  - 人物立绘：`assets-_mx-spinecharacters-ch0145_spr-_mxdependency-*_assets_all_*.bundle`
  - 回忆大厅：`assets-_mx-spinelobbies-ch0145_home-_mxdependency-*_assets_all_*.bundle`
  - .....

手机版文件名同理

### 致谢

- 信息来自 [基沃托斯古书馆](https://kivo.wiki)，感谢
- API用法参考：[说明](https://github.com/Agent-0808/bluearchive-api-kivowiki)（[原仓库](https://github.com/Dale233/bluearchive-api-kivowiki)）
- 代码由AI编写
- 网页也是AI搞的，我完全不懂前端orz