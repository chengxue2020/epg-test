# EPG Merger

自动合并多个EPG源的频道节目信息

## 功能特性

- ✅ 支持 `.xml` 和 `.xml.gz` 格式的EPG源
- ✅ 自动去重（频道和节目）
- ✅ 支持多源合并
- ✅ 时间范围过滤
- ✅ 自动重试机制
- ✅ GitHub Actions 自动更新

## 使用方法

1. 编辑 `source_epg.txt` 配置文件
2. 运行脚本：`python epg_merger.py`
3. 生成的 `epg.xml` 即为合并后的EPG文件

## 配置文件格式
