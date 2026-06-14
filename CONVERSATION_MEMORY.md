# Conversation Memory

## 用户目标

- 建立每天 09:15 自动执行的 AI 研究日报。
- 日报要求 Markdown 和 HTML 两种产物。
- 邮件正文发送 HTML，不发送 Markdown 附件。
- 页面风格要求简约彩色。
- 日历门户作为单独入口，日报页面保持独立。
- 不保留“我的信息”模块。
- 不允许虚构数据；抓不到的数据源需要明确说明。

## 已确定的数据源方向

- Claude Blog。
- OpenAI Research。
- Tavily / tvly-cli 本地搜索能力。
- GitHub AI/Agent 高星和高涨星项目。
- AG-UI、Agent 设计、Workflow、RAG、Memory 等技术线索。
- 微信 Top20 AI 公告号。
- Twitter 美国 6 大科技公司 AI 动态。
- 10 大证券公司 AI 研报。

## 已确定的邮件策略

- 收件人：`845761321@qq.com`。
- SMTP 用户：`yaogemail@gmail.com`。
- SMTP 密码类型：Gmail 应用专用密码。
- 运行方式：Codex/自动化主流程只生成终端邮件队列，随后由本地终端发送器发送。
- TLS 修复：优先使用系统 CA；必要时可设置 `DAILY_AI_BRIEF_SMTP_INSECURE=1` 兜底。
- 发送历史：2026-06-14 手动测试队列文件 `daily_ai_terminal_20260614_104444_907970.json`，发送结果 `SUCCESS`。

## 已确定的状态策略

- 状态记录默认只保留最近 14 条。
- 如果当天和前 7 天有关系，需要合并并说明今日差异。
- 如果电脑在 09:15 没开机，后续运行时通过 `DAILY_AI_BRIEF_CATCHUP_DAYS=3` 追溯补齐最近 3 天缺失日报。

## 迁移记录

- 新项目路径：`/Users/jiubao/Desktop/codex_workplace/AI_daily_report`。
- 来源路径：`/Users/jiubao/Desktop/workplace/projects/selfTrading`。
- 已复制主脚本、邮件发送器、状态文件、浏览器快照、历史 report、邮件队列和自动化配置快照。
- 后续如果从新项目继续维护，应优先以本目录为准。

## 当前自动化修正

- 原问题：`DAILY_AI_BRIEF_EMAIL_TO` 未配置，导致 `EMAIL: skip`，邮件未发送。
- 修正：自动化 prompt 已加入 `DAILY_AI_BRIEF_EMAIL_TO=845761321@qq.com`。
- 进一步修正：自动化只执行 runner 输出的 `EMAIL_TERMINAL_CMD`，不发送无关历史队列。
