# AI Daily Report

本项目从 `/Users/jiubao/Desktop/workplace/projects/selfTrading` 拆出，用于独立运行 AI 研究日报。

## 核心功能

- 每日 AI 研究日报生成：输出 Markdown 和 HTML。
- 官方优先渠道：Claude Blog、OpenAI Research 最近一周内容。
- AI 搜索补充：通过本地 Tavily/tvly-cli 逻辑和已有搜索渠道补齐新闻线索。
- GitHub 趋势项目：关注星标超过 1 万、近一周涨星较高的 AI/Agent 工具。
- AG-UI / Agent / Workflow / RAG / Memory 等方向跟踪。
- 微信 AI 公告号、Twitter 美国科技公司、券商 AI 研报作为扩展数据源，抓不到时不伪造数据。
- 日报 HTML 展示：简约彩色风格，适合本地浏览。
- 日历门户：按年月日展示历史日报，点击日期查看对应日报。
- 邮件发送：日报 HTML 作为邮件正文，通过本地终端 SMTP 发送。
- 记录保留：状态记录默认保留最近 14 条。
- 缺失补跑：通过 `DAILY_AI_BRIEF_CATCHUP_DAYS` 追溯补齐最近 N 天。
- 关联合并：每日记录会和前 7 天有关联的数据合并，并说明今日差异。

## 关键文件

- `daily_ai_brief_runner.py`：主流程，负责抓取、生成 Markdown/HTML、更新状态、生成门户、写邮件队列。
- `daily_ai_terminal_sender.py`：终端邮件发送器，读取 `daily_ai_terminal_unsent/*.json` 并通过 SMTP 发送。
- `daily_ai_brief_state.json`：日报历史状态、合并关系、保留策略记录。
- `daily_ai_brief_browser_snapshot.json`：浏览器补充抓取快照。
- `report/`：日报 Markdown/HTML 和日历门户输出目录。
- `daily_ai_terminal_unsent/`：终端邮件待发送队列。
- `daily_ai_unsent/`：非终端邮件失败队列。
- `automation_daily-ai-brief-local/automation.toml`：从 Codex 自动化复制过来的定时任务配置快照。
- `automation_daily-ai-brief-local/memory.md`：自动化运行记录快照。
- `CONVERSATION_MEMORY.md`：本次迁移和会话上下文摘要。

## 手动运行

```bash
cd /Users/jiubao/Desktop/codex_workplace/AI_daily_report
DAILY_AI_BRIEF_EMAIL_TO=845761321@qq.com \
DAILY_AI_BRIEF_SMTP_USER=yaogemail@gmail.com \
DAILY_AI_BRIEF_SMTP_PASSWORD='your-gmail-app-password' \
DAILY_AI_BRIEF_SEND_EMAIL=1 \
DAILY_AI_BRIEF_EMAIL_TRANSPORT=smtp \
DAILY_AI_BRIEF_CATCHUP_DAYS=3 \
python3 daily_ai_brief_runner.py
```

如果发送未成功或配置了终端模式，则执行 `EMAIL_TERMINAL_CMD` 并带上 SMTP 凭据：

```bash
DAILY_AI_BRIEF_SMTP_USER=xxx@gmail.com \
DAILY_AI_BRIEF_SMTP_PASSWORD='your-gmail-app-password' \
python3 daily_ai_terminal_sender.py --file daily_ai_terminal_unsent/<queue-file>.json
```

## 自动化策略

- 当前定时：每天 09:15。
- 收件人：`845761321@qq.com`。
- 邮件内容：日报 HTML 正文。
- 发送策略：只发送本次 runner 输出的 `EMAIL_TERMINAL_CMD` 对应队列文件，避免误发历史队列。
- 本地机器关闭时无法真正执行本地任务；开机后由补跑窗口追溯最近 3 天缺失日报。

## 注意事项

- 抓取不到的数据源必须明确标注不可达或为空，不得编造文档和数据。
- Gmail 使用应用专用密码，不使用普通账号密码。
- `daily_ai_terminal_sender.py` 已支持系统 CA 路径和 `DAILY_AI_BRIEF_SMTP_INSECURE=1` 兜底。
- 旧项目路径仍保留一份原始副本；后续建议把自动化 cwd 切到本项目。
