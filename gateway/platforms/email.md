# Hermes Gateway 邮件平台

允许用户通过发送邮件与 Hermes Agent 交互。

## 架构

- **接收**: IMAP (轮询新邮件)
- **发送**: SMTP
- **邮件过滤**: 自动忽略 noreply、automated 来源的邮件

## 环境变量

| 变量 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `EMAIL_ADDRESS` | 是 | — | Agent 邮箱地址 |
| `EMAIL_PASSWORD` | 是 | — | 邮箱密码或 App 密码 |
| `EMAIL_IMAP_HOST` | 是 | — | IMAP 服务器 (如 `imap.gmail.com`) |
| `EMAIL_SMTP_HOST` | 是 | — | SMTP 服务器 (如 `smtp.gmail.com`) |
| `EMAIL_IMAP_PORT` | 否 | 993 | IMAP 端口 |
| `EMAIL_SMTP_PORT` | 否 | 587 | SMTP 端口 |
| `EMAIL_POLL_INTERVAL` | 否 | 15 | 轮询间隔 (秒) |
| `EMAIL_ALLOWED_USERS` | 否 | — | 允许的发件人 (逗号分隔) |
| `EMAIL_HOME_ADDRESS` | 否 | — | Cron 任务默认投递地址 |
| `EMAIL_ALLOW_ALL_USERS` | 否 | false | 允许所有发件人 (不安全) |

## 配置示例

```bash
# .env
EMAIL_ADDRESS=hermes@gmail.com
EMAIL_PASSWORD=abcd efgh ijkl mnop    # App 密码
EMAIL_IMAP_HOST=imap.gmail.com
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_ALLOWED_USERS=your@email.com,colleague@work.com
EMAIL_POLL_INTERVAL=15
```

## YAML 配置

```yaml
platforms:
  email:
    enabled: true
    # 跳过邮件附件 (节省带宽/安全)
    skip_attachments: false
```

## 安全特性

1. **白名单**: `EMAIL_ALLOWED_USERS` 限制可交互的用户
2. **自动过滤**: 忽略以下发件人
   - `noreply*`, `no-reply*`, `mailer-daemon`, `postmaster`
   - 包含 `Auto-Submitted: yes`, `List-Unsubscribe` 等邮件头的邮件
3. **附件处理**: 图片自动缓存到本地，其他文件可选择跳过

## 邮件线程

- 使用 `In-Reply-To` 和 `References` 头实现回复关联
- 通过发件人 + 主题追踪对话上下文
- `_thread_context` 字典维护每个发件人的 subject 和 message_id
- IMAP UID 去重：`_seen_uids` 集合存储已处理邮件 UID，只处理 UNSEEN 新邮件

### 分支处理逻辑

| 场景 | 处理方式 |
|------|----------|
| 同一发件人连续多封邮件 | 只处理第一封未读的，后续标记已读后跳过 |
| 回复原始邮件 | 通过 `In-Reply-To` 头关联，形成邮件链 |
| **不同发件人** | **独立会话** — `chat_id = sender_addr`，session_key = `email:发件人地址` |

## 开启新会话

发送 `/new` 或 `/reset` 命令的邮件可开启新会话。

### 有效示例

**方式一：正文命令**
```
Subject: (任意)
Body: /new
```

**方式二：直接发新邮件（不回复）**
```
Subject: /new
Body: (任意内容)
```

### 无效示例（不会触发）

```
Subject: Re: 上次的对话  ← 带 Re: 前缀，无法识别命令
Body: /new

Subject: 请帮我开启新会话  ← 不是 /new 命令
Body: /new
```

**重点**：回复邮件（Subject 带 `Re:`）无法触发命令，必须在**正文**中输入 `/new` 或 `/reset`。

## 已知限制

- 单条邮件最大 50,000 字符
- 轮询间隔不宜低于 5 秒 (增加 IMAP 连接负载)