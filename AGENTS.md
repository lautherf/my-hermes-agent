# Hermes Agent 开发指南

## 项目概述

- **类型**: Python AI Agent 框架 (Nous Research)
- **入口**: `hermes` CLI 命令 → `hermes_cli/main.py`
- **Python**: 3.11+
- **配置**: `~/.hermes/config.yaml` + `~/.hermes/.env`

## 核心模块

| 目录 | 用途 |
|------|------|
| `run_agent.py` | AIAgent 类 - 对话循环核心 |
| `cli.py` | HermesCLI 类 - 交互式 CLI |
| `model_tools.py` | 工具编排 + handle_function_call() |
| `tools/registry.py` | 工具注册中心 |
| `hermes_cli/` | CLI 子命令 (config, models, tools, skills, gateway 等) |
| `gateway/` | 消息平台网关 (telegram, discord, slack 等) |
| `agent/` | Agent 内部 (prompt_builder, context_compressor 等) |
| `tools/` | 工具实现 (file_tools, terminal_tool, web_tools 等) |
| `tools/environments/` | 终端后端 (local, docker, ssh, modal, daytona) |
| `plugins/memory/` | 记忆系统插件 |

## 常用命令

```bash
source venv/bin/activate  # 必须激活虚拟环境

hermes              # 交互式 CLI
hermes model        # 选择模型
hermes tools        # 配置工具
hermes gateway     # 启动消息网关
hermes setup       # 运行设置向导

python -m pytest tests/ -q          # 完整测试 (~3000)
python -m pytest tests/tools/ -q      # 工具测试
python -m pytest tests/gateway/ -q   # 网关测试
```

## 工具注册流程 (3 文件)

1. 创建 `tools/your_tool.py` - 调用 `registry.register()`
2. 在 `model_tools.py` 的 `_discover_tools()` 添加 import
3. 在 `toolsets.py` 添加工具到 toolset

## 配置注意事项

- 添加配置项到 `hermes_cli/config.py` 的 `DEFAULT_CONFIG`
- 需同时更新 `_config_version` (当前 13) 以触发迁移

## Profile 支持

- 使用 `get_hermes_home()` 获取路径 (来自 `hermes_constants`)
- 不要硬编码 `~/.hermes`

## 已知陷阱

- 禁止在工具 schema 中引用其他工具集的工具名称 (会导致幻觉调用)
- 测试不能写入 `~/.hermes/` (使用 `tests/conftest.py` 的 fixture)