# astrbot_plugin_qqofficial_ws

QQ 官方机器人 WebSocket 适配器 —— AstrBot 插件版。

## 特性

- 完整消息收发（文本/图片/语音/视频/文件/Markdown/流式）
- **群成员加入/退出事件**探测
- **消息按钮（Keyboard）**便捷构建与回调处理

## 安装

```bash
# 在 AstrBot 插件目录中
git clone <repo_url>
# 重启 AstrBot 后在管理面板启用插件
```

## 配置

```yaml
appid: ""                       # 机器人 AppID
secret: ""                      # 机器人 AppSecret
enable_group_c2c: true           # 启用群聊+C2C私聊
enable_guild_direct_message: true # 启用频道私信
enable_group_member_events: true  # 启用群成员加入/退出事件
```

## 使用

### 消息按钮

```python
from astrbot_plugin_qqofficial_ws.keyboard import KeyboardBuilder, button_press
from astrbot_plugin_qqofficial_ws.types import Button

# 发送带按钮的消息
chain = MessageChain().message("请选择操作：")
chain.keyboard = KeyboardBuilder()
    .add_row()
        .add_button(Button("btn_ok", "确认", data="confirm", style=2))
        .add_button(Button("btn_no", "取消", data="cancel"))
    .build()
await event.send(chain)

# 接收按钮回调
@button_press("confirm")
async def on_confirm(self, event: AstrMessageEvent):
    click = event.button_click
    await event.send(MessageChain().message(
        f"用户 {click.user_openid} 点击了确认"
    ))
```

### Markdown 消息

```python
chain = MessageChain().message("# 标题\n**加粗**内容")
chain.use_markdown_ = True
await event.send(chain)
```

### 群成员事件

```python
from astrbot_plugin_qqofficial_ws.types import GroupEventType, GroupMemberEvent

# 监听群成员进出
@event_message_type(EventMessageType.OTHER_MESSAGE)
async def on_group_event(self, event: AstrMessageEvent):
    gm = getattr(event, "group_member_event", None)
    if gm is None:
        return
    if gm.event_type == GroupEventType.MEMBER_ADD:
        logger.info(f"新成员加入: {gm.member_openid}")
```

## 依赖

- `astrbot` >= 4.0.0
- `botpy` >= 1.2.0
- `aiohttp` >= 3.8
- `aiofiles`
- `tenacity`
