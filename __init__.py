"""QQ 官方机器人 WebSocket 适配器 —— 插件版。

特性:
- 完整的消息收发（文本/图片/语音/视频/文件/Markdown/流式）
- 群成员加入/退出事件探测
- 消息按钮（Keyboard）便捷构建与回调处理
"""

from astrbot.api.star import Context, Star
from astrbot import logger


class QQOfficialWSPlugin(Star):
    def __init__(self, context: Context) -> None:
        super().__init__(context)
        # 导入触发适配器注册
        from .adapter import QQOfficialWSAdapter  # noqa: F401

    async def on_loaded(self):
        logger.info("[QQOfficialWS] 插件已加载 — QQ 官方机器人 WS 适配器（插件版）")

    async def on_unloaded(self):
        logger.info("[QQOfficialWS] 插件已卸载")
