from __future__ import annotations

from typing import TYPE_CHECKING

from astrbot.api.event.filter import CustomFilter
from astrbot.core.star.register.star_handler import get_handler_or_create, get_handler_full_name
from astrbot.core.star.star_handler import EventType, star_handlers_registry

from .types import Button, Keyboard, KeyboardButtonClick

if TYPE_CHECKING:
    from astrbot.api.config import AstrBotConfig
    from astrbot.api.event import AstrMessageEvent


class KeyboardBuilder:
    """链式构建 QQ 官方消息按钮（Keyboard）。

    用法:

        chain = MessageChain().message("请选择")
        chain.keyboard = KeyboardBuilder()
            .add_row()
                .add_button(Button("btn_1", "选项A", data="a"))
                .add_button(Button("btn_2", "选项B", data="b"))
            .build()
        await event.send(chain)
    """

    def __init__(self) -> None:
        self._rows: list[list[Button]] = []

    def add_row(self) -> "KeyboardBuilder":
        self._rows.append([])
        return self

    def add_button(self, button: Button) -> "KeyboardBuilder":
        if not self._rows:
            self.add_row()
        self._rows[-1].append(button)
        return self

    def build(self) -> Keyboard:
        return Keyboard(rows=self._rows)


def button_press(button_data: str):
    """注册一个按钮点击回调处理器，按 button_data 精确过滤。

    使用事件对象上的 ``event.button_click`` 获取回调数据。

    用法:

        @button_press("confirm")
        async def on_confirm(self, event: AstrMessageEvent):
            click = event.button_click  # KeyboardButtonClick
            await event.send(MessageChain().message(
                f"用户 {click.user_openid} 点击了确认"
            ))
    """

    _filter = KeyboardInteractionFilter(button_data=button_data)

    def decorator(awaitable):
        handler_md = get_handler_or_create(awaitable, EventType.AdapterMessageEvent)
        handler_md.event_filters.append(_filter)
        return awaitable

    return decorator


class KeyboardInteractionFilter(CustomFilter):
    """按 ``button_data`` 过滤按钮点击回调事件。

    通常不直接使用，推荐用 ``@button_press(...)`` 装饰器。
    """

    def __init__(
        self,
        button_data: str | None = None,
        raise_error: bool = False,
    ) -> None:
        super().__init__(raise_error=raise_error)
        self.button_data = button_data

    def filter(self, event: "AstrMessageEvent", cfg: "AstrBotConfig") -> bool:
        click: KeyboardButtonClick | None = getattr(event, "button_click", None)
        if click is None:
            return False
        if self.button_data is not None and click.button_data != self.button_data:
            return False
        return True
