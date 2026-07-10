from __future__ import annotations

import enum
from dataclasses import dataclass, field


class GroupEventType(enum.Enum):
    MEMBER_ADD = "GROUP_MEMBER_ADD"
    MEMBER_REMOVE = "GROUP_MEMBER_REMOVE"


@dataclass
class GroupMemberEvent:
    group_openid: str
    member_openid: str
    timestamp: int
    event_type: GroupEventType


@dataclass
class KeyboardButtonClick:
    """按钮点击回调携带的数据。"""
    interaction_id: str
    button_id: str
    button_data: str
    user_openid: str
    group_openid: str | None = None
    group_member_openid: str | None = None
    channel_id: str | None = None
    guild_id: str | None = None
    chat_type: int | None = None
    message_id: str | None = None
    raw: dict | None = None


@dataclass
class Button:
    """消息按钮。"""
    id: str
    label: str
    data: str = ""
    style: int = 1
    visited_label: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "render_data": {
                "label": self.label,
                "visited_label": self.visited_label or self.label,
                "style": self.style,
            },
            "action": {
                "type": 2,
                "permission": {"type": 2},
                "data": self.data,
                "unsupport_tips": "",
            },
        }


@dataclass
class Keyboard:
    """键盘 = 多行按钮。"""
    rows: list[list[Button]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "content": {
                "rows": [
                    {"buttons": [b.to_dict() for b in row]} for row in self.rows
                ]
            }
        }
