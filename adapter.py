from __future__ import annotations

import asyncio
import logging
import random
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import botpy
import botpy.message
import botpy.types.message
from botpy import Client
from botpy.connection import ConnectionState
from botpy.gateway import BotWebSocket
from botpy.types.message import MarkdownPayload, Media

from astrbot import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, File, Image, Plain, Record, Reply, Video
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.message.components import BaseMessageComponent
from astrbot.core.platform.astr_message_event import MessageSesion
from astrbot.core.utils.media_utils import MediaResolver

from .event import QQOfficialWSMessageEvent
from .types import GroupEventType, GroupMemberEvent, KeyboardButtonClick

# -------- botpy compat patches --------

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


def _patch_botpy_formdata() -> None:
    try:
        from botpy.http import _FormData
        if not hasattr(_FormData, "_is_processed"):
            setattr(_FormData, "_is_processed", False)
    except Exception:
        logger.debug("[QQOfficialWS] Skip botpy FormData patch.")


_patch_botpy_formdata()


# -------- Patched message classes (preserve raw fields) --------

def _set_raw_message_fields(message: Any, data: dict[str, Any]) -> None:
    if not isinstance(data, dict):
        data = {}
    message.raw_data = data
    message.message_type = data.get("message_type")
    msg_elements = data.get("msg_elements")
    message.msg_elements = msg_elements if isinstance(msg_elements, list) else []


class PatchedMessage(botpy.message.Message):
    __slots__ = ("raw_data", "message_type", "msg_elements")

    def __init__(self, api: Any, event_id: str | None, data: dict[str, Any]) -> None:
        super().__init__(api, event_id, data)
        _set_raw_message_fields(self, data)


class PatchedDirectMessage(botpy.message.DirectMessage):
    __slots__ = ("raw_data", "message_type", "msg_elements")

    def __init__(self, api: Any, event_id: str | None, data: dict[str, Any]) -> None:
        super().__init__(api, event_id, data)
        _set_raw_message_fields(self, data)


class PatchedC2CMessage(botpy.message.C2CMessage):
    __slots__ = ("raw_data", "message_type", "msg_elements")

    def __init__(self, api: Any, event_id: str | None, data: dict[str, Any]) -> None:
        super().__init__(api, event_id, data)
        _set_raw_message_fields(self, data)


class PatchedGroupMessage(botpy.message.GroupMessage):
    __slots__ = ("raw_data", "message_type", "msg_elements")

    def __init__(self, api: Any, event_id: str | None, data: dict[str, Any]) -> None:
        super().__init__(api, event_id, data)
        _set_raw_message_fields(self, data)


# -------- Extend ConnectionState for group member events --------

def _ensure_group_member_parsers() -> None:
    """Register parsers for GROUP_MEMBER_ADD and GROUP_MEMBER_REMOVE."""

    def _make_parser(event_name: str, event_type: GroupEventType):
        def parse_event(self, payload: dict[str, Any]) -> None:
            d = payload.get("d", {}) if isinstance(payload, dict) else {}
            data = GroupMemberEvent(
                group_openid=d.get("group_openid", ""),
                member_openid=d.get("member_openid", ""),
                timestamp=d.get("timestamp", 0),
                event_type=event_type,
            )
            self._dispatch(event_name, data)

        return parse_event

    if not hasattr(ConnectionState, "parse_group_member_add"):
        setattr(
            ConnectionState,
            "parse_group_member_add",
            _make_parser("group_member_add", GroupEventType.MEMBER_ADD),
        )

    if not hasattr(ConnectionState, "parse_group_member_remove"):
        setattr(
            ConnectionState,
            "parse_group_member_remove",
            _make_parser("group_member_remove", GroupEventType.MEMBER_REMOVE),
        )


def _ensure_message_parsers() -> None:
    """Register qq-botpy message parsers with raw field preservation."""

    def _build_parser(event_name: str, message_cls: type) -> Any:
        def parse_message(self, payload: dict[str, Any]) -> None:
            qq_message = message_cls(self.api, payload.get("id"), payload.get("d", {}))
            self._dispatch(event_name, qq_message)

        return parse_message

    specs = {
        "message_create": ("message_create", PatchedMessage),
        "at_message_create": ("at_message_create", PatchedMessage),
        "direct_message_create": ("direct_message_create", PatchedDirectMessage),
        "group_at_message_create": ("group_at_message_create", PatchedGroupMessage),
        "c2c_message_create": ("c2c_message_create", PatchedC2CMessage),
        "group_message_create": ("group_message_create", PatchedGroupMessage),
    }
    for parser_name, (event_name, message_cls) in specs.items():
        if not hasattr(ConnectionState, f"parse_{parser_name}"):
            setattr(
                ConnectionState,
                f"parse_{parser_name}",
                _build_parser(event_name, message_cls),
            )





# -------- Extend ConnectionState for interaction events --------

def _ensure_interaction_parsers() -> None:
    """Register parser for INTERACTION_CREATE events."""

    def _parse_interaction(self, payload: dict[str, Any]) -> None:
        # Always dispatch raw payload; on_interaction_create handles extraction
        self._dispatch("interaction_create", payload)

    # Force override: setattr on the class so ConnectionState.__init__
    # picks it up via inspect.getmembers (scanning parse_* methods).
    # ConnectionState.parsers is an instance dict rebuilt per session,
    # so do NOT write to it at class level.
    setattr(ConnectionState, "parse_interaction_create", _parse_interaction)
# -------- Managed WebSocket --------

class ManagedBotWebSocket(BotWebSocket):
    def __init__(self, session, connection: Any, client: "BotClient"):
        super().__init__(session, connection)
        self._client = client

    async def on_closed(self, close_status_code, close_msg):
        if self._client.is_shutting_down:
            logger.debug("[QQOfficialWS] Ignore WS reconnect during shutdown.")
            return
        await super().on_closed(close_status_code, close_msg)

    async def close(self) -> None:
        self._can_reconnect = False
        if self._conn is not None and not self._conn.closed:
            await self._conn.close()


# -------- Bot Client --------

class BotClient(Client):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._shutting_down = False
        self._active_websockets: set[ManagedBotWebSocket] = set()

    def set_platform(self, platform: "QQOfficialWSAdapter") -> None:
        self.platform = platform

    @property
    def is_shutting_down(self) -> bool:
        return self._shutting_down or self.is_closed()

    # ---- message handlers ----

    async def on_group_at_message_create(
        self, message: botpy.message.GroupMessage
    ) -> None:
        abm = await QQOfficialWSAdapter._parse_from_qqofficial(
            message, MessageType.GROUP_MESSAGE, force_group_mention=True
        )
        abm.group_id = cast(str, message.group_openid)
        abm.session_id = abm.group_id
        self.platform.remember_session_scene(abm.session_id, "group")
        self._commit(abm)

    async def on_group_message_create(
        self, message: botpy.message.GroupMessage
    ) -> None:
        abm = await QQOfficialWSAdapter._parse_from_qqofficial(
            message, MessageType.GROUP_MESSAGE
        )
        abm.group_id = cast(str, message.group_openid)
        abm.session_id = abm.group_id
        self.platform.remember_session_scene(abm.session_id, "group")
        self._commit(abm)

    async def on_at_message_create(self, message: botpy.message.Message) -> None:
        abm = await QQOfficialWSAdapter._parse_from_qqofficial(
            message, MessageType.GROUP_MESSAGE
        )
        abm.group_id = message.channel_id
        abm.session_id = abm.group_id
        self.platform.remember_session_scene(abm.session_id, "channel")
        self._commit(abm)

    async def on_direct_message_create(
        self, message: botpy.message.DirectMessage
    ) -> None:
        abm = await QQOfficialWSAdapter._parse_from_qqofficial(
            message, MessageType.FRIEND_MESSAGE
        )
        abm.session_id = abm.sender.user_id
        self.platform.remember_session_scene(abm.session_id, "friend")
        self._commit(abm)

    async def on_c2c_message_create(
        self, message: botpy.message.C2CMessage
    ) -> None:
        abm = await QQOfficialWSAdapter._parse_from_qqofficial(
            message, MessageType.FRIEND_MESSAGE
        )
        abm.session_id = abm.sender.user_id
        self.platform.remember_session_scene(abm.session_id, "friend")
        self._commit(abm)

    # ---- group member event handlers ----

    async def on_group_member_add(self, data: GroupMemberEvent) -> None:
        logger.info(
            f"[QQOfficialWS] 群成员加入: group={data.group_openid} member={data.member_openid}"
        )
        abm = AstrBotMessage()
        abm.type = MessageType.OTHER_MESSAGE
        abm.timestamp = data.timestamp
        abm.message_id = f"gm_add_{data.group_openid}_{data.member_openid}_{data.timestamp}"
        abm.session_id = data.group_openid
        abm.group_id = data.group_openid
        abm.sender = MessageMember(user_id=data.member_openid, nickname="")
        abm.message_str = f"[群成员加入] member={data.member_openid}"
        abm.message = [Plain(abm.message_str)]
        abm.raw_message = data
        abm.self_id = "qq_official_ws"
        self.platform.remember_session_scene(abm.session_id, "group")
        self._commit_with_data(abm, data)

    async def on_group_member_remove(self, data: GroupMemberEvent) -> None:
        logger.info(
            f"[QQOfficialWS] 群成员退出: group={data.group_openid} member={data.member_openid}"
        )
        abm = AstrBotMessage()
        abm.type = MessageType.OTHER_MESSAGE
        abm.timestamp = data.timestamp
        abm.message_id = f"gm_remove_{data.group_openid}_{data.member_openid}_{data.timestamp}"
        abm.session_id = data.group_openid
        abm.group_id = data.group_openid
        abm.sender = MessageMember(user_id=data.member_openid, nickname="")
        abm.message_str = f"[群成员退出] member={data.member_openid}"
        abm.message = [Plain(abm.message_str)]
        abm.raw_message = data
        abm.self_id = "qq_official_ws"
        self.platform.remember_session_scene(abm.session_id, "group")
        self._commit_with_data(abm, data)

    # ---- interaction (button click) handler ----

    async def on_interaction_create(self, payload: dict[str, Any]) -> None:
        """处理按钮点击回调 (INTERACTION_CREATE)。"""
        d = payload.get("d", payload) if isinstance(payload, dict) else {}
        interaction_type = d.get("type") or d.get("interaction_type", 0)

        # INLINE_KEYBOARD = 11
        if int(interaction_type) != 11:
            return

        data_obj = d.get("data", {}) or {}
        if isinstance(data_obj, dict):
            resolved = data_obj.get("resolved") or {}
        else:
            resolved = {}
        if isinstance(resolved, dict):
            _bid = resolved.get("button_id") or resolved.get("buttonId") or data_obj.get("button_id") or data_obj.get("buttonId", "")
            _bdata = resolved.get("button_data") or resolved.get("buttonData") or data_obj.get("button_data") or data_obj.get("buttonData", "")
        else:
            _bid = data_obj.get("button_id", data_obj.get("buttonId", ""))
            _bdata = data_obj.get("button_data", data_obj.get("buttonData", ""))
        interaction_id = str(d.get("id", ""))

        # Immediately ack the interaction to prevent QQ button timeout.
        # QQ expects a PUT /interactions/{id} callback; without it shows 超时.
        try:
            await self.api.on_interaction_result(interaction_id, code=0)
        except Exception:
            logger.debug("[QQOfficialWS] interaction ack failed (non-fatal)", exc_info=True)

        click = KeyboardButtonClick(
            interaction_id=interaction_id,
            button_id=str(_bid),
            button_data=str(_bdata),
            user_openid=str(d.get("user_openid", "")),
            group_openid=str(d.get("group_openid", "")),
            group_member_openid=str(d.get("group_member_openid", "")),
            channel_id=str(d.get("channel_id", "")),
            guild_id=str(d.get("guild_id", "")),
            chat_type=int(d.get("chat_type", 0)) if d.get("chat_type") else None,
            message_id=str(d.get("message_id", "")),
            raw=d,
        )

        logger.info(
            f"[QQOfficialWS] 按钮点击: id={click.button_id} data={click.button_data} "
            f"user={click.user_openid}"
        )

        session_id = click.group_openid or click.user_openid
        abm = AstrBotMessage()
        abm.type = MessageType.OTHER_MESSAGE
        abm.timestamp = int(time.time())
        abm.message_id = click.interaction_id
        abm.session_id = session_id
        abm.group_id = click.group_openid
        abm.sender = MessageMember(user_id=click.user_openid, nickname="")
        abm.message_str = f"[按钮点击] button_data={click.button_data}"
        abm.message = [Plain(abm.message_str)]
        abm.raw_message = click
        abm.self_id = "qq_official_ws"

        self.platform.remember_session_scene(abm.session_id, "group" if click.group_openid else "friend")
        self._commit_with_click(abm, click)

    # ---- ws lifecycle ----

    async def bot_connect(self, session) -> None:
        logger.info("[QQOfficialWS] WebSocket session starting.")
        ws = ManagedBotWebSocket(session, self._connection, self)
        self._active_websockets.add(ws)
        try:
            await ws.ws_connect()
        except Exception as e:
            if not self.is_shutting_down:
                await ws.on_error(e)
        finally:
            self._active_websockets.discard(ws)

    async def shutdown(self) -> None:
        if self.is_shutting_down:
            return
        self._shutting_down = True
        await asyncio.gather(
            *(ws.close() for ws in list(self._active_websockets)),
            return_exceptions=True,
        )
        await self.close()

    # ---- helpers ----

    def _commit(self, abm: AstrBotMessage) -> None:
        self.platform.remember_session_message_id(abm.session_id, abm.message_id)
        self.platform.commit_event(self.platform.create_event(abm))

    def _commit_with_data(self, abm: AstrBotMessage, data: GroupMemberEvent) -> None:
        event = self.platform.create_event(abm)
        setattr(event, "group_member_event", data)
        self.platform.commit_event(event)

    def _commit_with_click(self, abm: AstrBotMessage, click: KeyboardButtonClick) -> None:
        event = self.platform.create_event(abm)
        setattr(event, "button_click", click)
        self.platform.commit_event(event)


# -------- Adapter --------

@register_platform_adapter(
    "qq_official_ws",
    "QQ 官方机器人 WS 适配器(插件版)",
    default_config_tmpl={
        "appid": "",
        "secret": "",
        "enable_group_c2c": True,
        "enable_guild_direct_message": True,
        "enable_group_member_events": True,
    },
)
class QQOfficialWSAdapter(Platform):
    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)

        self.appid = platform_config["appid"]
        self.secret = platform_config["secret"]
        qq_group = platform_config.get("enable_group_c2c", True)
        guild_dm = platform_config.get("enable_guild_direct_message", True)
        self._enable_group_member_events = platform_config.get(
            "enable_group_member_events", True
        )

        # Build intents bitmask
        intent_bits = 0
        if qq_group:
            intent_bits |= 1 << 25  # 群聊消息
            if self._enable_group_member_events:
                intent_bits |= 1 << 24  # 群成员事件
        if guild_dm:
            intent_bits |= 1 << 12  # 频道私信
        intent_bits |= 1 << 0  # 频道（需要 GUILD 权限以接收频道消息）
        intent_bits |= 1 << 26  # 按钮交互回调 (INTERACTION_CREATE)

        # Use botpy Intents, but override via _intents_raw for custom bitmask
        self.intents = botpy.Intents(
            public_messages=bool(intent_bits & (1 << 25)),
            public_guild_messages=True,
            direct_message=guild_dm,
        )
        try:
            self.intents._value = intent_bits
        except Exception:
            pass

        self.client = BotClient(intents=self.intents, bot_log=False, timeout=20)
        self.client.set_platform(self)

        _ensure_message_parsers()
        if self._enable_group_member_events:
            _ensure_group_member_parsers()
        _ensure_interaction_parsers()

        self._session_last_message_id: dict[str, str] = {}
        self._session_scene: dict[str, str] = {}
        self._allow_group_proactive_send = True

    # ---- Platform interface ----

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="qq_official_ws",
            description="QQ 官方机器人 WS 适配器(插件版)",
            id=cast(str, self.config.get("id")),
            support_proactive_message=True,
        )

    def create_event(self, message: AstrBotMessage) -> QQOfficialWSMessageEvent:
        return QQOfficialWSMessageEvent(
            message.message_str,
            message,
            self.meta(),
            message.session_id,
            self.client,
        )

    def run(self):
        return self.client.start(appid=self.appid, secret=self.secret)

    async def terminate(self) -> None:
        await self.client.shutdown()
        logger.info("[QQOfficialWS] 适配器已关闭")

    def get_client(self) -> BotClient:
        return self.client

    # ---- send_by_session ----

    async def send_by_session(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        await self._send_by_session_common(session, message_chain)

    async def _send_by_session_common(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        message_chains = QQOfficialWSMessageEvent._split_message_chain_by_media(
            message_chain
        )
        if len(message_chains) > 1:
            for mc in message_chains:
                await self._send_by_session_common(session, mc)
            return

        (
            plain_text,
            image_base64,
            image_path,
            record_file_path,
            video_file_source,
            file_source,
            file_name,
        ) = await QQOfficialWSMessageEvent._parse_to_qqofficial(message_chain)

        if (
            not plain_text
            and not image_path
            and not image_base64
            and not record_file_path
            and not video_file_source
            and not file_source
        ):
            return

        msg_id = self._session_last_message_id.get(session.session_id)
        scene = self._session_scene.get(session.session_id)
        allow_proactive = (
            session.message_type == MessageType.GROUP_MESSAGE
            and scene == "group"
            and getattr(self, "_allow_group_proactive_send", False)
        )

        if (
            not msg_id
            and session.message_type != MessageType.FRIEND_MESSAGE
            and not allow_proactive
        ):
            logger.warning(
                "[QQOfficialWS] No cached msg_id for session: %s, skip send_by_session",
                session.session_id,
            )
            return

        payload: dict[str, Any] = {"content": plain_text}
        if msg_id and not allow_proactive:
            payload["msg_id"] = msg_id

        ret: Any = None
        send_helper = SimpleNamespace(bot=self.client)

        # keyboard support
        keyboard = getattr(message_chain, "keyboard", None)
        if keyboard is not None:
            payload["keyboard"] = keyboard.to_dict()

        if session.message_type == MessageType.GROUP_MESSAGE:
            if scene == "group":
                payload["msg_seq"] = random.randint(1, 10000)
                if image_base64:
                    media = await QQOfficialWSMessageEvent.upload_group_and_c2c_image(
                        send_helper, image_base64,
                        QQOfficialWSMessageEvent.IMAGE_FILE_TYPE,
                        group_openid=session.session_id,
                    )
                    payload["media"] = media
                    payload["msg_type"] = 7
                if record_file_path:
                    media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                        send_helper, record_file_path,
                        QQOfficialWSMessageEvent.VOICE_FILE_TYPE,
                        group_openid=session.session_id,
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                if video_file_source:
                    media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                        send_helper, video_file_source,
                        QQOfficialWSMessageEvent.VIDEO_FILE_TYPE,
                        group_openid=session.session_id,
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("msg_id", None)
                if file_source:
                    media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                        send_helper, file_source,
                        QQOfficialWSMessageEvent.FILE_FILE_TYPE,
                        file_name=file_name,
                        group_openid=session.session_id,
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("msg_id", None)
                ret = await self.client.api.post_group_message(
                    group_openid=session.session_id, **payload
                )
            else:
                if image_path:
                    payload["file_image"] = image_path
                ret = await self.client.api.post_message(
                    channel_id=session.session_id, **payload
                )

        elif session.message_type == MessageType.FRIEND_MESSAGE:
            payload.pop("msg_id", None)
            payload["msg_seq"] = random.randint(1, 10000)
            if image_base64:
                media = await QQOfficialWSMessageEvent.upload_group_and_c2c_image(
                    send_helper, image_base64,
                    QQOfficialWSMessageEvent.IMAGE_FILE_TYPE,
                    openid=session.session_id,
                )
                payload["media"] = media
                payload["msg_type"] = 7
            if record_file_path:
                media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                    send_helper, record_file_path,
                    QQOfficialWSMessageEvent.VOICE_FILE_TYPE,
                    openid=session.session_id,
                )
                if media:
                    payload["media"] = media
                    payload["msg_type"] = 7
            if video_file_source:
                media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                    send_helper, video_file_source,
                    QQOfficialWSMessageEvent.VIDEO_FILE_TYPE,
                    openid=session.session_id,
                )
                if media:
                    payload["media"] = media
                    payload["msg_type"] = 7
            if file_source:
                media = await QQOfficialWSMessageEvent.upload_group_and_c2c_media(
                    send_helper, file_source,
                    QQOfficialWSMessageEvent.FILE_FILE_TYPE,
                    file_name=file_name,
                    openid=session.session_id,
                )
                if media:
                    payload["media"] = media
                    payload["msg_type"] = 7
            ret = await QQOfficialWSMessageEvent.post_c2c_message(
                send_helper, openid=session.session_id, **payload
            )
        else:
            logger.warning(
                "[QQOfficialWS] Unsupported message type: %s", session.message_type
            )
            return

        sent_id = self._extract_message_id(ret)
        if sent_id:
            self.remember_session_message_id(session.session_id, sent_id)
        await Platform.send_by_session(self, session, message_chain)

    def remember_session_message_id(self, session_id: str, message_id: str) -> None:
        if not session_id or not message_id:
            return
        self._session_last_message_id[session_id] = message_id

    def remember_session_scene(self, session_id: str, scene: str) -> None:
        if not session_id or not scene:
            return
        self._session_scene[session_id] = scene

    @staticmethod
    def _extract_message_id(ret: Any) -> str | None:
        if isinstance(ret, dict):
            mid = ret.get("id")
            return str(mid) if mid else None
        mid = getattr(ret, "id", None)
        return str(mid) if mid else None

    # ---- Message parsing (static) ----

    @staticmethod
    async def _parse_from_qqofficial(
        message: botpy.message.Message
        | botpy.message.GroupMessage
        | botpy.message.DirectMessage
        | botpy.message.C2CMessage,
        message_type: MessageType,
        force_group_mention: bool = False,
    ) -> AstrBotMessage:
        abm = AstrBotMessage()
        abm.type = message_type
        abm.timestamp = int(time.time())
        abm.raw_message = message
        abm.message_id = message.id
        msg: list[BaseMessageComponent] = []

        # reply
        message_reference = getattr(message, "message_reference", None)
        quoted_message_id = getattr(message_reference, "message_id", None)
        raw_message_type = getattr(message, "message_type", None)
        try:
            is_quoted = int(raw_message_type or 0) == 103
        except (TypeError, ValueError):
            is_quoted = False
        msg_elements = getattr(message, "msg_elements", None)
        quoted_str = ""
        quoted_element_id = ""
        quoted_chain: list[BaseMessageComponent] = []

        if is_quoted and isinstance(msg_elements, list) and msg_elements:
            qe = msg_elements[0]
            if isinstance(qe, dict):
                q_content = qe.get("content")
                q_attachments = qe.get("attachments")
                quoted_element_id = str(qe.get("id") or qe.get("message_id") or "")
            else:
                q_content = getattr(qe, "content", None)
                q_attachments = getattr(qe, "attachments", None)
                quoted_element_id = str(
                    getattr(qe, "id", None) or getattr(qe, "message_id", None) or ""
                )
            quoted_str = QQOfficialWSAdapter._parse_face_message(
                str(q_content or "").strip()
            )
            if quoted_str:
                quoted_chain.append(Plain(quoted_str))
            if isinstance(q_attachments, list):
                await QQOfficialWSAdapter._append_attachments(
                    quoted_chain, q_attachments
                )

        if quoted_message_id or quoted_element_id or quoted_chain:
            msg.append(
                Reply(
                    id=str(quoted_message_id or quoted_element_id or ""),
                    chain=quoted_chain,
                    message_str=quoted_str,
                )
            )

        # group / c2c
        if isinstance(message, (botpy.message.GroupMessage, botpy.message.C2CMessage)):
            if isinstance(message, botpy.message.GroupMessage):
                abm.sender = MessageMember(
                    message.author.member_openid,
                    getattr(message.author, "username", "") or "",
                )
                abm.group_id = message.group_openid
                bot_mentions = [
                    m for m in (getattr(message, "mentions", None) or [])
                    if getattr(m, "is_you", False) is True and getattr(m, "id", None) is not None
                ]
                bot_mention_ids = [str(m.id) for m in bot_mentions]
                group_mentioned = bool(bot_mention_ids) or force_group_mention
                plain_raw = message.content or ""
                for mid in bot_mention_ids:
                    plain_raw = plain_raw.replace(f"<@{mid}>", "").replace(f"<@!{mid}>", "")
                abm.message_str = QQOfficialWSAdapter._parse_face_message(plain_raw.strip())
                abm.self_id = bot_mention_ids[0] if bot_mention_ids else "qq_official_ws"
                if group_mentioned:
                    mention_name = (
                        getattr(bot_mentions[0], "username", "") if bot_mentions else ""
                    )
                    msg.append(At(qq=abm.self_id, name=mention_name))
            else:
                abm.sender = MessageMember(
                    message.author.user_openid,
                    getattr(message.author, "username", "") or "",
                )
                abm.message_str = QQOfficialWSAdapter._parse_face_message(
                    (message.content or "").strip()
                )
                abm.self_id = "unknown_selfid"
            msg.append(Plain(abm.message_str))
            await QQOfficialWSAdapter._append_attachments(msg, message.attachments)
            abm.message = msg

        elif isinstance(message, (botpy.message.Message, botpy.message.DirectMessage)):
            if isinstance(message, botpy.message.Message):
                abm.self_id = str(message.mentions[0].id) if message.mentions else ""
            else:
                abm.self_id = ""
            plain_content = QQOfficialWSAdapter._parse_face_message(
                message.content.replace(
                    "<@!" + str(abm.self_id) + ">", ""
                ).strip()
            )
            await QQOfficialWSAdapter._append_attachments(msg, message.attachments)
            abm.message_str = plain_content
            abm.sender = MessageMember(
                str(message.author.id), str(message.author.username)
            )
            msg.append(At(qq="qq_official_ws"))
            msg.append(Plain(plain_content))
            abm.message = msg
            if isinstance(message, botpy.message.Message):
                abm.group_id = message.channel_id
        else:
            raise ValueError(f"Unknown message type: {type(message)}")

        if not abm.self_id:
            abm.self_id = "qq_official_ws"
        return abm

    @staticmethod
    def _parse_face_message(content: str) -> str:
        import base64 as _b64
        import json as _json
        import re as _re

        def _replace(match):
            tag = match.group(0)
            m = _re.search(r'ext="([^"]*)"', tag)
            if m:
                try:
                    decoded = _b64.b64decode(m.group(1)).decode("utf-8")
                    data = _json.loads(decoded)
                    text = data.get("text", "")
                    if text:
                        return f"[表情:{text}]"
                except Exception:
                    pass
            return "[表情]"

        return _re.sub(r"<faceType=\d+[^>]*>", _replace, content)

    @staticmethod
    def _normalize_attachment_url(url: str | None) -> str:
        if not url:
            return ""
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"https://{url}"

    @staticmethod
    async def _prepare_audio_attachment(url: str, filename: str) -> Record:
        ext = Path(filename).suffix.lower()
        source_ext = ext or ".audio"
        path_wav = await MediaResolver(
            url, media_type="audio", default_suffix=source_ext
        ).to_path(target_format="wav")
        return Record(file=path_wav, url=path_wav)

    @staticmethod
    async def _append_attachments(
        msg: list[BaseMessageComponent], attachments: list | None
    ) -> None:
        if not attachments:
            return
        for att in attachments:
            if isinstance(att, dict):
                ct = str(att.get("content_type") or att.get("contentType") or "").lower()
                url = QQOfficialWSAdapter._normalize_attachment_url(
                    cast(str | None, att.get("url"))
                )
                filename = cast(
                    str,
                    att.get("filename") or att.get("name") or "attachment",
                )
            else:
                ct = cast(str, getattr(att, "content_type", "") or "").lower()
                url = QQOfficialWSAdapter._normalize_attachment_url(
                    cast(str | None, getattr(att, "url", None))
                )
                filename = cast(
                    str,
                    getattr(att, "filename", None)
                    or getattr(att, "name", None)
                    or "attachment",
                )
            if not url:
                continue
            ext = Path(filename).suffix.lower()
            img_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
            audio_exts = {".mp3", ".wav", ".ogg", ".m4a", ".amr", ".silk"}
            video_exts = {".mp4", ".mov", ".avi", ".mkv", ".webm"}

            if ct.startswith("image"):
                msg.append(Image.fromURL(url))
            elif ct.startswith("voice") or ext in audio_exts:
                try:
                    msg.append(
                        await QQOfficialWSAdapter._prepare_audio_attachment(url, filename)
                    )
                except Exception as e:
                    logger.warning(
                        "[QQOfficialWS] Failed to prepare audio attachment %s: %s", url, e
                    )
                    msg.append(Record.fromURL(url))
            elif ct.startswith("video") or ext in video_exts:
                msg.append(Video.fromURL(url))
            elif ct.startswith("image") or ext in img_exts:
                msg.append(Image.fromURL(url))
            else:
                msg.append(File(name=filename, file=url, url=url))
