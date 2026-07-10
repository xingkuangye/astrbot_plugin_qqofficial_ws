from __future__ import annotations

import asyncio
import base64
import logging
import random
from typing import Any, cast

import aiofiles
import botpy
import botpy.errors
import botpy.message
import botpy.types
import botpy.types.message
from botpy import Client
from botpy.http import Route
from botpy.types import message
from botpy.types.message import MarkdownPayload, Media
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import File, Image, Plain, Record, Video
from astrbot.api.platform import AstrBotMessage, PlatformMetadata
from astrbot.core.utils.media_utils import MediaResolver, file_uri_to_path, is_file_uri


class APIReturnNoneError(Exception):
    pass


_QQOFFICIAL_SEND_API_ERRORS = (
    botpy.errors.ForbiddenError,
    botpy.errors.MethodNotAllowedError,
    botpy.errors.NotFoundError,
    botpy.errors.SequenceNumberError,
    botpy.errors.ServerError,
)


def _qqofficial_retry(max_attempts: int = 5):
    return retry(
        retry=retry_if_exception_type(
            (
                botpy.errors.ServerError,
                botpy.errors.SequenceNumberError,
                OSError,
                asyncio.TimeoutError,
                APIReturnNoneError,
            )
        ),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


class QQOfficialWSMessageEvent(AstrMessageEvent):
    MARKDOWN_NOT_ALLOWED_ERROR = "不允许发送原生 markdown"
    IMAGE_FILE_TYPE = 1
    VIDEO_FILE_TYPE = 2
    VOICE_FILE_TYPE = 3
    FILE_FILE_TYPE = 4
    STREAM_MARKDOWN_NEWLINE_ERROR = "流式消息md分片需要\\n结束"

    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        bot: Client,
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.bot = bot
        self.send_buffer: MessageChain | None = None

    async def send(self, message: MessageChain) -> None:
        self.send_buffer = message
        await self._post_send()

    async def send_streaming(self, generator, use_fallback: bool = False):
        await super().send_streaming(generator, use_fallback)
        stream_payload = {"state": 1, "id": None, "index": 0, "reset": False}
        last_edit_time = 0
        throttle_interval = 1
        ret = None
        source = self.message_obj.raw_message
        try:
            async for chain in generator:
                source = self.message_obj.raw_message
                if not isinstance(source, botpy.message.C2CMessage):
                    if not self.send_buffer:
                        self.send_buffer = chain
                    else:
                        self.send_buffer.chain.extend(chain.chain)
                    continue

                if chain.type == "break":
                    if self.send_buffer:
                        stream_payload["state"] = 10
                        ret = await self._post_send(stream=stream_payload)
                        ret_id = self._extract_response_message_id(ret)
                        if ret_id is not None:
                            stream_payload["id"] = ret_id
                    stream_payload = {"state": 1, "id": None, "index": 0, "reset": False}
                    last_edit_time = 0
                    continue

                if not self.send_buffer:
                    self.send_buffer = chain
                else:
                    self.send_buffer.chain.extend(chain.chain)

                current_time = asyncio.get_running_loop().time()
                if current_time - last_edit_time >= throttle_interval:
                    ret = cast(message.Message, await self._post_send(stream=stream_payload))
                    stream_payload["index"] += 1
                    ret_id = self._extract_response_message_id(ret)
                    if ret_id is not None:
                        stream_payload["id"] = ret_id
                    last_edit_time = asyncio.get_running_loop().time()
                    self.send_buffer = None

            if isinstance(source, botpy.message.C2CMessage):
                stream_payload["state"] = 10
                ret = await self._post_send(stream=stream_payload)
            else:
                ret = await self._post_send()
        except Exception as e:
            logger.error(f"[QQOfficialWS] 发送流式消息时出错: {e}", exc_info=True)
            self.send_buffer = None
        return None

    @staticmethod
    def _extract_response_message_id(ret) -> str | None:
        if ret is None:
            return None
        if isinstance(ret, dict):
            rid = ret.get("id")
            return str(rid) if rid is not None else None
        rid = getattr(ret, "id", None)
        return str(rid) if rid is not None else None

    @staticmethod
    def _split_message_chain_by_media(message: MessageChain) -> list[MessageChain]:
        chunks: list[MessageChain] = []
        current_chain: list = []
        current_has_media = False

        for comp in message.chain:
            is_media = isinstance(comp, (Image, Record, Video, File))
            if is_media and current_has_media:
                chunks.append(MessageChain(chain=current_chain))
                current_chain = []
                current_has_media = False
            current_chain.append(comp)
            current_has_media = current_has_media or is_media

        if current_chain or not message.chain:
            chunks.append(MessageChain(chain=current_chain))
        return chunks

    async def _post_send(self, stream: dict | None = None):
        if not self.send_buffer:
            return None
        keyboard = getattr(self.send_buffer, "keyboard", None)
        message_chains = self._split_message_chain_by_media(self.send_buffer)
        stream_for_chain = stream if len(message_chains) == 1 else None
        ret = None
        for i, mc in enumerate(message_chains):
            if keyboard is not None and i == 0:
                mc.keyboard = keyboard
            ret = await self._post_send_one(mc, stream_for_chain)
        self.send_buffer = None
        return ret

    async def _post_send_one(
        self, message_to_send: MessageChain, stream: dict | None = None
    ):
        if not message_to_send:
            return None

        source = self.message_obj.raw_message
        if not isinstance(
            source,
            (
                botpy.message.Message,
                botpy.message.GroupMessage,
                botpy.message.DirectMessage,
                botpy.message.C2CMessage,
            ),
        ):
            logger.warning(f"[QQOfficialWS] 不支持的消息源类型: {type(source)}")
            return None

        (
            plain_text,
            image_base64,
            image_path,
            record_file_path,
            video_file_source,
            file_source,
            file_name,
        ) = await self._parse_to_qqofficial(message_to_send)

        if record_file_path:
            self.track_temporary_local_file(record_file_path)

        if stream and (image_base64 or record_file_path or video_file_source or file_source):
            stream = None

        if (
            not plain_text
            and not image_base64
            and not image_path
            and not record_file_path
            and not video_file_source
            and not file_source
        ):
            return None

        if (
            stream
            and stream.get("state") == 10
            and plain_text
            and not plain_text.endswith("\n")
        ):
            plain_text = plain_text + "\n"

        # Markdown handling
        use_md = getattr(self.send_buffer, "use_markdown_", None) if self.send_buffer else None
        if use_md is False:
            payload: dict[str, Any] = {
                "content": plain_text,
                "msg_type": 0,
                "msg_id": self.message_obj.message_id,
            }
        else:
            payload = {
                "markdown": MarkdownPayload(content=plain_text) if plain_text else None,
                "msg_type": 2,
                "msg_id": self.message_obj.message_id,
            }

        if not isinstance(source, (botpy.message.Message, botpy.message.DirectMessage)):
            payload["msg_seq"] = random.randint(1, 10000)

        # keyboard support
        keyboard = getattr(message_to_send, "keyboard", None)
        if keyboard is not None:
            payload["keyboard"] = keyboard.to_dict()

        ret = None

        match source:
            case botpy.message.GroupMessage():
                if not source.group_openid:
                    logger.error("[QQOfficialWS] GroupMessage 缺少 group_openid")
                    return None
                if image_base64:
                    media = await self.upload_group_and_c2c_image(
                        image_base64, self.IMAGE_FILE_TYPE, group_openid=source.group_openid
                    )
                    payload["media"] = media
                    payload["msg_type"] = 7
                    payload.pop("markdown", None)
                    payload["content"] = plain_text or None
                if record_file_path:
                    media = await self.upload_group_and_c2c_media(
                        record_file_path, self.VOICE_FILE_TYPE, group_openid=source.group_openid
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None
                if video_file_source:
                    media = await self.upload_group_and_c2c_media(
                        video_file_source, self.VIDEO_FILE_TYPE, group_openid=source.group_openid
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None
                if file_source:
                    media = await self.upload_group_and_c2c_media(
                        file_source, self.FILE_FILE_TYPE, file_name=file_name,
                        group_openid=source.group_openid,
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None
                ret = await self._send_with_markdown_fallback(
                    send_func=lambda p: self.bot.api.post_group_message(
                        group_openid=source.group_openid, **p
                    ),
                    payload=payload,
                    plain_text=plain_text,
                    stream=stream,
                )

            case botpy.message.C2CMessage():
                if image_base64:
                    media = await self.upload_group_and_c2c_image(
                        image_base64, self.IMAGE_FILE_TYPE, openid=source.author.user_openid
                    )
                    payload["media"] = media
                    payload["msg_type"] = 7
                    payload.pop("markdown", None)
                    payload["content"] = plain_text or None
                if record_file_path:
                    media = await self.upload_group_and_c2c_media(
                        record_file_path, self.VOICE_FILE_TYPE, openid=source.author.user_openid
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None
                if video_file_source:
                    media = await self.upload_group_and_c2c_media(
                        video_file_source, self.VIDEO_FILE_TYPE, openid=source.author.user_openid
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None
                if file_source:
                    media = await self.upload_group_and_c2c_media(
                        file_source, self.FILE_FILE_TYPE, file_name=file_name,
                        openid=source.author.user_openid,
                    )
                    if media:
                        payload["media"] = media
                        payload["msg_type"] = 7
                        payload.pop("markdown", None)
                        payload["content"] = plain_text or None

                if stream:
                    ret = await self._send_with_markdown_fallback(
                        send_func=lambda p: self.post_c2c_message(
                            openid=source.author.user_openid, stream=stream, **p
                        ),
                        payload=payload,
                        plain_text=plain_text,
                        stream=stream,
                    )
                else:
                    ret = await self._send_with_markdown_fallback(
                        send_func=lambda p: self.post_c2c_message(
                            openid=source.author.user_openid, **p
                        ),
                        payload=payload,
                        plain_text=plain_text,
                        stream=stream,
                    )

            case botpy.message.Message():
                if image_path:
                    payload["file_image"] = image_path
                payload.pop("msg_type", None)
                ret = await self._send_with_markdown_fallback(
                    send_func=lambda p: self.bot.api.post_message(
                        channel_id=source.channel_id, **p
                    ),
                    payload=payload,
                    plain_text=plain_text,
                    stream=stream,
                )

            case botpy.message.DirectMessage():
                if image_path:
                    payload["file_image"] = image_path
                payload.pop("msg_type", None)
                ret = await self._send_with_markdown_fallback(
                    send_func=lambda p: self.bot.api.post_dms(
                        guild_id=source.guild_id, **p
                    ),
                    payload=payload,
                    plain_text=plain_text,
                    stream=stream,
                )

            case _:
                pass

        await super().send(message_to_send)
        return ret

    async def _send_with_markdown_fallback(
        self,
        send_func,
        payload: dict,
        plain_text: str,
        stream: dict | None = None,
    ):
        try:
            return await send_func(payload)
        except _QQOFFICIAL_SEND_API_ERRORS as err:
            logger.info("[QQOfficialWS] 回复消息失败: %s, 尝试使用主动发送接口。", err)
            if payload.get("msg_id"):
                fb = payload.copy()
                fb.pop("msg_id", None)
                try:
                    ret = await send_func(fb)
                    logger.info("[QQOfficialWS] 使用主动发送接口发送成功。")
                    return ret
                except _QQOFFICIAL_SEND_API_ERRORS as fb_err:
                    err = fb_err
                    payload = fb

            if not isinstance(err, botpy.errors.ServerError):
                raise

            if stream and self.STREAM_MARKDOWN_NEWLINE_ERROR in str(err):
                retry_payload = payload.copy()
                md_payload = retry_payload.get("markdown")
                if isinstance(md_payload, dict):
                    md_content = cast(str, md_payload.get("content", "") or "")
                    if md_content and not md_content.endswith("\n"):
                        retry_payload["markdown"] = {"content": md_content + "\n"}
                content = cast(str | None, retry_payload.get("content"))
                if content and not content.endswith("\n"):
                    retry_payload["content"] = content + "\n"
                logger.warning("[QQOfficialWS] 流式 markdown 分片换行校验失败，已修正后重试。")
                return await send_func(retry_payload)

            if (
                self.MARKDOWN_NOT_ALLOWED_ERROR not in str(err)
                or not payload.get("markdown")
                or not plain_text
            ):
                raise

            logger.warning("[QQOfficialWS] markdown 发送被拒绝，回退到 content 模式重试。")
            fb = payload.copy()
            fb.pop("markdown", None)
            fb["content"] = plain_text
            if fb.get("msg_type") == 2:
                fb["msg_type"] = 0
            if stream:
                fb_content = cast(str, fb.get("content") or "")
                if fb_content and not fb_content.endswith("\n"):
                    fb["content"] = fb_content + "\n"
            return await send_func(fb)

    # ---- Media upload ----

    async def upload_group_and_c2c_image(
        self, image_base64: str, file_type: int, **kwargs
    ) -> botpy.types.message.Media:
        p = {"file_data": image_base64, "file_type": file_type, "srv_send_msg": False}

        @_qqofficial_retry()
        async def _do():
            if "openid" in kwargs:
                p["openid"] = kwargs["openid"]
                route = Route("POST", "/v2/users/{openid}/files", openid=kwargs["openid"])
            elif "group_openid" in kwargs:
                p["group_openid"] = kwargs["group_openid"]
                route = Route(
                    "POST", "/v2/groups/{group_openid}/files", group_openid=kwargs["group_openid"]
                )
            else:
                raise ValueError("Invalid upload params")
            result = await self.bot.api._http.request(route, json=p)
            if result is None:
                raise APIReturnNoneError("上传API返回None")
            return result

        try:
            result = await _do()
        except APIReturnNoneError:
            logger.warning(f"[QQOfficialWS] 上传图片API返回None，已重试: {p}")
            raise

        if not isinstance(result, dict):
            raise ValueError(f"Unexpected upload result: {result}")
        return {"file_uuid": result.get("file_uuid", ""), "file_info": result.get("file_info", ""), "ttl": result.get("ttl", 0)}

    async def upload_group_and_c2c_media(
        self, file_source: str, file_type: int, file_name: str = "", **kwargs
    ) -> Media | None:
        try:
            if is_file_uri(file_source):
                file_source = file_uri_to_path(file_source)
        except Exception:
            pass

        data = None
        try:
            async with aiofiles.open(file_source, "rb") as f:
                data = await f.read()
        except Exception:
            url = await MediaResolver(file_source, media_type="file").to_url()
            if url:
                data = url

        if data is None:
            logger.warning("[QQOfficialWS] 无法读取媒体文件: %s", file_source)
            return None

        @_qqofficial_retry()
        async def _do():
            form = aiofiles.tempfile if hasattr(aiofiles, "tempfile") else None
            if isinstance(data, bytes):
                payload = {
                    "file_data": base64.b64encode(data).decode("utf-8"),
                    "file_type": file_type,
                    "srv_send_msg": False,
                }
            else:
                payload = {"url": str(data), "file_type": file_type, "srv_send_msg": False}

            if "openid" in kwargs:
                payload["openid"] = kwargs["openid"]
                route = Route("POST", "/v2/users/{openid}/files", openid=kwargs["openid"])
            elif "group_openid" in kwargs:
                payload["group_openid"] = kwargs["group_openid"]
                route = Route(
                    "POST", "/v2/groups/{group_openid}/files", group_openid=kwargs["group_openid"]
                )
            else:
                raise ValueError("Invalid upload params")
            result = await self.bot.api._http.request(route, json=payload)
            if result is None:
                raise APIReturnNoneError("上传API返回None")
            return result

        try:
            result = await _do()
        except APIReturnNoneError:
            logger.warning(f"[QQOfficialWS] 上传媒体API返回None: file_type={file_type}")
            raise

        if not isinstance(result, dict):
            return None
        return {"file_uuid": result.get("file_uuid", ""), "file_info": result.get("file_info", ""), "ttl": result.get("ttl", 0)}

    # ---- C2C message posting ----

    @staticmethod
    @_qqofficial_retry()
    async def post_c2c_message(
        sender, openid: str, stream: dict | None = None, **payload
    ):
        if stream and payload.get("markdown"):
            md = payload["markdown"]
            md_content = md.content if hasattr(md, "content") else md.get("content", "")
            payload["markdown"] = {
                "content": md_content,
                "stream": stream,
            }
        route = Route("POST", "/v2/users/{openid}/messages", openid=openid)
        result = await sender.bot.api._http.request(route, json=payload)
        if result is None:
            raise APIReturnNoneError("C2C发消息API返回None")
        return result

    # ---- Parse MessageChain to QQ format ----

    @staticmethod
    async def _parse_to_qqofficial(message: MessageChain):
        plain_text = ""
        image_base64 = ""
        image_path = ""
        record_file_path = ""
        video_file_source = ""
        file_source = ""
        file_name = ""

        for comp in message.chain:
            if isinstance(comp, Plain):
                plain_text += comp.text
            elif isinstance(comp, Image):
                img_path = await comp.convert_to_file_path()
                if not img_path:
                    continue
                if is_file_uri(img_path):
                    img_path = file_uri_to_path(img_path)
                try:
                    async with aiofiles.open(img_path, "rb") as f:
                        data = await f.read()
                    image_base64 = base64.b64encode(data).decode("utf-8")
                except Exception:
                    image_path = img_path
            elif isinstance(comp, Record):
                record_file_path = await comp.convert_to_file_path()
            elif isinstance(comp, Video):
                video_file_source = await comp.convert_to_file_path()
            elif isinstance(comp, File):
                try:
                    file_source = await comp.convert_to_file_path()
                except Exception:
                    file_source = comp.url or comp.file_ or ""
                file_name = comp.name or ""

        return (
            plain_text,
            image_base64,
            image_path,
            record_file_path,
            video_file_source,
            file_source,
            file_name,
        )
