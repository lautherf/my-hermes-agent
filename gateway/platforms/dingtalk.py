"""
DingTalk platform adapter using Stream Mode.

Uses dingtalk-stream SDK for real-time message reception without webhooks.
Responses are sent via DingTalk's session webhook (markdown format).

Requires:
    pip install dingtalk-stream httpx
    DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET env vars

Configuration in config.yaml:
    platforms:
      dingtalk:
        enabled: true
        extra:
          client_id: "your-app-key"      # or DINGTALK_CLIENT_ID env var
          client_secret: "your-secret"   # or DINGTALK_CLIENT_SECRET env var
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import dingtalk_stream
    from dingtalk_stream import ChatbotHandler, ChatbotMessage
    DINGTALK_STREAM_AVAILABLE = True
except ImportError:
    DINGTALK_STREAM_AVAILABLE = False
    dingtalk_stream = None  # type: ignore[assignment]

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 20000
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]


def check_dingtalk_requirements() -> bool:
    """Check if DingTalk dependencies are available and configured."""
    if not DINGTALK_STREAM_AVAILABLE or not HTTPX_AVAILABLE:
        return False
    if not os.getenv("DINGTALK_CLIENT_ID") or not os.getenv("DINGTALK_CLIENT_SECRET"):
        return False
    return True


class DingTalkAdapter(BasePlatformAdapter):
    """DingTalk chatbot adapter using Stream Mode.

    The dingtalk-stream SDK maintains a long-lived WebSocket connection.
    Incoming messages arrive via a ChatbotHandler callback. Replies are
    sent via the incoming message's session_webhook URL using httpx.
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.DINGTALK)

        extra = config.extra or {}
        self._client_id: str = extra.get("client_id") or os.getenv("DINGTALK_CLIENT_ID", "")
        self._client_secret: str = extra.get("client_secret") or os.getenv("DINGTALK_CLIENT_SECRET", "")

        self._stream_client: Any = None
        self._stream_task: Optional[asyncio.Task] = None
        self._http_client: Optional["httpx.AsyncClient"] = None

        # Message deduplication: msg_id -> timestamp
        self._seen_messages: Dict[str, float] = {}
        # Map chat_id -> session_webhook for reply routing
        self._session_webhooks: Dict[str, str] = {}

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self) -> bool:
        """Connect to DingTalk via Stream Mode."""
        logger.info("[DingTalk] 🔌 Attempting to connect to DingTalk...")
        
        if not DINGTALK_STREAM_AVAILABLE:
            logger.error("[DingTalk] dingtalk-stream not installed. Run: pip install dingtalk-stream")
            return False
        if not HTTPX_AVAILABLE:
            logger.error("[DingTalk] httpx not installed. Run: pip install httpx")
            return False
        if not self._client_id or not self._client_secret:
            logger.error("[DingTalk] DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET required")
            return False

        logger.info("[DingTalk] 🔑 Client ID available: %s", self._client_id[:8] + "...")
        
        try:
            logger.info("[DingTalk] 🌐 Creating HTTP client...")
            self._http_client = httpx.AsyncClient(timeout=30.0)

            logger.info("[DingTalk] 🎫 Creating DingTalk credential...")
            credential = dingtalk_stream.Credential(self._client_id, self._client_secret)
            
            logger.info("[DingTalk] 📡 Creating stream client...")
            self._stream_client = dingtalk_stream.DingTalkStreamClient(credential)

            # Capture the current event loop for cross-thread dispatch
            loop = asyncio.get_running_loop()
            logger.info("[DingTalk] 🔄 Creating message handler...")
            handler = _IncomingHandler(self, loop)
            self._stream_client.register_callback_handler(
                dingtalk_stream.ChatbotMessage.TOPIC, handler
            )

            logger.info("[DingTalk] 🚀 Starting stream task...")
            self._stream_task = asyncio.create_task(self._run_stream())
            self._mark_connected()
            logger.info("[DingTalk] ✅ Connected successfully via Stream Mode")
            return True
        except Exception as e:
            logger.error("[DingTalk] ❌ Failed to connect: %s", e)
            logger.exception("[DingTalk] Full connection error trace:")
            return False

    async def _run_stream(self) -> None:
        """Run the blocking stream client with auto-reconnection."""
        backoff_idx = 0
        stream_task = None
        while self._running:
            try:
                logger.debug("[%s] Starting stream client...", self.name)
                # start() runs forever in a loop, create task and don't await
                # This allows us to cancel it when needed for reconnection
                stream_task = asyncio.create_task(self._stream_client.start())
                # Wait for the task to complete (will only complete on error or cancellation)
                await stream_task
                stream_task = None
                # If we get here, the stream ended unexpectedly - fall through to reconnect
            except asyncio.CancelledError:
                if stream_task:
                    stream_task.cancel()
                    stream_task = None
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] Stream client error: %s", self.name, e)

            if not self._running:
                return

            delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            logger.info("[%s] Reconnecting in %ds...", self.name, delay)
            await asyncio.sleep(delay)
            backoff_idx += 1

    async def disconnect(self) -> None:
        """Disconnect from DingTalk."""
        self._running = False
        self._mark_disconnected()

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        self._stream_client = None
        self._session_webhooks.clear()
        self._seen_messages.clear()
        logger.info("[%s] Disconnected", self.name)

    # -- Inbound message processing -----------------------------------------

    async def _on_message(self, message: "ChatbotMessage") -> None:
        """Process an incoming DingTalk chatbot message."""
        import traceback
        msg_id = getattr(message, "message_id", None) or uuid.uuid4().hex
        
        logger.info("[DingTalk] 🔔 [_ON_MESSAGE START] Processing message ID: %s", msg_id)
        
        # Handle new CallbackMessage format with 'data' attribute
        data = getattr(message, "data", None)
        if data and isinstance(data, dict):
            logger.info("[DingTalk] 🔔 Using new message format with data attribute")
            # Extract from nested data structure
            text = data.get("text", {})
            if isinstance(text, dict):
                message.text = text.get("content", "")
            else:
                message.text = str(text) if text else ""
            
            message.conversation_id = data.get("conversationId", "")
            message.sender_id = data.get("senderId", "")
            message.sender_nick = data.get("senderNick", "")
            message.sender_staff_id = data.get("senderStaffId", "")
            message.conversation_type = data.get("conversationType", "1")
            message.session_webhook = data.get("sessionWebhook", "")
            message.message_id = data.get("msgId", msg_id)
            
            # Handle createAt timestamp
            create_at = data.get("createAt")
            if create_at:
                message.create_at = create_at
                
        logger.info("[DingTalk] 🔔 Message details: conversation_id=%s, sender_id=%s, text=%s", 
                    getattr(message, "conversation_id", "?"),
                    getattr(message, "sender_id", "?"),
                    str(getattr(message, "text", "?"))[:50])
        
        if self._is_duplicate(msg_id):
            logger.debug("[DingTalk] Duplicate message %s, skipping", msg_id)
            return

        text = self._extract_text(message)
        if not text:
            logger.debug("[DingTalk] Empty message, skipping")
            return

        logger.info("[DingTalk] 📝 Extracted text: %s", text[:100])

        # Chat context
        conversation_id = getattr(message, "conversation_id", "") or ""
        conversation_type = getattr(message, "conversation_type", "1")
        is_group = str(conversation_type) == "2"
        sender_id = getattr(message, "sender_id", "") or ""
        sender_nick = getattr(message, "sender_nick", "") or sender_id
        sender_staff_id = getattr(message, "sender_staff_id", "") or ""

        chat_id = conversation_id or sender_id
        chat_type = "group" if is_group else "dm"

        logger.info("[DingTalk] 👤 Sender: %s (ID: %s), Chat: %s (%s)", 
                    sender_nick, sender_id, chat_id[:20] if chat_id else "?", chat_type)

        # Store session webhook for reply routing
        session_webhook = getattr(message, "session_webhook", None) or ""
        if session_webhook and chat_id:
            self._session_webhooks[chat_id] = session_webhook
            logger.debug("[DingTalk] 📍 Stored session webhook for chat: %s", chat_id[:20])

        source = self.build_source(
            chat_id=chat_id,
            chat_name=getattr(message, "conversation_title", None),
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_nick,
            user_id_alt=sender_staff_id if sender_staff_id else None,
        )

        # Parse timestamp
        create_at = getattr(message, "create_at", None)
        try:
            timestamp = datetime.fromtimestamp(int(create_at) / 1000, tz=timezone.utc) if create_at else datetime.now(tz=timezone.utc)
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=msg_id,
            raw_message=message,
            timestamp=timestamp,
        )

        logger.info("[DingTalk] 🚀 Starting message handling for event...")
        try:
            logger.info("[DingTalk] ⏳ About to call handle_message()...")
            await self.handle_message(event)
            logger.info("[DingTalk] ✅ Message handling completed successfully")
        except Exception as e:
            logger.error("[DingTalk] ❌ Error in message handling: %s", e)
            logger.exception("[DingTalk] Full error trace:")
        
        logger.info("[DingTalk] 🔚 [_ON_MESSAGE END] Finished processing message ID: %s", msg_id)

    @staticmethod
    def _extract_text(message: "ChatbotMessage") -> str:
        """Extract plain text from a DingTalk chatbot message."""
        # Debug: log all message attributes
        logger.info("[DingTalk] 🔍 Message object type: %s", type(message).__name__)
        
        # Handle CallbackMessage with 'data' attribute
        data = getattr(message, "data", None)
        if data and isinstance(data, dict):
            logger.info("[DingTalk] 🔍 Found data attribute, extracting from nested structure")
            # Text is nested: data['text']['content']
            text_obj = data.get("text", {})
            if isinstance(text_obj, dict):
                content = text_obj.get("content", "").strip()
            else:
                content = str(text_obj).strip()
            
            if not content:
                # Fall back to rich text
                rich_text = data.get("rich_text")
                if rich_text and isinstance(rich_text, list):
                    parts = [item["text"] for item in rich_text
                             if isinstance(item, dict) and item.get("text")]
                    content = " ".join(parts).strip()
            
            if not content:
                # Try content field directly
                content = data.get("content", "").strip()
                
            logger.info("[DingTalk] 🔍 Extracted from data.text.content: %s", content)
            return content
        
        # Original handling for direct text attribute
        text = getattr(message, "text", None) or ""
        if isinstance(text, dict):
            content = text.get("content", "").strip()
        else:
            content = str(text).strip()

        # Fall back to rich text if present
        if not content:
            rich_text = getattr(message, "rich_text", None)
            if rich_text and isinstance(rich_text, list):
                parts = [item["text"] for item in rich_text
                         if isinstance(item, dict) and item.get("text")]
                content = " ".join(parts).strip()
                        
        logger.info("[DingTalk] 🔍 Final extracted content: %s", content[:100] if content else "(empty)")
        return content

    # -- Deduplication ------------------------------------------------------

    def _is_duplicate(self, msg_id: str) -> bool:
        """Check and record a message ID. Returns True if already seen."""
        now = time.time()
        if len(self._seen_messages) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen_messages = {k: v for k, v in self._seen_messages.items() if v > cutoff}

        if msg_id in self._seen_messages:
            return True
        self._seen_messages[msg_id] = now
        return False

    # -- Outbound messaging -------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a markdown reply via DingTalk session webhook."""
        metadata = metadata or {}

        logger.info("[DingTalk] 📤 Attempting to send message to chat: %s", chat_id[:20])
        
        session_webhook = metadata.get("session_webhook") or self._session_webhooks.get(chat_id)
        if not session_webhook:
            logger.error("[DingTalk] ❌ No session_webhook available for chat: %s", chat_id[:20])
            return SendResult(success=False,
                              error="No session_webhook available. Reply must follow an incoming message.")

        if not self._http_client:
            logger.error("[DingTalk] ❌ HTTP client not initialized")
            return SendResult(success=False, error="HTTP client not initialized")

        payload = {
            "msgtype": "markdown",
            "markdown": {"title": "Hermes", "text": content[:self.MAX_MESSAGE_LENGTH]},
        }

        logger.debug("[DingTalk] 📋 Sending payload: %s", payload)

        try:
            logger.info("[DingTalk] 🌐 HTTP POST to session webhook...")
            resp = await self._http_client.post(session_webhook, json=payload, timeout=15.0)
            logger.debug("[DingTalk] 📊 Response status: %d", resp.status_code)
            
            if resp.status_code < 300:
                message_id = uuid.uuid4().hex[:12]
                logger.info("[DingTalk] ✅ Message sent successfully with ID: %s", message_id)
                return SendResult(success=True, message_id=message_id)
            else:
                body = resp.text
                logger.warning("[DingTalk] ⚠️ Send failed HTTP %d: %s", resp.status_code, body[:200])
                return SendResult(success=False, error=f"HTTP {resp.status_code}: {body[:200]}")
        except httpx.TimeoutException:
            logger.error("[DingTalk] ⏰ Timeout sending message to DingTalk")
            return SendResult(success=False, error="Timeout sending message to DingTalk")
        except Exception as e:
            logger.error("[DingTalk] 💥 Send error: %s", e)
            logger.exception("[DingTalk] Full send error trace:")
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """DingTalk does not support typing indicators."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic info about a DingTalk conversation."""
        return {"name": chat_id, "type": "group" if "group" in chat_id.lower() else "dm"}


# ---------------------------------------------------------------------------
# Internal stream handler
# ---------------------------------------------------------------------------

class _IncomingHandler(ChatbotHandler if DINGTALK_STREAM_AVAILABLE else object):
    """dingtalk-stream ChatbotHandler that forwards messages to the adapter."""

    def __init__(self, adapter: DingTalkAdapter, loop: asyncio.AbstractEventLoop):
        if DINGTALK_STREAM_AVAILABLE:
            super().__init__()
        self._adapter = adapter
        self._loop = loop

    async def process(self, message: "ChatbotMessage"):
        """Called by dingtalk-stream in its thread when a message arrives.

        Must be async and return a tuple (code, message) for dingtalk_stream raw_process.

        IMPORTANT: This runs on dingtalk-stream's websocket thread. We must NOT block
        waiting for the message handler - that can take minutes as it runs the agent.
        Instead, we fire-and-forget the message processing and return immediately.
        """
        msg_id = getattr(message, "message_id", "unknown") or getattr(message, "text", "unknown")
        logger.info("[DingTalk] 📥 [PROCESS START] Received message: %s", msg_id)
        
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.error("[DingTalk] ❌ Event loop unavailable or closed, cannot dispatch message")
            return dingtalk_stream.AckMessage.STATUS_OK, "OK"

        logger.info("[DingTalk] 🔄 Event loop is running: %s, is_closed: %s", loop.is_running(), loop.is_closed())
        
        # Fire-and-forget: submit the async work to the event loop without waiting.
        # This is critical - blocking here would hang the websocket connection.
        # The message handler runs the agent which can take minutes, so we can't wait.
        try:
            # Submit the work to the event loop and immediately return
            # Don't wait for result! The handler will run asynchronously in the background.
            # If we waited here and the handler took >60s, the websocket would timeout.
            logger.info("[DingTalk] 🚀 Submitting message to event loop (fire-and-forget)...")
            future = asyncio.run_coroutine_threadsafe(self._adapter._on_message(message), loop)
            logger.info("[DingTalk] ✅ Message submitted successfully, future: %s", future)
            
            # Check if pending immediately (should be since we didn't wait)
            logger.info("[DingTalk] 🔍 Future not done (should be True): %s", not future.done())
            
            # Note: NOT waiting for result - this is the key fix!
            logger.debug("[DingTalk] Returning OK to dingtalk-stream immediately")

        except Exception as e:
            logger.exception("[DingTalk] ❌ Error dispatching incoming message: %s", e)

        logger.info("[DingTalk] 👋 [PROCESS END] Returning OK, message is being handled in background")
        return dingtalk_stream.AckMessage.STATUS_OK, "OK"
