#!/usr/bin/env python3
"""
Invite-gated Telegram bot for a private supergroup (python-telegram-bot v21+).

How it works
------------
* Every new member is muted right away (can_send_messages=False) but keeps the
  right to invite other users (can_invite_users=True).
* Every member gets a personal invite link (created by the bot, stored in a DB).
* When somebody joins through a personal link, the link owner gets +1 invite.
* At REQUIRED_INVITES (3) invites the owner is unmuted automatically.
* If a member with fewer invites manages to write a message, the bot deletes it,
  mutes the member again and posts their personal link.
* Admins and the group owner are never restricted.

Storage
-------
* DATABASE_URL set  -> PostgreSQL (e.g. a free Neon database). Use this on hosts
                       with a temporary filesystem, such as Render's free tier.
* DATABASE_URL unset -> local SQLite file (DB_PATH). Use this on your own PC/VPS.

Environment variables
---------------------
BOT_TOKEN     (required) Bot token from @BotFather.
DATABASE_URL  (optional) PostgreSQL connection string. Enables PostgreSQL mode.
DB_PATH       (optional) SQLite file path (SQLite mode only). Default: bot.db
GROUP_ID      (optional) Numeric ID of the supergroup (e.g. -1001234567890).
              If unset, the bot works in the group it sees first.
PORT          (set automatically by Render) If present, the bot starts a tiny
              HTTP server that answers "ok". Render's free Web Service needs an
              open port, and an external pinger can use it to keep the bot awake.

Bot requirements in the group
-----------------------------
The bot must be an ADMIN with these rights: "Restrict members",
"Delete messages", "Invite users via link". Without admin rights Telegram does
not deliver chat_member updates, so joins could not be detected.
The group's default permissions must allow sending messages (members are
restored to the group's default permissions when unlocked).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, Optional

from telegram import (
    Chat,
    ChatMember,
    ChatPermissions,
    LinkPreviewOptions,
    Update,
)
from telegram.constants import ChatMemberStatus, ChatType, ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip() or None
USE_POSTGRES = DATABASE_URL is not None
DB_PATH = os.getenv("DB_PATH", "bot.db")
_group_id_raw = os.getenv("GROUP_ID", "").strip()
GROUP_ID: Optional[int] = int(_group_id_raw) if _group_id_raw else None

REQUIRED_INVITES = 3  # invites needed to unlock chatting
NOTICE_TTL = 120      # seconds until the bot's notices in the group are deleted

# Permissions for muted members: cannot write, but can still invite people.
# (Without use_independent_chat_permissions, can_send_messages=False also
# disables all other "send ..." permissions such as media, polls, stickers.)
LOCKED_PERMISSIONS = ChatPermissions(can_send_messages=False, can_invite_users=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("invite-bot")

# In-memory caches: they keep the database idle most of the time, which matters
# for free serverless databases that bill/suspend by activity.
_known_chats: set[int] = set()
_qualified: set[tuple[int, int]] = set()  # (chat_id, user_id) with enough invites


# --------------------------------------------------------------------------- #
# Database layer (SQLite or PostgreSQL, same SQL for both)
# --------------------------------------------------------------------------- #

SCHEMA = [
    # One row per (group, user): personal link + invite counter.
    """
    CREATE TABLE IF NOT EXISTS users (
        chat_id     BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        invite_link TEXT,
        invites     INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (chat_id, user_id)
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_link ON users (invite_link)",
    # Each invited user is counted only once per group, so leaving and
    # re-joining via the same link cannot be used to farm invites.
    """
    CREATE TABLE IF NOT EXISTS joins (
        chat_id         BIGINT NOT NULL,
        invited_user_id BIGINT NOT NULL,
        inviter_id      BIGINT NOT NULL,
        PRIMARY KEY (chat_id, invited_user_id)
    )
    """,
    # Groups the bot has seen (used to resolve the group in private chats).
    "CREATE TABLE IF NOT EXISTS chats (chat_id BIGINT PRIMARY KEY)",
]


class _PgConnection:
    """Thin wrapper so the SQL below can use '?' placeholders on PostgreSQL too."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def execute(self, sql: str, params: tuple = ()):
        return self._conn.execute(sql.replace("?", "%s"), params or None)


def _open_connection():
    """Open a connection; PostgreSQL gets a few retries (serverless DBs wake up slowly)."""
    if not USE_POSTGRES:
        return sqlite3.connect(DB_PATH)

    import psycopg  # imported lazily: not needed in SQLite mode

    last_error: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            # prepare_threshold=None disables server-side prepared statements,
            # which keeps us compatible with pooled connection strings.
            return psycopg.connect(DATABASE_URL, connect_timeout=20, prepare_threshold=None)
        except psycopg.OperationalError as exc:
            last_error = exc
            logger.warning("Database connection attempt %s/3 failed: %s", attempt, exc)
            time.sleep(2 * attempt)
    raise last_error  # type: ignore[misc]


@contextmanager
def db() -> Iterator:
    """Short-lived connection: commit on success, roll back on error, always close."""
    conn = _open_connection()
    try:
        yield _PgConnection(conn) if USE_POSTGRES else conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if they do not exist yet."""
    with db() as conn:
        for statement in SCHEMA:
            conn.execute(statement)


def remember_chat(chat_id: int) -> None:
    if chat_id in _known_chats:
        return
    with db() as conn:
        conn.execute(
            "INSERT INTO chats (chat_id) VALUES (?) ON CONFLICT DO NOTHING", (chat_id,)
        )
    _known_chats.add(chat_id)


def target_chat_id() -> Optional[int]:
    """Group used by private-chat commands: GROUP_ID, else the first known group."""
    if GROUP_ID is not None:
        return GROUP_ID
    with db() as conn:
        row = conn.execute("SELECT chat_id FROM chats LIMIT 1", ()).fetchone()
    return row[0] if row else None


def get_invites(chat_id: int, user_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT invites FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        ).fetchone()
    return row[0] if row else 0


def get_saved_link(chat_id: int, user_id: int) -> Optional[str]:
    with db() as conn:
        row = conn.execute(
            "SELECT invite_link FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        ).fetchone()
    return row[0] if row else None


def save_link(chat_id: int, user_id: int, link: str) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO users (chat_id, user_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
            (chat_id, user_id),
        )
        conn.execute(
            "UPDATE users SET invite_link = ? WHERE chat_id = ? AND user_id = ?",
            (link, chat_id, user_id),
        )


def find_inviter(chat_id: int, link: str) -> Optional[int]:
    """Return the user ID that owns the given personal link."""
    with db() as conn:
        row = conn.execute(
            "SELECT user_id FROM users WHERE chat_id = ? AND invite_link = ?",
            (chat_id, link),
        ).fetchone()
    return row[0] if row else None


def register_join(chat_id: int, invited_id: int, inviter_id: int) -> Optional[int]:
    """
    Credit one invite to `inviter_id` for bringing in `invited_id`.

    Returns the inviter's new invite count, or None if this user was already
    counted before (no double counting).
    """
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO joins (chat_id, invited_user_id, inviter_id) VALUES (?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            (chat_id, invited_id, inviter_id),
        )
        if cur.rowcount == 0:
            return None
        conn.execute(
            "INSERT INTO users (chat_id, user_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
            (chat_id, inviter_id),
        )
        conn.execute(
            "UPDATE users SET invites = invites + 1 WHERE chat_id = ? AND user_id = ?",
            (chat_id, inviter_id),
        )
        row = conn.execute(
            "SELECT invites FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, inviter_id),
        ).fetchone()
    return row[0]


# --------------------------------------------------------------------------- #
# Telegram helpers
# --------------------------------------------------------------------------- #

def is_target_chat(chat: Chat) -> bool:
    """Only act inside the configured supergroup."""
    if chat.type != ChatType.SUPERGROUP:
        return False
    return GROUP_ID is None or chat.id == GROUP_ID


def is_member_status(member: ChatMember) -> bool:
    """True if the user is currently inside the chat."""
    if member.status in (
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    ):
        return True
    # A restricted user may still be in the chat (is_member=True) or not.
    return member.status == ChatMemberStatus.RESTRICTED and bool(
        getattr(member, "is_member", False)
    )


async def is_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Admins and the owner are exempt from all restrictions."""
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError as exc:
        logger.warning("get_chat_member failed (%s, %s): %s", chat_id, user_id, exc)
        return False
    return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)


async def is_group_member(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError:
        return False
    return is_member_status(member)


async def lock_user(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Mute a user (they can still invite others)."""
    try:
        await context.bot.restrict_chat_member(
            chat_id=chat_id, user_id=user_id, permissions=LOCKED_PERMISSIONS
        )
        return True
    except TelegramError as exc:
        logger.warning("Could not restrict user %s in %s: %s", user_id, chat_id, exc)
        return False


async def unlock_user(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """Restore the group's default permissions for a user."""
    try:
        chat = await context.bot.get_chat(chat_id)
        permissions = chat.permissions or ChatPermissions.all_permissions()
        await context.bot.restrict_chat_member(
            chat_id=chat_id, user_id=user_id, permissions=permissions
        )
        return True
    except TelegramError as exc:
        logger.warning("Could not unrestrict user %s in %s: %s", user_id, chat_id, exc)
        return False


async def get_or_create_link(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int
) -> Optional[str]:
    """Return the user's personal invite link, creating it on first use."""
    link = get_saved_link(chat_id, user_id)
    if link:
        return link
    try:
        invite = await context.bot.create_chat_invite_link(
            chat_id=chat_id,
            name=f"invited by {user_id}"[:32],  # Telegram limit: 32 chars
        )
    except TelegramError as exc:
        logger.error("Could not create invite link for %s in %s: %s", user_id, chat_id, exc)
        return None
    save_link(chat_id, user_id, invite.invite_link)
    return invite.invite_link


async def _delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id, message_id)
    except TelegramError:
        pass  # already deleted or no rights - nothing to do


async def send_notice(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    """Post a temporary bot message in the group (auto-deleted after NOTICE_TTL)."""
    try:
        sent = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except TelegramError as exc:
        logger.warning("Could not send notice to %s: %s", chat_id, exc)
        return
    context.application.create_task(
        _delete_later(context, chat_id, sent.message_id, NOTICE_TTL)
    )


def link_text(link: Optional[str]) -> str:
    return link or "(link is unavailable right now, please contact an admin)"


# --------------------------------------------------------------------------- #
# Group event handlers
# --------------------------------------------------------------------------- #

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remember the group when the bot is added to it / promoted."""
    event = update.my_chat_member
    if event is None or not is_target_chat(event.chat):
        return
    if event.new_chat_member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
        remember_chat(event.chat.id)
        logger.info("Bot is active in chat %s (%s)", event.chat.id, event.chat.title)


async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle joins: mute the newcomer, credit the inviter, send the personal link."""
    event = update.chat_member
    if event is None or not is_target_chat(event.chat):
        return

    chat_id = event.chat.id
    remember_chat(chat_id)

    old, new = event.old_chat_member, event.new_chat_member
    # React only to "was outside, now inside" transitions. Our own restrict /
    # unrestrict calls (member -> restricted, etc.) are ignored here.
    if is_member_status(old) or not is_member_status(new):
        return

    user = new.user
    if user.is_bot:
        return

    # 1) Credit the inviter if the user joined through somebody's personal link.
    link_obj = event.invite_link
    if link_obj and link_obj.invite_link:
        inviter_id = find_inviter(chat_id, link_obj.invite_link)
        if inviter_id and inviter_id != user.id:
            count = register_join(chat_id, user.id, inviter_id)
            # Exactly at the threshold -> unlock the inviter.
            if count == REQUIRED_INVITES:
                if await unlock_user(context, chat_id, inviter_id):
                    _qualified.add((chat_id, inviter_id))
                    try:
                        inviter = (await context.bot.get_chat_member(chat_id, inviter_id)).user
                        await send_notice(
                            context,
                            chat_id,
                            f"ð {inviter.mention_html()} has invited {REQUIRED_INVITES} people "
                            f"and can now write messages!",
                        )
                    except TelegramError:
                        pass

    # 2) Admins/owner are never restricted.
    if new.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        return

    # 3) Returning members who already earned enough invites stay unrestricted.
    count = get_invites(chat_id, user.id)
    if count >= REQUIRED_INVITES:
        _qualified.add((chat_id, user.id))
        return

    # 4) Mute the newcomer and hand out their personal link.
    await lock_user(context, chat_id, user.id)
    link = await get_or_create_link(context, chat_id, user.id)
    await send_notice(
        context,
        chat_id,
        f"ð Welcome, {user.mention_html()}!\n\n"
        f"You can start writing here after you invite {REQUIRED_INVITES} people "
        f"using your personal invite link:\n{link_text(link)}\n\n"
        f"Progress: {count}/{REQUIRED_INVITES} invites.\n"
        f"Send /link or /invites to me in a private chat any time.",
    )


async def guard_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete messages from members who have not invited enough people yet."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if msg is None or user is None or chat is None or not is_target_chat(chat):
        return

    remember_chat(chat.id)

    # Skip anonymous admins, channel posts and bots.
    if msg.sender_chat or user.is_bot:
        return

    # Fast path: users with enough invites are cached in memory (no DB, no API calls).
    key = (chat.id, user.id)
    if key in _qualified:
        return
    count = get_invites(chat.id, user.id)
    if count >= REQUIRED_INVITES:
        _qualified.add(key)
        return

    # Admins and the owner are exempt.
    if await is_admin(context, chat.id, user.id):
        return

    # Delete the message, mute the user again and send the personal link.
    try:
        await msg.delete()
    except TelegramError as exc:
        logger.warning("Could not delete message %s in %s: %s", msg.message_id, chat.id, exc)

    await lock_user(context, chat.id, user.id)
    link = await get_or_create_link(context, chat.id, user.id)
    await send_notice(
        context,
        chat.id,
        f"â ï¸ {user.mention_html()}, your message was removed.\n\n"
        f"You need to invite {REQUIRED_INVITES} people before you can write here. "
        f"Your personal invite link:\n{link_text(link)}\n\n"
        f"Progress: {count}/{REQUIRED_INVITES} invites.",
    )


# --------------------------------------------------------------------------- #
# Commands (private chat with the bot)
# --------------------------------------------------------------------------- #

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "ð Hi! I look after an invite-only community.\n\n"
        f"Every new member starts muted. Invite {REQUIRED_INVITES} people with your "
        f"personal invite link and I will unmute you automatically.\n\n"
        "Commands:\n"
        "/link - get your personal invite link and see how many invites are left\n"
        "/invites - show how many people you have invited\n\n"
        "Admins and the group owner are never restricted."
    )


async def _resolve_group_for_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Find the group for a private-chat command and make sure the user is in it."""
    chat_id = target_chat_id()
    if chat_id is None:
        await update.effective_message.reply_text(
            "I don't know which group to work with yet. Please try again later."
        )
        return None
    if not await is_group_member(context, chat_id, update.effective_user.id):
        await update.effective_message.reply_text(
            "You need to be a member of the group to use this command."
        )
        return None
    return chat_id


async def cmd_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _resolve_group_for_user(update, context)
    if chat_id is None:
        return

    user_id = update.effective_user.id
    link = await get_or_create_link(context, chat_id, user_id)
    count = get_invites(chat_id, user_id)
    remaining = max(REQUIRED_INVITES - count, 0)

    if await is_admin(context, chat_id, user_id):
        status = "You are an admin, so no restrictions apply to you."
    elif remaining == 0:
        status = "You have enough invites and can write messages. ð"
    else:
        status = f"Invite {remaining} more {'person' if remaining == 1 else 'people'} to unlock chatting."

    await update.effective_message.reply_text(
        f"ð Your personal invite link:\n{link_text(link)}\n\n"
        f"Invited so far: {count}/{REQUIRED_INVITES}\n{status}",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


async def cmd_invites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _resolve_group_for_user(update, context)
    if chat_id is None:
        return

    count = get_invites(chat_id, update.effective_user.id)
    remaining = max(REQUIRED_INVITES - count, 0)
    text = f"ð¥ You have invited {count} {'person' if count == 1 else 'people'}."
    if remaining:
        text += f"\n{remaining} more to go. Use /link to get your invite link."
    else:
        text += "\nYou have reached the goal and can write messages. ð"
    await update.effective_message.reply_text(text)


# --------------------------------------------------------------------------- #
# Tiny HTTP server (needed by Render's Web Service; also usable for pings)
# --------------------------------------------------------------------------- #

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args) -> None:  # silence per-request logging
        pass


def start_health_server() -> None:
    """Start the HTTP server in a background thread if a PORT is provided."""
    port = os.getenv("PORT")
    if not port:
        return
    server = ThreadingHTTPServer(("0.0.0.0", int(port)), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health server listening on port %s", port)


# --------------------------------------------------------------------------- #
# Application setup
# --------------------------------------------------------------------------- #

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception while processing an update", exc_info=context.error)


def main() -> None:
    if not BOT_TOKEN:
        sys.exit("Error: the BOT_TOKEN environment variable is not set.")

    init_db()
    logger.info("Storage: %s", "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})")
    start_health_server()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands work in private chats only (muted members cannot write in the group anyway).
    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler("start", cmd_start, filters=private))
    app.add_handler(CommandHandler("link", cmd_link, filters=private))
    app.add_handler(CommandHandler("invites", cmd_invites, filters=private))

    # Join tracking (requires the bot to be an admin) and bot's own status changes.
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Any regular message in the supergroup (service messages are excluded).
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.StatusUpdate.ALL, guard_messages)
    )

    app.add_error_handler(on_error)

    logger.info("Bot started. Polling for updates...")
    # chat_member updates are NOT delivered by default, so request all update types.
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
