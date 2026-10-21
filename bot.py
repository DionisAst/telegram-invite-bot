#!/usr/bin/env python3
"""
Invite-gated posting bot for a Telegram supergroup (python-telegram-bot v21+).

Logic (soft pressure, nobody is muted, the group stays quiet)
-------------------------------------------------------------
* Anybody can join and write, but a post from a member who has invited fewer
  than REQUIRED_INVITES (3) people is deleted by the bot.
* The bot explains what to do in a PRIVATE message to that member (with the
  member's personal invite link and progress), so nothing is visible in the group.
* Telegram does not let a bot message someone first. The bot can write to a person
  (a) after they pressed Start in the bot, or (b) right when they send a join
  request to the group. So:
    - personal invite links are created as "join by request"; when a person asks
      to join, the bot greets them in private and approves the request itself;
    - if a member never opened the bot, the post is still deleted and a short
      hint with an "Open the bot" button appears in the group for HINT_TTL seconds.
* Invites are counted when someone joins through the member's personal link, or
  when the member adds someone manually (Add Members). Each invited user is
  counted once per group.
* At REQUIRED_INVITES invites the member can publish without limits (the bot
  tells them in private).
* Admins and the group owner are never affected.

Storage
-------
* DATABASE_URL set   -> PostgreSQL (e.g. a free Neon database).
* DATABASE_URL unset -> local SQLite file (DB_PATH).

Environment variables
---------------------
BOT_TOKEN       (required) Bot token from @BotFather.
DATABASE_URL    (optional) PostgreSQL connection string. Enables PostgreSQL mode.
DB_PATH         (optional) SQLite file path (SQLite mode only). Default: bot.db
GROUP_ID        (optional) Numeric ID of the supergroup (e.g. -1001234567890).
GROUP_NAME      (optional) Name shown in bot messages. Default: DREC
BOT_LANG        (optional) "ru" (default) or "en" - language of all bot messages.
JOIN_REQUESTS   (optional) "1" (default): personal links work by join request and
                the bot approves requests itself. "0": plain links.
HINT_IN_GROUP   (optional) "1" (default): short auto-deleted hint in the group when
                the bot cannot message the person. "0": delete silently.
GATE_ONLY_NEW   (optional) "0" (default): the rule applies to everybody.
                "1": only people who joined after the bot was started.
PORT            (set automatically by Render) Enables a tiny HTTP server that
                answers "ok" (Render's free Web Service needs an open port).

Bot rights in the group
-----------------------
The bot must be an ADMIN with "Delete messages" and "Invite users via link".
Admin status is also required for Telegram to deliver chat_member updates.

Note: this file is intentionally pure ASCII (non-ASCII text is written as
\\uXXXX escapes) so it survives copy/paste and web editors without corruption.
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
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Update,
)
from telegram.constants import ChatMemberStatus, ChatType, ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ChatJoinRequestHandler,
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
GROUP_NAME = os.getenv("GROUP_NAME", "DREC").strip() or "DREC"
LANG = os.getenv("BOT_LANG", "ru").strip().lower()
JOIN_REQUESTS = os.getenv("JOIN_REQUESTS", "1").strip() != "0"
HINT_IN_GROUP = os.getenv("HINT_IN_GROUP", "1").strip() != "0"
GATE_ONLY_NEW = os.getenv("GATE_ONLY_NEW", "0").strip() == "1"

REQUIRED_INVITES = 3     # invites needed to publish without limits
HINT_TTL = 30            # seconds until the short group hint is deleted
NOTICE_COOLDOWN = 120    # min. seconds between messages to the same user (anti-spam)
ADMIN_CACHE_TTL = 300    # seconds to remember that a user is (not) an admin

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("invite-bot")

# --------------------------------------------------------------------------- #
# Bot messages (placeholders: {mention} {group} {n} {link} {count} {remaining})
# --------------------------------------------------------------------------- #

TEXTS = {
    "ru": {
        "start": (
            "\u041f\u0440\u0438\u0432\u0435\u0442! \u042f \u043f\u043e\u043c\u043e\u0433\u0430\u044e \u0441\u043b\u0435\u0434\u0438\u0442\u044c \u0437\u0430 \u043f\u043e\u0440\u044f\u0434\u043a\u043e\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group}.\n\n"
            "\u0427\u0442\u043e\u0431\u044b \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u0441\u0432\u043e\u0438 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f, \u043d\u0443\u0436\u043d\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443 {n} "
            "\u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432: \u043f\u043e \u0432\u0430\u0448\u0435\u0439 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u043e\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u0438\u043b\u0438 \u0434\u043e\u0431\u0430\u0432\u0438\u0432 \u043b\u044e\u0434\u0435\u0439 "
            "\u0432\u0440\u0443\u0447\u043d\u0443\u044e. \u041f\u043e\u0441\u043b\u0435 \u044d\u0442\u043e\u0433\u043e \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438 \u0431\u0443\u0434\u0443\u0442 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439.\n\n"
            "\u041a\u043e\u043c\u0430\u043d\u0434\u044b:\n"
            "/link - \u0432\u0430\u0448\u0430 \u0441\u0441\u044b\u043b\u043a\u0430 \u0438 \u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0435\u0449\u0451 \u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c\n"
            "/invites - \u0441\u043a\u043e\u043b\u044c\u043a\u043e \u043b\u044e\u0434\u0435\u0439 \u0432\u044b \u0443\u0436\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438\n\n"
            "\u0410\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u044b \u0433\u0440\u0443\u043f\u043f\u044b \u043d\u0435 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0438\u0432\u0430\u044e\u0442\u0441\u044f."
        ),
        "blocked_dm": (
            "\u0414\u043b\u044f \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438 \u0441\u0432\u043e\u0435\u0433\u043e \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group} \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u0435 "
            "\u043f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432.\n\n"
            "\u0412\u0430\u0448\u0430 \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0430. \u041a\u0430\u043a \u0442\u043e\u043b\u044c\u043a\u043e \u0443\u0441\u043b\u043e\u0432\u0438\u0435 \u0431\u0443\u0434\u0435\u0442 \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d\u043e, \u0432\u044b "
            "\u0441\u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439.\n\n"
            "\u0412\u0430\u0448\u0430 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {count} \u0438\u0437 {n}. \u041d\u043e\u0432\u044b\u043c\u0438 \u0441\u0447\u0438\u0442\u0430\u044e\u0442\u0441\u044f \u0442\u0435, \u043a\u0442\u043e \u0432\u0441\u0442\u0443\u043f\u0438\u0442 \u043f\u043e "
            "\u0432\u0430\u0448\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u0438\u043b\u0438 \u043a\u043e\u0433\u043e \u0432\u044b \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0443 \u0441\u0430\u043c\u0438.\n\n"
            "\u0423\u0437\u043d\u0430\u0442\u044c \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: /invites"
        ),
        "join_dm": (
            "\u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443 {group}! \u0412\u0430\u0448\u0430 \u0437\u0430\u044f\u0432\u043a\u0430 \u043d\u0430 \u0432\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0430.\n\n"
            "\u0427\u0442\u043e\u0431\u044b \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u0441\u0432\u043e\u0438 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f, \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u0435 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432. "
            "\u0412\u0430\u0448\u0430 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u041d\u043e\u0432\u044b\u043c\u0438 \u0441\u0447\u0438\u0442\u0430\u044e\u0442\u0441\u044f \u0442\u0435, \u043a\u0442\u043e \u0432\u0441\u0442\u0443\u043f\u0438\u0442 \u043f\u043e \u0432\u0430\u0448\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u0438\u043b\u0438 \u043a\u043e\u0433\u043e \u0432\u044b \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u0435 "
            "\u0432 \u0433\u0440\u0443\u043f\u043f\u0443 \u0441\u0430\u043c\u0438. \u0423\u0437\u043d\u0430\u0442\u044c \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: /invites"
        ),
        "hint": (
            "{mention}, \u0432\u0430\u0448\u0430 \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0430. \u041e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 \u0431\u043e\u0442\u0430 \u0438 \u043d\u0430\u0436\u043c\u0438\u0442\u0435 Start, "
            "\u0447\u0442\u043e\u0431\u044b \u0443\u0437\u043d\u0430\u0442\u044c, \u043a\u0430\u043a \u043e\u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0435 \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group}."
        ),
        "hint_button": "\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u0431\u043e\u0442\u0430",
        "unlocked_dm": (
            "\u0412\u044b \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438 {n} \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432. \u0422\u0435\u043f\u0435\u0440\u044c \u0432\u044b \u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c "
            "\u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group} \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439!"
        ),
        "link_reply": (
            "\u0412\u0430\u0448\u0430 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {count} \u0438\u0437 {n}\n{status}"
        ),
        "status_admin": "\u0412\u044b \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440, \u043f\u043e\u044d\u0442\u043e\u043c\u0443 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u044f \u0432\u0430\u0441 \u043d\u0435 \u043a\u0430\u0441\u0430\u044e\u0442\u0441\u044f.",
        "status_done": "\u0412\u044b \u043d\u0430\u0431\u0440\u0430\u043b\u0438 \u043d\u0443\u0436\u043d\u043e\u0435 \u0447\u0438\u0441\u043b\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0439 \u0438 \u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439.",
        "status_left": "\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c: {remaining}.",
        "invites_reply": "\u0412\u044b \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438: {count} \u0438\u0437 {n}.",
        "invites_left": "\u041e\u0441\u0442\u0430\u043b\u043e\u0441\u044c \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c: {remaining}. \u041a\u043e\u043c\u0430\u043d\u0434\u0430 /link \u043f\u043e\u043a\u0430\u0436\u0435\u0442 \u0432\u0430\u0448\u0443 \u0441\u0441\u044b\u043b\u043a\u0443.",
        "invites_done": "\u0412\u044b \u043d\u0430\u0431\u0440\u0430\u043b\u0438 \u043d\u0443\u0436\u043d\u043e\u0435 \u0447\u0438\u0441\u043b\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0439 \u0438 \u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439.",
        "no_group": "\u042f \u043f\u043e\u043a\u0430 \u043d\u0435 \u0437\u043d\u0430\u044e, \u0441 \u043a\u0430\u043a\u043e\u0439 \u0433\u0440\u0443\u043f\u043f\u043e\u0439 \u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043f\u043e\u0437\u0436\u0435.",
        "not_member": "\u042d\u0442\u0430 \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430 \u0442\u043e\u043b\u044c\u043a\u043e \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u0430\u043c \u0433\u0440\u0443\u043f\u043f\u044b.",
        "link_unavailable": "(\u0441\u0441\u044b\u043b\u043a\u0430 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430, \u043e\u0431\u0440\u0430\u0442\u0438\u0442\u0435\u0441\u044c \u043a \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0443)",
    },
    "en": {
        "start": (
            "Hi! I keep order in the {group} group.\n\n"
            "To publish your own ads, invite {n} new members: with your personal "
            "invite link or by adding people manually. After that you can "
            "publish without limits.\n\n"
            "Commands:\n"
            "/link - your link and how many invites are left\n"
            "/invites - how many people you have invited\n\n"
            "Group admins are never restricted."
        ),
        "blocked_dm": (
            "To publish your ad in the {group} group, please invite {n} new "
            "members.\n\n"
            "Your post was deleted. Once you reach the goal, you can publish "
            "ads without limits.\n\n"
            "Your personal invite link:\n{link}\n\n"
            "Invited: {count} of {n}. New members are those who join through "
            "your link or whom you add to the group yourself.\n\n"
            "Check progress: /invites"
        ),
        "join_dm": (
            "Welcome to the {group} group! Your join request has been approved.\n\n"
            "To publish your own ads, invite {n} new members. Your personal "
            "invite link:\n{link}\n\n"
            "New members are those who join through your link or whom you add to "
            "the group yourself. Check progress: /invites"
        ),
        "hint": (
            "{mention}, your post was deleted. Open the bot and press Start to "
            "find out how to publish ads in the {group} group."
        ),
        "hint_button": "Open the bot",
        "unlocked_dm": (
            "You have invited {n} members. You can now publish ads in the "
            "{group} group without limits!"
        ),
        "link_reply": (
            "Your personal invite link:\n{link}\n\n"
            "Invited: {count} of {n}\n{status}"
        ),
        "status_admin": "You are an admin, so no restrictions apply to you.",
        "status_done": "You have enough invites and can publish without limits.",
        "status_left": "Invites left: {remaining}.",
        "invites_reply": "You have invited: {count} of {n}.",
        "invites_left": "Invites left: {remaining}. Use /link to get your invite link.",
        "invites_done": "You have enough invites and can publish without limits.",
        "no_group": "I don't know which group to work with yet. Please try again later.",
        "not_member": "This command is available to group members only.",
        "link_unavailable": "(link is unavailable right now, please contact an admin)",
    },
}
T = TEXTS.get(LANG, TEXTS["ru"])


def say(key: str, **kwargs) -> str:
    """Return a bot message with the common placeholders filled in."""
    return T[key].format(group=GROUP_NAME, n=REQUIRED_INVITES, **kwargs)


# In-memory caches: they keep the database and the Telegram API quiet.
_known_chats: set[int] = set()
_qualified: set[tuple[int, int]] = set()          # (chat_id, user_id) with enough invites
_recorded: set[tuple[int, int]] = set()           # users seen joining after launch
_last_notice: dict[tuple[int, int], float] = {}   # last message sent per user
_admin_cache: dict[tuple[int, int], tuple[bool, float]] = {}


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
    # re-joining cannot be used to farm invites.
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
    # People seen joining the group while the bot was running (for GATE_ONLY_NEW).
    """
    CREATE TABLE IF NOT EXISTS members (
        chat_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        PRIMARY KEY (chat_id, user_id)
    )
    """,
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
            # prepare_threshold=None keeps us compatible with pooled connection strings.
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


def record_member(chat_id: int, user_id: int) -> None:
    """Remember that this user joined while the bot was running."""
    key = (chat_id, user_id)
    if key in _recorded:
        return
    with db() as conn:
        conn.execute(
            "INSERT INTO members (chat_id, user_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
            (chat_id, user_id),
        )
    _recorded.add(key)


def is_recorded_member(chat_id: int, user_id: int) -> bool:
    key = (chat_id, user_id)
    if key in _recorded:
        return True
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM members WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        ).fetchone()
    if row:
        _recorded.add(key)
        return True
    return False


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
    """Admins and the owner are exempt from all rules (result cached for a few minutes)."""
    key = (chat_id, user_id)
    cached = _admin_cache.get(key)
    if cached and time.monotonic() - cached[1] < ADMIN_CACHE_TTL:
        return cached[0]
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError as exc:
        logger.warning("get_chat_member failed (%s, %s): %s", chat_id, user_id, exc)
        return False
    result = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    _admin_cache[key] = (result, time.monotonic())
    return result


async def is_group_member(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError:
        return False
    return is_member_status(member)


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
            creates_join_request=JOIN_REQUESTS,
        )
    except TelegramError as exc:
        logger.error("Could not create invite link for %s in %s: %s", user_id, chat_id, exc)
        return None
    save_link(chat_id, user_id, invite.invite_link)
    return invite.invite_link


async def send_dm(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> bool:
    """
    Send a private message. Returns False if Telegram refuses (the person never
    started the bot or has blocked it) - bots cannot message people first.
    """
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        return True
    except TelegramError as exc:
        logger.info("Cannot send a private message to %s: %s", chat_id, exc)
        return False


async def _delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id, message_id)
    except TelegramError:
        pass  # already deleted or no rights - nothing to do


async def send_group_hint(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_markup=None
) -> None:
    """Post a short temporary bot message in the group (deleted after HINT_TTL)."""
    try:
        sent = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except TelegramError as exc:
        logger.warning("Could not send a hint to %s: %s", chat_id, exc)
        return
    context.application.create_task(
        _delete_later(context, chat_id, sent.message_id, HINT_TTL)
    )


def link_text(link: Optional[str]) -> str:
    return link or T["link_unavailable"]


async def credit_invite(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, invited_id: int, inviter_id: Optional[int]
) -> None:
    """Give the inviter +1 (once per invited user) and tell them privately at the goal."""
    if inviter_id is None or inviter_id == invited_id:
        return
    count = register_join(chat_id, invited_id, inviter_id)
    if count == REQUIRED_INVITES:
        _qualified.add((chat_id, inviter_id))
        await send_dm(context, inviter_id, say("unlocked_dm"))


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


async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Someone asked to join. This is the one moment when a bot may write to a person
    who never started it: greet them privately, approve them, credit the inviter.
    """
    req = update.chat_join_request
    if req is None or not is_target_chat(req.chat):
        return

    chat_id = req.chat.id
    remember_chat(chat_id)
    user = req.from_user
    if user.is_bot:
        return

    # 1) Private greeting with the person's own link (only possible right now).
    link = await get_or_create_link(context, chat_id, user.id)
    count = get_invites(chat_id, user.id)
    await send_dm(
        context, req.user_chat_id, say("join_dm", link=link_text(link), count=count)
    )

    # 2) Approve the request.
    try:
        await req.approve()
    except TelegramError as exc:
        logger.warning("Could not approve join request of %s in %s: %s", user.id, chat_id, exc)
        return
    record_member(chat_id, user.id)

    # 3) Credit the owner of the personal link that was used.
    link_obj = req.invite_link
    if link_obj and link_obj.invite_link:
        await credit_invite(context, chat_id, user.id, find_inviter(chat_id, link_obj.invite_link))


async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Count invites: personal-link joins and members added manually."""
    event = update.chat_member
    if event is None or not is_target_chat(event.chat):
        return

    chat_id = event.chat.id
    remember_chat(chat_id)

    old, new = event.old_chat_member, event.new_chat_member
    # React only to "was outside, now inside" transitions.
    if is_member_status(old) or not is_member_status(new):
        return

    user = new.user
    if user.is_bot:
        return

    record_member(chat_id, user.id)

    # Who gets the credit?
    inviter_id: Optional[int] = None
    link_obj = event.invite_link
    if link_obj and link_obj.invite_link:
        # Joined through an invite link: credit the owner of a personal link.
        inviter_id = find_inviter(chat_id, link_obj.invite_link)
    elif event.from_user and event.from_user.id != user.id and not event.from_user.is_bot:
        # Added manually ("Add members"): credit the person who added them.
        inviter_id = event.from_user.id

    await credit_invite(context, chat_id, user.id, inviter_id)


async def guard_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete posts of members who have not invited enough people yet."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if msg is None or user is None or chat is None or not is_target_chat(chat):
        return

    remember_chat(chat.id)

    # Skip anonymous admins, channel posts and bots.
    if msg.sender_chat or user.is_bot:
        return

    # Fast path: members with enough invites are cached in memory.
    key = (chat.id, user.id)
    if key in _qualified:
        return
    # Optional mode: the rule applies only to people who joined after the bot started.
    if GATE_ONLY_NEW and not is_recorded_member(chat.id, user.id):
        return
    count = get_invites(chat.id, user.id)
    if count >= REQUIRED_INVITES:
        _qualified.add(key)
        return

    # Admins and the owner are exempt.
    if await is_admin(context, chat.id, user.id):
        return

    # 1) Delete the publication.
    try:
        await msg.delete()
    except TelegramError as exc:
        logger.warning("Could not delete message %s in %s: %s", msg.message_id, chat.id, exc)

    # 2) Explain privately (at most once per NOTICE_COOLDOWN seconds, so several
    #    quick posts or an album do not produce a pile of messages).
    now = time.monotonic()
    last = _last_notice.get(key)
    if last is not None and now - last < NOTICE_COOLDOWN:
        return
    _last_notice[key] = now

    link = await get_or_create_link(context, chat.id, user.id)
    delivered = await send_dm(
        context, user.id, say("blocked_dm", link=link_text(link), count=count)
    )

    # 3) The bot cannot message people who never opened it: show a short hint
    #    with an "Open the bot" button; it disappears by itself.
    if not delivered and HINT_IN_GROUP:
        button = InlineKeyboardButton(
            T["hint_button"], url=f"https://t.me/{context.bot.username}?start=post"
        )
        await send_group_hint(
            context,
            chat.id,
            say("hint", mention=user.mention_html()),
            reply_markup=InlineKeyboardMarkup([[button]]),
        )


# --------------------------------------------------------------------------- #
# Commands (private chat with the bot)
# --------------------------------------------------------------------------- #

async def build_link_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> str:
    """Text with the user's personal link, progress and status."""
    link = await get_or_create_link(context, chat_id, user_id)
    count = get_invites(chat_id, user_id)
    remaining = max(REQUIRED_INVITES - count, 0)

    if await is_admin(context, chat_id, user_id):
        status = say("status_admin")
    elif remaining == 0:
        status = say("status_done")
    else:
        status = say("status_left", remaining=remaining)
    return say("link_reply", link=link_text(link), count=count, status=status)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Intro text; group members also get their personal link right away."""
    text = say("start")
    chat_id = target_chat_id()
    user_id = update.effective_user.id
    if chat_id is not None and await is_group_member(context, chat_id, user_id):
        text += "\n\n" + await build_link_message(context, chat_id, user_id)
    await update.effective_message.reply_text(
        text, link_preview_options=LinkPreviewOptions(is_disabled=True)
    )


async def _resolve_group_for_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Find the group for a private-chat command and make sure the user is in it."""
    chat_id = target_chat_id()
    if chat_id is None:
        await update.effective_message.reply_text(say("no_group"))
        return None
    if not await is_group_member(context, chat_id, update.effective_user.id):
        await update.effective_message.reply_text(say("not_member"))
        return None
    return chat_id


async def cmd_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _resolve_group_for_user(update, context)
    if chat_id is None:
        return
    text = await build_link_message(context, chat_id, update.effective_user.id)
    await update.effective_message.reply_text(
        text, link_preview_options=LinkPreviewOptions(is_disabled=True)
    )


async def cmd_invites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _resolve_group_for_user(update, context)
    if chat_id is None:
        return

    count = get_invites(chat_id, update.effective_user.id)
    remaining = max(REQUIRED_INVITES - count, 0)
    text = say("invites_reply", count=count)
    text += "\n" + (say("invites_left", remaining=remaining) if remaining else say("invites_done"))
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
    logger.info(
        "Storage: %s | language: %s | join requests: %s | only new members: %s",
        "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})",
        LANG if LANG in TEXTS else "ru",
        JOIN_REQUESTS,
        GATE_ONLY_NEW,
    )
    start_health_server()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands work in private chats only.
    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler("start", cmd_start, filters=private))
    app.add_handler(CommandHandler("link", cmd_link, filters=private))
    app.add_handler(CommandHandler("invites", cmd_invites, filters=private))

    # Join requests, join tracking (requires the bot to be an admin) and the bot's own status.
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Any regular message in the supergroup (service messages are excluded).
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.StatusUpdate.ALL, guard_messages)
    )

    app.add_error_handler(on_error)

    logger.info("Bot started. Polling for updates...")
    # chat_member / chat_join_request updates are NOT delivered by default.
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
