#!/usr/bin/env python3
"""
Invite-gated posting bot for a Telegram supergroup (python-telegram-bot v21+).

Logic
-----
* Anybody can join and write short messages. Ad-like posts of a member who has
  invited fewer than REQUIRED_INVITES (3) people are deleted by the bot.
  A message counts as an ad if it has media (photo/video/file/voice), a forwarded
  story, a forwarded message, a contact or a place, a long text (AD_MIN_LENGTH
  characters or more), a link / @username / phone number / e-mail, or an ad keyword
  (rent, sale, ...). Ordinary short chat messages and replies are left alone.
* Right after deleting, the bot posts a short message IN THE GROUP (it disappears
  after NOTICE_TTL seconds): the member must invite 3 people to publish ads, until
  then only short messages are available. The message contains the member's
  personal invite link, so nothing else is needed (no Start, no private messages).
* Invites are counted when someone joins through the member's personal link, or
  when the member adds someone manually (Add Members). Each invited user is
  counted once per group.
* At REQUIRED_INVITES invites the member can publish without limits.
* Admins and the group owner are never affected.
* Admin analytics (private chat with the bot, group admins only):
  /stats, /who @username, /export (CSV with every invite: who invited whom, when, how).

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
JOIN_REQUESTS   (optional) "0" (default): personal links are plain links.
                "1": links work by join request and the bot approves them.
SEND_DM         (optional) "0" (default): the bot writes only in the group.
                "1": also send a private copy to people who started the bot.
GATE_ONLY_NEW   (optional) "0" (default): the rule applies to everybody.
                "1": only people who joined after the bot was started.
AD_MIN_LENGTH   (optional) Texts with this many characters or more count as ads.
                Default: 150
AD_KEYWORDS     (optional) Comma-separated ad words (matched at the start of a word,
                case-insensitive). Replaces the built-in list when set.
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
import csv
import io
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, Optional

from telegram import Chat, ChatMember, LinkPreviewOptions, Update
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
JOIN_REQUESTS = os.getenv("JOIN_REQUESTS", "0").strip() == "1"
SEND_DM = os.getenv("SEND_DM", "0").strip() == "1"
GATE_ONLY_NEW = os.getenv("GATE_ONLY_NEW", "0").strip() == "1"
AD_MIN_LENGTH = int(os.getenv("AD_MIN_LENGTH", "150").strip() or 150)

REQUIRED_INVITES = 3     # invites needed to publish without limits
NOTICE_TTL = 60          # seconds until the bot's notices in the group are deleted
NOTICE_COOLDOWN = 60     # min. seconds between notices to the same user (anti-spam)
ADMIN_CACHE_TTL = 300    # seconds to remember that a user is (not) an admin
LIST_LIMIT = 30          # max. lines in a /who list (use /export for everything)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("invite-bot")

# --------------------------------------------------------------------------- #
# Bot messages (placeholders: {mention} {group} {n} {link} {count} {remaining} ...)
# --------------------------------------------------------------------------- #

TEXTS = {
    "ru": {
        "start": (
            "\u041f\u0440\u0438\u0432\u0435\u0442! \u042f \u043f\u043e\u043c\u043e\u0433\u0430\u044e \u0441\u043b\u0435\u0434\u0438\u0442\u044c \u0437\u0430 \u043f\u043e\u0440\u044f\u0434\u043a\u043e\u043c \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group}.\n\n"
            "\u041f\u043e\u043a\u0430 \u0432\u044b \u043d\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432, \u0432\u0430\u043c \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e "
            "\u043a\u043e\u0440\u043e\u0442\u043a\u0438\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f. \u041e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f (\u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0441 \u0444\u043e\u0442\u043e, \u0441\u0441\u044b\u043b\u043a\u0430\u043c\u0438, "
            "\u043a\u043e\u043d\u0442\u0430\u043a\u0442\u0430\u043c\u0438, \u0438\u0441\u0442\u043e\u0440\u0438\u044f\u043c\u0438 \u0438\u043b\u0438 \u0434\u043b\u0438\u043d\u043d\u044b\u043c \u0442\u0435\u043a\u0441\u0442\u043e\u043c) \u0443\u0434\u0430\u043b\u044f\u044e\u0442\u0441\u044f. \u041f\u043e\u0441\u043b\u0435 {n} "
            "\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0439 \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439.\n\n"
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u0442\u044c \u043c\u043e\u0436\u043d\u043e \u043f\u043e \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u043e\u0439 \u0441\u0441\u044b\u043b\u043a\u0435 \u0438\u043b\u0438 \u0434\u043e\u0431\u0430\u0432\u0438\u0432 \u043b\u044e\u0434\u0435\u0439 \u0432\u0440\u0443\u0447\u043d\u0443\u044e.\n\n"
            "\u041a\u043e\u043c\u0430\u043d\u0434\u044b:\n"
            "/link - \u0432\u0430\u0448\u0430 \u0441\u0441\u044b\u043b\u043a\u0430 \u0438 \u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0435\u0449\u0451 \u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u044c\n"
            "/invites - \u0441\u043a\u043e\u043b\u044c\u043a\u043e \u043b\u044e\u0434\u0435\u0439 \u0432\u044b \u0443\u0436\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438\n\n"
            "\u0410\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u044b \u0433\u0440\u0443\u043f\u043f\u044b \u043d\u0435 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0438\u0432\u0430\u044e\u0442\u0441\u044f."
        ),
        "admin_help": (
            "\n\n\u041a\u043e\u043c\u0430\u043d\u0434\u044b \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430:\n"
            "/stats - \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0439 \u0438 \u0442\u043e\u043f\n"
            "/who @username - \u043a\u0442\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b \u0447\u0435\u043b\u043e\u0432\u0435\u043a\u0430 \u0438 \u043a\u043e\u0433\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b \u043e\u043d\n"
            "/export - \u0432\u0441\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044f \u0444\u0430\u0439\u043b\u043e\u043c CSV"
        ),
        "blocked_group": (
            "{mention}, \u0432\u0430\u0448\u0435 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0435 \u0443\u0434\u0430\u043b\u0435\u043d\u043e. \u0414\u043b\u044f \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438 \u0441\u0432\u043e\u0435\u0433\u043e \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f "
            "\u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group} \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u0435 \u043f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432. \u0421\u0435\u0439\u0447\u0430\u0441 "
            "\u0432\u0430\u043c \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e \u043a\u043e\u0440\u043e\u0442\u043a\u0438\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f.\n\n"
            "\u0412\u0430\u0448\u0430 \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {count} \u0438\u0437 {n}. \u041c\u043e\u0436\u043d\u043e \u0442\u0430\u043a\u0436\u0435 \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u043b\u044e\u0434\u0435\u0439 \u0432\u0440\u0443\u0447\u043d\u0443\u044e "
            "\u0447\u0435\u0440\u0435\u0437 \u00ab\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432\u00bb."
        ),
        "blocked_dm": (
            "\u0414\u043b\u044f \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u0438 \u0441\u0432\u043e\u0435\u0433\u043e \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group} \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u0435 "
            "\u043f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432.\n\n"
            "\u0412\u0430\u0448\u0430 \u043f\u0443\u0431\u043b\u0438\u043a\u0430\u0446\u0438\u044f \u0443\u0434\u0430\u043b\u0435\u043d\u0430. \u0421\u0435\u0439\u0447\u0430\u0441 \u0432\u0430\u043c \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b \u0442\u043e\u043b\u044c\u043a\u043e \u043a\u043e\u0440\u043e\u0442\u043a\u0438\u0435 "
            "\u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f.\n\n"
            "\u0412\u0430\u0448\u0430 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {count} \u0438\u0437 {n}. \u0423\u0437\u043d\u0430\u0442\u044c \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: /invites"
        ),
        "join_dm": (
            "\u0414\u043e\u0431\u0440\u043e \u043f\u043e\u0436\u0430\u043b\u043e\u0432\u0430\u0442\u044c \u0432 \u0433\u0440\u0443\u043f\u043f\u0443 {group}! \u0412\u0430\u0448\u0430 \u0437\u0430\u044f\u0432\u043a\u0430 \u043d\u0430 \u0432\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u0435 \u043e\u0434\u043e\u0431\u0440\u0435\u043d\u0430.\n\n"
            "\u0427\u0442\u043e\u0431\u044b \u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u0441\u0432\u043e\u0438 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f, \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u0442\u0435 {n} \u043d\u043e\u0432\u044b\u0445 \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432. "
            "\u0412\u0430\u0448\u0430 \u043f\u0435\u0440\u0441\u043e\u043d\u0430\u043b\u044c\u043d\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430-\u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435:\n{link}\n\n"
            "\u0423\u0437\u043d\u0430\u0442\u044c \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: /invites"
        ),
        "unlocked_group": (
            "{mention}, \u0432\u044b \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b\u0438 {n} \u0443\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432. \u0422\u0435\u043f\u0435\u0440\u044c \u0432\u044b \u043c\u043e\u0436\u0435\u0442\u0435 "
            "\u043f\u0443\u0431\u043b\u0438\u043a\u043e\u0432\u0430\u0442\u044c \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group} \u0431\u0435\u0437 \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u0439!"
        ),
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
        "admin_only": "\u042d\u0442\u0430 \u043a\u043e\u043c\u0430\u043d\u0434\u0430 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430 \u0442\u043e\u043b\u044c\u043a\u043e \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430\u043c \u0433\u0440\u0443\u043f\u043f\u044b.",
        "stats": (
            "\u041f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044f \u0432 \u0433\u0440\u0443\u043f\u043f\u0435 {group}\n\n"
            "\u0412\u0441\u0435\u0433\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {total}\n"
            "\u0423\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432, \u043a\u043e\u0442\u043e\u0440\u044b\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u043b\u0438: {inviters}\n"
            "\u0412\u044b\u043f\u043e\u043b\u043d\u0438\u043b\u0438 \u0443\u0441\u043b\u043e\u0432\u0438\u0435 ({n}+): {qualified}\n\n"
            "\u0422\u043e\u043f \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u044e\u0449\u0438\u0445:\n{top}"
        ),
        "stats_none": "\u043f\u043e\u043a\u0430 \u043d\u0438\u043a\u043e\u0433\u043e",
        "who_usage": "\u0418\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u0438\u0435: /who @username \u0438\u043b\u0438 /who 123456789",
        "who_not_found": "\u0422\u0430\u043a\u043e\u0433\u043e \u0447\u0435\u043b\u043e\u0432\u0435\u043a\u0430 \u043d\u0435\u0442 \u0432 \u0431\u0430\u0437\u0435 \u0431\u043e\u0442\u0430. \u0411\u043e\u0442 \u0437\u0430\u043f\u043e\u043c\u0438\u043d\u0430\u0435\u0442 \u043b\u044e\u0434\u0435\u0439, \u043a\u043e\u0442\u043e\u0440\u044b\u0445 \u0432\u0438\u0434\u0438\u0442 \u043f\u043e\u0441\u043b\u0435 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f.",
        "who_by": "\u0415\u0433\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b: {inviter} ({when}, {source})",
        "who_by_none": "\u041a\u0442\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b: \u0434\u0430\u043d\u043d\u044b\u0445 \u043d\u0435\u0442 (\u0432\u0441\u0442\u0443\u043f\u0438\u043b \u043d\u0435 \u043f\u043e \u0441\u0441\u044b\u043b\u043a\u0435 \u0438 \u043d\u0435 \u0447\u0435\u0440\u0435\u0437 \u0440\u0443\u0447\u043d\u043e\u0435 \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u043b\u0438\u0431\u043e \u0434\u043e \u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u043e\u0434\u0441\u0447\u0451\u0442\u0430).",
        "who_list": "\u041f\u0440\u0438\u0433\u043b\u0430\u0441\u0438\u043b: {count}\n{lines}",
        "who_list_none": "\u041d\u0438\u043a\u043e\u0433\u043e \u043d\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u043b.",
        "more": "...\u0438 \u0435\u0449\u0451 {count}. \u041f\u043e\u043b\u043d\u044b\u0439 \u0441\u043f\u0438\u0441\u043e\u043a: /export",
        "export_empty": "\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u043d\u0438 \u043e\u0434\u043d\u043e\u0433\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044f.",
        "export_caption": "\u0412\u0441\u0435 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u044f: {count}.",
        "src_link": "\u043f\u043e \u0441\u0441\u044b\u043b\u043a\u0435",
        "src_manual": "\u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d \u0432\u0440\u0443\u0447\u043d\u0443\u044e",
        "src_request": "\u043f\u043e \u0437\u0430\u044f\u0432\u043a\u0435",
        "src_unknown": "\u0441\u043f\u043e\u0441\u043e\u0431 \u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u0435\u043d",
        "unknown_date": "\u0434\u0430\u0442\u0430 \u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430",
    },
    "en": {
        "start": (
            "Hi! I keep order in the {group} group.\n\n"
            "Until you have invited {n} new members, only short messages are "
            "available to you. Ads (messages with photos, links, contacts, "
            "stories or long text) are deleted. After {n} invites you can "
            "publish without limits.\n\n"
            "You can invite with your personal link or by adding people manually.\n\n"
            "Commands:\n"
            "/link - your link and how many invites are left\n"
            "/invites - how many people you have invited\n\n"
            "Group admins are never restricted."
        ),
        "admin_help": (
            "\n\nAdmin commands:\n"
            "/stats - invite statistics and the top list\n"
            "/who @username - who invited a person and whom they invited\n"
            "/export - all invites as a CSV file"
        ),
        "blocked_group": (
            "{mention}, your ad was deleted. To publish your ad in the {group} "
            "group, please invite {n} new members. For now only short messages "
            "are available to you.\n\n"
            "Your invite link:\n{link}\n\n"
            "Invited: {count} of {n}. You can also add people manually with "
            "\"Add members\"."
        ),
        "blocked_dm": (
            "To publish your ad in the {group} group, please invite {n} new "
            "members.\n\n"
            "Your post was deleted. For now only short messages are available "
            "to you.\n\n"
            "Your personal invite link:\n{link}\n\n"
            "Invited: {count} of {n}. Check progress: /invites"
        ),
        "join_dm": (
            "Welcome to the {group} group! Your join request has been approved.\n\n"
            "To publish your own ads, invite {n} new members. Your personal "
            "invite link:\n{link}\n\n"
            "Check progress: /invites"
        ),
        "unlocked_group": (
            "{mention}, you have invited {n} members. You can now publish ads "
            "in the {group} group without limits!"
        ),
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
        "admin_only": "This command is available to group admins only.",
        "stats": (
            "Invites in the {group} group\n\n"
            "Total invited: {total}\n"
            "Members who invited someone: {inviters}\n"
            "Reached the goal ({n}+): {qualified}\n\n"
            "Top inviters:\n{top}"
        ),
        "stats_none": "nobody yet",
        "who_usage": "Usage: /who @username or /who 123456789",
        "who_not_found": "This person is not in the bot's database. The bot remembers people it sees after the update.",
        "who_by": "Invited by: {inviter} ({when}, {source})",
        "who_by_none": "Invited by: no data (joined without a link or manual add, or before counting started).",
        "who_list": "Invited: {count}\n{lines}",
        "who_list_none": "Has not invited anyone.",
        "more": "...and {count} more. Full list: /export",
        "export_empty": "There are no invites yet.",
        "export_caption": "All invites: {count}.",
        "src_link": "by link",
        "src_manual": "added manually",
        "src_request": "by join request",
        "src_unknown": "method unknown",
        "unknown_date": "date unknown",
    },
}
T = TEXTS.get(LANG, TEXTS["ru"])


def say(key: str, **kwargs) -> str:
    """Return a bot message with the common placeholders filled in."""
    return T[key].format(group=GROUP_NAME, n=REQUIRED_INVITES, **kwargs)


# --------------------------------------------------------------------------- #
# Ad detection: only ad-like messages are moderated
# --------------------------------------------------------------------------- #

# Built-in ad words, matched at the START of a word (so the stem "\u0430\u0440\u0435\u043d\u0434" catches
# "\u0430\u0440\u0435\u043d\u0434\u0430", "\u0430\u0440\u0435\u043d\u0434\u0443\u044e" ...). Set AD_KEYWORDS to replace this list.
DEFAULT_AD_KEYWORDS = (
    "\u0430\u0440\u0435\u043d\u0434", "\u0441\u0434\u0430\u044e", "\u0441\u0434\u0430\u043c", "\u0441\u0434\u0430\u0435\u043c", "\u0441\u0434\u0430\u0451\u043c", "\u0441\u0434\u0430\u0435\u0442\u0441\u044f", "\u0441\u0434\u0430\u0451\u0442\u0441\u044f",
    "\u043f\u0440\u043e\u0434\u0430\u044e", "\u043f\u0440\u043e\u0434\u0430\u043c", "\u043f\u0440\u043e\u0434\u0430\u0435\u043c", "\u043f\u0440\u043e\u0434\u0430\u0451\u043c", "\u043f\u0440\u043e\u0434\u0430\u0435\u0442\u0441\u044f", "\u043f\u0440\u043e\u0434\u0430\u0451\u0442\u0441\u044f",
    "\u043f\u0440\u043e\u0434\u0430\u0436\u0430", "\u043a\u0443\u043f\u043b\u044e",
    "for rent", "for sale", "to let", "selling",
)
_keywords_env = os.getenv("AD_KEYWORDS", "").strip()
AD_KEYWORDS = tuple(
    k.strip().lower() for k in _keywords_env.split(",") if k.strip()
) if _keywords_env else DEFAULT_AD_KEYWORDS

KEYWORD_RE = re.compile(
    "(?<!\\w)(?:" + "|".join(re.escape(k) for k in AD_KEYWORDS) + ")", re.IGNORECASE
) if AD_KEYWORDS else None
# Links written as plain text (Telegram also marks real links as entities).
LINK_RE = re.compile(
    "https?://|www\\.|t\\.me/|(?<!\\w)[\\w-]+\\.(?:com|ae|ru|net|org|io|me|co|info|app)(?!\\w)",
    re.IGNORECASE,
)
PHONE_RE = re.compile("\\+?\\d[\\d\\s().-]{7,}\\d")
AD_ENTITY_TYPES = ("url", "text_link", "phone_number", "email")


def _has_phone(text: str) -> bool:
    """True if the text contains a phone-like number (9+ digits)."""
    return any(sum(c.isdigit() for c in m.group()) >= 9 for m in PHONE_RE.finditer(text))


def _entities(msg):
    """Yield (entity, text) for the entities of the message text and caption."""
    try:
        for entity in msg.entities or ():
            yield entity, msg.parse_entity(entity)
        for entity in msg.caption_entities or ():
            yield entity, msg.parse_caption_entity(entity)
    except Exception:  # never let parsing problems break moderation
        return


def looks_like_ad(msg) -> bool:
    """Decide whether a message is an advertisement (True) or ordinary chat (False)."""
    # 1) Photos, videos, files, voice messages.
    if (
        msg.photo or msg.video or msg.video_note or msg.animation
        or msg.document or msg.audio or getattr(msg, "voice", None)
    ):
        return True

    # 2) Other formats people use to sneak an offer in: stories, contacts, places,
    #    forwarded messages (for example from channels).
    if (
        getattr(msg, "story", None)
        or getattr(msg, "contact", None)
        or getattr(msg, "venue", None)
        or getattr(msg, "forward_origin", None)
        or getattr(msg, "forward_date", None)
    ):
        return True

    text = msg.text or msg.caption or ""

    # 3) Long text.
    if len(text) >= AD_MIN_LENGTH:
        return True

    # 4) Links, @username, phone numbers, e-mail.
    replied_to = None
    reply = msg.reply_to_message
    if reply is not None and reply.from_user is not None and reply.from_user.username:
        replied_to = reply.from_user.username.lower()
    for entity, value in _entities(msg):
        if entity.type in AD_ENTITY_TYPES:
            return True
        # An @mention of the person you are answering is just a reply, not a contact.
        if entity.type == "mention" and (value or "").lstrip("@").lower() != replied_to:
            return True
    if LINK_RE.search(text) or _has_phone(text):
        return True

    # 5) Ad words ("rent", "sale", ...).
    return bool(KEYWORD_RE and KEYWORD_RE.search(text))


# In-memory caches: they keep the database and the Telegram API quiet.
_known_chats: set[int] = set()
_qualified: set[tuple[int, int]] = set()          # (chat_id, user_id) with enough invites
_recorded: set[tuple[int, int]] = set()           # users seen joining after launch
_last_notice: dict[tuple[int, int], float] = {}   # last notice shown per user
_admin_cache: dict[tuple[int, int], tuple[bool, float]] = {}
_person_cache: dict[int, tuple[Optional[str], Optional[str]]] = {}  # id -> (username, name)


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
    # re-joining cannot be used to farm invites. Also the analytics log.
    """
    CREATE TABLE IF NOT EXISTS joins (
        chat_id         BIGINT NOT NULL,
        invited_user_id BIGINT NOT NULL,
        inviter_id      BIGINT NOT NULL,
        joined_at       TEXT,
        source          TEXT,
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
    # Names and @usernames, so analytics can show people instead of bare IDs.
    """
    CREATE TABLE IF NOT EXISTS people (
        user_id    BIGINT PRIMARY KEY,
        username   TEXT,
        full_name  TEXT,
        updated_at TEXT
    )
    """,
]

# Columns added after the first release (ignored when they already exist).
MIGRATIONS = [
    "ALTER TABLE joins ADD COLUMN joined_at TEXT",
    "ALTER TABLE joins ADD COLUMN source TEXT",
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
    """Create tables if they do not exist yet and add newer columns to old tables."""
    with db() as conn:
        for statement in SCHEMA:
            conn.execute(statement)
    for statement in MIGRATIONS:
        try:
            with db() as conn:
                conn.execute(statement)
        except Exception:
            pass  # the column already exists - nothing to do


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def remember_person(user) -> None:
    """Store the name and @username of a user (only when something changed)."""
    if user is None or user.is_bot:
        return
    username = user.username or None
    full_name = (user.full_name or "").strip() or None
    if _person_cache.get(user.id) == (username, full_name):
        return
    try:
        with db() as conn:
            conn.execute(
                "INSERT INTO people (user_id, username, full_name, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (user_id) DO UPDATE SET username = excluded.username, "
                "full_name = excluded.full_name, updated_at = excluded.updated_at",
                (user.id, username, full_name, _now()),
            )
        _person_cache[user.id] = (username, full_name)
    except Exception as exc:  # analytics must never break moderation
        logger.warning("Could not save person %s: %s", user.id, exc)


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


def register_join(
    chat_id: int, invited_id: int, inviter_id: int, source: str
) -> Optional[int]:
    """
    Credit one invite to `inviter_id` for bringing in `invited_id`.

    `source` is "link", "manual" or "request". Returns the inviter's new invite
    count, or None if this user was already counted before (no double counting).
    """
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO joins (chat_id, invited_user_id, inviter_id, joined_at, source) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (chat_id, invited_id, inviter_id, _now(), source),
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


# --- analytics queries (used by the admin commands) --------------------------- #

def stats_summary(chat_id: int):
    """Totals and the top-10 inviters."""
    with db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM joins WHERE chat_id = ?", (chat_id,)
        ).fetchone()[0]
        inviters = conn.execute(
            "SELECT COUNT(*) FROM users WHERE chat_id = ? AND invites > 0", (chat_id,)
        ).fetchone()[0]
        qualified = conn.execute(
            "SELECT COUNT(*) FROM users WHERE chat_id = ? AND invites >= ?",
            (chat_id, REQUIRED_INVITES),
        ).fetchone()[0]
        top = conn.execute(
            "SELECT u.user_id, u.invites, p.username, p.full_name FROM users u "
            "LEFT JOIN people p ON p.user_id = u.user_id "
            "WHERE u.chat_id = ? AND u.invites > 0 "
            "ORDER BY u.invites DESC, u.user_id LIMIT 10",
            (chat_id,),
        ).fetchall()
    return total, inviters, qualified, top


def find_user_id(query: str) -> Optional[int]:
    """Resolve '@username' or a numeric ID to a user ID."""
    q = query.strip()
    if q.lstrip("-").isdigit():
        return int(q)
    with db() as conn:
        row = conn.execute(
            "SELECT user_id FROM people WHERE LOWER(username) = ?", (q.lstrip("@").lower(),)
        ).fetchone()
    return row[0] if row else None


def who_data(chat_id: int, user_id: int):
    """Who invited this user, and whom this user invited."""
    with db() as conn:
        me = conn.execute(
            "SELECT username, full_name FROM people WHERE user_id = ?", (user_id,)
        ).fetchone()
        by = conn.execute(
            "SELECT j.inviter_id, j.joined_at, j.source, p.username, p.full_name "
            "FROM joins j LEFT JOIN people p ON p.user_id = j.inviter_id "
            "WHERE j.chat_id = ? AND j.invited_user_id = ?",
            (chat_id, user_id),
        ).fetchone()
        total = conn.execute(
            "SELECT COUNT(*) FROM joins WHERE chat_id = ? AND inviter_id = ?",
            (chat_id, user_id),
        ).fetchone()[0]
        invited = conn.execute(
            "SELECT j.invited_user_id, j.joined_at, j.source, p.username, p.full_name "
            "FROM joins j LEFT JOIN people p ON p.user_id = j.invited_user_id "
            "WHERE j.chat_id = ? AND j.inviter_id = ? "
            "ORDER BY j.joined_at LIMIT ?",
            (chat_id, user_id, LIST_LIMIT),
        ).fetchall()
    return me, by, total, invited


def export_rows(chat_id: int):
    """Every invite with names, for the CSV export."""
    with db() as conn:
        return conn.execute(
            "SELECT j.joined_at, j.source, j.invited_user_id, p1.username, p1.full_name, "
            "j.inviter_id, p2.username, p2.full_name "
            "FROM joins j "
            "LEFT JOIN people p1 ON p1.user_id = j.invited_user_id "
            "LEFT JOIN people p2 ON p2.user_id = j.inviter_id "
            "WHERE j.chat_id = ? ORDER BY j.joined_at",
            (chat_id,),
        ).fetchall()


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


async def fetch_user(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int):
    """Look a user up in the group, remember their name and return the User object."""
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except TelegramError:
        return None
    remember_person(member.user)
    return member.user


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


async def send_group_notice(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str) -> None:
    """Post a short temporary bot message in the group (deleted after NOTICE_TTL)."""
    try:
        sent = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except TelegramError as exc:
        logger.warning("Could not send a notice to %s: %s", chat_id, exc)
        return
    context.application.create_task(
        _delete_later(context, chat_id, sent.message_id, NOTICE_TTL)
    )


def link_text(link: Optional[str]) -> str:
    return link or T["link_unavailable"]


async def credit_invite(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    invited_user,
    inviter_id: Optional[int],
    source: str,
) -> None:
    """Give the inviter +1 (once per invited user) and announce when the goal is reached."""
    if inviter_id is None or inviter_id == invited_user.id:
        return
    remember_person(invited_user)
    count = register_join(chat_id, invited_user.id, inviter_id, source)
    if count is None:
        return  # this person was already counted before

    if count == REQUIRED_INVITES:
        _qualified.add((chat_id, inviter_id))
        inviter = await fetch_user(context, chat_id, inviter_id)
        if inviter is not None:
            await send_group_notice(
                context, chat_id, say("unlocked_group", mention=inviter.mention_html())
            )
        if SEND_DM:
            await send_dm(context, inviter_id, say("unlocked_dm"))
    elif inviter_id not in _person_cache:
        await fetch_user(context, chat_id, inviter_id)  # remember the name for analytics


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
    Someone asked to join (links created with JOIN_REQUESTS=1, or a group that
    approves new members). Approve them, credit the inviter, optionally greet them.
    """
    req = update.chat_join_request
    if req is None or not is_target_chat(req.chat):
        return

    chat_id = req.chat.id
    remember_chat(chat_id)
    user = req.from_user
    if user.is_bot:
        return

    # Optional private greeting (only possible right now, before approval).
    if SEND_DM:
        link = await get_or_create_link(context, chat_id, user.id)
        await send_dm(context, req.user_chat_id, say("join_dm", link=link_text(link)))

    # Approve the request.
    try:
        await req.approve()
    except TelegramError as exc:
        logger.warning("Could not approve join request of %s in %s: %s", user.id, chat_id, exc)
        return
    record_member(chat_id, user.id)
    remember_person(user)

    # Credit the owner of the personal link that was used.
    link_obj = req.invite_link
    if link_obj and link_obj.invite_link:
        inviter_id = find_inviter(chat_id, link_obj.invite_link)
        await credit_invite(context, chat_id, user, inviter_id, "request")


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
    remember_person(user)

    # Who gets the credit?
    inviter_id: Optional[int] = None
    source = "link"
    link_obj = event.invite_link
    if link_obj and link_obj.invite_link:
        # Joined through an invite link: credit the owner of a personal link.
        inviter_id = find_inviter(chat_id, link_obj.invite_link)
    elif event.from_user and event.from_user.id != user.id and not event.from_user.is_bot:
        # Added manually ("Add members"): credit the person who added them.
        inviter_id = event.from_user.id
        source = "manual"
        remember_person(event.from_user)

    await credit_invite(context, chat_id, user, inviter_id, source)


async def guard_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete ad-like posts of members who have not invited enough people yet."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if msg is None or user is None or chat is None or not is_target_chat(chat):
        return

    remember_chat(chat.id)

    # Skip anonymous admins, channel posts and bots.
    if msg.sender_chat or user.is_bot:
        return

    # Only ad-like posts are moderated; ordinary chat messages are left alone.
    if not looks_like_ad(msg):
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

    # 2) Explain in the group (at most once per NOTICE_COOLDOWN seconds, so several
    #    quick posts or an album do not produce a pile of messages).
    now = time.monotonic()
    last = _last_notice.get(key)
    if last is not None and now - last < NOTICE_COOLDOWN:
        return
    _last_notice[key] = now

    remember_person(user)
    link = await get_or_create_link(context, chat.id, user.id)
    await send_group_notice(
        context,
        chat.id,
        say("blocked_group", mention=user.mention_html(), link=link_text(link), count=count),
    )

    # 3) Optional private copy (works only for people who started the bot).
    if SEND_DM:
        await send_dm(context, user.id, say("blocked_dm", link=link_text(link), count=count))


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
    user = update.effective_user
    if chat_id is not None and await is_group_member(context, chat_id, user.id):
        remember_person(user)
        if await is_admin(context, chat_id, user.id):
            text += say("admin_help")
        text += "\n\n" + await build_link_message(context, chat_id, user.id)
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
    remember_person(update.effective_user)
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


# --- admin analytics ----------------------------------------------------------- #

def fmt_person(user_id: int, username: Optional[str], full_name: Optional[str]) -> str:
    """'Name (@username, id 123)' with whatever is known."""
    if full_name and username:
        return f"{full_name} (@{username}, id {user_id})"
    if full_name:
        return f"{full_name} (id {user_id})"
    if username:
        return f"@{username} (id {user_id})"
    return f"id {user_id}"


def fmt_source(source: Optional[str]) -> str:
    return T.get(f"src_{source}", T["src_unknown"])


async def _admin_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Return the group ID if the sender is an admin of the group, else reply and return None."""
    chat_id = target_chat_id()
    if chat_id is None:
        await update.effective_message.reply_text(say("no_group"))
        return None
    if not await is_admin(context, chat_id, update.effective_user.id):
        await update.effective_message.reply_text(say("admin_only"))
        return None
    return chat_id


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _admin_chat(update, context)
    if chat_id is None:
        return
    total, inviters, qualified, top = stats_summary(chat_id)
    lines = [
        f"{i}. {fmt_person(uid, uname, fname)} - {n}"
        for i, (uid, n, uname, fname) in enumerate(top, 1)
    ]
    await update.effective_message.reply_text(
        say(
            "stats",
            total=total,
            inviters=inviters,
            qualified=qualified,
            top="\n".join(lines) or T["stats_none"],
        )
    )


async def cmd_who(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _admin_chat(update, context)
    if chat_id is None:
        return
    query = " ".join(context.args or []).strip()
    if not query:
        await update.effective_message.reply_text(say("who_usage"))
        return
    user_id = find_user_id(query)
    if user_id is None:
        await update.effective_message.reply_text(say("who_not_found"))
        return

    me, by, total, invited = who_data(chat_id, user_id)
    uname, fname = me if me else (None, None)
    parts = [fmt_person(user_id, uname, fname)]

    if by:
        inviter_id, when, source, i_uname, i_fname = by
        parts.append(
            say(
                "who_by",
                inviter=fmt_person(inviter_id, i_uname, i_fname),
                when=when or T["unknown_date"],
                source=fmt_source(source),
            )
        )
    else:
        parts.append(say("who_by_none"))

    if total:
        lines = [
            f"- {fmt_person(uid, u, n)} ({when or T['unknown_date']}, {fmt_source(src)})"
            for uid, when, src, u, n in invited
        ]
        block = say("who_list", count=total, lines="\n".join(lines))
        if total > len(invited):
            block += "\n" + say("more", count=total - len(invited))
        parts.append(block)
    else:
        parts.append(say("who_list_none"))

    await update.effective_message.reply_text("\n\n".join(parts))


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = await _admin_chat(update, context)
    if chat_id is None:
        return
    rows = export_rows(chat_id)
    if not rows:
        await update.effective_message.reply_text(say("export_empty"))
        return

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "date_utc", "source", "invited_id", "invited_username", "invited_name",
        "inviter_id", "inviter_username", "inviter_name",
    ])
    writer.writerows(rows)
    # The BOM makes Excel open Cyrillic names correctly.
    data = ("\ufeff" + buffer.getvalue()).encode("utf-8")
    await update.effective_message.reply_document(
        document=data,
        filename="invites.csv",
        caption=say("export_caption", count=len(rows)),
    )


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
        "Storage: %s | language: %s | join requests: %s | private copies: %s | only new members: %s",
        "PostgreSQL" if USE_POSTGRES else f"SQLite ({DB_PATH})",
        LANG if LANG in TEXTS else "ru",
        JOIN_REQUESTS,
        SEND_DM,
        GATE_ONLY_NEW,
    )
    start_health_server()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands work in private chats only.
    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler("start", cmd_start, filters=private))
    app.add_handler(CommandHandler("link", cmd_link, filters=private))
    app.add_handler(CommandHandler("invites", cmd_invites, filters=private))
    # Admin analytics (the handlers check that the sender is a group admin).
    app.add_handler(CommandHandler("stats", cmd_stats, filters=private))
    app.add_handler(CommandHandler("who", cmd_who, filters=private))
    app.add_handler(CommandHandler("export", cmd_export, filters=private))

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
