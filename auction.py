import asyncio
import json
import os
import re
import sqlite3
import time
import uuid
from typing import Dict, Any, Optional, Tuple, List, Any
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Message, MessageEntity
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

DB_PATH = "auction_bot_data.json"
USERNAMES_DB_PREFERRED = "/mnt/data/usernames.db"
COUNTDOWN_SECONDS = 30

ANNOUNCE_IMAGE = "https://graph.org/file/d4418e31aad740bc446b4-e53cd62a0356d0946f.jpg"
UNSOLD_IMAGE = "https://graph.org/file/c620581d8df952b61ee45-71e6dcc1ee66bf0e77.jpg"
SOLD_IMAGE = "https://graph.org/file/23469938a57a36b713e5b-d1d1fbdc745fb08a6e.jpg"

def _ensure_json_db():
    global DB_PATH
    dirpath = os.path.dirname(DB_PATH)
    if dirpath and not os.path.exists(dirpath):
        try:
            os.makedirs(dirpath, exist_ok=True)
        except Exception:
            DB_PATH = os.path.join(os.getcwd(), "auction_bot_data.json")
    if not os.path.exists(DB_PATH):
        with open(DB_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)

def load_db() -> Dict[str, Any]:
    _ensure_json_db()
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            try:
                return json.load(f) or {}
            except Exception:
                return {}
    except Exception:
        try:
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump({}, f)
        except Exception:
            pass
        return {}

def save_db(data: Dict[str, Any]) -> None:
    _ensure_json_db()
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_PATH)

def _choose_usernames_db_path() -> str:
    try:
        pref_dir = os.path.dirname(USERNAMES_DB_PREFERRED) or "/"
        if pref_dir and not os.path.exists(pref_dir):
            os.makedirs(pref_dir, exist_ok=True)
        test_path = os.path.join(pref_dir, ".write_test")
        with open(test_path, "w", encoding="utf-8") as t:
            t.write("ok")
        os.remove(test_path)
        return USERNAMES_DB_PREFERRED
    except Exception:
        fallback = os.path.join(os.getcwd(), "usernames.db")
        return fallback

USERNAMES_DB_PATH = _choose_usernames_db_path()

def get_session(chat_id: int) -> Dict[str, Any]:
    db = load_db()
    sessions = db.get("auction_sessions", {})
    key = str(chat_id)
    if key not in sessions:
        sessions[key] = {
            "active": False,
            "message_key": None,
            "host_id": None,
            "host_name": None,
            "tables": None,
            "teams": {},
            "team_budgets": {},
            "budget": None,
            "players_list": [],
            "current_slot": None,
            "paused": False,
            "pause_start": None,
            "last_table_msg_key": None,
            "logs": [],
            "current_run_id": None,
            "assistants": {},
            "access_users": [],
            "min_buy": None,
            "max_buy": None,
            "min_choice_key": None,
            "max_choice_key": None
        }
        db["auction_sessions"] = sessions
        save_db(db)
    return sessions[key]

def save_session(chat_id: int, session: Dict[str, Any]) -> None:
    db = load_db()
    sessions = db.get("auction_sessions", {})
    sessions[str(chat_id)] = session
    db["auction_sessions"] = sessions
    save_db(db)

def start_new_run(chat_id: int, session: Dict[str, Any]) -> str:
    db = load_db()
    history = db.get("auction_history", {})
    run_id = str(int(time.time())) + "-" + uuid.uuid4().hex[:8]
    run = {
        "run_id": run_id,
        "chat_id": chat_id,
        "started_at": int(time.time()),
        "ended_at": None,
        "host_id": session.get("host_id"),
        "host_name": session.get("host_name"),
        "tables": session.get("tables"),
        "teams": {},
        "budget": session.get("budget"),
        "players_loaded": len(session.get("players_list") or []),
        "current_slot": None,
        "sold_players": [],
        "unsold_players": [],
        "logs": []
    }
    if str(chat_id) not in history:
        history[str(chat_id)] = []
    history[str(chat_id)].append(run)
    db["auction_history"] = history
    save_db(db)
    session["current_run_id"] = run_id
    save_session(chat_id, session)
    return run_id

def get_run(chat_id: int, run_id: str) -> Optional[Dict[str, Any]]:
    db = load_db()
    history = db.get("auction_history", {})
    runs = history.get(str(chat_id), [])
    for r in runs:
        if r.get("run_id") == run_id:
            return r
    return None

def save_run(chat_id: int, run: Dict[str, Any]) -> None:
    db = load_db()
    history = db.get("auction_history", {})
    runs = history.get(str(chat_id), [])
    for i in range(len(runs)):
        if runs[i].get("run_id") == run.get("run_id"):
            runs[i] = run
            break
    history[str(chat_id)] = runs
    db["auction_history"] = history
    save_db(db)

def append_run_log(chat_id: int, run_id: str, log: Dict[str, Any]) -> None:
    run = get_run(chat_id, run_id)
    if not run:
        return
    run_logs = run.get("logs", [])
    run_logs.append(log)
    run["logs"] = run_logs
    save_run(chat_id, run)

def build_start_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("I am a host", callback_data="auction_host:claim"), InlineKeyboardButton("Cancel auction", callback_data="auction_host:cancel")]])

def build_table_keyboard():
    buttons = [InlineKeyboardButton(str(n), callback_data=f"auction_table_choice:{n}") for n in range(2, 21)]
    rows = [buttons[i:i+5] for i in range(0, len(buttons), 5)]
    return InlineKeyboardMarkup(rows)

def build_load_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Internal load", callback_data="auction_load:internal"), InlineKeyboardButton("Load players", callback_data="auction_load:external")]])

def build_end_confirm(host_id: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton("Yes — End Auction", callback_data=f"end_confirm:yes:{host_id}"), InlineKeyboardButton("No — Continue", callback_data=f"end_confirm:no:{host_id}")]])

def ensure_usernames_table(conn: sqlite3.Connection):
    conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, username_lower TEXT UNIQUE, added_at TEXT)")
    conn.commit()

countdown_tasks: Dict[int, asyncio.Task] = {}

async def _send_message(bot, chat_id: int, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    try:
        return await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        return await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

async def start_auction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if session.get("active"):
        title = getattr(chat, "title", str(chat_id))
        await msg.reply_text(f"Auction has already started in {title}.")
        return
    session["active"] = False
    session["host_id"] = None
    session["host_name"] = None
    session["tables"] = None
    session["teams"] = {}
    session["team_budgets"] = {}
    session["budget"] = None
    session["players_list"] = []
    session["current_slot"] = None
    session["paused"] = False
    session["pause_start"] = None
    session["last_table_msg_key"] = None
    session["logs"] = []
    session["current_run_id"] = None
    session["assistants"] = {}
    session["access_users"] = []
    session["min_buy"] = None
    session["max_buy"] = None
    session["min_choice_key"] = None
    session["max_choice_key"] = None
    save_session(chat_id, session)
    text = ("🎉🚀🔥  Diving Into Auction Mode!  🔥🚀🎉\n\n" "🔔 <b>BOT</b> is now entering <b>Auction Mode</b> 🪙⚡\n\n" "📢 Before the auction begins, please ensure a Host is ready to guide the event smoothly.\n\n" "Tap below to claim host or cancel the auction.")
    sent = await _send_message(context.bot, chat_id, text, reply_markup=build_start_keyboard())
    session["message_key"] = f"{chat_id}:{sent.message_id}"
    save_session(chat_id, session)

start_auction_handler = CommandHandler("start_auction", start_auction)

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    user = query.from_user
    chat = query.message.chat if query.message else update.effective_chat
    if not chat:
        try:
            await query.answer("Cannot determine chat", show_alert=True)
        except:
            pass
        return
    chat_id = chat.id
    session = get_session(chat_id)
    try:
        if data.startswith("auction_host:"):
            action = data.split(":",1)[1]
            if action == "claim":
                if session.get("host_id"):
                    try:
                        await query.answer("A host is already assigned", show_alert=True)
                    except:
                        pass
                    return
                session["host_id"] = user.id
                session["host_name"] = getattr(user, "full_name", None) or f"{getattr(user,'first_name','') or ''} {getattr(user,'last_name','') or ''}".strip()
                session["active"] = True
                save_session(chat_id, session)
                if not session.get("current_run_id"):
                    start_new_run(chat_id, session)
                new_text = ("🔥 <b>Auction Host Claimed</b> 🔥\n\n" f"Host: {session['host_name']}\n\n" "Now please set the number tables (/set_table)\n" "Host commands: /set_table  /team  /budget  /load  /next  /pause  /resume  /end_auction  /summary\n")
                edited = False
                if query.message and hasattr(query.message, "edit_text"):
                    try:
                        await query.message.edit_text(new_text, parse_mode=ParseMode.HTML)
                        edited = True
                    except Exception:
                        edited = False
                if not edited:
                    try:
                        msg_key = session.get("message_key")
                        if msg_key:
                            parts = str(msg_key).split(":")
                            if len(parts) == 2:
                                mid = int(parts[1])
                                try:
                                    await context.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=new_text, parse_mode=ParseMode.HTML)
                                    edited = True
                                except Exception:
                                    edited = False
                    except Exception:
                        edited = False
                if not edited:
                    try:
                        sent = await _send_message(context.bot, chat_id, new_text, parse_mode=ParseMode.HTML)
                        session["message_key"] = f"{chat_id}:{sent.message_id}"
                        save_session(chat_id, session)
                    except Exception:
                        try:
                            await query.message.reply_text(new_text, parse_mode=ParseMode.HTML)
                        except Exception:
                            pass
                try:
                    await query.answer("You are now the auction host.")
                except:
                    pass
                return
            if action == "cancel":
                if session.get("host_id"):
                    try:
                        await query.answer("Host already assigned, cannot cancel", show_alert=True)
                    except:
                        pass
                    return
                session["active"] = False
                save_session(chat_id, session)
                try:
                    await query.message.edit_text("Auction cancelled by admin.")
                except Exception:
                    await _send_message(context.bot, chat_id, "Auction cancelled by admin.")
                try:
                    await query.answer("Auction cancelled")
                except:
                    pass
                return
        if data.startswith("auction_table_choice:"):
            _, num_s = data.split(":",1)
            try:
                n = int(num_s)
            except:
                try:
                    await query.answer("Invalid number", show_alert=True)
                except:
                    pass
                return
            if session.get("host_id") != user.id:
                try:
                    await query.answer("Only the host can choose tables", show_alert=True)
                except:
                    pass
                return
            try:
                if query.message:
                    await query.message.delete()
            except Exception:
                pass
            session["tables"] = n
            save_session(chat_id, session)
            run_id = session.get("current_run_id") or start_new_run(chat_id, session)
            run = get_run(chat_id, run_id)
            if run:
                run["tables"] = n
                save_run(chat_id, run)
            txt = f"✅ Auction tables confirmed ({n}).\n\nNow set minimum buying limit for all teams:"
            min_buttons = [InlineKeyboardButton(str(x), callback_data=f"min_buy:{x}") for x in range(3,13)]
            kb_rows = [min_buttons[i:i+5] for i in range(0, len(min_buttons), 5)]
            sent = await _send_message(context.bot, chat_id, txt, reply_markup=InlineKeyboardMarkup(kb_rows))
            session["min_choice_key"] = f"{chat_id}:{sent.message_id}"
            save_session(chat_id, session)
            try:
                await query.answer("Tables set - choose minimum buying")
            except:
                pass
            return
        if data.startswith("auction_load:"):
            _, mode = data.split(":",1)
            if session.get("host_id") != user.id and user.id not in session.get("access_users", []):
                try:
                    await query.answer("Only the host can load players", show_alert=True)
                except:
                    pass
                return
            try:
                await query.answer()
            except:
                pass
            if query.message:
                try:
                    await query.message.delete()
                except:
                    pass
            if mode == "internal":
                db = load_db()
                regs = []
                tournaments = db.get("tournaments", {})
                for t in tournaments.values():
                    regs.extend([r for r in t.get("registrations", []) if r.get("status") == "accepted"])
                players = []
                for r in regs:
                    players.append({
                        "user_id": r.get("user_id"),
                        "username": (r.get("username") or "").lstrip("@"),
                        "name": r.get("name"),
                        "role": r.get("role"),
                        "player_code": r.get("player_code")
                    })
                session["players_list"] = players
                save_session(chat_id, session)
                run_id = session.get("current_run_id") or start_new_run(chat_id, session)
                run = get_run(chat_id, run_id)
                if run:
                    run["players_loaded"] = len(players)
                    save_run(chat_id, run)
                await _send_message(context.bot, chat_id, f"✅ All set — players loaded from registered list ({len(players)} players). Hey host {session.get('host_name')}, you can now start auction by sending /next <player_identifier> <base_price>")
                try:
                    await query.answer("Players loaded")
                except:
                    pass
                return
            else:
                await _send_message(context.bot, chat_id, "Reply now to the message containing comma/space/newline separated @usernames or @username list and then run /load (reply to that message).")
                return
        if data.startswith("end_confirm:"):
            parts = data.split(":")
            if len(parts) < 3:
                try:
                    await query.answer()
                except:
                    pass
                return
            choice = parts[1]
            host_token = int(parts[2]) if parts[2].isdigit() else None
            if host_token != session.get("host_id"):
                try:
                    await query.answer("Only the host can confirm", show_alert=True)
                except:
                    pass
                return
            if choice == "yes":
                task = countdown_tasks.get(chat_id)
                if task and not task.done():
                    task.cancel()
                session["active"] = False
                session["host_id"] = None
                session["current_slot"] = None
                run_id = session.get("current_run_id")
                if run_id:
                    run = get_run(chat_id, run_id)
                    if run:
                        run["ended_at"] = int(time.time())
                        save_run(chat_id, run)
                save_session(chat_id, session)
                try:
                    await query.message.edit_text("✅ Auction ended. All auction operations stopped.")
                except Exception:
                    await _send_message(context.bot, chat_id, "✅ Auction ended. All auction operations stopped.")
                try:
                    await query.answer("Auction ended")
                except:
                    pass
                return
            else:
                try:
                    await query.answer("Auction end cancelled")
                except:
                    pass
                return
        if data.startswith("min_buy:"):
            _, val = data.split(":",1)
            try:
                chosen = int(val)
            except:
                try:
                    await query.answer("Invalid choice", show_alert=True)
                except:
                    pass
                return
            if session.get("host_id") != user.id:
                try:
                    await query.answer("Only host can choose", show_alert=True)
                except:
                    pass
                return
            session["min_buy"] = chosen
            save_session(chat_id, session)
            try:
                await query.message.edit_text(f"Minimum buying per team set to {chosen}. Now choose maximum buying per team:")
            except:
                pass
            max_buttons = [InlineKeyboardButton(str(x), callback_data=f"max_buy:{x}") for x in range(13,27)]
            kb_rows = [max_buttons[i:i+5] for i in range(0, len(max_buttons), 5)]
            try:
                sent = await _send_message(context.bot, chat_id, "Choose maximum buying per team:", reply_markup=InlineKeyboardMarkup(kb_rows))
                session["max_choice_key"] = f"{chat_id}:{sent.message_id}"
                save_session(chat_id, session)
            except:
                pass
            try:
                await query.answer(f"Minimum set to {chosen}")
            except:
                pass
            return
        if data.startswith("max_buy:"):
            _, val = data.split(":",1)
            try:
                chosen = int(val)
            except:
                try:
                    await query.answer("Invalid choice", show_alert=True)
                except:
                    pass
                return
            if session.get("host_id") != user.id:
                try:
                    await query.answer("Only host can choose", show_alert=True)
                except:
                    pass
                return
            session["max_buy"] = chosen
            save_session(chat_id, session)
            try:
                if query.message:
                    await query.message.edit_text(f"Maximum buying per team set to {chosen}. Min: {session.get('min_buy')}, Max: {session.get('max_buy')}")
            except:
                pass
            try:
                await query.answer(f"Maximum set to {chosen}")
            except:
                pass
            return
        await query.answer()
    except Exception:
        try:
            await query.answer("Internal error", show_alert=True)
        except:
            pass

callback_handler = CallbackQueryHandler(callback_router)

async def set_table_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not msg or not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if session.get("host_id") != user.id and user.id not in session.get("access_users", []):
        await msg.reply_text("Only the auction host can set the number of tables.")
        return
    sent = await _send_message(context.bot, chat_id, "Please choose number of auction table between 2 - 20:", reply_markup=build_table_keyboard())
    session["last_table_msg_key"] = f"{chat_id}:{sent.message_id}"
    save_session(chat_id, session)

set_table_handler = CommandHandler("set_table", set_table_cmd)

async def table_reply_handler_func(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.reply_to_message or not msg.text:
        return
    chat = update.effective_chat or (msg.chat if msg else None)
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    last_key = session.get("last_table_msg_key")
    if not last_key:
        return
    try:
        last_chat, last_msg_id = last_key.split(":")
        last_msg_id = int(last_msg_id)
    except:
        return
    if msg.reply_to_message.message_id != last_msg_id:
        return
    try:
        n = int(msg.text.strip().split()[0])
    except:
        await msg.reply_text("Please reply with a number between 2 and 20.")
        return
    if n < 2 or n > 20:
        await msg.reply_text("Please reply with a number between 2 and 20.")
        return
    session["tables"] = n
    save_session(chat_id, session)
    run_id = session.get("current_run_id") or start_new_run(chat_id, session)
    run = get_run(chat_id, run_id)
    if run:
        run["tables"] = n
        save_run(chat_id, run)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
    except Exception:
        pass
    txt = f"✅ Auction tables confirmed ({n}).\n\nNow set minimum buying limit for all teams:"
    min_buttons = [InlineKeyboardButton(str(x), callback_data=f"min_buy:{x}") for x in range(3,13)]
    kb_rows = [min_buttons[i:i+5] for i in range(0, len(min_buttons), 5)]
    sent = await _send_message(context.bot, chat_id, txt, reply_markup=InlineKeyboardMarkup(kb_rows))
    session["min_choice_key"] = f"{chat_id}:{sent.message_id}"
    save_session(chat_id, session)
    return

table_reply_handler = MessageHandler(filters.TEXT & filters.REPLY & ~filters.COMMAND & (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP), table_reply_handler_func)

def _extract_username_from_entities(text: str, entities: Optional[list]) -> Optional[str]:
    if not text or not entities:
        return None
    for ent in entities:
        if isinstance(ent, dict):
            ent_type = ent.get("type")
            offset = ent.get("offset")
            length = ent.get("length")
        elif isinstance(ent, MessageEntity):
            ent_type = ent.type
            offset = ent.offset
            length = ent.length
        else:
            continue
        if ent_type == "mention":
            try:
                return text[offset:offset+length].lstrip("@")
            except Exception:
                continue
    return None

def _find_registration_by_username_or_code(db: Dict[str, Any], identifier: str) -> Optional[Dict[str, Any]]:
    if not identifier:
        return None
    tournaments = db.get("tournaments", {})
    for t in tournaments.values():
        for r in t.get("registrations", []) or []:
            if (r.get("username") or "").lstrip("@").lower() == identifier.lstrip("@").lower():
                return r
            if r.get("player_code") and str(r.get("player_code")) == str(identifier):
                return r
            if r.get("user_id") and str(r.get("user_id")) == str(identifier):
                return r
    return None

def _recursive_find_userid_by_username(obj: Any, username: str) -> Optional[int]:
    if not obj:
        return None
    uname_l = username.lstrip("@").lower()
    if isinstance(obj, dict):
        u = obj.get("username") or obj.get("user") or obj.get("user_name")
        uid = obj.get("user_id") or obj.get("id") or obj.get("tg_id")
        if u and isinstance(u, str) and uid:
            if u.lstrip("@").lower() == uname_l:
                try:
                    return int(uid)
                except:
                    return None
        for k, v in obj.items():
            found = _recursive_find_userid_by_username(v, username)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _recursive_find_userid_by_username(item, username)
            if found:
                return found
    return None

def _extract_target_from_message(message: Message) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not message:
        return None, "no message"
    if getattr(message, "from_user", None):
        u = message.from_user
        return {"user_id": getattr(u, "id", None), "first_name": getattr(u, "first_name", ""), "last_name": getattr(u, "last_name", ""), "full_name": getattr(u, "full_name", None) or f"{getattr(u,'first_name','') or ''} {getattr(u,'last_name','') or ''}".strip()}, None
    if getattr(message, "forward_from", None):
        f = message.forward_from
        return {"user_id": getattr(f, "id", None), "first_name": getattr(f, "first_name", ""), "last_name": getattr(f, "last_name", ""), "full_name": getattr(f, "full_name", None) or f"{getattr(f,'first_name','') or ''} {getattr(f,'last_name','') or ''}".strip()}, None
    if getattr(message, "sender_chat", None):
        return None, "message is from a channel/sender_chat (anonymous)"
    text = message.text or message.caption or ""
    ent_username = _extract_username_from_entities(text, getattr(message, "entities", None))
    if ent_username:
        db = load_db()
        reg = _find_registration_by_username_or_code(db, ent_username)
        if reg:
            return {"user_id": reg.get("user_id"), "first_name": reg.get("name") or reg.get("username") or ent_username, "last_name": ""}, None
        return {"user_id": None, "first_name": ent_username, "last_name": ""}, None
    return None, "could not identify target user from replied message"

def _is_host_or_access(session: Dict[str, Any], user_id: int) -> bool:
    if not session:
        return False
    if session.get("host_id") == user_id:
        return True
    access = session.get("access_users") or []
    if user_id in access:
        return True
    return False

def _is_team_owner(session: Dict[str, Any], user_id: int, team_name: Optional[str] = None) -> Optional[str]:
    teams = session.get("teams", {}) or {}
    if team_name:
        members = teams.get(team_name) or []
        if members and members[0] == user_id:
            return team_name
        return None
    for tname, members in teams.items():
        if members and members[0] == user_id:
            return tname
    return None

async def team_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not msg or not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id) and _is_team_owner(session, user.id) is None:
        await msg.reply_text("Only the auction host can assign teams.")
        return
    raw = (msg.text or "").strip()
    if not raw:
        await msg.reply_text("Usage: /team <TeamName> (reply to user message) OR /team <TeamName> @username or /team <TeamName> <user_id>")
        return
    if raw.lower().startswith("/team"):
        remainder = raw.split(maxsplit=1)
        remainder = remainder[1].strip() if len(remainder) > 1 else ""
    else:
        remainder = raw
    if msg.reply_to_message:
        if not remainder:
            await msg.reply_text("Usage: reply to a user's message with /team <TeamName>")
            return
        team_name = remainder
        reply_msg = msg.reply_to_message
        target_info, reason = _extract_target_from_message(reply_msg)
        if not target_info:
            text_reason = reason or "unknown"
            await msg.reply_text(f"Could not identify the target user. Reason: {text_reason}. Make sure you are replying to a real user's message (not an anonymous admin or channel post).")
            return
        target_id = target_info.get("user_id")
        target_name = target_info.get("full_name") or f"{target_info.get('first_name','') or ''} {target_info.get('last_name','') or ''}".strip()
    else:
        if not remainder:
            await msg.reply_text("When not replying, include the username or user id after the team name. Example: /team RCB @username")
            return
        parts = remainder.split(maxsplit=1)
        team_name = parts[0] if parts else remainder
        arg = parts[1].strip() if len(parts) > 1 else ""
        if not arg:
            await msg.reply_text("When not replying, include the username or user id after the team name. Example: /team RCB @username")
            return
        candidate = arg.lstrip("@")
        target_id = None
        target_name = None
        session_players = session.get("players_list") or []
        for p in session_players:
            if p and p.get("username") and p.get("username").lstrip("@").lower() == candidate.lower():
                target_id = p.get("user_id")
                target_name = p.get("name") or p.get("username")
                break
            if p and p.get("player_code") and str(p.get("player_code")) == candidate:
                target_id = p.get("user_id")
                target_name = p.get("name") or p.get("username")
                break
            if p and p.get("user_id") and str(p.get("user_id")) == candidate:
                target_id = p.get("user_id")
                target_name = p.get("name") or p.get("username")
                break
        if not target_id:
            db = load_db()
            reg = _find_registration_by_username_or_code(db, candidate)
            if reg:
                target_id = reg.get("user_id")
                target_name = reg.get("name") or reg.get("username") or candidate
        if not target_id:
            try:
                maybe_id = int(candidate)
                target_id = maybe_id
                target_name = candidate
            except:
                target_id = None
                target_name = candidate
    teams = session.get("teams", {})
    existing_key = None
    for k in teams.keys():
        if k.lower() == team_name.lower():
            existing_key = k
            break
    key = existing_key or team_name
    if key not in teams:
        teams[key] = []
    if target_id not in teams[key]:
        teams[key].append(target_id)
    session["teams"] = teams
    if session.get("budget") and key not in session.get("team_budgets", {}):
        tb = session.get("team_budgets", {})
        tb[key] = session.get("budget")
        session["team_budgets"] = tb
    save_session(chat_id, session)
    run_id = session.get("current_run_id") or start_new_run(chat_id, session)
    run = get_run(chat_id, run_id)
    if run:
        run_teams = run.get("teams", {})
        if key not in run_teams:
            run_teams[key] = []
        if target_id not in run_teams[key]:
            run_teams[key].append(target_id)
        run["teams"] = run_teams
        save_run(chat_id, run)
        append_run_log(chat_id, run.get("run_id"), {"event": "team_assigned", "team": key, "user_id": target_id, "user_name": target_name or "", "ts": int(time.time())})
    tables_total = session.get("tables") or 0
    assigned_count = len(teams.keys())
    if target_id:
        reply_text = (f"🔥 ✅ {target_name} has been assigned to team <b>{key}</b> 🏷️\n\n" f"🏟️ Teams set: <b>{assigned_count}</b> / <b>{tables_total}</b>\n")
    else:
        reply_text = (f"🔥 ✅ <b>{target_name}</b> has been assigned to team <b>{key}</b> (no numeric user_id available) 🏷️\n\n" f"🏟️ Teams set: <b>{assigned_count}</b> / <b>{tables_total}</b>\n")
    await _send_message(context.bot, chat_id, reply_text, parse_mode=ParseMode.HTML)
    if assigned_count >= tables_total and tables_total > 0:
        await _send_message(context.bot, chat_id, f"All team tables set now ({assigned_count}). Please set the budget using /budget <number>.")
    return

team_handler = CommandHandler("team", team_cmd)

async def budget_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can set the budget.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply_text("Usage: /budget <number>")
        return
    try:
        amt = int(parts[1].replace(",", "").strip())
    except:
        await msg.reply_text("Please provide a valid integer amount.")
        return
    session["budget"] = amt
    tb = session.get("team_budgets", {}) or {}
    for k in (session.get("teams") or {}).keys():
        tb.setdefault(k, amt)
    session["team_budgets"] = tb
    save_session(chat_id, session)
    run_id = session.get("current_run_id") or start_new_run(chat_id, session)
    run = get_run(chat_id, run_id)
    if run:
        run["budget"] = amt
        save_run(chat_id, run)
    await _send_message(context.bot, chat_id, f"✅ Budget set to {amt} for all teams.\nNow choose player load method: internal or load from provided list.", reply_markup=build_load_keyboard())
    return

budget_handler = CommandHandler("budget", budget_cmd)

async def load_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    host_id = session.get("host_id")
    if host_id != user.id and user.id not in session.get("access_users", []):
        try:
            member = await context.bot.get_chat_member(chat_id, user.id)
            if member.status not in ("administrator", "creator"):
                await msg.reply_text("Only the auction host (or a chat admin) can load players.")
                return
        except Exception:
            await msg.reply_text("Only the auction host (or a chat admin) can load players.")
            return
    reply_msg = msg.reply_to_message
    if not reply_msg:
        await msg.reply_text("Reply to the message containing the player list and run /load.\nAcceptable formats:\n1) Comma-separated: @u1, @u2\n2) Newline-separated:\n@u1\n@u2\nThen reply to that message with /load (as host).")
        return
    source_text = ""
    if getattr(reply_msg, "text", None):
        source_text = reply_msg.text
    elif getattr(reply_msg, "caption", None):
        source_text = reply_msg.caption
    else:
        source_text = ""
    if not source_text or not source_text.strip():
        await msg.reply_text("The replied message contains no textual usernames to parse.")
        return
    raw = source_text.strip()
    usernames_set = []
    entities = []
    if getattr(reply_msg, "entities", None):
        entities.extend(reply_msg.entities)
    if getattr(reply_msg, "caption_entities", None):
        entities.extend(reply_msg.caption_entities)
    for ent in entities:
        try:
            ent_type = ent.type if isinstance(ent, MessageEntity) else (ent.get("type") if isinstance(ent, dict) else None)
            if ent_type == "mention":
                off = ent.offset if isinstance(ent, MessageEntity) else ent.get("offset")
                ln = ent.length if isinstance(ent, MessageEntity) else ent.get("length")
                mention_text = raw[off:off+ln] if off is not None and ln is not None else None
                if mention_text:
                    candidate = mention_text.lstrip("@").strip()
                    candidate = re.sub(r'[^A-Za-z0-9_]', '', candidate)
                    if candidate and candidate not in usernames_set:
                        usernames_set.append(candidate)
            elif ent_type == "text_mention":
                u = ent.user if isinstance(ent, MessageEntity) else ent.get("user")
                if getattr(u, "username", None):
                    candidate = getattr(u, "username")
                    candidate = candidate.lstrip("@")
                    candidate = re.sub(r'[^A-Za-z0-9_]', '', candidate)
                    if candidate and candidate not in usernames_set:
                        usernames_set.append(candidate)
                else:
                    uid = getattr(u, "id", None)
                    if uid:
                        cand = str(uid)
                        if cand not in usernames_set:
                            usernames_set.append(cand)
        except Exception:
            continue
    found_at = re.findall(r'@([A-Za-z0-9_]{1,64})', raw)
    for f in found_at:
        if f and f not in usernames_set:
            usernames_set.append(f)
    parts = re.split(r'[\n,]+', raw)
    for p in parts:
        p = p.strip()
        if not p:
            continue
        tok = p
        if tok.startswith("@"):
            tok = tok[1:]
        tok = tok.strip()
        tok = re.sub(r'[^A-Za-z0-9_]', '', tok)
        if tok and tok not in usernames_set:
            usernames_set.append(tok)
    if not usernames_set:
        parts = re.split(r'[\s,]+', raw)
        for p in parts:
            p = p.strip()
            if not p:
                continue
            tok = p.lstrip("@")
            tok = re.sub(r'[^A-Za-z0-9_]', '', tok)
            if tok and tok not in usernames_set:
                usernames_set.append(tok)
    if not usernames_set:
        await msg.reply_text("No usernames detected. Please ensure the message contains @usernames or plain usernames separated by commas/newlines.")
        return
    db = load_db()
    tournaments = db.get("tournaments", {})
    regs = []
    for t in tournaments.values():
        regs.extend(t.get("registrations", []) or [])
    reg_map = {}
    for r in regs:
        key = (r.get("username") or "").lstrip("@").lower()
        if key:
            reg_map[key] = r
        if r.get("player_code"):
            reg_map[str(r.get("player_code"))] = r
        if r.get("user_id"):
            reg_map[str(r.get("user_id"))] = r
    players = []
    matched_count = 0
    for uname in usernames_set:
        key = str(uname).lstrip("@").lower()
        reg = reg_map.get(key)
        if reg:
            players.append({
                "user_id": reg.get("user_id"),
                "username": (reg.get("username") or "").lstrip("@"),
                "name": reg.get("name"),
                "role": reg.get("role"),
                "player_code": reg.get("player_code")
            })
            matched_count += 1
        else:
            players.append({
                "user_id": None,
                "username": uname,
                "name": uname,
                "role": None,
                "player_code": None,
                "placeholder": True
            })
    inserted = 0
    db_path = USERNAMES_DB_PATH
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        ensure_usernames_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT username_lower FROM users")
        existing = {r[0] for r in cur.fetchall() if r and r[0]}
        to_insert = []
        for p in players:
            uname_store = p.get("username")
            if not uname_store:
                continue
            clean = uname_store.lstrip("@")
            lower = clean.lower()
            if lower in existing:
                continue
            display = "@" + clean
            to_insert.append((display, lower, time.strftime("%Y-%m-%dT%H:%M:%S")))
            existing.add(lower)
        if to_insert:
            cur.executemany("INSERT OR IGNORE INTO users (username, username_lower, added_at) VALUES (?, ?, ?)", to_insert)
            conn.commit()
            inserted = len(to_insert)
    except Exception as e:
        try:
            await msg.reply_text(f"Failed to write to usernames DB: {e}")
        except:
            pass
        if conn:
            try:
                conn.close()
            except:
                pass
        return
    if conn:
        try:
            conn.close()
        except:
            pass
    existing_players = session.get("players_list") or []
    existing_map = {}
    for p in existing_players:
        if p and p.get("username"):
            existing_map[str(p.get("username")).lstrip("@").lower()] = p
        elif p and p.get("user_id"):
            existing_map[str(p.get("user_id"))] = p
    for p in players:
        key = (str(p.get("username")).lstrip("@").lower() if p.get("username") else (str(p.get("user_id")) if p.get("user_id") else None))
        if not key:
            continue
        existing_map[key] = p
    merged = list(existing_map.values())
    session["players_list"] = merged
    save_session(chat_id, session)
    run_id = session.get("current_run_id") or start_new_run(chat_id, session)
    run = get_run(chat_id, run_id)
    if run:
        run["players_loaded"] = len(merged)
        save_run(chat_id, run)
    await msg.reply_text(f"✅ Players list processed. Total processed: {len(players)} — matched registered: {matched_count}, placeholders: {len(players)-matched_count}. New usernames inserted into DB: {inserted}. Host can now start auction slots using /next <player_identifier> <base_price>")
    return

load_handler = CommandHandler("load", load_cmd)
load_msg_handler = MessageHandler(filters.REPLY & ~filters.COMMAND & (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP), load_cmd)
load_reply_fallback_handler = load_msg_handler

def _get_team_of_user(session: Dict[str, Any], user_id: int) -> Optional[str]:
    teams = session.get("teams", {}) or {}
    for tname, members in teams.items():
        if members and user_id in members:
            return tname
    return None

def _normalize_player_entry(p: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(p or {})
    if p.get("username"):
        p["username"] = str(p["username"]).lstrip("@")
    if p.get("user_id") is not None:
        try:
            p["user_id"] = int(p["user_id"])
        except:
            p["user_id"] = p["user_id"]
    return p

async def find_player_async(session: Dict[str, Any], identifier: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    if not identifier:
        return None
    id_clean = str(identifier).strip()
    if id_clean.startswith("@"):
        id_clean = id_clean[1:]
    players = session.get("players_list", []) or []
    for p in players:
        p2 = _normalize_player_entry(p)
        uname = (p2.get("username") or "").lstrip("@")
        if uname and uname.lower() == id_clean.lower():
            db = load_db()
            try:
                found_id = _recursive_find_userid_by_username(db, uname)
            except Exception:
                found_id = None
            if found_id:
                p2["user_id"] = p2.get("user_id") or found_id
                try:
                    tg_chat = await context.bot.get_chat(int(found_id))
                    if tg_chat:
                        profile_username = getattr(tg_chat, "username", None)
                        first = getattr(tg_chat, "first_name", "") or ""
                        last = getattr(tg_chat, "last_name", "") or ""
                        profile_fullname = f"{first} {last}".strip() if (first or last) else None
                        if profile_username:
                            p2["profile_username"] = profile_username.lstrip("@")
                        if profile_fullname:
                            p2["profile_fullname"] = profile_fullname
                except Exception:
                    pass
            else:
                try:
                    tg_chat = await context.bot.get_chat(f"@{uname}")
                    if tg_chat:
                        profile_username = getattr(tg_chat, "username", None)
                        first = getattr(tg_chat, "first_name", "") or ""
                        last = getattr(tg_chat, "last_name", "") or ""
                        profile_fullname = f"{first} {last}".strip() if (first or last) else None
                        if profile_username:
                            p2["profile_username"] = profile_username.lstrip("@")
                        if profile_fullname:
                            p2["profile_fullname"] = profile_fullname
                        if getattr(tg_chat, "id", None):
                            p2["user_id"] = p2.get("user_id") or getattr(tg_chat, "id", None)
                except Exception:
                    pass
            return p2
    for p in players:
        p2 = _normalize_player_entry(p)
        if p2.get("player_code") and str(p2.get("player_code")) == id_clean:
            if p2.get("user_id"):
                try:
                    tg_chat = await context.bot.get_chat(int(p2.get("user_id")))
                    if tg_chat:
                        profile_username = getattr(tg_chat, "username", None)
                        first = getattr(tg_chat, "first_name", "") or ""
                        last = getattr(tg_chat, "last_name", "") or ""
                        p2["profile_fullname"] = f"{first} {last}".strip() if (first or last) else p2.get("name")
                        if profile_username:
                            p2["profile_username"] = profile_username.lstrip("@")
                except Exception:
                    pass
            return p2
    if re.fullmatch(r"\d+", id_clean):
        try:
            tg_id = int(id_clean)
        except:
            tg_id = None
        if tg_id:
            profile_username = None
            profile_fullname = None
            try:
                tg_user = await context.bot.get_chat(tg_id)
                profile_username = getattr(tg_user, "username", None)
                first = getattr(tg_user, "first_name", "") or ""
                last = getattr(tg_user, "last_name", "") or ""
                profile_fullname = f"{first} {last}".strip() if (first or last) else None
            except Exception:
                profile_username = None
                profile_fullname = None
            if profile_username:
                for p in players:
                    p2 = _normalize_player_entry(p)
                    if p2.get("username") and p2.get("username").lstrip("@").lower() == profile_username.lstrip("@").lower():
                        p2["profile_username"] = profile_username.lstrip("@")
                        if profile_fullname:
                            p2["profile_fullname"] = profile_fullname
                        p2["user_id"] = p2.get("user_id") or tg_id
                        return p2
            if profile_username or profile_fullname:
                return {"user_id": tg_id, "username": profile_username.lstrip("@") if profile_username else None, "name": profile_fullname or profile_username or str(tg_id), "profile_username": profile_username.lstrip("@") if profile_username else None, "profile_fullname": profile_fullname}
            return None
    db = load_db()
    try:
        found_id = _recursive_find_userid_by_username(db, id_clean)
    except Exception:
        found_id = None
    if found_id:
        try:
            tg_user = await context.bot.get_chat(found_id)
            profile_username = getattr(tg_user, "username", None)
            first = getattr(tg_user, "first_name", "") or ""
            last = getattr(tg_user, "last_name", "") or ""
            profile_fullname = f"{first} {last}".strip() if (first or last) else None
        except Exception:
            profile_username = None
            profile_fullname = None
        if profile_username:
            for p in players:
                p2 = _normalize_player_entry(p)
                if p2.get("username") and p2.get("username").lstrip("@").lower() == profile_username.lstrip("@").lower():
                    p2["profile_username"] = profile_username.lstrip("@")
                    if profile_fullname:
                        p2["profile_fullname"] = profile_fullname
                    p2["user_id"] = p2.get("user_id") or found_id
                    return p2
        return {"user_id": found_id, "username": profile_username.lstrip("@") if profile_username else id_clean, "name": profile_fullname or id_clean, "profile_username": profile_username.lstrip("@") if profile_username else None, "profile_fullname": profile_fullname}
    reg = _find_registration_by_username_or_code(db, id_clean)
    if reg:
        return {"user_id": reg.get("user_id"), "username": (reg.get("username") or "").lstrip("@"), "name": reg.get("name"), "role": reg.get("role"), "player_code": reg.get("player_code")}
    return None

def _team_total_spent(session: Dict[str, Any], team_name: str) -> int:
    logs = session.get("logs", []) or []
    members = session.get("teams", {}).get(team_name, []) or []
    total = 0
    for l in logs:
        if l.get("buyer_id") in (members or []) and l.get("price"):
            total += int(l.get("price") or 0)
    return total

async def parse_next_identifier_and_price(text: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    if not text:
        return None, None, None
    t = text.strip()
    t = re.sub(r'^\s*/next(@[^\s]+)?\s*', '/next ', t, flags=re.IGNORECASE)
    price_match = re.search(r'(\d+)\s*$', t)
    if not price_match:
        return None, None, None
    price = int(price_match.group(1))
    before_price = t[:price_match.start()].strip()
    at_matches = list(re.finditer(r'@([A-Za-z0-9_]{1,64})', before_price))
    if at_matches:
        last = at_matches[-1]
        identifier = last.group(0)
        pre_text = before_price[:last.start()].strip()
        return identifier, price, pre_text
    numeric_match = None
    tokens = before_price.split()
    for i in range(len(tokens)-1, -1, -1):
        tok = tokens[i].strip()
        if re.fullmatch(r'\d+', tok):
            numeric_match = tok
            pre_text = " ".join(tokens[:i]).strip()
            return numeric_match, price, pre_text
    if tokens:
        identifier = tokens[-1]
        pre_text = " ".join(tokens[:-1]).strip()
        return identifier, price, pre_text
    return None, price, None

async def next_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can start next slot.")
        return
    parsed_identifier, parsed_price, pre_text = await parse_next_identifier_and_price(msg.text or "")
    if parsed_identifier is None or parsed_price is None:
        await msg.reply_text("Usage: /next <player_identifier> <base_price>  (also supports /next <extra_text> <player_identifier> <base_price>)")
        return
    identifier = parsed_identifier
    price = parsed_price
    if session.get("current_slot"):
        await msg.reply_text("A player is currently on auction. Wait for slot to finish or /pause + finalize.")
        return
    identifier_clean = identifier.strip()
    if identifier_clean.startswith("@"):
        identifier_clean = identifier_clean[1:]
    player = None
    try:
        player = await find_player_async(session, identifier_clean, chat_id, context)
    except Exception:
        player = None
    profile_fullname = None
    profile_username = None
    user_id_for_photo = None
    if player:
        profile_username = player.get("profile_username") or player.get("username")
        profile_fullname = player.get("profile_fullname") or player.get("name")
        if player.get("user_id"):
            try:
                user_id_for_photo = int(player.get("user_id"))
            except:
                user_id_for_photo = None
    else:
        try:
            tg_chat_try = None
            if re.fullmatch(r'\d+', identifier_clean):
                try:
                    tg_chat_try = await context.bot.get_chat(int(identifier_clean))
                except Exception:
                    tg_chat_try = None
            else:
                try:
                    tg_chat_try = await context.bot.get_chat(f"@{identifier_clean}")
                except Exception:
                    tg_chat_try = None
            if tg_chat_try:
                profile_username = getattr(tg_chat_try, "username", None)
                first = getattr(tg_chat_try, "first_name", "") or ""
                last = getattr(tg_chat_try, "last_name", "") or ""
                profile_fullname = f"{first} {last}".strip() if (first or last) else None
                if getattr(tg_chat_try, "id", None):
                    user_id_for_photo = getattr(tg_chat_try, "id")
                if profile_username:
                    player = await find_player_async(session, profile_username, chat_id, context)
                    if player:
                        player["profile_username"] = profile_username.lstrip("@")
                        if profile_fullname:
                            player["profile_fullname"] = profile_fullname
                        if user_id_for_photo:
                            player["user_id"] = player.get("user_id") or user_id_for_photo
        except Exception:
            pass
    if not player:
        await msg.reply_text("Player not found in loaded list. If you passed a Telegram ID, ensure that ID's username is present in the loaded list or load the player first with /load.")
        return
    if not profile_fullname:
        if pre_text:
            profile_fullname = pre_text
    sold_already = False
    for l in session.get("logs", []) or []:
        if l.get("player_id") and player.get("user_id") and str(l.get("player_id")) == str(player.get("user_id")) and l.get("price"):
            sold_already = True
            break
    run_id = session.get("current_run_id")
    if not sold_already and run_id:
        run = get_run(chat_id, run_id)
        for s in (run.get("sold_players", []) if run else []):
            if s.get("player_id") and player.get("user_id") and str(s.get("player_id")) == str(player.get("user_id")):
                sold_already = True
                break
    if sold_already:
        await msg.reply_text("This player has already sold.")
        return
    start_price = price
    deadline = int(time.time()) + COUNTDOWN_SECONDS
    player = _normalize_player_entry(player)
    profile_name = profile_fullname or player.get("profile_fullname") or player.get("name") or (player.get("username") or "")
    profile_username = profile_username or player.get("profile_username") or player.get("username")
    user_id_for_photo = user_id_for_photo or player.get("user_id")
    slot = {"player": player, "start_price": start_price, "deadline": deadline, "highest": None, "mg_message": None, "started_at": int(time.time()), "announced": {}}
    session["current_slot"] = slot
    save_session(chat_id, session)
    run_id = session.get("current_run_id") or start_new_run(chat_id, session)
    run = get_run(chat_id, run_id)
    if run:
        run["current_slot"] = {"player": player, "start_price": start_price, "deadline": deadline, "highest": None, "started_at": int(time.time())}
        save_run(chat_id, run)
    host_display = session.get('host_name') or ''
    name_plain = profile_name or (player.get("username") or "Unknown")
    caption = ("🔥 ⚔️ 𝑷𝒍𝒂𝒚𝒆𝒓 𝑼𝒑 𝑭𝒐𝒓 𝑨𝒖𝒄𝒕𝒊𝒐𝒏 ⚔️ 🔥\n\n" f"👤 𝑵𝒂𝒎𝒆: {name_plain}  \n" f"🏷️ 𝑼𝒔𝒆𝒓𝒏𝒂𝒎𝒆: @{(profile_username or '')}  \n" f"🎯 𝑹𝒐𝒍𝒆: {player.get('role') or 'None'}  \n" f"🆔 𝑻𝒆𝒍𝒆𝒈𝒓𝒂𝒎 𝑰𝒅: {player.get('user_id') or 'None'}  \n" f"💰 𝑩𝒂𝒔𝒆 𝑷𝒓𝒊𝒄𝒆: {start_price} CR  \n\n" f"👑 𝑯𝒐𝒔𝒕: 『{host_display}』\n\n" f"⏳ Place your bids now! 💸 Use /bid  or just /bid for a 1% default bid. 🕒 Auction Time: {COUNTDOWN_SECONDS}s\n")
    try:
        sent = await context.bot.send_photo(chat_id=chat_id, photo=ANNOUNCE_IMAGE, caption=caption, parse_mode=ParseMode.HTML)
        session["current_slot"]["mg_message"] = f"{chat_id}:{sent.message_id}"
        save_session(chat_id, session)
        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id, disable_notification=True)
        except Exception:
            pass
    except Exception:
        sent = await _send_message(context.bot, chat_id, caption, parse_mode=ParseMode.HTML)
        session["current_slot"]["mg_message"] = f"{chat_id}:{sent.message_id}"
        save_session(chat_id, session)
        try:
            await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id, disable_notification=True)
        except Exception:
            pass
    task = asyncio.create_task(slot_countdown(chat_id, context))
    countdown_tasks[chat_id] = task
    return

next_handler = CommandHandler("next", next_cmd)

async def slot_countdown(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        while True:
            session = get_session(chat_id)
            slot = session.get("current_slot")
            if not slot:
                return
            if session.get("paused"):
                await asyncio.sleep(1)
                continue
            now_t = int(time.time())
            deadline = slot.get("deadline", now_t)
            remaining = deadline - now_t
            if remaining <= 0:
                highest = slot.get("highest")
                player = slot.get("player", {})
                run_id = session.get("current_run_id")
                run = get_run(chat_id, run_id) if run_id else None
                if highest:
                    buyer_id = highest.get("user_id")
                    price = highest.get("amount")
                    buyer_name = highest.get("name")
                    buyer_team = _get_team_of_user(session, buyer_id)
                    buyer_display = buyer_team if buyer_team else f"{buyer_name}"
                    caption = ("🏁 💥 𝑷𝒍𝒂𝒚𝒆𝒓 𝑺𝒐𝒍𝒅 💥 🏁\n\n" f"👤 𝑵𝒂𝒎𝒆: {player.get('name')}  \n" f"🏷️ 𝑼𝒔𝒆𝒓𝒏𝒂𝒎𝒆: @{(player.get('username') or '')}  \n" f"🎯 𝑹𝒐𝒍𝒆: {player.get('role') or 'None'}  \n" f"🆔 𝑻𝒆𝒍𝒆𝒈𝒓𝒂𝒎 𝑰𝒅: {player.get('user_id') or 'None'}  \n" f"💰 𝑭𝒊𝒏𝒂𝒍 𝑷𝒓𝒊𝒄𝒆: {price} CR  \n\n" f"🤝 𝑩𝒖𝒚𝒆𝒓: {buyer_display}\n")
                    try:
                        sent = await context.bot.send_photo(chat_id=chat_id, photo=SOLD_IMAGE, caption=caption, parse_mode=ParseMode.HTML)
                        try:
                            await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id, disable_notification=True)
                        except Exception:
                            pass
                    except Exception:
                        await context.bot.send_message(chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML)
                    session["logs"].append({"ts": int(time.time()), "player_id": player.get("user_id"), "player_name": player.get("name"), "price": price, "buyer_id": buyer_id, "player_username": player.get("username"), "player_role": player.get("role")})
                    if run:
                        run_sold = run.get("sold_players", [])
                        run_sold.append({"ts": int(time.time()), "player_id": player.get("user_id"), "player_name": player.get("name"), "price": price, "buyer_id": buyer_id})
                        run["sold_players"] = run_sold
                        run["current_slot"] = None
                        append_run_log(chat_id, run.get("run_id"), {"event": "sold", "player": player, "price": price, "buyer_id": buyer_id, "ts": int(time.time())})
                        save_run(chat_id, run)
                else:
                    caption = ("⏹️ ⚠️ 𝑷𝒍𝒂𝒚𝒆𝒓 𝑼𝒏𝒔𝒐𝒍𝒅 ⚠️ ⏹️\n\n" f"👤 𝑵𝒂𝒎𝒆: {player.get('name')}  \n" f"🏷️ 𝑼𝒔𝒆𝒓𝒏𝒂𝒎𝒆: @{(player.get('username') or '')}  \n" f"🎯 𝑹𝒐𝒍𝒆: {player.get('role') or 'None'}  \n" f"🆔 𝑻𝒆𝒍𝒆𝒈𝒓𝒂𝒎 𝑰𝒅: {player.get('user_id') or 'None'}  \n" f"💰 𝑩𝒂𝒔𝒆 𝑷𝒓𝒊𝒄𝒆: {slot.get('start_price')} CR  \n\n" "📉 No bids received. Player remains unsold.\n")
                    try:
                        sent = await context.bot.send_photo(chat_id=chat_id, photo=UNSOLD_IMAGE, caption=caption, parse_mode=ParseMode.HTML)
                        try:
                            await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id, disable_notification=True)
                        except Exception:
                            pass
                    except Exception:
                        await context.bot.send_message(chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML)
                    session["logs"].append({"ts": int(time.time()), "player_id": player.get("user_id"), "player_name": player.get("name"), "price": None, "buyer_id": None, "player_username": player.get("username"), "player_role": player.get("role")})
                    if run:
                        run_unsold = run.get("unsold_players", [])
                        run_unsold.append({"ts": int(time.time()), "player_id": player.get("user_id"), "player_name": player.get("name"), "base_price": slot.get("start_price")})
                        run["unsold_players"] = run_unsold
                        run["current_slot"] = None
                        append_run_log(chat_id, run.get("run_id"), {"event": "unsold", "player": player, "base_price": slot.get("start_price"), "ts": int(time.time())})
                        save_run(chat_id, run)
                session["current_slot"] = None
                save_session(chat_id, session)
                return
            if remaining == 10 and not slot.get("announced", {}).get("10"):
                try:
                    await context.bot.send_message(chat_id=chat_id, text="🔟 10 seconds remaining for bid")
                except Exception:
                    pass
                slot.setdefault("announced", {})["10"] = True
                session["current_slot"] = slot
                save_session(chat_id, session)
                await asyncio.sleep(1)
                continue
            if remaining <= 5 and remaining > 0:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=f"{remaining} second{'s' if remaining!=1 else ''} remaining for bid")
                except Exception:
                    pass
                await asyncio.sleep(1)
                continue
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        return
    except Exception:
        return

async def bid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    slot = session.get("current_slot")
    if not slot:
        await msg.reply_text("No active auction slot right now.")
        return
    teams = session.get("teams", {}) or {}
    bidder_team = None
    bidder_is_assistant = False
    for tname, members in teams.items():
        if members and user.id in members:
            bidder_team = tname
            break
    if not bidder_team:
        for tname, aid in (session.get("assistants", {}) or {}).items():
            if aid and int(aid) == user.id:
                bidder_team = tname
                bidder_is_assistant = True
                break
    if not bidder_team:
        await msg.reply_text("Only assigned team owners or assistants can place bids.")
        return
    parts = msg.text.strip().split(maxsplit=1)
    amt = None
    if len(parts) < 2:
        budget = session.get("budget")
        if not budget:
            amt = 1
        else:
            amt = max(1, round(budget * 0.01))
    else:
        try:
            amt = int(parts[1].replace(",", "").strip())
        except:
            await msg.reply_text("Please send a valid integer bid.")
            return
    if amt < slot.get("start_price", 0):
        await msg.reply_text(f"Your bid must be at least the base price ({slot.get('start_price')} CR).")
        return
    highest = slot.get("highest")
    if highest and amt <= highest.get("amount", 0):
        await msg.reply_text(f"Your bid must be higher than current highest bid ({highest.get('amount')} CR).")
        return
    team_budget_map = session.get("team_budgets", {}) or {}
    team_budget = team_budget_map.get(bidder_team)
    if team_budget is not None:
        spent = _team_total_spent(session, bidder_team)
        remaining = team_budget - spent
        if amt > remaining:
            await msg.reply_text(f"Insufficient budget. Remaining: {remaining} CR.")
            return
    purchased_count = 0
    logs = session.get("logs", []) or []
    for l in logs:
        if l.get("buyer_id") and _get_team_of_user(session, l.get("buyer_id")) == bidder_team and l.get("price"):
            purchased_count += 1
    max_buy = session.get("max_buy")
    if isinstance(max_buy, int) and purchased_count >= max_buy:
        await msg.reply_text(f"Team {bidder_team} has reached the maximum purchases ({max_buy}). Cannot bid further.")
        return
    timestamp = int(time.time())
    slot["highest"] = {"user_id": user.id, "amount": amt, "name": user.first_name or "", "ts": timestamp}
    slot["deadline"] = int(time.time()) + COUNTDOWN_SECONDS
    slot["announced"] = {}
    session["current_slot"] = slot
    save_session(chat_id, session)
    run_id = session.get("current_run_id")
    if run_id:
        run = get_run(chat_id, run_id)
        if run:
            run_current = run.get("current_slot", {})
            run_current["highest"] = slot["highest"]
            run_current["deadline"] = slot["deadline"]
            run["current_slot"] = run_current
            save_run(chat_id, run)
            append_run_log(chat_id, run_id, {"event": "bid", "user_id": user.id, "amount": amt, "player": slot.get("player"), "ts": timestamp})
    bidder_link = f'<a href="tg://user?id={user.id}">{(user.first_name or "")}</a>'
    text = ("🎯 <b>BID PLACED!</b>\n\n" f"👤 <b>Player:</b> {(slot.get('player') or {}).get('name')}\n" f"💰 <b>Current Bid:</b> ₹{amt} Cr\n" f"🏏 <b>Bidder:</b> {bidder_link}\n" f"🛡️ <b>Team:</b> <b>{bidder_team}</b>\n\n" f"⏳ Time reset: {COUNTDOWN_SECONDS} seconds for next bid.")
    try:
        await _send_message(context.bot, chat_id, text, parse_mode=ParseMode.HTML)
    except:
        await msg.reply_text(text, parse_mode=ParseMode.HTML)
    return

bid_handler = CommandHandler("bid", bid_cmd)

async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can pause.")
        return
    session["paused"] = True
    session["pause_start"] = int(time.time())
    save_session(chat_id, session)
    await msg.reply_text("⏸️ Auction paused. Countdown halted. Use /resume to continue.")
    return

pause_handler = CommandHandler("pause", pause_cmd)

async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can resume.")
        return
    if not session.get("paused"):
        await msg.reply_text("Auction is not paused.")
        return
    pause_start = session.get("pause_start")
    if pause_start:
        paused_seconds = int(time.time()) - pause_start
    else:
        paused_seconds = 0
    slot = session.get("current_slot")
    if slot and slot.get("deadline"):
        slot["deadline"] = slot.get("deadline", int(time.time())) + paused_seconds
        session["current_slot"] = slot
    session["paused"] = False
    session["pause_start"] = None
    save_session(chat_id, session)
    run_id = session.get("current_run_id")
    if run_id:
        run = get_run(chat_id, run_id)
        if run:
            run_current = run.get("current_slot") or {}
            if run_current.get("deadline"):
                run_current["deadline"] = run_current.get("deadline") + paused_seconds
                run["current_slot"] = run_current
                save_run(chat_id, run)
    await msg.reply_text("▶️ Auction resumed. Countdown continues.")
    return

resume_handler = CommandHandler("resume", resume_cmd)

async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can view summary.")
        return
    tables = session.get("tables") or 0
    teams_count = len(session.get("teams", {}).keys())
    budget = session.get("budget") or 0
    players_count = len(session.get("players_list") or [])
    sold = len([l for l in session.get("logs", []) if l.get("price")])
    unsold = len([l for l in session.get("logs", []) if l.get("price") is None])
    available = players_count - sold - unsold
    txt = ("🎯 <b>Auction Summary</b> 🎯\n\n" f"🏷️ Total Tables: {teams_count}/{tables}\n" f"💰 Auction Budget (per team): {budget}\n" f"📦 Total Players Loaded: {players_count}\n" f"🛒 Available for Buying: {available}\n" f"✅ Sold Players: {sold}\n" f"⛔ Unsold Players: {unsold}\n\n" "⚡ Keep the energy up — good luck!")
    await _send_message(context.bot, chat_id, txt, parse_mode=ParseMode.HTML)
    return

summary_handler = CommandHandler("summary", summary_cmd)

async def end_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if not _is_host_or_access(session, user.id):
        await msg.reply_text("Only the auction host can end the auction.")
        return
    await _send_message(context.bot, chat_id, "Are you sure you want to end the auction?", reply_markup=build_end_confirm(user.id))
    return

end_handler = CommandHandler("end_auction", end_cmd)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    text = (msg.text or "").strip()
    if msg.reply_to_message:
        reply_msg = msg.reply_to_message
        target_info, reason = _extract_target_from_message(reply_msg)
        if not target_info:
            await msg.reply_text("Could not identify the target user.")
            return
        target_username = (target_info.get("first_name") or "").strip()
        target_user_id = target_info.get("user_id")
    else:
        parts = text.split(maxsplit=1)
        if len(parts) > 1:
            candidate = parts[1].strip()
            if candidate.startswith("@"):
                candidate = candidate[1:]
            target_user_id = None
            db = load_db()
            reg = _find_registration_by_username_or_code(db, candidate)
            if reg and reg.get("user_id"):
                target_user_id = reg.get("user_id")
            else:
                try:
                    target_user_id = int(candidate)
                except:
                    target_user_id = None
            target_username = candidate
        else:
            if not _is_host_or_access(session, user.id) and _is_team_owner(session, user.id) is None:
                await msg.reply_text("Only host or team owners can use status.")
                return
            teams = session.get("teams", {}) or {}
            budget = session.get("budget")
            logs = session.get("logs", []) or []
            parts = []
            idx = 1
            for tname, members in teams.items():
                bought_count = 0
                spent = 0
                for l in logs:
                    if l.get("buyer_id") in (members or []) and l.get("price"):
                        bought_count += 1
                        spent += l.get("price", 0)
                remaining = None
                if isinstance(budget, (int, float)):
                    remaining = budget - spent
                parts.append(f"{idx} {tname}\nPlayers bought: {str(bought_count).zfill(2)}\nRemaining Budget: {str(remaining).zfill(3) if remaining is not None else 'Not set'}\n")
                idx += 1
            if not parts:
                await msg.reply_text("No teams assigned yet.")
                return
            final_text = "Teams status\n\n" + "\n".join(parts)
            await _send_message(context.bot, chat_id, final_text)
            return
    found_entry = None
    if target_user_id:
        for l in session.get("logs", []) or []:
            if l.get("player_id") and str(l.get("player_id")) == str(target_user_id):
                found_entry = l
                break
        if not found_entry:
            run_id = session.get("current_run_id")
            if run_id:
                run = get_run(chat_id, run_id)
                if run:
                    for s in (run.get("sold_players", []) or []):
                        if s.get("player_id") and str(s.get("player_id")) == str(target_user_id):
                            found_entry = {"player_name": s.get("player_name"), "price": s.get("price"), "buyer_id": s.get("buyer_id")}
                            break
    else:
        target_un = (target_username or "").lstrip("@").lower()
        for l in session.get("logs", []) or []:
            if (l.get("player_username") or "").lstrip("@").lower() == target_un:
                found_entry = l
                break
        if not found_entry:
            run_id = session.get("current_run_id")
            if run_id:
                run = get_run(chat_id, run_id)
                if run:
                    for s in (run.get("sold_players", []) or []):
                        if (s.get("player_name") or "").strip().lower() == target_un:
                            found_entry = {"player_name": s.get("player_name"), "price": s.get("price"), "buyer_id": s.get("buyer_id")}
                            break
    if found_entry:
        if found_entry.get("price"):
            buyer_team = None
            buyer_id = found_entry.get("buyer_id")
            buyer_team = _get_team_of_user(session, buyer_id)
            buyer_label = buyer_team if buyer_team else "Team Owner"
            await msg.reply_text(f"Player {found_entry.get('player_name')} has been SOLD to {buyer_label} for {found_entry.get('price')} CR.")
            return
        else:
            await msg.reply_text(f"Player {found_entry.get('player_name')} was UNSOLD.")
            return
    else:
        for p in session.get("players_list") or []:
            p2 = _normalize_player_entry(p)
            if p2.get("user_id") and target_user_id and str(p2.get("user_id")) == str(target_user_id):
                await msg.reply_text(f"Player {p2.get('name') or p2.get('username')} is loaded but not auctioned yet.")
                return
            if p2.get("username") and (target_username or "") and p2.get("username").lstrip("@").lower() == (target_username or "").lstrip("@").lower():
                await msg.reply_text(f"Player {p2.get('name') or p2.get('username')} is loaded but not auctioned yet.")
                return
        await msg.reply_text("Player not found in loaded list or auction logs.")
        return

status_handler = CommandHandler("status", status_cmd)

async def my_team_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    teams = session.get("teams", {}) or {}
    user_teams = []
    for tname, members in teams.items():
        if members and user.id in members:
            user_teams.append(tname)
    if not user_teams:
        await msg.reply_text("You are not assigned to any team.")
        return
    budget = session.get("budget")
    logs = session.get("logs", []) or []
    parts = []
    for tname in user_teams:
        team_members = teams.get(tname) or []
        team_purchases = []
        total_spent = 0
        for l in logs:
            if l.get("buyer_id") in (team_members or []) and l.get("price"):
                player_name = l.get("player_name") or ""
                player_username = l.get("player_username") or ""
                player_role = l.get("player_role") or ""
                price = l.get("price") or 0
                total_spent += price if isinstance(price, (int, float)) else 0
                team_purchases.append({"name": player_name, "username": player_username, "role": player_role, "price": price})
        remaining = None
        if isinstance(budget, (int, float)):
            remaining = budget - total_spent
        header = f"🎯 **My Team — {tname}** 🎯\n\n"
        budget_line = f"💰 **Budget:** {budget if budget is not None else 'Not set'}\n💸 **Spent:** {total_spent}\n🪙 **Remaining:** {remaining if remaining is not None else 'Not set'}\n\n"
        if not team_purchases:
            header += budget_line + "No purchases yet for your team."
            parts.append(header)
            continue
        body_lines = []
        for p in team_purchases:
            uname = ("@" + p.get("username")) if p.get("username") else "—"
            block = (f"👤 ** Name:** {p.get('name')}\n" f"📩 **Username:** {uname}\n" f"🎭 **Role:** {p.get('role')}\n" f"💵 **Sold Price:** {p.get('price')} CR\n\n")
            body_lines.append(block)
        full = header + budget_line + "\n".join(body_lines)
        parts.append(full)
    final_text = "\n\n".join(parts)
    await _send_message(context.bot, chat_id, final_text, parse_mode=ParseMode.HTML)
    return

my_team_handler = CommandHandler("my_team", my_team_cmd)

async def unsold_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat or (msg.chat if msg else None)
    user = update.effective_user
    if not chat:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    teams = session.get("teams", {}) or {}
    logs = session.get("logs", []) or []
    sold_ids = {str(l.get("player_id")) for l in logs if l.get("player_id") and l.get("price")}
    all_players = session.get("players_list", []) or []
    unsold_players = []
    for p in all_players:
        p2 = _normalize_player_entry(p)
        pid = p2.get("user_id")
        key = str(pid) if pid else (p2.get("username") or "")
        if key and str(key) not in sold_ids:
            unsold_players.append(p2)
    if not unsold_players:
        await msg.reply_text("No unsold players (all loaded players are either sold or none loaded).")
        return
    lines = []
    idx = 1
    for u in unsold_players:
        name = u.get("profile_fullname") or u.get("name") or u.get("username") or "Unknown"
        uname = ("@" + u.get("username")) if u.get("username") else "—"
        tid = u.get("user_id") or "None"
        role = u.get("role") or "None"
        block = (f"{idx}. 👤 <b>{name}</b>\n" f"   🏷️ <b>Username:</b> {uname}\n" f"   🎯 <b>Role:</b> {role}\n" f"   🆔 <b>Telegram ID:</b> {tid}\n")
        lines.append(block)
        idx += 1
    header = "🟦 <b>Unsold Players</b> 🟦\n\n"
    txt = header + "\n".join(lines)
    await _send_message(context.bot, chat_id, txt, parse_mode=ParseMode.HTML)
    return

unsold_handler = CommandHandler("unsold", unsold_cmd)

async def assist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    text = (msg.text or "").strip()
    args = text.split()[1:] if text else []
    target_info = None
    if msg.reply_to_message:
        target_info, _ = _extract_target_from_message(msg.reply_to_message)
    else:
        if args:
            maybe = args[-1]
            if maybe.startswith("@"):
                maybe = maybe[1:]
            db = load_db()
            reg = _find_registration_by_username_or_code(db, maybe)
            if reg and reg.get("user_id"):
                target_info = {"user_id": reg.get("user_id"), "first_name": reg.get("name") or reg.get("username")}
            else:
                try:
                    uid = int(maybe)
                    target_info = {"user_id": uid, "first_name": str(uid)}
                except:
                    target_info = None
    if not target_info or not target_info.get("user_id"):
        await msg.reply_text("Could not identify the user to assign as assistant. Reply to the user's message or pass @username/userid.")
        return
    target_id = int(target_info.get("user_id"))
    assigned_team = None
    if _is_host_or_access(session, user.id):
        if args:
            possible_team = args[0]
            if possible_team.startswith("@"):
                possible_team = possible_team[1:]
            teams = session.get("teams", {}) or {}
            for tname in teams.keys():
                if tname.lower() == possible_team.lower():
                    assigned_team = tname
                    break
            if not assigned_team:
                await msg.reply_text("Team not found. Provide correct team name.")
                return
        else:
            await msg.reply_text("As host you must provide the team name to assign an assistant. Usage: /assist <TeamName> @username or reply to user with /assist <TeamName>")
            return
    else:
        owner_team = _is_team_owner(session, user.id)
        if not owner_team:
            await msg.reply_text("Only host or a team owner can assign an assistant.")
            return
        assigned_team = owner_team
    assistants = session.get("assistants", {}) or {}
    current_assistant = assistants.get(assigned_team)
    if current_assistant and current_assistant != target_id:
        try:
            u = await context.bot.get_chat(current_assistant)
            name = getattr(u, "first_name", "") or str(current_assistant)
        except:
            name = str(current_assistant)
        await msg.reply_text(f"This team already has an assistant: {name}. Remove them first with /dem.")
        return
    assistants[assigned_team] = target_id
    session["assistants"] = assistants
    save_session(chat_id, session)
    try:
        tgt = await context.bot.get_chat(target_id)
        name_display = getattr(tgt, "first_name", "") or str(target_id)
    except:
        name_display = str(target_id)
    await msg.reply_text(f"{name_display} is now assistant for team {assigned_team}. Assistants can bid alongside the owner.")
    return

assist_handler = CommandHandler("assist", assist_cmd)

async def dem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    text = (msg.text or "").strip()
    args = text.split()[1:] if text else []
    target_info = None
    if msg.reply_to_message:
        target_info, _ = _extract_target_from_message(msg.reply_to_message)
    else:
        if args:
            maybe = args[-1]
            if maybe.startswith("@"):
                maybe = maybe[1:]
            db = load_db()
            reg = _find_registration_by_username_or_code(db, maybe)
            if reg and reg.get("user_id"):
                target_info = {"user_id": reg.get("user_id"), "first_name": reg.get("name") or reg.get("username")}
            else:
                try:
                    uid = int(maybe)
                    target_info = {"user_id": uid, "first_name": str(uid)}
                except:
                    target_info = None
    if not target_info or not target_info.get("user_id"):
        await msg.reply_text("Could not identify the assistant to demote. Reply to the assistant's message or pass @username/userid.")
        return
    target_id = int(target_info.get("user_id"))
    teams = session.get("teams", {}) or {}
    assistants = session.get("assistants", {}) or {}
    team_found = None
    for tname, aid in (assistants.items()):
        if aid and int(aid) == target_id:
            team_found = tname
            break
    if not team_found:
        await msg.reply_text("This user is not an assistant for any team.")
        return
    if _is_host_or_access(session, user.id):
        pass
    else:
        owner_team = _is_team_owner(session, user.id)
        if owner_team != team_found:
            await msg.reply_text("Only the host or the team owner can demote this assistant.")
            return
    assistants.pop(team_found, None)
    session["assistants"] = assistants
    save_session(chat_id, session)
    try:
        tgt = await context.bot.get_chat(target_id)
        name_display = getattr(tgt, "first_name", "") or str(target_id)
    except:
        name_display = str(target_id)
    await msg.reply_text(f"{name_display} has been demoted and removed as assistant from team {team_found}.")
    return

demote_handler = CommandHandler("dem", dem_cmd)

async def access_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if session.get("host_id") != user.id:
        await msg.reply_text("Only the host can grant access.")
        return
    target_info = None
    if msg.reply_to_message:
        target_info, _ = _extract_target_from_message(msg.reply_to_message)
    else:
        parts = (msg.text or "").split()
        if len(parts) > 1:
            maybe = parts[1]
            if maybe.startswith("@"):
                maybe = maybe[1:]
            db = load_db()
            reg = _find_registration_by_username_or_code(db, maybe)
            if reg and reg.get("user_id"):
                target_info = {"user_id": reg.get("user_id"), "first_name": reg.get("name") or reg.get("username")}
            else:
                try:
                    uid = int(maybe)
                    target_info = {"user_id": uid, "first_name": str(uid)}
                except:
                    target_info = None
    if not target_info or not target_info.get("user_id"):
        await msg.reply_text("Could not identify the user to grant access. Reply to the user message or pass @username/userid.")
        return
    target_id = int(target_info.get("user_id"))
    access = session.get("access_users", []) or []
    if target_id in access:
        await msg.reply_text("This user already has host-level access.")
        return
    access.append(target_id)
    session["access_users"] = access
    save_session(chat_id, session)
    try:
        tgt = await context.bot.get_chat(target_id)
        name_display = getattr(tgt, "first_name", "") or str(target_id)
    except:
        name_display = str(target_id)
    await msg.reply_text(f"{name_display} has been granted host-like access. They can now run host commands.")
    return

access_handler = CommandHandler("access", access_cmd)

async def _resolve_username_to_userid(context: ContextTypes.DEFAULT_TYPE, username: str) -> Optional[int]:
    if not username:
        return None
    username = username.lstrip("@")
    db = load_db()
    found = _recursive_find_userid_by_username(db, username)
    if found:
        return found
    try:
        chat = await context.bot.get_chat(f"@{username}")
        if getattr(chat, "id", None):
            return int(chat.id)
    except:
        pass
    return None

async def _find_team_by_owner_userid(session: Dict[str, Any], userid: int) -> Optional[str]:
    teams = session.get("teams", {}) or {}
    for tname, members in teams.items():
        if members and members[0] == userid:
            return tname
    return None

def _parse_amount_token(tok: str) -> Optional[int]:
    if not tok:
        return None
    tok = tok.strip().lower().replace(" ", "")
    m = re.match(r'^(\d+)(cr)?$', tok)
    if not m:
        m = re.match(r'^(\d+)$', tok)
    if m:
        val = int(m.group(1))
        return val
    return None

async def deduct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if session.get("host_id") != user.id:
        await msg.reply_text("Only the host can use /deduct.")
        return
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) < 3:
        await msg.reply_text("Usage: /deduct <amount> <TeamName or @owner_username or @owner_userid>")
        return
    amt_token = parts[1]
    amount = _parse_amount_token(amt_token)
    if amount is None:
        await msg.reply_text("Invalid amount. Use numbers like 5 or 5cr.")
        return
    target = " ".join(parts[2:]).strip()
    if target.startswith("@"):
        username = target.lstrip("@")
        uid = await _resolve_username_to_userid(context, username)
        if not uid:
            await msg.reply_text("Cannot find that username in DB.")
            return
        team = await _find_team_by_owner_userid(session, uid)
        if not team:
            await msg.reply_text("This user is not a team owner.")
            return
    else:
        team = None
        for t in session.get("teams", {}) or {}:
            if t.lower() == target.lower():
                team = t
                break
        if team is None:
            try:
                uid = int(target)
                team = await _find_team_by_owner_userid(session, uid)
                if team is None:
                    await msg.reply_text("Team not found for that id.")
                    return
            except:
                await msg.reply_text("Team not found.")
                return
    tb = session.get("team_budgets", {}) or {}
    prev = tb.get(team, session.get("budget") or 0)
    newv = prev - amount
    tb[team] = newv
    session["team_budgets"] = tb
    save_session(chat_id, session)
    owner_id = session.get("teams", {}).get(team, [None])[0]
    try:
        owner_name = " ".join([getattr(await context.bot.get_chat(owner_id), "first_name", "")]) if owner_id else team
    except:
        owner_name = team
    await msg.reply_text(f"Deducted {amount} CR from team {team}. Previous: {prev} CR, Now: {newv} CR.")
    return

deduct_handler = CommandHandler("deduct", deduct_cmd)

async def plus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id
    session = get_session(chat_id)
    if session.get("host_id") != user.id:
        await msg.reply_text("Only the host can use /plus.")
        return
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) < 3:
        await msg.reply_text("Usage: /plus <amount> <TeamName or @owner_username or @owner_userid>")
        return
    amt_token = parts[1]
    amount = _parse_amount_token(amt_token)
    if amount is None:
        await msg.reply_text("Invalid amount. Use numbers like 5 or 5cr.")
        return
    target = " ".join(parts[2:]).strip()
    if target.startswith("@"):
        username = target.lstrip("@")
        uid = await _resolve_username_to_userid(context, username)
        if not uid:
            await msg.reply_text("Cannot find that username in DB.")
            return
        team = await _find_team_by_owner_userid(session, uid)
        if not team:
            await msg.reply_text("This user is not a team owner.")
            return
    else:
        team = None
        for t in session.get("teams", {}) or {}:
            if t.lower() == target.lower():
                team = t
                break
        if team is None:
            try:
                uid = int(target)
                team = await _find_team_by_owner_userid(session, uid)
                if team is None:
                    await msg.reply_text("Team not found for that id.")
                    return
            except:
                await msg.reply_text("Team not found.")
                return
    tb = session.get("team_budgets", {}) or {}
    prev = tb.get(team, session.get("budget") or 0)
    newv = prev + amount
    tb[team] = newv
    session["team_budgets"] = tb
    save_session(chat_id, session)
    owner_id = session.get("teams", {}).get(team, [None])[0]
    try:
        owner_name = " ".join([getattr(await context.bot.get_chat(owner_id), "first_name", "")]) if owner_id else team
    except:
        owner_name = team
    await msg.reply_text(f"Added {amount} CR to team {team}. Previous: {prev} CR, Now: {newv} CR.")
    return

plus_handler = CommandHandler("plus", plus_cmd)