import asyncio
import os
import json
import random
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid, UserAlreadyParticipant,
    UserNotParticipant, ChannelInvalid, ChannelPrivate, UsernameNotOccupied,
    InviteHashInvalid, InviteHashExpired, InviteRequestSent
)

API_ID = ""
API_HASH = ""
BOT_TOKEN = ""
ACCOUNTS_FILE = "accounts.json"


# Группы состояний для FSM
class AuthStates(StatesGroup):
    phone = State()
    code = State()
    password = State()


class BroadcastStates(StatesGroup):
    text = State()
    target = State()


class MassActionStates(StatesGroup):
    link = State()
    count = State()
    time = State()


class AccountManager:
    def __init__(self):
        self.accounts = {}
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher()
        self.stop_flags = {}

        os.makedirs("sessions", exist_ok=True)
        self.load_accounts()
        self.setup_handlers()

    def load_accounts(self):
        if not os.path.exists(ACCOUNTS_FILE):
            return
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for phone, acc_data in data.items():
                client = Client(
                    name=f"sessions/{phone}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=acc_data["session_string"],
                )
                self.accounts[phone] = {
                    "client": client,
                    "phone": phone,
                    "session_string": acc_data["session_string"],
                }
            print(f"[INIT] Загружено {len(self.accounts)} аккаунтов.")
        except Exception as e:
            print(f"[ERROR] Ошибка загрузки аккаунтов: {e}")

    def save_accounts(self):
        try:
            data = {
                phone: {
                    "phone": acc["phone"],
                    "session_string": acc["session_string"],
                }
                for phone, acc in self.accounts.items()
            }
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ERROR] Ошибка при сохранении аккаунтов: {e}")

    # === Клавиатуры ===
    def get_main_keyboard(self):
        builder = ReplyKeyboardBuilder()
        builder.add(types.KeyboardButton(text="📊 Статус аккаунтов"))
        builder.add(types.KeyboardButton(text="📈 Статистика подписок"))
        builder.add(types.KeyboardButton(text="👤 Добавить аккаунт"))
        builder.add(types.KeyboardButton(text="📢 Массовая подписка"))
        builder.add(types.KeyboardButton(text="❌ Массовая отписка"))
        builder.add(types.KeyboardButton(text="✉️ Рассылка"))
        builder.add(types.KeyboardButton(text="🛑 Остановить операцию"))
        builder.adjust(2, 2, 2, 1)
        return builder.as_markup(resize_keyboard=True)

    def get_cancel_keyboard(self):
        builder = ReplyKeyboardBuilder()
        builder.add(types.KeyboardButton(text="❌ Отмена"))
        return builder.as_markup(resize_keyboard=True)

    # === Основные команды ===
    async def cmd_start(self, message: types.Message):
        welcome_text = (
            "👋 Привет! Я современный менеджер Telegram-аккаунтов\n\n"
            "💡 Используй кнопки ниже для управления:\n"
            "• 📊 Статус - список активных аккаунтов\n"
            "• 📈 Статистика - подписки аккаунтов\n"
            "• 👤 Добавить - новый аккаунт\n"
            "• 📢 Подписка - массовая подписка\n"
            "• ❌ Отписка - массовая отписка\n"
            "• ✉️ Рассылка - отправка сообщений\n"
            "• 🛑 Стоп - остановка операций"
        )
        await message.answer(welcome_text, reply_markup=self.get_main_keyboard())

    async def cmd_cancel(self, message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Операция отменена", reply_markup=self.get_main_keyboard())

    # === Статус аккаунтов ===
    async def show_status(self, message: types.Message):
        if not self.accounts:
            await message.answer("❌ Нет активных аккаунтов", reply_markup=self.get_main_keyboard())
            return

        text = "🟢 Активные аккаунты:\n\n"
        for i, acc in enumerate(self.accounts.values(), 1):
            text += f"{i}. {acc['phone']}\n"

        text += f"\n📊 Всего: {len(self.accounts)} аккаунтов"
        await message.answer(text, reply_markup=self.get_main_keyboard())

    # === Добавление аккаунта ===
    async def start_auth(self, message: types.Message, state: FSMContext):
        await message.answer(
            "📱 Введите номер телефона для входа:\n\n"
            "Формат: +79991234567",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(AuthStates.phone)

    async def process_phone(self, message: types.Message, state: FSMContext):
        phone = message.text.strip()
        normalized_phone = phone.replace("+", "")

        if normalized_phone in self.accounts:
            await message.answer("⚠️ Этот аккаунт уже добавлен")
            await state.clear()
            return

        session_name = f"sessions/{normalized_phone}"
        if os.path.exists(session_name + ".session"):
            os.remove(session_name + ".session")

        client = Client(session_name, api_id=API_ID, api_hash=API_HASH)

        try:
            await client.connect()
            sent = await client.send_code(phone)

            await state.update_data(
                phone=normalized_phone,
                client=client,
                phone_code_hash=sent.phone_code_hash
            )

            await message.answer(
                "📩 Код отправлен! Введите код из Telegram:",
                reply_markup=self.get_cancel_keyboard()
            )
            await state.set_state(AuthStates.code)

        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
            await client.disconnect()
            await state.clear()

    async def process_code(self, message: types.Message, state: FSMContext):
        code = message.text.strip()
        data = await state.get_data()
        client = data["client"]
        phone = data["phone"]

        try:
            await client.sign_in(phone, data["phone_code_hash"], code)

            self.accounts[phone] = {
                "client": client,
                "phone": phone,
                "session_string": await client.export_session_string(),
            }
            self.save_accounts()

            await message.answer(
                f"✅ Аккаунт {phone} успешно добавлен!",
                reply_markup=self.get_main_keyboard()
            )
            await state.clear()

        except SessionPasswordNeeded:
            await message.answer(
                "🔐 Аккаунт защищён паролем. Введите пароль:",
                reply_markup=self.get_cancel_keyboard()
            )
            await state.set_state(AuthStates.password)
        except PhoneCodeInvalid:
            await message.answer("❌ Неверный код. Попробуйте снова:")

    async def process_password(self, message: types.Message, state: FSMContext):
        password = message.text.strip()
        data = await state.get_data()
        client = data["client"]
        phone = data["phone"]

        try:
            await client.check_password(password)

            self.accounts[phone] = {
                "client": client,
                "phone": phone,
                "session_string": await client.export_session_string(),
            }
            self.save_accounts()

            await message.answer(
                f"✅ Аккаунт {phone} успешно добавлен!",
                reply_markup=self.get_main_keyboard()
            )

        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
        finally:
            await state.clear()

    # === Массовые действия ===
    async def start_mass_subscribe(self, message: types.Message, state: FSMContext):
        await state.update_data(action_type="subscribe")
        await message.answer(
            "🔗 Введите ссылку для подписки:\n\n"
            "Примеры:\n"
            "• https://t.me/channel_name\n"
            "• @channel_name\n"
            "• +invite_hash",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(MassActionStates.link)

    async def start_mass_unsubscribe(self, message: types.Message, state: FSMContext):
        await state.update_data(action_type="unsubscribe")
        await message.answer(
            "🔗 Введите ссылку для отписки:\n\n"
            "Примеры:\n"
            "• https://t.me/channel_name\n"
            "• @channel_name\n"
            "• +invite_hash",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(MassActionStates.link)

    async def process_mass_link(self, message: types.Message, state: FSMContext):
        link = message.text.strip()
        await state.update_data(link=link)

        await message.answer(
            "🔢 Введите количество аккаунтов:",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(MassActionStates.count)

    async def process_mass_count(self, message: types.Message, state: FSMContext):
        try:
            count = int(message.text.strip())
            if count <= 0:
                await message.answer("❌ Введите положительное число:")
                return

            await state.update_data(count=count)

            await message.answer(
                "⏰ Введите время выполнения (например: 1h, 30m, 10s):",
                reply_markup=self.get_cancel_keyboard()
            )
            await state.set_state(MassActionStates.time)

        except ValueError:
            await message.answer("❌ Введите корректное число:")

    async def process_mass_time(self, message: types.Message, state: FSMContext):
        period = message.text.strip().lower()

        try:
            if period.endswith("h"):
                total_seconds = float(period[:-1]) * 3600
            elif period.endswith("m"):
                total_seconds = float(period[:-1]) * 60
            elif period.endswith("s"):
                total_seconds = float(period[:-1])
            else:
                total_seconds = float(period)

            if total_seconds <= 0:
                await message.answer("❌ Введите положительное время:")
                return

        except Exception:
            await message.answer("❌ Неверный формат времени. Используйте: 1h, 30m, 10s")
            return

        data = await state.get_data()
        action_type = data["action_type"]
        link = data["link"]
        count = data["count"]

        action_text = "подписка" if action_type == "subscribe" else "отписка"

        # Используем другой разделитель для callback_data
        callback_data = f"mass_action|{action_type}|{count}|{period}|{link}"

        builder = InlineKeyboardBuilder()
        builder.add(types.InlineKeyboardButton(
            text="✅ Запустить",
            callback_data=callback_data
        ))
        builder.add(types.InlineKeyboardButton(
            text="❌ Отмена",
            callback_data="cancel_mass"
        ))

        await message.answer(
            f"📋 Подтвердите параметры:\n\n"
            f"• Действие: {action_text}\n"
            f"• Ссылка: {link}\n"
            f"• Аккаунтов: {count}\n"
            f"• Время: {period}\n\n"
            f"⚠️ Для остановки во время выполнения используйте кнопку '🛑 Остановить операцию'",
            reply_markup=builder.as_markup()
        )
        await state.clear()

    async def handle_mass_action_callback(self, callback: types.CallbackQuery):
        if callback.data == "cancel_mass":
            await callback.message.edit_text("❌ Операция отменена")
            return

        if callback.data.startswith("mass_action|"):
            try:
                # Разбираем данные с использованием правильного разделителя
                parts = callback.data.split("|")
                if len(parts) >= 5:
                    action_type = parts[1]
                    count_str = parts[2]
                    period = parts[3]
                    # Объединяем оставшиеся части как ссылку (на случай если в ссылке есть |)
                    link = "|".join(parts[4:])

                    count = int(count_str)

                    await callback.message.edit_text("🚀 Запуск операции...")
                    await self.execute_mass_action(callback.message, link, count, period, action_type)
                else:
                    await callback.answer("❌ Ошибка в данных")
            except Exception as e:
                await callback.answer(f"❌ Ошибка: {str(e)}")

    SUB_FILE = "subscriptions.json"

    def load_subs(self):
        if os.path.exists(self.SUB_FILE):
            try:
                with open(self.SUB_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_subs(self, subs):
        try:
            with open(self.SUB_FILE, "w", encoding="utf-8") as f:
                json.dump(subs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ERROR] save_subs: {e}")

    async def execute_mass_action(self, message, link, count, period, action):
        user_id = message.from_user.id
        self.stop_flags[user_id] = False

        accounts = list(self.accounts.values())
        if not accounts:
            await message.answer("❌ Нет аккаунтов для выполнения операции")
            return

        actual_count = min(count, len(accounts))
        subs = self.load_subs()

        period_str = period.strip().lower()
        try:
            if period_str.endswith("h"):
                total_seconds = float(period_str[:-1]) * 3600
            elif period_str.endswith("m"):
                total_seconds = float(period_str[:-1]) * 60
            elif period_str.endswith("s"):
                total_seconds = float(period_str[:-1])
            else:
                total_seconds = float(period_str)
        except Exception:
            await message.answer("❌ Ошибка в формате времени")
            return

        interval = total_seconds / actual_count if actual_count > 0 else 0

        action_text = "подписка" if action == "subscribe" else "отписка"
        progress_msg = await message.answer(
            f"🚀 Запущена {action_text}\n"
            f"🔗 {link}\n"
            f"👥 {actual_count} аккаунтов\n"
            f"⏱ {period}\n\n"
            f"📊 Прогресс: 0/{actual_count}\n"
            f"✅ Успешно: 0\n"
            f"❌ Ошибок: 0\n\n"
            f"🛑 Для остановки используйте кнопку 'Остановить операцию'"
        )

        success, fail = 0, 0

        for i, acc in enumerate(accounts[:actual_count], 1):
            if self.stop_flags.get(user_id, False):
                await message.answer(f"🛑 Операция остановлена\nОбработано: {i - 1} аккаунтов")
                break

            client = acc["client"]
            phone = acc["phone"]

            try:
                if not client.is_connected:
                    await client.connect()

                # Очищаем ссылку для обработки
                link_clean = link.replace("https://t.me/", "").replace("@", "").strip()
                is_invite = link_clean.startswith("+")

                if action == "subscribe":
                    try:
                        chat = None
                        if is_invite:
                            # Для инвайт-ссылок
                            chat = await client.join_chat(link)
                        else:
                            # Для публичных каналов/чатов
                            chat = await client.join_chat(link_clean)

                        success += 1

                        # Сохраняем информацию о чате (ссылка + ID)
                        subs.setdefault(phone, [])
                        chat_info = {
                            "link": link,
                            "chat_id": chat.id,
                            "title": getattr(chat, 'title', 'Unknown'),
                            "username": getattr(chat, 'username', None),
                            "joined_at": datetime.now().isoformat()
                        }

                        # Проверяем, нет ли уже такой записи
                        existing_chat = next(
                            (c for c in subs[phone] if c.get("link") == link or c.get("chat_id") == chat.id), None)
                        if not existing_chat:
                            subs[phone].append(chat_info)
                        else:
                            # Обновляем существующую запись
                            existing_chat.update(chat_info)

                        self.save_subs(subs)
                        print(f"✅ {phone}: подписан на {chat.id} ({getattr(chat, 'title', 'Unknown')})")

                    except UserAlreadyParticipant:
                        # Если уже участник, получаем информацию о чате
                        try:
                            chat = await client.get_chat(link_clean)
                            success += 1

                            # Сохраняем информацию о чате
                            subs.setdefault(phone, [])
                            chat_info = {
                                "link": link,
                                "chat_id": chat.id,
                                "title": getattr(chat, 'title', 'Unknown'),
                                "username": getattr(chat, 'username', None),
                                "already_member": True
                            }

                            existing_chat = next(
                                (c for c in subs[phone] if c.get("link") == link or c.get("chat_id") == chat.id), None)
                            if not existing_chat:
                                subs[phone].append(chat_info)
                            else:
                                existing_chat.update(chat_info)

                            self.save_subs(subs)
                            print(f"ℹ️ {phone}: уже участник {chat.id} ({getattr(chat, 'title', 'Unknown')})")
                        except Exception as e:
                            print(f"⚠️ {phone}: не удалось получить информацию о чате: {e}")
                            success += 1

                    except (UsernameNotOccupied, ChannelInvalid, ChannelPrivate) as e:
                        print(f"❌ {phone}: чат не существует или приватный - {e}")
                        fail += 1
                    except (InviteHashInvalid, InviteHashExpired) as e:
                        print(f"❌ {phone}: неверная или устаревшая инвайт-ссылка - {e}")
                        fail += 1
                    except InviteRequestSent:
                        print(f"⚠️ {phone}: запрос на вступление отправлен")
                        success += 1
                    except Exception as e:
                        print(f"❌ {phone}: {e}")
                        fail += 1

                elif action == "unsubscribe":
                    try:
                        # Ищем сохраненный chat_id для этой ссылки
                        chat_id = None
                        chat_info = None

                        if phone in subs:
                            for sub in subs[phone]:
                                if sub.get("link") == link:
                                    chat_id = sub.get("chat_id")
                                    chat_info = sub
                                    break

                        if chat_id:
                            # Пробуем отписаться по chat_id
                            await client.leave_chat(chat_id)
                            success += 1
                            print(f"✅ {phone}: отписан от {chat_id}")

                            # Удаляем из подписок
                            if phone in subs and chat_info in subs[phone]:
                                subs[phone].remove(chat_info)
                                self.save_subs(subs)

                        else:
                            # Если chat_id нет, пробуем отписаться по ссылке
                            link_clean = link.replace("https://t.me/", "").replace("@", "").strip()
                            is_invite = link_clean.startswith("+")

                            try:
                                if is_invite:
                                    # Для инвайт-ссылок получаем чат
                                    chat = await client.join_chat(link_clean)
                                    await client.leave_chat(chat.id)
                                else:
                                    # Для публичных каналов/чатов
                                    await client.leave_chat(link_clean)
                                success += 1
                            except (UsernameNotOccupied, ChannelInvalid, ChannelPrivate) as e:
                                # Если чат не существует, считаем успешной отпиской
                                print(f"ℹ️ {phone}: чат не существует или недоступен - {e}")
                                success += 1

                            # Удаляем из подписок в любом случае
                            if phone in subs:
                                subs[phone] = [sub for sub in subs[phone] if sub.get("link") != link]
                                self.save_subs(subs)

                    except UserNotParticipant:
                        # Если не участник, считаем успешной отпиской и удаляем запись
                        print(f"ℹ️ {phone}: не участник чата")
                        success += 1
                        if phone in subs:
                            subs[phone] = [sub for sub in subs[phone] if sub.get("link") != link]
                            self.save_subs(subs)
                    except Exception as e:
                        print(f"❌ {phone}: {e}")
                        fail += 1

            except Exception as e:
                fail += 1
                print(f"❌ {phone}: {e}")

            # Обновляем прогресс каждые 5 аккаунтов или на последнем
            if i % 5 == 0 or i == actual_count:
                await progress_msg.edit_text(
                    f"🚀 Выполняется {action_text}\n"
                    f"🔗 {link}\n"
                    f"👥 {actual_count} аккаунтов\n\n"
                    f"📊 Прогресс: {i}/{actual_count}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {fail}\n\n"
                    f"🛑 Для остановки используйте кнопку 'Остановить операцию'"
                )

            if not self.stop_flags.get(user_id, False) and interval > 0:
                await asyncio.sleep(interval)

        self.stop_flags.pop(user_id, None)

        if not self.stop_flags.get(user_id, False):
            await progress_msg.edit_text(
                f"📊 {action_text.capitalize()} завершена!\n\n"
                f"✅ Успешно: {success}\n"
                f"❌ Ошибок: {fail}\n"
                f"🔗 Цель: {link}"
            )

    # === Статистика подписок ===
    async def show_stats(self, message: types.Message):
        subs = self.load_subs()
        if not subs:
            await message.answer("📭 Нет данных о подписках", reply_markup=self.get_main_keyboard())
            return

        text = "📊 *Статистика подписок:*\n\n"
        for phone, channels in subs.items():
            channel_list = []
            for channel in channels[:4]:  # Показываем только первые 4
                title = channel.get('title', 'Unknown')
                username = channel.get('username')
                if username:
                    channel_list.append(f"{title} (@{username})")
                else:
                    channel_list.append(f"{title} (ID: {channel.get('chat_id', '?')})")

            display_list = ", ".join(channel_list)
            if len(channels) > 4:
                display_list += f" ... (+{len(channels) - 4})"

            text += f"• `{phone}` → {display_list if channels else '—'}\n"

        await message.answer(text, parse_mode="Markdown", reply_markup=self.get_main_keyboard())

    # === Рассылка ===
    async def start_broadcast(self, message: types.Message, state: FSMContext):
        await message.answer(
            "✉️ Введите сообщение для рассылки:",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(BroadcastStates.text)

    async def process_broadcast_text(self, message: types.Message, state: FSMContext):
        await state.update_data(text=message.text)
        await message.answer(
            "🎯 Введите цель рассылки (юзернейм или ID чата):",
            reply_markup=self.get_cancel_keyboard()
        )
        await state.set_state(BroadcastStates.target)

    async def process_broadcast_target(self, message: types.Message, state: FSMContext):
        target = message.text.strip()
        data = await state.get_data()
        text = data["text"]

        builder = InlineKeyboardBuilder()
        builder.add(types.InlineKeyboardButton(
            text="✅ Начать рассылку",
            callback_data=f"broadcast|{target}"
        ))
        builder.add(types.InlineKeyboardButton(
            text="❌ Отмена",
            callback_data="cancel_broadcast"
        ))

        await message.answer(
            f"📋 Подтвердите рассылку:\n\n"
            f"• Цель: {target}\n"
            f"• Сообщение: {text[:100]}{'...' if len(text) > 100 else ''}\n"
            f"• Аккаунтов: {len(self.accounts)}\n\n"
            f"⚠️ Для остановки используйте кнопку '🛑 Остановить операцию'",
            reply_markup=builder.as_markup()
        )
        await state.update_data(target=target)

    async def handle_broadcast_callback(self, callback: types.CallbackQuery, state: FSMContext):
        if callback.data == "cancel_broadcast":
            await callback.message.edit_text("❌ Рассылка отменена")
            await state.clear()
            return

        if callback.data.startswith("broadcast|"):
            try:
                target = callback.data.split("|")[1]
                data = await state.get_data()
                text = data["text"]

                await callback.message.edit_text("🚀 Запуск рассылки...")
                await self.execute_broadcast(callback.message, text, target)
                await state.clear()
            except Exception as e:
                await callback.answer(f"❌ Ошибка: {str(e)}")

    async def execute_broadcast(self, message, text, target):
        user_id = message.from_user.id
        self.stop_flags[user_id] = False

        if not self.accounts:
            await message.answer("❌ Нет аккаунтов для рассылки")
            return

        progress_msg = await message.answer(
            f"📤 Начата рассылка\n"
            f"🎯 Цель: {target}\n"
            f"👥 Аккаунтов: {len(self.accounts)}\n\n"
            f"📊 Прогресс: 0/{len(self.accounts)}\n"
            f"✅ Успешно: 0\n"
            f"❌ Ошибок: 0\n\n"
            f"🛑 Для остановки используйте кнопку 'Остановить операцию'"
        )

        success, fail = 0, 0

        for i, acc in enumerate(self.accounts.values(), 1):
            if self.stop_flags.get(user_id, False):
                await message.answer(f"🛑 Рассылка остановлена\nОбработано: {i - 1} аккаунтов")
                break

            client = acc["client"]
            try:
                if not client.is_connected:
                    await client.connect()
                await client.send_message(target, text)
                success += 1
            except Exception as e:
                fail += 1
                print(f"❌ Ошибка рассылки для {acc['phone']}: {e}")

            # Обновляем прогресс каждые 5 аккаунтов или на последнем
            if i % 5 == 0 or i == len(self.accounts):
                await progress_msg.edit_text(
                    f"📤 Идет рассылка\n"
                    f"🎯 Цель: {target}\n"
                    f"👥 Аккаунтов: {len(self.accounts)}\n\n"
                    f"📊 Прогресс: {i}/{len(self.accounts)}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {fail}\n\n"
                    f"🛑 Для остановки используйте кнопку 'Остановить операцию'"
                )

        self.stop_flags.pop(user_id, None)

        if not self.stop_flags.get(user_id, False):
            await progress_msg.edit_text(
                f"📊 Рассылка завершена!\n\n"
                f"✅ Успешно: {success}\n"
                f"❌ Ошибок: {fail}\n"
                f"🎯 Цель: {target}"
            )

    # === Остановка операций ===
    async def stop_operation(self, message: types.Message):
        user_id = message.from_user.id
        if user_id in self.stop_flags:
            self.stop_flags[user_id] = True
            await message.answer("🛑 Запрос на остановку отправлен...")
        else:
            await message.answer("❌ Нет активных операций для остановки")

    # === Настройка хэндлеров ===
    def setup_handlers(self):
        # Основные команды
        self.dp.message.register(self.cmd_start, Command("start"))
        self.dp.message.register(self.cmd_cancel, F.text == "❌ Отмена")
        self.dp.message.register(self.cmd_cancel, Command("cancel"))

        # Обработчики кнопок главного меню
        self.dp.message.register(self.show_status, F.text == "📊 Статус аккаунтов")
        self.dp.message.register(self.show_stats, F.text == "📈 Статистика подписок")
        self.dp.message.register(self.start_auth, F.text == "👤 Добавить аккаунт")
        self.dp.message.register(self.stop_operation, F.text == "🛑 Остановить операцию")

        # Массовые действия
        self.dp.message.register(self.start_mass_subscribe, F.text == "📢 Массовая подписка")
        self.dp.message.register(self.start_mass_unsubscribe, F.text == "❌ Массовая отписка")

        # Рассылка
        self.dp.message.register(self.start_broadcast, F.text == "✉️ Рассылка")

        # FSM хэндлеры для авторизации
        self.dp.message.register(self.process_phone, AuthStates.phone)
        self.dp.message.register(self.process_code, AuthStates.code)
        self.dp.message.register(self.process_password, AuthStates.password)

        # FSM хэндлеры для массовых действий
        self.dp.message.register(self.process_mass_link, MassActionStates.link)
        self.dp.message.register(self.process_mass_count, MassActionStates.count)
        self.dp.message.register(self.process_mass_time, MassActionStates.time)

        # FSM хэндлеры для рассылки
        self.dp.message.register(self.process_broadcast_text, BroadcastStates.text)
        self.dp.message.register(self.process_broadcast_target, BroadcastStates.target)

        # Callback хэндлеры
        self.dp.callback_query.register(self.handle_mass_action_callback,
                                        F.data.startswith(("mass_action|", "cancel_mass")))
        self.dp.callback_query.register(self.handle_broadcast_callback,
                                        F.data.startswith(("broadcast|", "cancel_broadcast")))

    # === Имитация онлайна ===
    async def simulate_human_activity(self):
        while True:
            await asyncio.sleep(random.randint(60, 300))  # Случайный интервал 1-3 минуты

            if not self.accounts:
                continue

            acc = random.choice(list(self.accounts.values()))
            phone = acc["phone"]
            client = acc["client"]

            try:
                # Проверяем подключение
                if not client.is_connected:
                    await client.connect()
                    print(f"✅ {phone}: переподключен")

                # Просто отправляем сообщение себе (в Saved Messages)
                await client.send_message("me", "💭")
                print(f"💬 {phone}: активность (отправлено себе)")

            except Exception as e:
                print(f"❌ {phone}: ошибка активности - {e}")
                try:
                    # Пытаемся переподключиться при ошибке
                    if client.is_connected:
                        await client.disconnect()
                    await client.connect()
                    print(f"🔁 {phone}: переподключен после ошибки")
                except Exception as reconnect_error:
                    print(f"🚫 {phone}: критическая ошибка подключения - {reconnect_error}")

    async def run(self):
        # Подключаем все аккаунты при старте
        for acc in self.accounts.values():
            try:
                if not acc["client"].is_connected:
                    await acc["client"].connect()
                    print(f"✅ Подключен: {acc['phone']}")
            except Exception as e:
                print(f"❌ Ошибка подключения {acc['phone']}: {e}")
            await asyncio.sleep(1)

        await asyncio.gather(
            self.dp.start_polling(self.bot),
            self.simulate_human_activity()
        )


