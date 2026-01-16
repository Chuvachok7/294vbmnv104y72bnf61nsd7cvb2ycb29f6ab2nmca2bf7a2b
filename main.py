import os
import re
import ast
import sys
import uuid
import html
import asyncio
import tempfile
import traceback
import subprocess
from os import getenv
from dotenv import load_dotenv
from time import perf_counter_ns
from aiogram.filters import Filter
from aiogram import Bot, Dispatcher, F
from dotenv import find_dotenv, load_dotenv
from datetime import timedelta, datetime, timezone
from aiogram.client.default import DefaultBotProperties
from aiogram.filters.logic import invert_f, and_f, or_f
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

load_dotenv(find_dotenv())
API_TOKEN = getenv('BOT_TOKEN')
if not API_TOKEN:
    print("Ошибка: BOT_TOKEN не найден в .env")
    sys.exit(1)

ALLOWED_CHATS = [-1003569460645, -1002264373639, 5235155365]
CLEAN_TEXT = False

def escape_html(text: str, /) -> str:
    return html.escape(str(text))

def unescape_html(text: str, /) -> str:
    return str(text).replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')

def clean_text(text: str, /) -> str:
    if not CLEAN_TEXT:
        return text
    cwd = os.getcwd()
    text = str(text).replace(cwd, 'Bot')
    if os.sep == '\\':
        text = text.replace(cwd.replace('\\', '/'), 'Bot')
    else:
        text = text.replace(cwd.replace('/', '\\'), 'Bot')
    return text

BOT_USERNAME = None

def _split_first_line_and_tail(text: str, prefixes: str = './!') -> tuple[str, list[str]]:
    if text is None:
        return '', []
    text = text.lstrip(prefixes).rstrip()
    if text == '':
        return '', []
    lines = text.split('\n')
    first_line = lines[0].strip()
    tail = lines[1:]
    return first_line, tail

def parse_first_line(text: str, prefixes: str = './!') -> tuple[str, str | None, str, list[str]]:
    if text is None:
        return '', None, '', []
    text = text.lstrip(prefixes).strip()
    if text == '':
        return '', None, '', []
    lines = text.split('\n')
    first_line = lines[0].strip()
    m = re.match(r'^(\S+)(?:\s+(.*))?$', first_line)
    if not m:
        return '', None, '', lines[1:]
    token, same_line_rest = m.groups()
    same_line_args = same_line_rest or ''
    if '@' in token:
        before, after = token.split('@', 1)
        command_token = before
        bot_username = after
    else:
        command_token = token
        bot_username = None
    return command_token.lower(), (bot_username.lower() if bot_username else None), same_line_args, lines[1:]

def match_command_in_first_line(first_line: str, commands: list[str]) -> tuple[str | None, str | None, str]:
    if not first_line:
        return None, None, ''
    low = first_line.lower()
    best = None
    for cmd in commands:
        cmd_l = cmd.lower().strip()
        if not cmd_l:
            continue
        words = cmd_l.split()
        if len(words) == 1:
            pattern = r'^' + re.escape(words[0]) + r'(?:@(?P<uname>\S+))?(?:\s+(?P<args>.*))?$'
        else:
            prefix = r'\s+'.join(re.escape(w) for w in words[:-1])
            last = re.escape(words[-1])
            pattern = r'^' + prefix + r'\s+' + last + r'(?:@(?P<uname>\S+))?(?:\s+(?P<args>.*))?$'
        m = re.match(pattern, low)
        if m:
            args = m.group('args') or ''
            uname = m.group('uname') or None
            cmd_length = len(cmd_l)
            if best is None or cmd_length > best[3]:
                best = (cmd_l, uname, args, cmd_length)
    if best:
        return best[0], (best[1].lower() if best[1] else None), best[2]
    return None, None, ''

class CustomFilter(Filter):
    async def __call__(self, msg: Message) -> bool:
        raise NotImplementedError
    def __and__(self, other):
        return and_f(self, other)
    def __or__(self, other):
        return or_f(self, other)
    def __invert__(self):
        return invert_f(self)

class CWithArgsMultiline(CustomFilter):
    def __init__(self, *cmds: str, prefixes: str = './!'):
        self.commands: list[str] = [c.lower().strip() for c in cmds]
        self.prefixes = prefixes

    async def __call__(self, msg: Message) -> bool:
        if not msg.text:
            return False
        first_line, tail = _split_first_line_and_tail(msg.text, self.prefixes)
        cmd_token, bot_username, same_line_args = match_command_in_first_line(first_line, self.commands)
        if cmd_token is None:
            cmd_token, bot_username, same_line_args, tail2 = parse_first_line(msg.text, self.prefixes)
        if bot_username and BOT_USERNAME and bot_username != BOT_USERNAME.lower():
            return False
        return cmd_token in self.commands

class AllowedChats(CustomFilter):
    async def __call__(self, d: Message | CallbackQuery) -> bool:
        return d.chat.id in ALLOWED_CHATS

# --- Локальная реализация meval ---
# We dont modify locals VVVV ; this lets us keep the message available to the user-provided function
async def meval(code, globs, **kwargs):
    # This function is released in the public domain. Feel free to kang it (although I like credit)
    # Note to self: please don't set globals here as they will be lost.
    # Don't clutter locals
    locs = {}
    # Restore globals later
    globs = globs.copy()
    # This code saves __name__ and __package into a kwarg passed to the function.
    # It is set before the users code runs to make sure relative imports work
    global_args = "_globs"
    while global_args in globs.keys():
        # Make sure there's no name collision, just keep prepending _s
        global_args = "_" + global_args
    kwargs[global_args] = {}
    for glob in ["__name__", "__package__"]:
        # Copy data to args we are sending
        kwargs[global_args][glob] = globs[glob]

    root = ast.parse(code, "exec")
    code = root.body

    ret_name = "_ret"
    ok = False
    while True:
        if ret_name in globs.keys():
            ret_name = "_" + ret_name
            continue
        for node in ast.walk(root):
            if isinstance(node, ast.Name) and node.id == ret_name:
                ret_name = "_" + ret_name
                break
            ok = True
        if ok:
            break

    if not code:
        return None

    if not any(isinstance(node, ast.Return) for node in code):
        for i in range(len(code)):
            if isinstance(code[i], ast.Expr):
                if i == len(code) - 1 or not isinstance(code[i].value, ast.Call):
                    code[i] = ast.copy_location(ast.Expr(ast.Call(func=ast.Attribute(value=ast.Name(id=ret_name,
                                                                                                    ctx=ast.Load()),
                                                                                     attr="append", ctx=ast.Load()),
                                                                  args=[code[i].value], keywords=[])), code[-1])
    else:
        for node in code:
            if isinstance(node, ast.Return):
                node.value = ast.List(elts=[node.value], ctx=ast.Load())

    code.append(ast.copy_location(ast.Return(value=ast.Name(id=ret_name, ctx=ast.Load())), code[-1]))

    # globals().update(**<global_args>)
    glob_copy = ast.Expr(ast.Call(func=ast.Attribute(value=ast.Call(func=ast.Name(id="globals", ctx=ast.Load()),
                                                                    args=[], keywords=[]),
                                                     attr="update", ctx=ast.Load()),
                                  args=[], keywords=[ast.keyword(arg=None,
                                                                 value=ast.Name(id=global_args, ctx=ast.Load()))]))
    ast.fix_missing_locations(glob_copy)
    code.insert(0, glob_copy)
    ret_decl = ast.Assign(targets=[ast.Name(id=ret_name, ctx=ast.Store())], value=ast.List(elts=[], ctx=ast.Load()))
    ast.fix_missing_locations(ret_decl)
    code.insert(1, ret_decl)
    args = []
    for a in list(map(lambda x: ast.arg(x, None), kwargs.keys())):
        ast.fix_missing_locations(a)
        args += [a]
    args = ast.arguments(args=[], vararg=None, kwonlyargs=args, kwarg=None, defaults=[],
                         kw_defaults=[None for i in range(len(args))])
    args.posonlyargs = []
    fun = ast.AsyncFunctionDef(name="tmp", args=args, body=code, decorator_list=[], returns=None)
    ast.fix_missing_locations(fun)
    mod = ast.parse("")
    mod.body = [fun]
    comp = compile(mod, "<string>", "exec")

    exec(comp, {}, locs)

    r = await locs["tmp"](**kwargs)
    for i in range(len(r)):
        if hasattr(r[i], "__await__"):
            r[i] = await r[i]  # workaround for 3.5
    i = 0
    while i < len(r) - 1:
        if r[i] is None:
            del r[i]
        else:
            i += 1
    if len(r) == 1:
        [r] = r
    elif not r:
        r = None
    return r

bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode='HTML', link_preview_is_disabled=True))
dp = Dispatcher()

EVAL_TASKS: dict[str, dict] = {}

STRINGS = {
    'code': '<b>💻 Код:</b>\n<pre><code class="language-Python">{}</code></pre>\n\n',
    'eval': '<b>✅ Результат:</b>\n<pre><code class="language-Python">{}</code></pre>',
    'err': '<b>🚫 Ошибка:</b>\n{}',
    'spent': '\n\n<b>⌛️ Затрачено:</b> {}',
    'code_stop_executed': '<b>❌ Выполнение остановлено</b>',
    'code_stop_executed_btn': '❌ Остановить выполнение',
    'too_long': '📁 <b>Вывод результата выполнения слишком длинный, поэтому он отправлен файлом.</b>',
    'code_executing': '<b>💻 Код выполняется...</b>'
}

def parse_flags(text: str):
    if not text:
        return '', False, False

    txt = text.lstrip('./! ')
    mcmd = re.match(r'^(e|eval)(?:@\w+)?\b', txt, flags=re.IGNORECASE)
    if not mcmd:
        return text, False, False

    rest = txt[mcmd.end():].lstrip(' ')
    first, sep, after = rest.partition('\n')
    toks = first.split()

    flags = []
    i = 0
    while i < len(toks) and toks[i].startswith('-'):
        flags.append(toks[i])
        i += 1

    code = (' '.join(toks[i:]) + (('\n' + after) if sep else '')) if i < len(toks) else (after if sep else '')

    nf = set()
    for f in flags:
        if len(f) > 2:
            nf.update('-' + c for c in f[1:])
        else:
            nf.add(f)

    show_code = '-v' in nf
    not_clean = '-f' in nf

    code = code.strip() if code else ''
    return code, show_code, not_clean


def _format_traceback(exc_type, exc_value, tb, code: str) -> str:
    # Special handling for SyntaxError (and its subclasses like IndentationError)
    if issubclass(exc_type, SyntaxError):
        # Check if the error is in the user's code (filename 'exec' comes from ast.parse(..., "exec"))
        if exc_value.filename in ('<string>', 'exec'):
            ln = exc_value.lineno
            line_content = exc_value.text if exc_value.text else ''
            
            # Fallback if text is not available in exception
            if not line_content and ln is not None:
                lines = code.splitlines()
                if 0 <= ln - 1 < len(lines):
                    line_content = lines[ln - 1]

            result = []
            result.append(
                f'👉 "Input Code", line {ln}, in module\n'
                f'    {escape_html(line_content.strip())}'
            )
            result.append(f'🚫 {escape_html(exc_type.__name__)}: {escape_html(exc_value.msg)}')
            return '<pre><code class="language-Python">' + '\n'.join(result) + '</code></pre>'

    result = ['Traceback (most recent call last):']
    frames = traceback.extract_tb(tb)

    for frame in frames:
        # Скрываем внутренний вызов meval, ТОЛЬКО если это не пользовательский код (<string>)
        # Если пользователь назовет свою функцию meval, она будет в <string> и мы её покажем.
        if frame.name == 'meval' and frame.filename != '<string>':
            continue

        # Также можно скрыть обертку запуска, если нужно (опционально)
        if frame.name == '_execute_eval' and frame.filename != '<string>':
            continue
        
        # Skip internal ast.py frames (just in case)
        if 'ast.py' in frame.filename:
             continue

        if frame.filename == '<string>':
            lines = code.splitlines()
            ln = frame.lineno
            line_content = lines[ln - 1] if 0 <= ln - 1 < len(lines) else '<no source in Input Code>'
            result.append(
                f'👉 "Input Code", line {ln}, in {escape_html(frame.name)}\n'
                f'    {escape_html(line_content.strip())}'
            )
            continue

        filename = frame.filename
        line = frame.line if frame.line else '<no source>'
        result.append(
            f'👉 "{escape_html(filename)}", line {frame.lineno}, in {escape_html(frame.name)}\n'
            f'    {escape_html(line.strip())}'
        )

    exc_name = exc_type.__name__
    exc_msg = str(exc_value)
    result.append(f'🚫 {escape_html(exc_name)}: {escape_html(exc_msg)}')

    return '<pre><code class="language-Python">' + '\n'.join(result) + '</code></pre>'


async def _execute_eval(task_id: str):
    task_info = EVAL_TASKS[task_id]
    m = task_info['message']
    _m_ = task_info['status_message']

    code, show_code, not_clean = parse_flags(m.text)
    code = code or 'None'

    result: str | None = None
    error_msg: str | None = None

    _start_ = perf_counter_ns()
    try:
        result = await meval(
            code,
            globals(),
            **{
                'm': m,
                'msg': m,
                'message': m,
                'reply': m.reply_to_message,
                'bot': bot,
                'dp': dp,
            }
        )
        _end_ = perf_counter_ns()
    except asyncio.CancelledError:
        return
    except Exception:
        _end_ = perf_counter_ns()
        exc_type, exc_value, exc_tb = sys.exc_info()
        error_msg = _format_traceback(exc_type, exc_value, exc_tb, code)
    finally:
        EVAL_TASKS.pop(task_id, None)

    spent_val = f'{timedelta(seconds=round((_end_ - _start_) / 1e9, 3))}'[:-3]
    spent_str = STRINGS['spent'].format(spent_val)

    if error_msg:
        msg_body = STRINGS['err'].format(error_msg)
    else:
        msg_body = STRINGS['eval'].format(escape_html(result))

    full_msg = (STRINGS['code'].format(escape_html(code)) if show_code else '') + msg_body + spent_str

    if not not_clean:
        full_msg = clean_text(full_msg)

    if len(full_msg) < 4096:
        await _m_.edit_text(full_msg)
    else:
        await _m_.delete()
        await m.answer_document(
            BufferedInputFile(unescape_html(full_msg).encode(), 'result.txt'),
            caption=STRINGS['too_long'] + spent_str
        )

@dp.message(CWithArgsMultiline('e', 'eval'), AllowedChats())
async def _eval(m: Message):
    task_id = str(uuid.uuid4())
    _m_ = await m.answer(
        STRINGS['code_executing'],
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=STRINGS['code_stop_executed_btn'],
                callback_data=f'cancel_eval:{task_id}'
            )
        ]])
    )
    EVAL_TASKS[task_id] = {
        'message': m,
        'status_message': _m_,
        'start_time': perf_counter_ns(),
        'task': asyncio.create_task(_execute_eval(task_id))
    }


@dp.callback_query(F.data.startswith('cancel_eval:'), AllowedChats())
async def _cancel_eval(call: CallbackQuery):
    task_id = call.data.split(':')[1]
    if task_info := EVAL_TASKS.get(task_id):
        if task := task_info.get('task'):
            task.cancel()

        code, show_code, not_clean = parse_flags(task_info['message'].text)
        spent_val = f'{timedelta(seconds=round((perf_counter_ns() - task_info["start_time"]) / 1e9, 3))}'[:-3]
        spent_str = STRINGS['spent'].format(spent_val)

        await call.message.edit_text(
            (STRINGS['code'].format(escape_html(code)) if show_code else '') +
            STRINGS['code_stop_executed'] + spent_str
        )
        EVAL_TASKS.pop(task_id, None)
    await call.answer()


@dp.message(CWithArgsMultiline('ssh'), AllowedChats())
async def _ssh(m: Message):
    cmd = m.text.replace("/ssh", "", 1).strip()
    if not cmd:
        return await m.answer("Укажите команду")
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await proc.communicate()
        output = (stdout + stderr).decode(errors="ignore").strip()

        if not output:
            output = "[пустой вывод]"

        # Если вывод помещается в сообщение
        if len(output) <= 3900:
            await m.answer(
                f"<blockquote><pre>{output}</pre></blockquote>"
            )
        else:
            # Отправка как файл из памяти
            file = BufferedInputFile(
                output.encode("utf-8", errors="ignore"),
                filename="output.txt"
            )

            await m.answer_document(
                file,
                caption="📄 Вывод слишком большой, отправляю файлом"
            )

    except Exception as e:
        await m.answer(f"❌ Ошибка:\n<pre>{e}</pre>")


async def main():
    global BOT_USERNAME

    me = await bot.get_me()
    BOT_USERNAME = me.username
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        print('Bot started')
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot stopped')
