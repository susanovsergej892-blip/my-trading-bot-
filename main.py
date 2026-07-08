import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

TELEGRAM_TOKEN = "8348902868:AAHNy5i2l3VYrU-tlkX9SoCIBaAe1nYYjkI"
OPENROUTER_API_KEY = "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"

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("Бро, бот в строю и готов к работе!")

@dp.message()
async def chat(message: types.Message):
    await message.answer("Думаю...")
    async with aiohttp.ClientSession() as session:
        payload = {
            "model": "meta-llama/llama-3-8b-instruct:free",
            "messages": [{"role": "user", "content": message.text}]
        }
        headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
        async with session.post("https://openrouter.ai/api/v1/chat/completions", json=payload, headers=headers) as resp:
            data = await resp.json()
            ans = data["choices"][0]["message"]["content"]
            await message.answer(ans)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
