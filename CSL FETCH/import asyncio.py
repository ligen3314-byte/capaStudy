import asyncio
from playwright.async_api import async_playwright
# 需要先安装：pip install playwright-stealth
from playwright_stealth import stealth_async

async def run_cosco():
    async with async_playwright() as p:
        # 建议开启 headless=False 先观察。成功后可改为 True。
        browser = await p.chromium.launch(headless=False)
        
        # 模拟一个非常真实的浏览器上下文
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        
        page = await context.new_page()

        # 【关键】应用隐身插件，抹除 Playwright 特征
        await stealth_async(page)

        # 访问目标 URL
        url = "https://elines.coscoshipping.com/ebusiness/sailingSchedule/searchByService"
        
        try:
            print(f"正在尝试访问: {url}")
            # 使用 networkidle 等待网络空闲，确保 Token 和 Cookie 加载完毕
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            # 检查是否出现了验证码或者报错
            print("页面已加载，当前标题:", await page.title())
            
            # 在这里可以继续你的自动化填表逻辑...
            
        except Exception as e:
            print(f"访问失败: {e}")
        
        # 为了调试，先不关闭
        await asyncio.sleep(30)
        await browser.close()

asyncio.run(run_cosco())