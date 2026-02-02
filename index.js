const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const axios = require('axios');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

/**
 * HIZLI MOD: Ana sayfadaki "Son Konular" bloklarını hedefliyoruz.
 */
const SOURCES = [
    {
        name: 'r10',
        url: 'https://www.r10.net/',
        containerSelector: '#tab-sonAcilan .list ul',
        threadSelector: '#tab-sonAcilan .list ul li a.h5'
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li:not(.h) strong a'
    }
];

async function scrape() {
    console.log(`[${new Date().toISOString()}] Starting high-frequency homepage scrape...`);
    console.log(`Current Working Directory: ${process.cwd()}`);

    if (!WEBHOOK_URL || !SCRAPER_TOKEN) {
        console.error("Missing WEBHOOK_URL or SCRAPER_TOKEN!");
        process.exit(1);
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    });

    const page = await context.newPage();

    for (const source of SOURCES) {
        try {
            console.log(`Navigating to ${source.name} homepage: ${source.url}`);
            await page.goto(source.url, { waitUntil: 'networkidle', timeout: 45000 });

            const safeName = source.name.replace(/[^a-z0-0]/gi, '_').toLowerCase();

            // Take a screenshot immediately after navigation
            await page.screenshot({ path: `nav_${safeName}.png`, fullPage: true });

            try {
                await page.waitForSelector(source.containerSelector, { timeout: 15000 });
            } catch (e) {
                const title = await page.title();
                const content = await page.content();
                const isCloudflare = content.includes('cloudflare') || content.includes('ray-id') || title.includes('Attention Required');

                console.warn(`[${source.name}] Timeout! Page Title: "${title}"`);
                if (isCloudflare) {
                    console.error(`[${source.name}] 🛡️ Cloudflare detected!`);
                } else {
                    console.warn(`[${source.name}] Selector "${source.containerSelector}" not found.`);
                }

                require('fs').writeFileSync(`error_${safeName}.html`, content);
                await page.screenshot({ path: `error_${safeName}.png`, fullPage: true });
                console.log(`[${source.name}] Error artifacts saved.`);
            }

            // R10 ve WMAraci ana sayfadaki son konuları çek
            const threads = await page.evaluate((s) => {
                const results = [];
                const nodes = document.querySelectorAll(s.threadSelector);

                nodes.forEach(el => {
                    const title = el.innerText.trim();
                    const url = el.href;

                    if (title && url) {
                        if (url.includes('.html') || url.includes('/forum/') || url.includes('thread') || url.includes('konu')) {
                            if (!results.find(r => r.url === url)) {
                                results.push({ title, url });
                            }
                        }
                    }
                });
                return results.slice(0, 40);
            }, source);

            console.log(`[${source.name}] Found ${threads.length} topics.`);

            if (threads.length > 0) {
                await axios.post(WEBHOOK_URL, {
                    type: 'external_crawl',
                    token: SCRAPER_TOKEN,
                    source: source.name,
                    data: threads
                });
                console.log(`[${source.name}] Pushed data to webhook.`);
            } else {
                // If 0 topics found, save HTML to see why
                const content = await page.content();
                require('fs').writeFileSync(`zero_${safeName}.html`, content);
                await page.screenshot({ path: `zero_${safeName}.png`, fullPage: true });
                console.log(`[${source.name}] 0 topics found. Zero-match artifacts saved.`);
            }

        } catch (error) {
            console.error(`[${source.name}] Critical Error:`, error.message);
            const safeName = source.name.replace(/[^a-z0-0]/gi, '_').toLowerCase();
            await page.screenshot({ path: `critical_${safeName}.png` });
        }
    }

    await browser.close();
    console.log("Scrape finished.");
}

scrape();
