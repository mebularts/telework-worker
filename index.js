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
            console.log(`Navigating to ${source.name} homepage...`);
            await page.goto(source.url, { waitUntil: 'networkidle', timeout: 45000 });

            try {
                await page.waitForSelector(source.containerSelector, { timeout: 15000 });
            } catch (e) {
                const title = await page.title();
                const content = await page.content();
                const isCloudflare = content.includes('cloudflare') || content.includes('ray-id') || title.includes('Attention Required');

                console.warn(`[${source.name}] Timeout! Page Title: "${title}"`);
                if (isCloudflare) {
                    console.error(`[${source.name}] 🛡️ Cloudflare detected! GitHub Actions IP might be blocked.`);
                } else {
                    console.warn(`[${source.name}] Selector "${source.containerSelector}" not found, but Cloudflare not explicitly detected.`);
                }

                // Save screenshot and HTML for debugging
                const safeName = source.name.replace(/[^a-z0-0]/gi, '_').toLowerCase();
                await page.screenshot({ path: `error_${safeName}.png`, fullPage: true });
                require('fs').writeFileSync(`error_${safeName}.html`, content);
                console.log(`[${source.name}] Debug artifacts saved (error_${safeName}.png/html)`);
            }

            // R10 ve WMAraci ana sayfadaki son konuları çek
            const threads = await page.evaluate((s) => {
                const results = [];
                const nodes = document.querySelectorAll(s.threadSelector);

                nodes.forEach(el => {
                    const title = el.innerText.trim();
                    const url = el.href;

                    if (title && url) {
                        // Ensure it's a forum link
                        if (url.includes('.html') || url.includes('/forum/') || url.includes('thread') || url.includes('konu')) {
                            if (!results.find(r => r.url === url)) {
                                results.push({ title, url });
                            }
                        }
                    }
                });
                return results.slice(0, 40);
            }, source);

            console.log(`Found ${threads.length} topics on ${source.name} homepage.`);

            if (threads.length > 0) {
                await axios.post(WEBHOOK_URL, {
                    type: 'external_crawl',
                    token: SCRAPER_TOKEN,
                    source: source.name,
                    data: threads
                });
                console.log(`Pushed data for ${source.name}`);
            }

        } catch (error) {
            console.error(`Error on ${source.name}:`, error.message);
            await page.screenshot({ path: `critical_error_${source.name}.png` });
        }
    }

    await browser.close();
    console.log("Scrape finished.");
}

scrape();
