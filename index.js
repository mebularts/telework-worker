const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const axios = require('axios');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

/**
 * Full Content Mode: Fetches thread content for AI processing.
 */
const SOURCES = [
    {
        name: 'r10',
        url: 'https://www.r10.net/',
        containerSelector: '#tab-sonAcilan .list ul',
        threadSelector: '#tab-sonAcilan .list ul li.thread .title a',
        contentSelector: '.postbody .content, .messageContent, .post-content, article .content'
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li.open span a[href*="/forum/"]',
        contentSelector: '.message-body, .postMessage, .post-content, article'
    }
];

async function fetchThreadContent(page, url, contentSelector) {
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

        const content = await page.evaluate((selector) => {
            const el = document.querySelector(selector);
            return el ? el.innerText.trim().substring(0, 2000) : null; // Limit to 2000 chars
        }, contentSelector);

        return content;
    } catch (e) {
        console.warn(`Failed to fetch content from ${url}: ${e.message}`);
        return null;
    }
}

async function scrape() {
    console.log(`[${new Date().toISOString()}] Starting full-content scrape...`);

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
            await page.screenshot({ path: `nav_${safeName}.png`, fullPage: true });

            try {
                await page.waitForSelector(source.containerSelector, { timeout: 15000 });
            } catch (e) {
                console.warn(`[${source.name}] Timeout waiting for container.`);
                const content = await page.content();
                require('fs').writeFileSync(`error_${safeName}.html`, content);
                await page.screenshot({ path: `error_${safeName}.png`, fullPage: true });
                continue;
            }

            // Get thread links from homepage
            const threads = await page.evaluate((s) => {
                const results = [];
                const nodes = document.querySelectorAll(s.threadSelector);

                nodes.forEach(el => {
                    const title = el.innerText.trim();
                    const url = el.href;

                    if (title && url && (url.includes('.html') || url.includes('/forum/') || url.includes('thread') || url.includes('konu'))) {
                        if (!results.find(r => r.url === url)) {
                            results.push({ title, url });
                        }
                    }
                });
                return results.slice(0, 20); // Limit to 20 threads for speed
            }, source);

            console.log(`[${source.name}] Found ${threads.length} topics. Fetching content...`);

            // Fetch content for each thread (concurrently, max 5 at a time)
            const enrichedThreads = [];
            for (let i = 0; i < threads.length; i += 5) {
                const batch = threads.slice(i, i + 5);
                const results = await Promise.all(batch.map(async (thread) => {
                    const content = await fetchThreadContent(page, thread.url, source.contentSelector);
                    return { ...thread, original_content: content };
                }));
                enrichedThreads.push(...results);
            }

            const validThreads = enrichedThreads.filter(t => t.original_content);
            console.log(`[${source.name}] Successfully fetched content for ${validThreads.length} topics.`);

            if (validThreads.length > 0) {
                await axios.post(WEBHOOK_URL, {
                    type: 'external_crawl',
                    token: SCRAPER_TOKEN,
                    source: source.name,
                    data: validThreads
                });
                console.log(`[${source.name}] Pushed data to webhook.`);
            }

        } catch (error) {
            console.error(`[${source.name}] Critical Error:`, error.message);
            const safeName = source.name.replace(/[^a-z0-0]/gi, '_').toLowerCase();
            await page.screenshot({ path: `critical_${safeName}.png` });
        }
    }

    await browser.close();
    console.log("Full-content scrape finished.");
}

scrape();
