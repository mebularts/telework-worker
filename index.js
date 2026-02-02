const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const axios = require('axios');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

/**
 * Full Content Mode: Fetches thread content for AI processing.
 * Uses separate browser contexts for each thread to avoid ERR_ABORTED.
 */
const SOURCES = [
    {
        name: 'r10',
        url: 'https://www.r10.net/',
        containerSelector: '#tab-sonAcilan .list ul',
        threadSelector: '#tab-sonAcilan .list ul li.thread .title a',
        contentSelector: '.postbody .content, .messageContent, .post-content'
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li.open span a[href*="/forum/"]',
        contentSelector: '.message-body, .postMessage, .post-content'
    },
    {
        name: 'bhw',
        url: 'https://www.blackhatworld.com/forums/',
        containerSelector: '.block-body',
        threadSelector: '.structItem--thread .structItem-title a[data-preview-url]',
        contentSelector: '.message-body .bbWrapper, .message-content .bbWrapper'
    }
];

async function fetchThreadContent(context, url, contentSelector) {
    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 25000 });

        // Wait a bit for dynamic content
        await page.waitForTimeout(1000);

        const content = await page.evaluate((selector) => {
            const selectors = selector.split(', ');
            for (const sel of selectors) {
                const el = document.querySelector(sel.trim());
                if (el && el.innerText) {
                    return el.innerText.trim().substring(0, 2500);
                }
            }
            return null;
        }, contentSelector);

        return content;
    } catch (e) {
        console.warn(`Failed to fetch content from ${url}: ${e.message}`);
        return null;
    } finally {
        await page.close();
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

    for (const source of SOURCES) {
        const mainPage = await context.newPage();
        try {
            console.log(`Navigating to ${source.name} homepage: ${source.url}`);
            await mainPage.goto(source.url, { waitUntil: 'networkidle', timeout: 45000 });

            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `nav_${safeName}.png`, fullPage: false });

            try {
                await mainPage.waitForSelector(source.containerSelector, { timeout: 15000 });
            } catch (e) {
                console.warn(`[${source.name}] Timeout waiting for container.`);
                const content = await mainPage.content();
                require('fs').writeFileSync(`error_${safeName}.html`, content);
                await mainPage.screenshot({ path: `error_${safeName}.png`, fullPage: true });
                await mainPage.close();
                continue;
            }

            // Get thread links from homepage
            const threads = await mainPage.evaluate((s) => {
                const results = [];
                const nodes = document.querySelectorAll(s.threadSelector);

                nodes.forEach(el => {
                    const title = el.innerText.trim();
                    let url = el.href;

                    // Handle relative URLs
                    if (url && !url.startsWith('http')) {
                        url = new URL(url, window.location.origin).href;
                    }

                    if (title && url && (url.includes('.html') || url.includes('/forum/') || url.includes('thread') || url.includes('konu') || url.includes('/threads/'))) {
                        if (!results.find(r => r.url === url)) {
                            results.push({ title, url });
                        }
                    }
                });
                return results.slice(0, 15);
            }, source);

            console.log(`[${source.name}] Found ${threads.length} topics. Fetching content...`);
            await mainPage.close();

            // Fetch content for each thread using separate pages
            const enrichedThreads = [];
            for (const thread of threads) {
                // Add small delay between requests to avoid rate limiting
                await new Promise(resolve => setTimeout(resolve, 500));

                const content = await fetchThreadContent(context, thread.url, source.contentSelector);
                if (content) {
                    enrichedThreads.push({ ...thread, original_content: content });
                    console.log(`  ✓ Fetched: ${thread.title.substring(0, 50)}...`);
                } else {
                    // Still add the thread without content - PHP can try to fetch it
                    enrichedThreads.push({ ...thread, original_content: '' });
                    console.log(`  ✗ Failed: ${thread.title.substring(0, 50)}...`);
                }
            }

            const validCount = enrichedThreads.filter(t => t.original_content).length;
            console.log(`[${source.name}] Successfully fetched content for ${validCount}/${threads.length} topics.`);

            if (enrichedThreads.length > 0) {
                await axios.post(WEBHOOK_URL, {
                    type: 'external_crawl',
                    token: SCRAPER_TOKEN,
                    source: source.name,
                    data: enrichedThreads
                });
                console.log(`[${source.name}] Pushed ${enrichedThreads.length} items to webhook.`);
            }

        } catch (error) {
            console.error(`[${source.name}] Critical Error:`, error.message);
            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `critical_${safeName}.png` }).catch(() => { });
            await mainPage.close().catch(() => { });
        }
    }

    await browser.close();
    console.log("Full-content scrape finished.");
}

scrape();
