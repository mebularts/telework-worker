const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const axios = require('axios');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

const MAX_THREADS_PER_SOURCE = 15;
const MIN_CONTENT_LENGTH = 80;
const MAX_CONTENT_LENGTH = 4000;

const SALES_URL_HINTS = [
    '/firsat', '/kampanya'
];

const SALES_STRONG_PATTERNS = [
    /\bsatilik\b/i,
    /\bsatiliyor\b/i,
    /\bsatiyorum\b/i,
    /\bsatis\b/i,
    /\bsatisa\b/i,
    /\bsatmak\b/i,
    /\bsatma\b/i,
    /\bsatici\b/i,
    /\bsatin\s*al\b/i,
    /\bsatinalma\b/i,
    /\bbozum\b/i,
    /\bbozdur\b/i,
    /\bbozdurma\b/i,
    /\bfor sale\b/i,
    /\bselling\b/i,
    /\bwts\b/i,
    /\bwtb\b/i,
    /\bwtt\b/i,
    /\bmarketplace\b/i,
    /\bbuy[-/ ]?sell\b/i
];

const SALES_WEAK_PATTERNS = [
    /\bpaket\b/i,
    /\bfirsat\b/i,
    /\bkampanya\b/i,
    /\bindirim\b/i,
    /\bpromo\b/i,
    /\bpromosyon\b/i,
    /\bkupon\b/i,
    /\bhediye\b/i,
    /\btoken\b/i,
    /\bpremium\b/i
];

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
        contentSelector: '.postContent.userMessageSize, .postContent, .postbody .content, .post_message, #post_message_, article .content'
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li.open span a[href*="/forum/"]',
        contentSelector: '.message-body, .postMessage, .post-content, .forumPost .content'
    },
    {
        name: 'bhw',
        url: 'https://www.blackhatworld.com/',
        containerSelector: '.p-body',
        threadSelector: '.structItem--thread .structItem-title a, .block-row .contentRow-title a',
        contentSelector: '.message-body .bbWrapper, .message-content .bbWrapper, article .bbWrapper'
    }
];

function normalizeText(input) {
    if (!input) return '';
    return input
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/\u0131/g, 'i')
        .replace(/\s+/g, ' ')
        .trim();
}

function isSalesTopic({ title, content, url }) {
    const combined = [title, content, url].filter(Boolean).join(' ');
    const normalized = normalizeText(combined);

    for (const hint of SALES_URL_HINTS) {
        if (normalized.includes(hint)) {
            return { isSale: true, reason: `url:${hint}` };
        }
    }

    for (const pattern of SALES_STRONG_PATTERNS) {
        if (pattern.test(normalized)) {
            return { isSale: true, reason: `strong:${pattern}` };
        }
    }

    let weakHits = 0;
    for (const pattern of SALES_WEAK_PATTERNS) {
        if (pattern.test(normalized)) {
            weakHits += 1;
        }
    }

    const hasCurrency = /(?:\u20BA|\$|\u20AC|\btl\b|\btry\b|\busd\b|\beur\b|\bgbp\b)/i.test(combined);
    if (weakHits >= 2 || (weakHits >= 1 && hasCurrency)) {
        return { isSale: true, reason: `weak:${weakHits},currency:${hasCurrency}` };
    }

    return { isSale: false };
}

async function waitForAnySelector(page, selectorList, timeoutMs) {
    const selectors = selectorList
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);

    if (selectors.length === 0) return null;

    const perSelectorTimeout = Math.max(500, Math.floor(timeoutMs / selectors.length));
    for (const sel of selectors) {
        try {
            await page.waitForSelector(sel, { timeout: perSelectorTimeout });
            return sel;
        } catch {
            // Try next selector
        }
    }
    return null;
}

async function fetchThreadContent(context, url, contentSelector) {
    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await page.waitForTimeout(600);

        await waitForAnySelector(page, contentSelector, 8000);

        const content = await page.evaluate(({ selector, minLen, maxLen }) => {
            const selectors = selector.split(',').map(s => s.trim()).filter(Boolean);

            const cleanText = (el) => {
                if (!el) return '';
                const clone = el.cloneNode(true);
                const removeSelectors = [
                    'script', 'style', 'noscript', 'blockquote', '.quote', '.bbCodeBlock',
                    '.bbCodeBlock--quote', '.signature', '.postLike', '.postButtons', '.share'
                ];
                removeSelectors.forEach(sel => {
                    clone.querySelectorAll(sel).forEach(node => node.remove());
                });
                const text = clone.innerText || '';
                return text.replace(/\s+/g, ' ').trim();
            };

            const candidates = [];

            for (const sel of selectors) {
                const els = document.querySelectorAll(sel);
                if (els && els.length > 0) {
                    const text = cleanText(els[0]);
                    if (text) candidates.push({ text });
                }
            }

            if (!candidates.some(c => c.text.length >= minLen)) {
                for (const sel of selectors) {
                    document.querySelectorAll(sel).forEach(el => {
                        const text = cleanText(el);
                        if (text) candidates.push({ text });
                    });
                }
            }

            if (candidates.length === 0) {
                const fallback = document.querySelector('article, .post, .thread-content, main');
                const text = cleanText(fallback);
                if (text) candidates.push({ text });
            }

            if (candidates.length === 0) {
                const meta = document.querySelector('meta[name="description"], meta[property="og:description"], meta[name="twitter:description"]');
                const metaText = meta ? (meta.getAttribute('content') || '').trim() : '';
                if (metaText) candidates.push({ text: metaText });
            }

            if (candidates.length === 0) return null;

            let best = candidates.find(c => c.text.length >= minLen);
            if (!best) {
                best = candidates.reduce((a, b) => (b.text.length > a.text.length ? b : a), candidates[0]);
            }

            const text = best.text;
            return text.length > maxLen ? text.slice(0, maxLen) : text;
        }, { selector: contentSelector, minLen: MIN_CONTENT_LENGTH, maxLen: MAX_CONTENT_LENGTH });

        return content;
    } catch (e) {
        console.warn(`  [!] Fetch failed: ${e.message.split('\n')[0]}`);
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
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 },
        locale: 'tr-TR',
        extraHTTPHeaders: {
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7'
        }
    });
    context.setDefaultTimeout(30000);
    context.setDefaultNavigationTimeout(45000);

    await context.route('**/*', route => {
        const type = route.request().resourceType();
        if (['image', 'media', 'font'].includes(type)) {
            return route.abort();
        }
        return route.continue();
    });

    for (const source of SOURCES) {
        const mainPage = await context.newPage();
        try {
            console.log(`\n=== ${source.name.toUpperCase()} ===`);
            console.log(`Navigating to: ${source.url}`);

            await mainPage.goto(source.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
            await mainPage.waitForTimeout(1500);

            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `nav_${safeName}.png`, fullPage: false });

            try {
                await mainPage.waitForSelector(source.containerSelector, { timeout: 10000 });
            } catch (e) {
                console.warn(`[${source.name}] Container not found, trying alternate approach...`);
                // Save debug info
                const content = await mainPage.content();
                require('fs').writeFileSync(`debug_${safeName}.html`, content);
                await mainPage.screenshot({ path: `debug_${safeName}.png`, fullPage: true });
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

                    // Filter valid thread URLs
                    if (title && url && title.length > 5) {
                        const isThread = url.includes('.html') ||
                            url.includes('/forum/') ||
                            url.includes('thread') ||
                            url.includes('konu') ||
                            url.includes('/threads/') ||
                            /\.\d+\/?$/.test(url);
                        if (isThread && !results.find(r => r.url === url)) {
                            results.push({ title, url });
                        }
                    }
                });
                return results.slice(0, 50);
            }, source);

            console.log(`Found ${threads.length} topics. Prefiltering...`);
            await mainPage.close();

            const prefilteredThreads = [];
            for (const thread of threads) {
                const saleCheck = isSalesTopic({ title: thread.title, content: '', url: thread.url });
                if (saleCheck.isSale) {
                    console.log(`  [SKIP:SALE] ${thread.title.substring(0, 60)}...`);
                    continue;
                }
                prefilteredThreads.push(thread);
                if (prefilteredThreads.length >= MAX_THREADS_PER_SOURCE) {
                    break;
                }
            }

            console.log(`Fetching content for ${prefilteredThreads.length} topics...`);

            // Fetch content for each thread using separate pages
            const enrichedThreads = [];
            for (const thread of prefilteredThreads) {
                // Add delay between requests
                await new Promise(resolve => setTimeout(resolve, 600));

                const content = await fetchThreadContent(context, thread.url, source.contentSelector);
                if (content && content.length >= MIN_CONTENT_LENGTH) {
                    const saleCheck = isSalesTopic({ title: thread.title, content, url: thread.url });
                    if (saleCheck.isSale) {
                        console.log(`  [SKIP:SALE] ${thread.title.substring(0, 60)}...`);
                        continue;
                    }
                    enrichedThreads.push({ ...thread, original_content: content });
                    console.log(`  [OK] ${thread.title.substring(0, 60)}...`);
                } else {
                    console.log(`  [SKIP:NOCONTENT] ${thread.title.substring(0, 60)}...`);
                }
            }

            console.log(`Successfully fetched: ${enrichedThreads.length}/${prefilteredThreads.length}`);

            // Only push items that have content
            if (enrichedThreads.length > 0) {
                await axios.post(WEBHOOK_URL, {
                    type: 'external_crawl',
                    token: SCRAPER_TOKEN,
                    source: source.name,
                    data: enrichedThreads
                });
                console.log(`Pushed ${enrichedThreads.length} items to webhook.`);
            } else {
                console.log(`No items with content to push.`);
            }

        } catch (error) {
            console.error(`[${source.name}] Critical Error:`, error.message.split('\n')[0]);
            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `critical_${safeName}.png` }).catch(() => { });
            await mainPage.close().catch(() => { });
        }
    }

    await browser.close();
    console.log("\n=== Scrape finished ===");
}

scrape();





