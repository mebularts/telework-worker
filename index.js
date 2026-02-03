const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

const MAX_THREADS_PER_SOURCE = 15;
const MIN_CONTENT_LENGTH = 80;
const MAX_CONTENT_LENGTH = 4000;

const DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36';
const XML_PARSER = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '',
    textNodeName: 'text',
    trimValues: true
});

const SEEN_URLS = new Set();

const JOB_URL_HINTS = [
    '/is-ilan', '/is-ilani', '/is-ilanlari',
    '/is-arayan', '/is-veren', '/isveren',
    '/freelance', '/freelancer',
    '/proje', '/project',
    '/hizmet-alim', '/hizmet-alimi',
    '/talep', '/job', '/jobs', '/hiring'
];

const JOB_POSITIVE_STRONG_PATTERNS = [
    /\baranir\b/i,
    /\baraniyor\b/i,
    /\baraniyorum\b/i,
    /\baraniyoruz\b/i,
    /\baramaktadir\b/i,
    /\baramaktayim\b/i,
    /\baramaktayiz\b/i,
    /\baramakta\b/i,
    /\bariyorum\b/i,
    /\bariyoruz\b/i,
    /\balinacak\b/i,
    /\balinacaktir\b/i,
    /\balinir\b/i,
    /\balici\b/i,
    /\balim\b/i,
    /\bihtiyac\b/i,
    /\bgerekli\b/i,
    /\blazim\b/i,
    /\btalep\b/i,
    /\bteklif\b/i,
    /\bfiyat teklifi\b/i,
    /\bbutce\b/i,
    /\bbutcem\b/i,
    /\byaptirilacak\b/i,
    /\byaptirilacaktir\b/i,
    /\byaptirmak istiyorum\b/i,
    /\byapilacak\b/i,
    /\byapilacaktir\b/i,
    /\bkurulacak\b/i,
    /\bkurulacaktir\b/i,
    /\bhazirlanacak\b/i,
    /\bgelistirilecek\b/i,
    /\bgelistirme\b/i,
    /\bfreelance\b/i,
    /\bfreelancer\b/i,
    /\bhiring\b/i,
    /\bis ilani\b/i,
    /\bis ilanlari\b/i,
    /\bis ariyorum\b/i,
    /\bis ariyoruz\b/i,
    /\bis araniyor\b/i
];

const JOB_POSITIVE_WEAK_PATTERNS = [
    /\bproje\b/i,
    /\bproject\b/i,
    /\bdeveloper\b/i,
    /\byazilim\b/i,
    /\btasarim\b/i,
    /\bdesign\b/i,
    /\bseo\b/i,
    /\breklam\b/i,
    /\bads\b/i,
    /\bentegrasyon\b/i,
    /\botomasyon\b/i,
    /\bbot\b/i,
    /\bscraper\b/i
];

const JOB_NEGATIVE_STRONG_PATTERNS = [
    /\bsatilik\b/i,
    /\bsatiyorum\b/i,
    /\bsatiliyor\b/i,
    /\bsatilir\b/i,
    /\bsatis\b/i,
    /\bsatisa\b/i,
    /\bsatmak\b/i,
    /\bsatma\b/i,
    /\bbozum\b/i,
    /\bbozdur\b/i,
    /\bbozdurma\b/i
];

const JOB_NEGATIVE_WEAK_PATTERNS = [
    /\bpaket\b/i,
    /\bfirsat\b/i,
    /\bkampanya\b/i,
    /\bindirim\b/i,
    /\bpromo\b/i,
    /\bpromosyon\b/i,
    /\bkupon\b/i,
    /\bhediye\b/i,
    /\bpremium\b/i,
    /\blisans\b/i,
    /\btoken\b/i,
    /\bkod\b/i,
    /\bservis\b/i,
    /\bhizmetleri\b/i
];

/**
 * Full Content Mode: Fetches thread content for AI processing.
 * Uses separate browser contexts for each thread to avoid ERR_ABORTED.
 */
const CONTENT_SELECTORS = {
    r10: '.postContent.userMessageSize, .postContent, .postbody .content, .post_message, #post_message_, article .content',
    wmaraci: '.message-body, .postMessage, .post-content, .forumPost .content',
    bhw: '.message-body .bbWrapper, .message-content .bbWrapper, article .bbWrapper'
};

const SOURCES = [
    {
        name: 'r10',
        url: 'https://www.r10.net/',
        containerSelector: '#tab-sonAcilan .list ul',
        threadSelector: '#tab-sonAcilan .list ul li.thread .title a',
        contentSelector: CONTENT_SELECTORS.r10
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li.open span a[href*="/forum/"]',
        contentSelector: CONTENT_SELECTORS.wmaraci
    },
    {
        name: 'bhw',
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer/',
        containerSelector: '.p-body',
        threadSelector: '.structItem--thread .structItem-title a, .block-row .contentRow-title a',
        contentSelector: CONTENT_SELECTORS.bhw
    }
];

const FEED_SOURCES = [
    {
        name: 'r10-getnew',
        type: 'html',
        url: 'https://www.r10.net/search.php?do=getnew',
        linkSelector: 'a',
        contentSelector: CONTENT_SELECTORS.r10,
        emitAs: 'r10',
        prefilter: 'title'
    },
    {
        name: 'r10-sitemap',
        type: 'sitemap',
        url: 'https://www.r10.net/sitemap.xml',
        contentSelector: CONTENT_SELECTORS.r10,
        emitAs: 'r10',
        prefilter: 'none',
        maxThreads: 10,
        urlAllow: [/r10\.net\/.+/i]
    },
    {
        name: 'wmaraci-sitemap',
        type: 'sitemap',
        url: 'https://wmaraci.com/sitemap/forum.xml',
        contentSelector: CONTENT_SELECTORS.wmaraci,
        emitAs: 'wmaraci',
        prefilter: 'none',
        maxThreads: 10,
        urlAllow: [/wmaraci\.com\/forum\//i]
    },
    {
        name: 'bhw-rss',
        type: 'rss',
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer.77/index.rss',
        contentSelector: CONTENT_SELECTORS.bhw,
        emitAs: 'bhw',
        resolveUrl: resolveBhwRssUrl,
        prefilter: 'title',
        maxThreads: 15
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

function countMatches(patterns, text) {
    let count = 0;
    for (const pattern of patterns) {
        if (pattern.test(text)) {
            count += 1;
        }
    }
    return count;
}

function toArray(value) {
    if (!value) return [];
    return Array.isArray(value) ? value : [value];
}

function normalizeThreadUrl(url) {
    if (!url) return '';
    try {
        const u = new URL(url);
        u.hash = '';
        u.search = '';
        return u.toString();
    } catch {
        return url.split('#')[0].split('?')[0];
    }
}

function isLikelyThreadUrl(url) {
    if (!url) return false;
    return (
        url.includes('.html') ||
        url.includes('/forum/') ||
        url.includes('thread') ||
        url.includes('konu') ||
        url.includes('/threads/') ||
        /\.\d+\/?$/.test(url)
    );
}

function cleanThreadList(threads, baseUrl) {
    const cleaned = [];
    const seen = new Set();

    for (const item of threads) {
        let url = item.url || item.link || '';
        if (!url) continue;
        if (!url.startsWith('http')) {
            try {
                url = new URL(url, baseUrl).href;
            } catch {
                continue;
            }
        }
        url = normalizeThreadUrl(url);
        if (!isLikelyThreadUrl(url)) continue;
        if (seen.has(url)) continue;
        seen.add(url);

        cleaned.push({
            title: (item.title || '').trim(),
            url
        });
    }

    return cleaned;
}

async function fetchXml(url) {
    try {
        const response = await axios.get(url, {
            timeout: 30000,
            headers: {
                'User-Agent': DEFAULT_UA,
                'Accept': 'application/xml,text/xml,application/rss+xml'
            }
        });
        return typeof response.data === 'string' ? response.data : String(response.data);
    } catch (e) {
        console.warn(`  [!] XML fetch failed: ${url} -> ${e.message.split('\n')[0]}`);
        return null;
    }
}

function extractRssItems(xmlText, maxItems = 50) {
    try {
        const parsed = XML_PARSER.parse(xmlText);
        let items = [];

        if (parsed?.rss?.channel?.item) {
            items = toArray(parsed.rss.channel.item);
        } else if (parsed?.feed?.entry) {
            items = toArray(parsed.feed.entry);
        }

        const mapped = items.map((item) => {
            const title = typeof item.title === 'object' ? (item.title.text || '') : (item.title || '');
            let link = '';
            if (typeof item.link === 'string') {
                link = item.link;
            } else if (Array.isArray(item.link)) {
                for (const l of item.link) {
                    if (typeof l === 'string') {
                        link = l;
                        break;
                    }
                    if (l && l.href) {
                        link = l.href;
                        break;
                    }
                }
            } else if (item.link && item.link.href) {
                link = item.link.href;
            } else if (item.id) {
                link = item.id;
            }

            return {
                title: (title || '').trim(),
                url: (link || '').trim()
            };
        });

        return mapped.filter(item => item.url).slice(0, maxItems);
    } catch (e) {
        console.warn(`  [!] RSS parse failed: ${e.message.split('\n')[0]}`);
        return [];
    }
}

function extractSitemapUrls(xmlText) {
    try {
        const parsed = XML_PARSER.parse(xmlText);

        if (parsed?.sitemapindex?.sitemap) {
            const sitemaps = toArray(parsed.sitemapindex.sitemap).map(sm => ({
                loc: sm.loc || '',
                lastmod: sm.lastmod || ''
            }));
            return { type: 'index', sitemaps };
        }

        if (parsed?.urlset?.url) {
            const urls = toArray(parsed.urlset.url).map(u => u.loc || '').filter(Boolean);
            return { type: 'urlset', urls };
        }
    } catch (e) {
        console.warn(`  [!] Sitemap parse failed: ${e.message.split('\n')[0]}`);
    }

    return { type: 'unknown', urls: [] };
}

async function fetchSitemapUrls(url, maxUrls = 50, depth = 0) {
    if (depth > 2) return [];
    const xml = await fetchXml(url);
    if (!xml) return [];

    const parsed = extractSitemapUrls(xml);
    if (parsed.type === 'index') {
        const sorted = parsed.sitemaps
            .filter(sm => sm.loc)
            .sort((a, b) => new Date(b.lastmod || 0) - new Date(a.lastmod || 0));
        const pick = sorted.length > 0 ? sorted.slice(0, 3) : [];
        let urls = [];
        for (const sm of pick) {
            const childUrls = await fetchSitemapUrls(sm.loc, maxUrls - urls.length, depth + 1);
            urls = urls.concat(childUrls);
            if (urls.length >= maxUrls) break;
        }
        return urls.slice(0, maxUrls);
    }

    if (parsed.type === 'urlset') {
        return parsed.urls.slice(0, maxUrls);
    }

    return [];
}

async function resolveFinalUrl(url) {
    try {
        const response = await axios.get(url, {
            maxRedirects: 5,
            timeout: 20000,
            headers: { 'User-Agent': DEFAULT_UA }
        });
        return response?.request?.res?.responseUrl || url;
    } catch {
        return url;
    }
}

async function resolveBhwRssUrl(defaultUrl) {
    const forumBase = 'https://www.blackhatworld.com/forums/hire-a-freelancer/';
    const finalForumUrl = await resolveFinalUrl(forumBase);
    if (!finalForumUrl) return defaultUrl;

    if (finalForumUrl.includes('index.rss')) {
        return finalForumUrl;
    }

    const normalized = finalForumUrl.endsWith('/') ? finalForumUrl : `${finalForumUrl}/`;
    return `${normalized}index.rss`;
}

function isJobTopic({ title, content, url }) {
    const combined = [title, content, url].filter(Boolean).join(' ');
    const normalized = normalizeText(combined);

    const urlHit = JOB_URL_HINTS.some(hint => normalized.includes(hint));
    const posStrong = countMatches(JOB_POSITIVE_STRONG_PATTERNS, normalized);
    const posWeak = countMatches(JOB_POSITIVE_WEAK_PATTERNS, normalized);
    const negStrong = countMatches(JOB_NEGATIVE_STRONG_PATTERNS, normalized);
    const negWeak = countMatches(JOB_NEGATIVE_WEAK_PATTERNS, normalized);

    const hasCurrency = /(?:\u20BA|\$|\u20AC|\btl\b|\btry\b|\busd\b|\beur\b|\bgbp\b)/i.test(combined);
    const hasContact = /\b(?:pm|dm|whatsapp|telegram|tel|telefon|iletisim)\b/i.test(normalized);

    if (posStrong > 0) {
        return { isJob: true, reason: `posStrong:${posStrong}` };
    }

    if (urlHit && negStrong === 0) {
        return { isJob: true, reason: 'url' };
    }

    if (posWeak > 0 && negStrong === 0 && (hasCurrency || hasContact)) {
        return { isJob: true, reason: `posWeak:${posWeak}` };
    }

    return {
        isJob: false,
        reason: `negStrong:${negStrong},negWeak:${negWeak},posStrong:${posStrong},posWeak:${posWeak}`
    };
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

async function fetchThreadDetails(context, url, contentSelector, titleSelector) {
    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await page.waitForTimeout(600);

        await waitForAnySelector(page, contentSelector, 8000);

        const details = await page.evaluate(({ selector, titleSelector, minLen, maxLen }) => {
            const selectors = selector.split(',').map(s => s.trim()).filter(Boolean);
            const titleSelectors = (titleSelector || 'h1, .p-title-value, .thread-title, .title')
                .split(',')
                .map(s => s.trim())
                .filter(Boolean);

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

            if (candidates.length === 0) {
                return null;
            }

            let best = candidates.find(c => c.text.length >= minLen);
            if (!best) {
                best = candidates.reduce((a, b) => (b.text.length > a.text.length ? b : a), candidates[0]);
            }

            let pageTitle = '';
            for (const sel of titleSelectors) {
                const el = document.querySelector(sel);
                if (el && el.innerText) {
                    pageTitle = el.innerText.trim();
                    break;
                }
            }
            if (!pageTitle) {
                pageTitle = (document.title || '').trim();
            }

            const text = best.text;
            return {
                content: text.length > maxLen ? text.slice(0, maxLen) : text,
                title: pageTitle
            };
        }, {
            selector: contentSelector,
            titleSelector,
            minLen: MIN_CONTENT_LENGTH,
            maxLen: MAX_CONTENT_LENGTH
        });

        return details;
    } catch (e) {
        console.warn(`  [!] Fetch failed: ${e.message.split('\n')[0]}`);
        return null;
    } finally {
        await page.close();
    }
}

async function collectLinksFromPage(context, url, linkSelector) {
    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await page.waitForTimeout(1200);

        const links = await page.evaluate((selector) => {
            const nodes = document.querySelectorAll(selector || 'a');
            return Array.from(nodes).map((el) => ({
                title: (el.innerText || '').trim(),
                url: el.getAttribute('href') || el.href || ''
            }));
        }, linkSelector || 'a');

        return links;
    } catch (e) {
        console.warn(`  [!] HTML feed failed: ${e.message.split('\n')[0]}`);
        return [];
    } finally {
        await page.close();
    }
}

async function processThreadsForSource(source, threads, context) {
    const maxThreads = source.maxThreads || MAX_THREADS_PER_SOURCE;
    const prefilterMode = source.prefilter || 'title';

    const prefiltered = [];
    for (const thread of threads) {
        if (!thread.url) continue;
        if (SEEN_URLS.has(thread.url)) {
            continue;
        }

        if (prefilterMode !== 'none') {
            const jobCheck = isJobTopic({ title: thread.title, content: '', url: thread.url });
            if (!jobCheck.isJob) {
                console.log(`  [SKIP:NOTJOB] ${(thread.title || thread.url).substring(0, 60)}...`);
                continue;
            }
        }

        prefiltered.push(thread);
        SEEN_URLS.add(thread.url);

        if (prefiltered.length >= maxThreads) {
            break;
        }
    }

    console.log(`Fetching content for ${prefiltered.length} topics...`);

    const enrichedThreads = [];
    for (const thread of prefiltered) {
        await new Promise(resolve => setTimeout(resolve, 600));

        const details = await fetchThreadDetails(context, thread.url, source.contentSelector, source.titleSelector);
        const content = details?.content || '';
        const finalTitle = (thread.title || '').trim() || (details?.title || '').trim();

        if (!finalTitle) {
            console.log(`  [SKIP:NOTITLE] ${thread.url.substring(0, 60)}...`);
            continue;
        }

        if (content && content.length >= MIN_CONTENT_LENGTH) {
            const jobCheck = isJobTopic({ title: finalTitle, content, url: thread.url });
            if (!jobCheck.isJob) {
                console.log(`  [SKIP:NOTJOB] ${finalTitle.substring(0, 60)}...`);
                continue;
            }
            enrichedThreads.push({ ...thread, title: finalTitle, original_content: content });
            console.log(`  [OK] ${finalTitle.substring(0, 60)}...`);
        } else {
            console.log(`  [SKIP:NOCONTENT] ${finalTitle.substring(0, 60)}...`);
        }
    }

    if (enrichedThreads.length > 0) {
        const sourceLabel = source.emitAs || source.name;
        await axios.post(WEBHOOK_URL, {
            type: 'external_crawl',
            token: SCRAPER_TOKEN,
            source: sourceLabel,
            data: enrichedThreads
        });
        console.log(`Pushed ${enrichedThreads.length} items to webhook.`);
    } else {
        console.log('No items with content to push.');
    }
}

async function processFeedSource(feed, context) {
    console.log(`\n=== ${feed.name.toUpperCase()} ===`);
    let threads = [];

    if (feed.type === 'rss') {
        const feedUrl = feed.resolveUrl ? await feed.resolveUrl(feed.url) : feed.url;
        console.log(`Fetching RSS: ${feedUrl}`);
        const xml = await fetchXml(feedUrl);
        if (!xml) return;
        threads = extractRssItems(xml, feed.maxItems || 60);
        threads = cleanThreadList(threads, feedUrl);
    } else if (feed.type === 'sitemap') {
        console.log(`Fetching Sitemap: ${feed.url}`);
        const urls = await fetchSitemapUrls(feed.url, feed.maxItems || 60);
        const filtered = feed.urlAllow
            ? urls.filter(u => feed.urlAllow.some(r => r.test(u)))
            : urls;
        threads = cleanThreadList(filtered.map(url => ({ title: '', url })), feed.url);
    } else if (feed.type === 'html') {
        console.log(`Fetching HTML feed: ${feed.url}`);
        const rawLinks = await collectLinksFromPage(context, feed.url, feed.linkSelector || 'a');
        threads = cleanThreadList(rawLinks, feed.url);
    }

    if (threads.length === 0) {
        console.log('No threads found.');
        return;
    }

    console.log(`Found ${threads.length} topics. Prefiltering...`);
    await processThreadsForSource(feed, threads, context);
}

async function scrapeFeedSources(context) {
    for (const feed of FEED_SOURCES) {
        try {
            await processFeedSource(feed, context);
        } catch (error) {
            console.error(`[${feed.name}] Feed Error:`, error.message.split('\n')[0]);
        }
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
        userAgent: DEFAULT_UA,
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
            const rawThreads = await mainPage.evaluate((s) => {
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

            const threads = cleanThreadList(rawThreads, source.url);

            console.log(`Found ${threads.length} topics. Prefiltering...`);
            await mainPage.close();

            const prefilteredThreads = [];
            for (const thread of threads) {
                if (SEEN_URLS.has(thread.url)) {
                    continue;
                }
                const jobCheck = isJobTopic({ title: thread.title, content: '', url: thread.url });
                if (!jobCheck.isJob) {
                    console.log(`  [SKIP:NOTJOB] ${thread.title.substring(0, 60)}...`);
                    continue;
                }
                prefilteredThreads.push(thread);
                SEEN_URLS.add(thread.url);
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

                const details = await fetchThreadDetails(context, thread.url, source.contentSelector, source.titleSelector);
                const content = details?.content || '';
                const finalTitle = (thread.title || '').trim() || (details?.title || '').trim();

                if (content && content.length >= MIN_CONTENT_LENGTH) {
                    const jobCheck = isJobTopic({ title: finalTitle, content, url: thread.url });
                    if (!jobCheck.isJob) {
                        console.log(`  [SKIP:NOTJOB] ${thread.title.substring(0, 60)}...`);
                        continue;
                    }
                    enrichedThreads.push({ ...thread, title: finalTitle, original_content: content });
                    console.log(`  [OK] ${finalTitle.substring(0, 60)}...`);
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

    await scrapeFeedSources(context);

    await browser.close();
    console.log("\n=== Scrape finished ===");
}

scrape();
