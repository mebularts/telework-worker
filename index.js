const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
const RecaptchaPlugin = require('puppeteer-extra-plugin-recaptcha');

const fs = require('fs');
const path = require('path');

chromium.use(stealth);

if (process.env.CAPTCHA_TOKEN) {
    chromium.use(RecaptchaPlugin({
        provider: { id: '2captcha', token: process.env.CAPTCHA_TOKEN },
        visualFeedback: true // colorize reCAPTCHAs (violet = detected, green = solved)
    }));
    console.log('[Captcha] 2Captcha plugin enabled.');
} else {
    console.warn('[Captcha] CAPTCHA_TOKEN not found. Captcha solving disabled.');
}

const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

const MAX_THREADS_PER_SOURCE = 20;
const MIN_CONTENT_LENGTH = 80;
const MAX_CONTENT_LENGTH = 4000;

const DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36';
const XML_PARSER = new XMLParser({
    ignoreAttributes: false,
    removeNSPrefix: true,
    attributeNamePrefix: '',
    textNodeName: 'text',
    trimValues: true
});

const SEEN_URLS_FILE = path.join(__dirname, 'seen_urls.json');
let SEEN_URLS = new Set();
try {
    if (fs.existsSync(SEEN_URLS_FILE)) {
        const data = JSON.parse(fs.readFileSync(SEEN_URLS_FILE, 'utf8'));
        SEEN_URLS = new Set(data);
        console.log(`[Persistence] Loaded ${SEEN_URLS.size} seen URLs.`);
    }
} catch (e) {
    console.warn(`[Persistence] Failed to load seen_urls.json: ${e.message}`);
}
function saveSeenUrls() {
    try {
        fs.writeFileSync(SEEN_URLS_FILE, JSON.stringify([...SEEN_URLS], null, 2));
        console.log(`[Persistence] Saved ${SEEN_URLS.size} seen URLs.`);
    } catch (e) {
        console.warn(`[Persistence] Failed to save seen_urls.json: ${e.message}`);
    }
}

const COOKIE_ENV_KEY = 'SCRAPER_COOKIES_JSON';
const COOKIE_ENV_KEY_ALT = 'SCRAPER_COOKIES';
const COOKIE_FILES_ENV = 'SCRAPER_COOKIES_FILES';
const COOKIE_FILES_ENV_ALT = 'SCRAPER_COOKIES_FILE';
let HAS_COOKIES = false;

// Proxy Configuration
let AXIOS_PROXY_CONFIG = null;
let PLAYWRIGHT_PROXY_CONFIG = null;

let USE_SCRAPE_DO_API = false;

if (process.env.SCRAPE_DO_TOKEN) {
    console.log('[Proxy] Using Scrape.do API Gateway mode.');
    USE_SCRAPE_DO_API = true;
    // For Playwright, we can use their proxy server standard
    PLAYWRIGHT_PROXY_CONFIG = {
        server: 'http://proxy-server.scrape.do:8080',
        username: 'scraperapi',
        password: process.env.SCRAPE_DO_TOKEN
    };
} else if (process.env.SCRAPER_PROXY) {
    try {
        const proxyUrl = new URL(process.env.SCRAPER_PROXY);

        // For Playwright
        PLAYWRIGHT_PROXY_CONFIG = {
            server: `${proxyUrl.protocol}//${proxyUrl.host}`
        };
        if (proxyUrl.username) PLAYWRIGHT_PROXY_CONFIG.username = proxyUrl.username;
        if (proxyUrl.password) PLAYWRIGHT_PROXY_CONFIG.password = proxyUrl.password;

        // For Axios
        AXIOS_PROXY_CONFIG = {
            protocol: proxyUrl.protocol.replace(':', ''),
            host: proxyUrl.hostname,
            port: Number(proxyUrl.port) || (proxyUrl.protocol === 'https:' ? 443 : 80)
        };
        if (proxyUrl.username) {
            AXIOS_PROXY_CONFIG.auth = {
                username: proxyUrl.username,
                password: proxyUrl.password || ''
            };
        }
        console.log(`[Proxy] Configuration loaded for ${proxyUrl.hostname}`);
    } catch (e) {
        console.warn(`[Proxy] Invalid URL: ${e.message}`);
    }
}

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
    /\arayisim\b/i,
    /\arayis\b/i,
    /\balinacak\b/i,
    /\balinacaktir\b/i,
    /\balinir\b/i,
    /\balisi\b/i,
    /\balimi\b/i,
    /\baliyorum\b/i,
    /\bihtiyac\b/i,
    /\bihtiyacim\b/i,
    /\byaptirilacak\b/i,
    /\byaptirilir\b/i
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
    /\bscraper\b/i,
    /\bhizmet\b/i,
    /\baccount\b/i,
    /\bhesap\b/i
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
    /\bbozdurma\b/i,
    //    /\breklam\b/i, // Moved to weak
    //    /\btanitim\b/i, // Moved to weak
    /\bkampanya\b/i,
    /\bindirim\b/i,
    /\bfirsat\b/i,
    //    /\bpaket\b/i, // Too aggressive
    /\bbayilik\b/i,
    /\bkupon\b/i,
    /\bpromosyon\b/i,
    /\bhediye\b/i,
    /\bgaranti\b/i,
    /\bgarantili\b/i,
    /\bstok\b/i,
    /\btoplu\b/i,
    /\bsponsor\b/i,
    //    /\bhizmetleri\b/i, // Too aggressive
    //    /\bcozumleri\b/i, // Too aggressive
    //    /\bpaketleri\b/i, // Too aggressive
    //    /\bsatisi\b/i, // Too aggressive
    /\bscripti\b/i,
    /\byazilimi\b/i,
    /\btemasi\b/i,
    /\bhosting\b/i,
    /\bsunucu\b/i,
    /\bdomain\b/i,
    /\blisans\b/i,
    // /\bhesap\b/i, // Too aggressive (blocks 'hesap alinacak')
    // /\baccount\b/i, // Too aggressive
    /\bsatilik hesap\b/i,
    /\bhesap satisi\b/i,
    /\bhesap satilik\b/i,
    /\bmağaza\b/i,
    /\bstore\b/i,
    /\bmarket\b/i,
    /\bshop\b/i
];

const JOB_NEGATIVE_WEAK_PATTERNS = [
    /\bpromo\b/i,
    /\bpremium\b/i,
    /\blisans\b/i,
    /\btoken\b/i,
    /\bkod\b/i,
    /\bservis\b/i,
    /\bhizmetleri\b/i,
    /\bpanel\b/i,
    /\bportfolyo\b/i,
    /\breferans\b/i
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
        containerSelector: '.thread, #tab-sonAcilan .list ul li.thread',
        threadSelector: '.thread .title a, #tab-sonAcilan .list ul li.thread .title a, a[id^="thread_title_"]',
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
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer.76/',
        containerSelector: '.structItem--thread, .structItem',
        threadSelector: '.structItem-title a, .block-row .contentRow-title a',
        contentSelector: CONTENT_SELECTORS.bhw
    }
];

const FEED_SOURCES = [
    {
        name: 'r10-getnew',
        type: 'html',
        url: 'https://www.r10.net/search.php?do=getnew',
        itemSelector: '.threadList.search li.thread',
        titleSelector: '.title a',
        prefixSelector: '.title .prefix',
        forumSelector: '.forum a',
        contentSelector: CONTENT_SELECTORS.r10,
        emitAs: 'r10',
        prefilter: 'smart',
        maxThreads: 40
    },
    {
        name: 'r10-sitemap',
        type: 'sitemap',
        url: 'view-source:https://www.r10.net/sitemap.xml',
        allowViewSource: true,
        contentSelector: CONTENT_SELECTORS.r10,
        emitAs: 'r10',
        prefilter: 'smart',
        maxThreads: 30,
        maxItems: 120
    },
    {
        name: 'wmaraci-sitemap',
        type: 'sitemap',
        url: 'https://wmaraci.com/sitemap/forum.xml',
        contentSelector: CONTENT_SELECTORS.wmaraci,
        emitAs: 'wmaraci',
        prefilter: 'smart',
        maxThreads: 30,
        maxItems: 120
    },
    {
        name: 'bhw-rss',
        type: 'rss',
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer.76/index.rss',
        contentSelector: CONTENT_SELECTORS.bhw,
        emitAs: 'bhw',
        resolveUrl: resolveBhwRssUrl,
        prefilter: 'title',
        maxThreads: 15,
        maxItems: 60
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

function parseCookieEnv(raw) {
    if (!raw) return [];
    let text = raw.trim();
    if (!text) return [];

    if (text.startsWith('base64:')) {
        const b64 = text.slice('base64:'.length);
        try {
            text = Buffer.from(b64, 'base64').toString('utf8');
        } catch {
            return [];
        }
    }

    try {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) return parsed;
        if (parsed && Array.isArray(parsed.cookies)) return parsed.cookies;
        return [];
    } catch {
        return [];
    }
}

function parseCookieFilesEnv(raw) {
    if (!raw) return [];
    return raw
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
}

function readCookiesFromFile(filePath) {
    try {
        const resolved = path.resolve(filePath);
        if (!fs.existsSync(resolved)) return [];
        let text = fs.readFileSync(resolved, 'utf8').trim();
        if (!text) return [];

        if (text.startsWith('base64:')) {
            text = Buffer.from(text.slice('base64:'.length), 'base64').toString('utf8');
        }

        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
            return parsed;
        }
        if (parsed && Array.isArray(parsed.cookies)) {
            return parsed.cookies;
        }
    } catch {
        return [];
    }
    return [];
}

function normalizeCookie(cookie) {
    const c = { ...cookie };
    if (c.expirationDate && !c.expires) {
        c.expires = c.expirationDate;
        delete c.expirationDate;
    }
    if (c.sameSite === null || c.sameSite === undefined || c.sameSite === '') {
        delete c.sameSite;
    } else {
        const map = {
            lax: 'Lax',
            strict: 'Strict',
            none: 'None',
            no_restriction: 'None',
            unrestricted: 'None',
            unspecified: undefined,
            'no_restriction': 'None'
        };
        const key = String(c.sameSite).toLowerCase();
        const mapped = map[key];
        if (mapped) {
            c.sameSite = mapped;
        } else if (mapped === undefined && (key === 'unspecified')) {
            delete c.sameSite;
        }
    }
    return c;
}

async function applyCookiesFromEnv(context) {
    const raw = process.env[COOKIE_ENV_KEY] || process.env[COOKIE_ENV_KEY_ALT];
    const cookies = parseCookieEnv(raw).map(normalizeCookie);

    const fileList = parseCookieFilesEnv(process.env[COOKIE_FILES_ENV] || process.env[COOKIE_FILES_ENV_ALT]);
    // Auto-detect local cookie files
    const localFiles = ['r10-cookies.json', 'bhw-cookies.json', 'cookies.json'];
    for (const f of localFiles) {
        if (require('fs').existsSync(f)) {
            fileList.push(f);
        }
    }

    for (const filePath of fileList) {
        cookies.push(...readCookiesFromFile(filePath).map(normalizeCookie));
    }

    const deduped = new Map();
    for (const c of cookies) {
        if (!c || !c.name || !c.value || !c.domain) continue;
        const key = `${c.name}|${c.domain}|${c.path || '/'}`;
        deduped.set(key, c);
    }

    const finalCookies = Array.from(deduped.values());
    if (finalCookies.length === 0) {
        return;
    }
    try {
        await context.addCookies(finalCookies);
        console.log(`[cookies] Loaded ${finalCookies.length} cookies`);
        HAS_COOKIES = true;
    } catch (e) {
        console.warn(`[cookies] Failed to apply cookies: ${e.message.split('\n')[0]}`);
    }
}

function sanitizeFeedUrl(url, preserveViewSource = false) {
    if (!url) return '';
    let cleaned = url.trim();
    if (!preserveViewSource && cleaned.startsWith('view-source:')) {
        cleaned = cleaned.replace(/^view-source:/i, '');
    }
    return cleaned;
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
            ...item,
            title: (item.title || '').trim(),
            url
        });
    }

    return cleaned;
}

function extractXmlPayload(text) {
    if (!text) return '';
    const markers = [
        { open: '<rss', close: '</rss>' },
        { open: '<urlset', close: '</urlset>' },
        { open: '<sitemapindex', close: '</sitemapindex>' },
        { open: '<feed', close: '</feed>' }
    ];

    for (const marker of markers) {
        const start = text.indexOf(marker.open);
        if (start === -1) continue;
        const end = text.indexOf(marker.close, start);
        if (end !== -1) {
            return text.slice(start, end + marker.close.length);
        }
    }

    return text;
}

async function fetchXmlViaBrowser(url, context) {
    if (!context) return null;
    const isViewSource = url && url.startsWith('view-source:');
    const actualUrl = isViewSource ? url.replace(/^view-source:/i, '') : url;
    try {
        if (!isViewSource) {
            const response = await context.request.get(actualUrl, {
                headers: {
                    'User-Agent': DEFAULT_UA,
                    'Accept': 'application/xml,text/xml,application/rss+xml,*/*'
                },
                timeout: 30000
            });
            if (response.ok()) {
                const text = await response.text();
                const payload = extractXmlPayload(text);
                if (payload.includes('<urlset') || payload.includes('<rss') || payload.includes('<sitemapindex') || payload.includes('<feed')) {
                    return payload;
                }
            }
        }
    } catch (e) {
        console.warn(`  [!] Browser XML fetch failed: ${e.message.split('\n')[0]}`);
    }

    const page = await context.newPage();
    try {
        await page.setExtraHTTPHeaders({
            'Referer': actualUrl
        });
        await page.goto(isViewSource ? url : actualUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

        // Handle Cloudflare challenge check if present
        try {
            const challenge = await page.$('#challenge-running');
            if (challenge) {
                await page.waitForTimeout(5000);
            }
        } catch { }

        await page.waitForTimeout(2000);

        const raw = await page.evaluate(() => {
            const pre = document.querySelector('pre');
            if (pre && pre.innerText) {
                return pre.innerText;
            }
            return document.documentElement.innerText || '';
        });
        const payload = extractXmlPayload(raw);
        if (payload && (payload.includes('<urlset') || payload.includes('<rss') || payload.includes('<sitemapindex') || payload.includes('<feed'))) {
            return payload;
        }
    } catch (e) {
        console.warn(`  [!] Browser XML page fallback failed: ${e.message.split('\n')[0]}`);
    } finally {
        await page.close().catch(() => { });
    }
    return null;
}

async function fetchXml(url, context) {
    if (url && url.startsWith('view-source:')) {
        return await fetchXmlViaBrowser(url, context);
    }

    // Force browser for BHW RSS which consistently blocks axios (UNLESS Scrape.do is active)
    if (!USE_SCRAPE_DO_API && url.includes('blackhatworld.com') && url.includes('rss')) {
        console.log('  [BHW-RSS] Enforcing browser fetch (No Scrape.do)...');
        return await fetchXmlViaBrowser(url, context);
    }

    try {
        let reqConfig = {
            timeout: 30000,
            headers: {
                'User-Agent': DEFAULT_UA,
                'Accept': 'application/xml,text/xml,application/rss+xml,application/atom+xml',
                'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': new URL(url).origin + '/',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }
        };

        let targetUrl = url;

        if (USE_SCRAPE_DO_API && process.env.SCRAPE_DO_TOKEN) {
            // Use Scrape.do API Gateway
            targetUrl = `http://api.scrape.do?url=${encodeURIComponent(url)}&token=${process.env.SCRAPE_DO_TOKEN}`;
            // Remove proxy config if using Gateway API
            reqConfig.proxy = false;
        } else {
            // Use standard proxy if configured
            reqConfig.proxy = AXIOS_PROXY_CONFIG;
        }

        const response = await axios.get(targetUrl, reqConfig);
        const text = typeof response.data === 'string' ? response.data : String(response.data);
        return extractXmlPayload(text);
    } catch (e) {
        console.warn(`  [!] XML fetch failed: ${url} -> ${e.message.split('\n')[0]}`);
        return await fetchXmlViaBrowser(url, context);
    }
}

async function fetchHtmlViaScrapeDo(url) {
    if (!process.env.SCRAPE_DO_TOKEN) return null;
    try {
        const targetUrl = `http://api.scrape.do?url=${encodeURIComponent(url)}&token=${process.env.SCRAPE_DO_TOKEN}`;
        const response = await axios.get(targetUrl, { timeout: 60000 });
        return typeof response.data === 'string' ? response.data : String(response.data);
    } catch (e) {
        console.warn(`  [!] Scrape.do HTML fetch failed: ${url} -> ${e.message}`);
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
            const urls = toArray(parsed.urlset.url).map(u => ({
                loc: u.loc || '',
                lastmod: u.lastmod || ''
            }));
            return { type: 'urlset', urls };
        }
    } catch (e) {
        console.warn(`  [!] Sitemap parse failed: ${e.message.split('\n')[0]}`);
    }

    return { type: 'unknown', urls: [] };
}

async function fetchSitemapUrls(url, maxUrls = 50, depth = 0, context = null) {
    if (depth > 2) return [];
    const xml = await fetchXml(url, context);
    if (!xml) return [];

    const parsed = extractSitemapUrls(xml);
    if (parsed.type === 'index') {
        const sorted = parsed.sitemaps
            .filter(sm => sm.loc)
            .sort((a, b) => new Date(b.lastmod || 0) - new Date(a.lastmod || 0));
        const pick = sorted.length > 0 ? sorted.slice(0, 3) : [];
        let urls = [];
        for (const sm of pick) {
            const childUrls = await fetchSitemapUrls(sm.loc, maxUrls - urls.length, depth + 1, context);
            urls = urls.concat(childUrls);
            if (urls.length >= maxUrls) break;
        }
        return urls.slice(0, maxUrls);
    }

    if (parsed.type === 'urlset') {
        const sorted = parsed.urls
            .filter(u => u.loc)
            .sort((a, b) => new Date(b.lastmod || 0) - new Date(a.lastmod || 0))
            .map(u => u.loc);
        return sorted.slice(0, maxUrls);
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

async function resolveBhwRssUrl(defaultUrl, context) {
    const baseFromDefault = (defaultUrl || '').replace(/index\.rss.*$/i, '');
    const forumBase = baseFromDefault || 'https://www.blackhatworld.com/forums/hire-a-freelancer.76/';

    if (defaultUrl && defaultUrl.includes('.76/index.rss')) {
        return defaultUrl;
    }

    if (context) {
        const page = await context.newPage();
        try {
            await page.goto(forumBase, { waitUntil: 'domcontentloaded', timeout: 45000 });
            await page.waitForTimeout(1500);

            const rssHref = await page.evaluate(() => {
                const link = document.querySelector('link[rel="alternate"][type="application/rss+xml"]');
                if (link && link.href) return link.href;
                const alt = document.querySelector('a[href$="index.rss"]');
                return alt ? alt.href : '';
            });

            if (rssHref) {
                return rssHref;
            }
        } catch {
            // ignore, fallback below
        } finally {
            await page.close().catch(() => { });
        }
    }

    const finalForumUrl = await resolveFinalUrl(forumBase);
    if (!finalForumUrl) return defaultUrl;

    if (finalForumUrl.includes('index.rss')) {
        return finalForumUrl;
    }

    const normalized = finalForumUrl.endsWith('/') ? finalForumUrl : `${finalForumUrl}/`;
    return `${normalized}index.rss`;
}

function analyzeJobSignals({ title, content, url, prefix, forum }) {
    const combined = [title, content, url, prefix, forum].filter(Boolean).join(' ');
    const normalized = normalizeText(combined);

    const urlHit = JOB_URL_HINTS.some(hint => normalized.includes(hint));
    const posStrong = countMatches(JOB_POSITIVE_STRONG_PATTERNS, normalized);
    const posWeak = countMatches(JOB_POSITIVE_WEAK_PATTERNS, normalized);
    const negStrong = countMatches(JOB_NEGATIVE_STRONG_PATTERNS, normalized);
    const negWeak = countMatches(JOB_NEGATIVE_WEAK_PATTERNS, normalized);

    const hasCurrency = /(?:\u20BA|\$|\u20AC|\btl\b|\btry\b|\busd\b|\beur\b|\bgbp\b)/i.test(combined);
    const hasContact = /\b(?:pm|dm|whatsapp|telegram|tel|telefon|iletisim|contact)\b/i.test(normalized);

    const prefixNorm = normalizeText(prefix || '');
    const prefixDemand = /(alim|talep|aran|arayis|ihtiyac|istek|acil)/.test(prefixNorm);
    const prefixSupply = /(satis|satilik|hizmet|reklam|tanitim|kampanya|paket|sponsor|ilan)/.test(prefixNorm);

    return {
        combined,
        normalized,
        urlHit,
        posStrong,
        posWeak,
        negStrong,
        negWeak,
        hasCurrency,
        hasContact,
        prefixDemand,
        prefixSupply
    };
}

function isJobTopic({ title, content, url, prefix, forum }) {
    const s = analyzeJobSignals({ title, content, url, prefix, forum });

    const demandScore = s.posStrong + (s.prefixDemand ? 1 : 0);
    const supplyScore = s.negStrong + (s.prefixSupply ? 1 : 0);

    if (supplyScore > 0) {
        // If it has "Service/Sale" keywords, be very strict.
        // Must have very strong demand signals (e.g. "Hizmet Alinacak", "Script Alinacak")
        // demandScore must be significantly higher than supplyScore + 1 to override a negative signal.
        if (demandScore > supplyScore + 1 && s.posStrong >= 2) {
            return {
                isJob: true,
                reason: `OVERRIDE: demand:${demandScore} > supply:${supplyScore}`
            };
        }

        return {
            isJob: false,
            reason: `supply:${supplyScore} (demand:${demandScore} insufficient)`
        };
    }

    if (demandScore > 0) {
        return {
            isJob: true,
            reason: `demand:${demandScore}`
        };
    }

    if (demandScore > 0 && supplyScore > 0) {
        const isJob = demandScore >= supplyScore + 1 || s.posStrong >= 2;
        return {
            isJob,
            reason: `mixed: demand:${demandScore},supply:${supplyScore}`
        };
    }

    if (s.posWeak >= 2 && (s.hasCurrency || s.hasContact) && s.negStrong === 0 && s.negWeak <= 1) {
        return { isJob: true, reason: `posWeak:${s.posWeak}` };
    }

    if (s.posWeak >= 1 && s.urlHit && s.negStrong === 0 && s.negWeak === 0) {
        return { isJob: true, reason: `url+posWeak:${s.posWeak}` };
    }

    return {
        isJob: false,
        reason: `negStrong:${s.negStrong},negWeak:${s.negWeak},posStrong:${s.posStrong},posWeak:${s.posWeak}`
    };
}

function shouldPrefilterSkip({ title, url, prefix, forum }) {
    const s = analyzeJobSignals({ title, content: '', url, prefix, forum });
    const demandScore = s.posStrong + (s.prefixDemand ? 1 : 0);
    const supplyScore = s.negStrong + (s.prefixSupply ? 1 : 0);

    if (demandScore > 0 && supplyScore > 0) {
        // This block usually not reached due to above logic, but for safety:
        return {
            isJob: false,
            reason: `mixed-strict: demand:${demandScore},supply:${supplyScore}`
        };
    }

    if (supplyScore > 0) {
        return true;
    }

    if (s.negWeak >= 2 && s.posWeak === 0) {
        return true;
    }

    return false;
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

async function handleAntiBot(page) {
    try {
        const title = await page.title();
        if (title.includes('Just a moment') || title.includes('Cloudflare') || title.includes('Access denied') || title.includes('Bir dakika') || title.includes('Security Check')) {
            console.log('  [AntiBot] Challenge page detected: ' + title);
            await page.waitForTimeout(3000);

            // Attempt to click Cloudflare checkbox (supports Shadow DOM)
            const clicked = await page.evaluate(async () => {
                function clickWidget(root) {
                    // Check standard inputs
                    const inputs = root.querySelectorAll('input[type="checkbox"], #challenge-stage iframe, .ctp-checkbox-label, #turnstile-wrapper');
                    for (const input of inputs) {
                        // If it's an iframe, we can't easily pierce it from here unless it's same-origin, 
                        // but usually the click target is a label or div around it.
                        if (input.offsetParent !== null) { // is visible
                            input.click();
                            return true;
                        }
                    }

                    // Shadow DOM recursion
                    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
                    while (walker.nextNode()) {
                        const node = walker.currentNode;
                        if (node.shadowRoot) {
                            if (clickWidget(node.shadowRoot)) return true;
                        }
                    }
                    return false;
                }

                return clickWidget(document);
            });

            if (clicked) {
                console.log('  [AntiBot] Clicked potential Cloudflare widget via DOM.');
            } else {
                // Try looking in iframes from the context side
                const frames = page.frames();
                for (const frame of frames) {
                    try {
                        const btn = await frame.$('input[type="checkbox"], .ctp-checkbox-label');
                        if (btn) {
                            await btn.click();
                            console.log('  [AntiBot] Clicked checkbox in iframe: ' + frame.url());
                            break;
                        }
                    } catch { }
                }
            }

            console.log('  [AntiBot] Waiting for challenge to resolve...');
            await page.waitForTimeout(15000); // Give it time to reload
        }
    } catch (e) {
        console.log(`  [AntiBot] Error during handling: ${e.message}`);
    }
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

async function collectLinksFromPage(context, url, options) {
    const opts = typeof options === 'string' ? { linkSelector: options } : (options || {});
    const itemSelector = opts.itemSelector || '';
    const linkSelector = opts.linkSelector || opts.titleSelector || 'a';
    const titleSelector = opts.titleSelector || '';
    const prefixSelector = opts.prefixSelector || '';
    const forumSelector = opts.forumSelector || '';
    const r10Fallback = /r10\.net\/search\.php\?do=getnew/i.test(url);

    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await page.waitForTimeout(1200);

        const waitTarget = itemSelector || linkSelector || 'a';
        let found = await waitForAnySelector(page, waitTarget, 12000);
        if (!found) {
            await page.waitForLoadState('networkidle', { timeout: 12000 }).catch(() => { });
            found = await waitForAnySelector(page, waitTarget, 8000);
        }

        const links = await page.evaluate((params) => {
            const {
                itemSelector,
                linkSelector,
                titleSelector,
                prefixSelector,
                forumSelector,
                r10Fallback
            } = params;

            if (itemSelector) {
                const items = document.querySelectorAll(itemSelector);
                return Array.from(items).map((item) => {
                    const linkEl = item.querySelector(titleSelector || linkSelector || 'a');
                    const title = linkEl ? (linkEl.innerText || '').trim() : '';
                    const url = linkEl ? (linkEl.getAttribute('href') || linkEl.href || '') : '';
                    const prefix = prefixSelector ? (item.querySelector(prefixSelector)?.innerText || '').trim() : '';
                    const forum = forumSelector ? (item.querySelector(forumSelector)?.innerText || '').trim() : '';

                    return { title, url, prefix, forum };
                });
            }

            let fallbackLinks = [];
            const nodes = document.querySelectorAll(linkSelector || 'a');
            fallbackLinks = Array.from(nodes).map((el) => ({
                title: (el.innerText || '').trim(),
                url: el.getAttribute('href') || el.href || ''
            }));

            if (r10Fallback) {
                const anchors = document.querySelectorAll('a[id^="thread_title_"]');
                const r10Links = Array.from(anchors).map((el) => {
                    const item = el.closest('li.thread') || el.closest('li');
                    const prefix = item ? (item.querySelector('.title .prefix')?.innerText || '').trim() : '';
                    const forum = item ? (item.querySelector('.forum a')?.innerText || '').trim() : '';
                    return {
                        title: (el.innerText || '').trim(),
                        url: el.getAttribute('href') || el.href || '',
                        prefix,
                        forum
                    };
                });
                if (r10Links.length > 0) {
                    return r10Links;
                }
            }

            return fallbackLinks;
        }, {
            itemSelector,
            linkSelector,
            titleSelector,
            prefixSelector,
            forumSelector,
            r10Fallback
        });

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
    const prefilterMode = source.prefilter || 'smart';

    const prefiltered = [];
    for (const thread of threads) {
        if (!thread.url) continue;
        if (SEEN_URLS.has(thread.url)) {
            continue;
        }

        if (prefilterMode === 'strict') {
            const jobCheck = isJobTopic({
                title: thread.title,
                content: '',
                url: thread.url,
                prefix: thread.prefix,
                forum: thread.forum
            });
            if (!jobCheck.isJob) {
                console.log(`  [SKIP:NOTJOB] ${(thread.title || thread.url).substring(0, 60)}...`);
                continue;
            }
        } else if (prefilterMode === 'smart') {
            if (shouldPrefilterSkip({
                title: thread.title,
                url: thread.url,
                prefix: thread.prefix,
                forum: thread.forum
            })) {
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
    process.on('SIGINT', () => {
        console.log('\nGracefully shutting down...');
        saveSeenUrls();
        process.exit();
    });

    // saveSeenUrls moved to global scope
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
            const jobCheck = isJobTopic({
                title: finalTitle,
                content,
                url: thread.url,
                prefix: thread.prefix,
                forum: thread.forum
            });
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
        const feedUrl = sanitizeFeedUrl(
            feed.resolveUrl ? await feed.resolveUrl(feed.url, context) : feed.url,
            feed.allowViewSource
        );
        console.log(`Fetching RSS: ${feedUrl}`);
        const xml = await fetchXml(feedUrl, context);
        if (!xml) return;
        threads = extractRssItems(xml, feed.maxItems || 60);
        threads = cleanThreadList(threads, feedUrl);
    } else if (feed.type === 'sitemap') {
        const sitemapUrl = sanitizeFeedUrl(feed.url, feed.allowViewSource);
        console.log(`Fetching Sitemap: ${sitemapUrl}`);
        const urls = await fetchSitemapUrls(sitemapUrl, feed.maxItems || 60, 0, context);
        const filtered = feed.urlAllow
            ? urls.filter(u => feed.urlAllow.some(r => r.test(u)))
            : urls;
        threads = cleanThreadList(filtered.map(url => ({ title: '', url })), sitemapUrl);
    } else if (feed.type === 'html') {
        const feedUrl = sanitizeFeedUrl(feed.url);
        console.log(`Fetching HTML feed: ${feedUrl}`);
        const rawLinks = await collectLinksFromPage(context, feedUrl, feed);
        threads = cleanThreadList(rawLinks, feedUrl);
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

    // Proxy config logic moved to top of file
    let PLAYWRIGHT_PROXY_CONFIG = undefined;
    let AXIOS_PROXY_CONFIG = undefined;

    if (process.env.SCRAPER_PROXY) {
        try {
            const proxyUrl = new URL(process.env.SCRAPER_PROXY);
            PLAYWRIGHT_PROXY_CONFIG = {
                server: `${proxyUrl.protocol}//${proxyUrl.host}`,
                username: proxyUrl.username,
                password: proxyUrl.password
            };
            AXIOS_PROXY_CONFIG = {
                host: proxyUrl.hostname,
                port: proxyUrl.port,
                protocol: proxyUrl.protocol.replace(':', ''),
                auth: proxyUrl.username && proxyUrl.password ? {
                    username: proxyUrl.username,
                    password: proxyUrl.password
                } : undefined
            };
            console.log(`[proxy] Using proxy: ${PLAYWRIGHT_PROXY_CONFIG.server}`);
        } catch (e) {
            console.warn(`[proxy] Invalid proxy URL: ${e.message}`);
        }
    }

    const browserArgs = [
        '--disable-blink-features=AutomationControlled',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-infobars',
        '--window-position=0,0',
        '--ignore-certifcate-errors',
        '--ignore-certifcate-errors-spki-list',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        '--hide-scrollbars'
    ];

    const browser = await chromium.launch({
        headless: true,
        args: browserArgs,
        proxy: PLAYWRIGHT_PROXY_CONFIG ? PLAYWRIGHT_PROXY_CONFIG : undefined
    });

    // Create a fresh context (Stateless for GitHub Actions)
    const context = await browser.newContext({
        userAgent: DEFAULT_UA,
        viewport: { width: 1280, height: 800 },
        locale: 'tr-TR',
        extraHTTPHeaders: {
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7'
        }
    });
    await applyCookiesFromEnv(context);
    context.setDefaultTimeout(60000);
    context.setDefaultNavigationTimeout(60000);

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

            if (USE_SCRAPE_DO_API) {
                console.log(`[${source.name}] Fetching page via Scrape.do API...`);
                // For main pages, we fetch HTML via API then setContent to bypass Cloudflare
                const html = await fetchHtmlViaScrapeDo(source.url);
                if (html && html.length > 1000) {
                    await mainPage.setContent(html, { waitUntil: 'domcontentloaded' });
                    // Inject a base tag to handle relative links if needed
                    await mainPage.evaluate((baseUrl) => {
                        const base = document.createElement('base');
                        base.href = baseUrl;
                        document.head.prepend(base);
                    }, source.url);
                } else {
                    console.warn(`[${source.name}] Scrape.do returned empty/invalid HTML. Fallback to direct navigation.`);
                    await mainPage.goto(source.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
                }
            } else {
                await mainPage.goto(source.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
            }

            await mainPage.waitForTimeout(1500);

            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `nav_${safeName}.png`, fullPage: false });

            if (source.name === 'r10') {
                try {
                    await mainPage.waitForSelector('a[href="#tab-sonAcilan"]', { timeout: 10000 });
                    await mainPage.evaluate(() => {
                        const tab = document.querySelector('a[href="#tab-sonAcilan"]');
                        if (tab) tab.click();
                    });
                    // Wait for the AJAX content to load specifically
                    await mainPage.waitForResponse(response =>
                        response.url().includes('ajax.php') && response.status() === 200,
                        { timeout: 10000 }
                    ).catch(() => { }); // catch timeout if it already loaded

                    await mainPage.waitForTimeout(2000);
                } catch (e) {
                    console.warn(`[r10] Tab click non-fatal error: ${e.message}`);
                }
            }

            // Anti-bot & Challenge check
            await handleAntiBot(mainPage);

            let containerFound = false;
            try {
                await mainPage.waitForSelector(source.containerSelector, { timeout: 10000 });
                containerFound = true;
            } catch (e) {
                await mainPage.waitForLoadState('networkidle', { timeout: 12000 }).catch(() => { });
                try {
                    await mainPage.waitForSelector(source.containerSelector, { timeout: 8000 });
                    containerFound = true;
                } catch {
                    const title = await mainPage.title();
                    console.warn(`[${source.name}] Container not found. Page Title: "${title}"`);
                    if (title.includes('Just a moment') || title.includes('Cloudflare')) {
                        console.log(`[${source.name}] Challenge detected. Pausing...`);
                        await mainPage.waitForTimeout(10000);
                    }
                    console.warn(`[${source.name}] Container not found, trying alternate approach...`);

                    // Save debug info
                    const content = await mainPage.content();
                    require('fs').writeFileSync(`debug_${safeName}.html`, content);
                    await mainPage.screenshot({ path: `debug_${safeName}.png`, fullPage: true });
                }
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
            let skippedCount = 0;
            for (const thread of threads) {
                // Stop if we have seen 10 consecutive old topics
                if (skippedCount >= 10) {
                    console.log(`[${source.name}] Stopped early: 10 consecutive old topics found.`);
                    break;
                }

                if (SEEN_URLS.has(thread.url)) {
                    skippedCount++;
                    continue;
                }
                skippedCount = 0; // reset if we find a new one

                if (shouldPrefilterSkip({ title: thread.title, url: thread.url })) {
                    console.log(`  [SKIP:PREFILTER] ${thread.title}`);
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
            // Don't close mainPage here if persistent? actually we should.
            await mainPage.close().catch(() => { });
        }
    }

    await scrapeFeedSources(context);

    saveSeenUrls();
    await browser.close();
    console.log("\n=== Scrape finished ===");
}

scrape();
