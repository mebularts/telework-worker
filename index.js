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
const iconv = require('iconv-lite');
const cheerio = require('cheerio');

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

const MAX_THREADS_PER_SOURCE = 20;
const MIN_CONTENT_LENGTH = 40;
const MAX_CONTENT_LENGTH = 4000;
const ALLOW_MARKETPLACE = process.env.SCRAPER_ALLOW_MARKETPLACE === '1';
const DISABLE_JOB_FILTER = process.env.SCRAPER_DISABLE_JOB_FILTER === '1';
const MAX_THREAD_AGE_HOURS = Number(process.env.SCRAPER_MAX_THREAD_AGE_HOURS || '24');
const STRICT_DATE = process.env.SCRAPER_STRICT_DATE === '1';
const SCRAPER_DETAIL_CONCURRENCY = Number(process.env.SCRAPER_DETAIL_CONCURRENCY || '3');
const SCRAPER_DETAIL_DELAY_MS = Number(process.env.SCRAPER_DETAIL_DELAY_MS || '200');
const SCRAPER_PREFLIGHT = process.env.SCRAPER_PREFLIGHT !== '0';
const SCRAPER_PREFLIGHT_TIMEOUT_MS = Number(process.env.SCRAPER_PREFLIGHT_TIMEOUT_MS || '8000');
const UPWORK_ENABLED = process.env.UPWORK_ENABLED === '1';
const UPWORK_MAX_CATEGORIES = Number(process.env.UPWORK_MAX_CATEGORIES || '500');
const UPWORK_MAX_JOBS_PER_CATEGORY = Number(process.env.UPWORK_MAX_JOBS_PER_CATEGORY || '100');
const UPWORK_DELAY_MS = Number(process.env.UPWORK_DELAY_MS || '400');
const UPWORK_TIMEOUT_MS = Number(process.env.UPWORK_TIMEOUT_MS || '20000');
const UPWORK_RETRIES = Number(process.env.UPWORK_RETRIES || '1');
const UPWORK_BACKOFF_MS = Number(process.env.UPWORK_BACKOFF_MS || '800');
const UPWORK_CATEGORY_CONCURRENCY = Number(process.env.UPWORK_CATEGORY_CONCURRENCY || '2');
const UPWORK_CATEGORY_SLUGS = (process.env.UPWORK_CATEGORY_SLUGS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
const UPWORK_USE_SCRAPE_DO = process.env.UPWORK_USE_SCRAPE_DO === '1';
// Static category list captured from scraper/upwork-cat.html
const UPWORK_STATIC_CATEGORIES = [
    '2d-game-art',
    '3d-cad-design',
    '3d-design',
    '3d-modeling',
    '3d-printing',
    '3d-rendering',
    '3d-visualizations',
    'academic-editing',
    'academic-proofreading',
    'accounting',
    'acrylic-painting',
    'acting',
    'ad-creative',
    'administrative-support',
    'adobe-acrobat',
    'adobe-after-effects',
    'adobe-illustrator',
    'adobe-indesign',
    'adobe-photoshop',
    'adobe-photoshop-lightroom',
    'adobe-premiere-pro',
    'advertising',
    'affiliate-marketing',
    'airtable',
    'album-cover-design',
    'alexa-skill-kit',
    'algebra',
    'amazon-ec2',
    'amazon-fba',
    'amazon-seller-central',
    'amazon-web-services',
    'amazon-webstore',
    'android-app-development',
    'android-studio',
    'animation',
    'anime',
    'ansys',
    'apache-spark',
    'apa-formatting',
    'api-development',
    'apple-xcode',
    'app-store',
    'ap-style-writing',
    'arabic',
    'arbitration',
    'arcgis',
    'architectural-design',
    'architectural-rendering',
    'arduino',
    'article-writing',
    'artificial-intelligence',
    'arts',
    'aso',
    'aspnet',
    'audio-editing',
    'audio-mixing',
    'audio-production',
    'autocad',
    'autocad-civil-3d',
    'autodesk-maya',
    'autodesk-revit',
    'automotive-design',
    'babylon-js',
    'bahasa-indonesia',
    'bigquery',
    'blender3d',
    'blockchain',
    'blog-writing',
    'book-cover-design',
    'book-design',
    'bookkeeping',
    'bot-development',
    'branding',
    'brochure-design',
    'bulgarian',
    'business-coaching',
    'business-law',
    'business-presentation',
    'business-process-modelling',
    'buying',
    'cad',
    'cad-design',
    'cad-drafting',
    'calendar-management',
    'career',
    'cartography',
    'catalog',
    'certified-public-accountant',
    'character-design',
    'chatbot-development',
    'chat-support',
    'chemistry',
    'chief-architect',
    'childrens-book-illustrator',
    'chrome-extension',
    'circuit-design',
    'cloud-computing',
    'codeigniter',
    'cold-calling',
    'comic-art',
    'company-profile',
    'compositing',
    'computational-fluid-dynamics',
    'computer-engineering',
    'computer-vision',
    'constant-contact',
    'consulting',
    'contact-lists',
    'content-marketing',
    'content-strategy',
    'content-writing',
    'contract-drafting',
    'contract-law',
    'control-engineering',
    'cooking',
    'copy-editing',
    'copyright',
    'copywriting',
    'corel-draw',
    'c-plus-plus',
    'creative-writing',
    'criminal-law',
    'crm',
    'c-sharp',
    'css',
    'd3-js',
    'data-analysis',
    'data-analytics',
    'database-design',
    'database-design-and-construction',
    'data-cleansing',
    'data-entry',
    'data-extraction',
    'data-mining',
    'data-science',
    'data-scraping',
    'data-visualization',
    'deep-learning',
    'delphi',
    'desktop-publishing',
    'devops',
    'dietetics',
    'digital-art',
    'digital-design',
    'digital-marketing',
    'digital-signal-processing',
    'django',
    'docker',
    'drafting',
    'drawing',
    'drop-shipping',
    'ebay-listing-writing',
    'ebook-writing',
    'editing',
    'electrical-drawing',
    'electrical-engineering',
    'electronics',
    'elementor',
    'email-handling',
    'employment-law',
    'engineering-drawing',
    'english-india',
    'english-proofreading',
    'e-pub-formatting',
    'erlang',
    'erpnext',
    'error-detection',
    'esp32',
    'essay-writing',
    'estimating',
    'estimator',
    'estonian',
    'etsy-administration',
    'event-planning',
    'excel',
    'facebook-ads-manager',
    'family-law',
    'fashion-designing',
    'fashion-illustration',
    'ffmpeg',
    'figma',
    'filipino',
    'financial-analysis',
    'financial-modeling',
    'financial-writing',
    'flutter',
    'flyer-design',
    'forex-trading',
    'french',
    'frontend-development',
    'fusion-360',
    'game-design',
    'genesis-framework',
    'german',
    'german-germany',
    'ghostwriting',
    'gif',
    'gis',
    'google-adwords',
    'google-analytics',
    'google-apps-script',
    'google-cloud-platform',
    'google-data-studio',
    'google-sheets',
    'google-sketchup',
    'grammar',
    'grant-writing',
    'graphic-design',
    'guitar-family',
    'haskell',
    'high-ticket-closing',
    'html',
    'hubspot',
    'ibm-spss',
    'icd-coding',
    'illustration',
    'imageobject-recognition',
    'immigration-law',
    'industrial-design',
    'information-security',
    'injection-mold-design',
    'instagram',
    'instructional-design',
    'intellectual-property-law',
    'interior-architecture',
    'interior-design',
    'international-law',
    'intuit-quickbooks',
    'ios-development',
    'iphone-app-development',
    'italian',
    'it-outsourcing',
    'japanese',
    'javascript',
    'jewelry-design',
    'job-description-writing',
    'kindle-direct-publishing',
    'korean',
    'labview',
    'landscape-design',
    'latex',
    'latvian',
    'lead-generation',
    'lead-lists',
    'legal',
    'legal-assistance',
    'legal-consulting',
    'legal-research',
    'legal-transcription',
    'legal-writing',
    'line-editing',
    'link-building',
    'linkedin',
    'linux',
    'literature-review',
    'logo',
    'logo-animation',
    'logo-design',
    'lua',
    'lyrics-video',
    'lyrics-writing',
    'magento',
    'malaysian',
    'male-voice-over',
    'management-consulting',
    'marketing',
    'marketing-presentation',
    'matlab',
    'mechanical-engineering',
    'medical',
    'medical-billing-coding',
    'medical-illustration',
    'medical-transcription',
    'microsoft-dynamics-365',
    'microsoft-excel',
    'microsoft-excel-powerpivot',
    'microsoft-power-bi',
    'microsoft-powerpoint',
    'microsoft-project',
    'microsoft-sql-server-development',
    'microsoft-teams',
    'microsoft-visio',
    'microsoft-word',
    'mobile-app-design',
    'mobile-app-development',
    'mql4',
    'ms-office-365',
    'music-composition',
    'music-producer',
    'narration',
    'non-fiction-writing',
    'nopcommerce',
    'oberlo',
    'ocr-tesseract',
    'odoo',
    'online',
    'online-writing',
    'on-page-optimization',
    'ontology',
    'outbound-sales',
    'packaging-design',
    'pages',
    'page-speed-optimization',
    'paralegal',
    'pay-per-click',
    'pcb-design',
    'pdf-conversion',
    'performance-tuning',
    'photo-editing',
    'photography',
    'photorealistic-rendering',
    'photo-retouching',
    'php',
    'piano-composition',
    'pitch-decks',
    'podcasting',
    'poetry',
    'portait-painting',
    'postgresql',
    'powerpoint',
    'powershell',
    'presentation-design',
    'product-descriptions',
    'product-design',
    'product-formulation',
    'product-photography',
    'product-upload',
    'programmatic-campaigns',
    'project-management',
    'proofreading',
    'proposal-writing',
    'python',
    'python-script',
    'pytorch',
    'qt',
    'quality-control',
    'r',
    'raspberry-pi',
    'react-js',
    'react-native',
    'real-estate',
    'real-estate-acquisition',
    'reaper',
    'recipe-writing',
    'recruiting',
    'research',
    'research-papers',
    'responsive-web-design',
    'resume',
    'resume-design',
    'resume-writing',
    'reverse-engineering',
    'romance',
    'romanian',
    'ruby-on-rails',
    'salesforce',
    'salesforce-app-development',
    'salesforce-lightning',
    'sales-writing',
    'sas',
    'scientific-research',
    'scientific-writing',
    'screenplay',
    'script',
    'scriptwriting',
    'selenium',
    'selenium-webdriver',
    'seo',
    'seo-backlinking',
    'seo-keyword-research',
    'seo-writing',
    'sewing',
    'sharepoint',
    'shopify',
    'shopware',
    'short-story',
    'signage',
    'singing',
    'sitecore',
    'sketching',
    'smart-contracts',
    'social-media-design',
    'social-media-management',
    'social-media-marketing',
    'software-qa-testing',
    'solidworks',
    'spanish',
    'spanish-mexico',
    'spine',
    'sports-writing',
    'spreadsheets',
    'sql',
    'squarespace',
    'ssl',
    'startup-consulting',
    'stata',
    'statistics',
    'stock-management',
    'storyboard',
    'structural-engineering',
    'survey',
    'svg',
    'tableau',
    'task-coordination',
    'tax-law',
    'tax-preparation',
    'teaching-mathematics',
    'technical-support',
    'technical-writing',
    'textile-design',
    'thai',
    'time-management',
    'trademarks',
    'tradestation',
    'transcription',
    'translation',
    'translation-english-arabic',
    'translation-english-dutch',
    'translation-english-german',
    'translation-english-italian',
    'translation-english-korean',
    'translation-english-malay',
    'translation-english-polish',
    'translation-english-portuguese',
    'translation-english-spanish',
    'translation-japanese-english',
    'translation-korean-english',
    'tutoring',
    'typesetting',
    'ui-design',
    'ukrainian',
    'unity3d',
    'unity-3d',
    'user-experience-design',
    'vba',
    'vb-net',
    'vector-art',
    'vector-illustration',
    'vector-tracing',
    'verilog',
    'vfx-animation',
    'video-editing',
    'videography',
    'video-production',
    'virtual',
    'virtual-assistant',
    'visual-design',
    'voice-acting',
    'voice-over',
    'voice-over-american-accent',
    'voice-recording',
    'voice-talent',
    'vue-js',
    'vulnerability-assessment',
    'web-application',
    'web-content',
    'web-content-management',
    'web-crawler',
    'web-design',
    'webflow',
    'webgl',
    'webrtc',
    'web-scraping',
    'web-services',
    'website',
    'website-copywriting',
    'website-development',
    'website-redesign',
    'web-testing',
    'whiteboard-animation',
    'wix',
    'woocommerce',
    'wordpress',
    'wordpress-theme',
    'writing',
    'xactimate',
    'xamarin',
    'xero',
    'youtube',
    'youtube-api',
    'zapier',
    'zoom-video-conferencing'
];

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
    '/is-verenler', '/is-verenler/', '/is-verenler',
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
    /\byaptirilir\b/i,
    /\byaptirilacaktir\b/i,
    /\bistiyorum\b/i,
    /\bi̇stiyorum\b/i,
    /\bistiyoruz\b/i,
    /\bgerekli\b/i,
    /\blazim\b/i,
    /\blazım\b/i,
    /\bihtiyacimiz\b/i,
    /\bihtiyacımız\b/i,
    /\bihtiyacim\b/i,
    /\bihtiyacım\b/i,
    /\bvar mi\b/i,
    /\bvar mı\b/i,
    /\b(biri|birisi)\s+var\s+m[ıi]\b/i,
    /\byapabilecek\b/i,
    /\byapabilecek\s+biri\b/i,
    /\byardimci\s+olacak\b/i,
    /\byardımcı\s+olacak\b/i,
    /\bteklif\b/i,
    /\bfiyat\s+teklifi\b/i
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

const REQ_TR = [
    /\byapt[ıi]r(mak|aca[kğ]|ilacak)\b/i,
    /\bihtiya[çc]\b/i,
    /\baran[ıi]yor\b/i,
    /\bteklif\b/i,
    /\bfiyat teklifi\b/i,
    /\bb[uü]t[çc]e\b/i,
    /\bacil\b/i,
    /\b(yapabilecek|yard[ıi]mc[ıi])\b/i,
    /\b(al[ıi]nacak|aran[ıi]yor|aran[ıi]r)\b/i
];

const OFF_TR = [
    /\bsat[ıi]l[ıi]k\b/i,
    /\bsat[ıi]yorum\b/i,
    /\bhizmet veriyorum\b/i,
    /\bsunuyorum\b/i,
    /\bpaket(ler)?im\b/i,
    /\bindirim\b/i,
    /\bkampanya\b/i,
    /\blisans(l[ıi])?\b/i,
    /\bscript sat[ıi]/i,
    /\bhaz[ıi]r sistem\b/i,
    /\bnumara onay\b/i,
    /\btakip[çc]i\b/i,
    /\babone\b/i,
    /\bbeğeni\b/i,
    /\bsmm\b/i
];

const REQ_EN = [
    /\blooking for\b/i,
    /\bneed (a|an)\b/i,
    /\bhiring\b/i,
    /\bseeking\b/i,
    /\bwanted\b/i,
    /\bwtb\b/i,
    /\blf\b/i
];

const OFF_EN = [
    /\bi will\b/i,
    /\boffering\b/i,
    /\bfor sale\b/i,
    /\bservice\b/i,
    /\bwts\b/i,
    /\bfor hire\b/i
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
    /\breklam\b/i,
    /\btanitim\b/i,
    /\bkampanya\b/i,
    /\bindirim\b/i,
    /\bfirsat\b/i,
    /\bpaket\b/i,
    /\bbayilik\b/i,
    /\bkupon\b/i,
    /\bpromosyon\b/i,
    /\bhediye\b/i,
    /\bgaranti\b/i,
    /\bgarantili\b/i,
    /\bstok\b/i,
    /\btoplu\b/i,
    /\bsponsor\b/i,
    /\bhizmetleri\b/i,
    /\bnumara onay\b/i,
    /\btakip[çc]i\b/i,
    /\babone\b/i,
    /\bbeğeni\b/i,
    /\bsmm\b/i,
    /\bbacklink paketi\b/i,
    /\bscripti\b/i,
    /\byazilimi\b/i,
    /\btemasi\b/i,
    /\bhosting\b/i,
    /\bsunucu\b/i,
    /\bdomain\b/i,
    /\blisans\b/i,
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
        contentSelector: CONTENT_SELECTORS.r10,
        maxThreads: 20,
        minContentLength: 40,
        useScrapeDo: true,
        useScrapeDoApi: true
    },
    {
        name: 'wmaraci',
        url: 'https://wmaraci.com/',
        containerSelector: '.forumLastSubject .content ul',
        threadSelector: '.forumLastSubject .content ul li.open span a[href*="/forum/"]',
        contentSelector: CONTENT_SELECTORS.wmaraci,
        maxThreads: 20,
        minContentLength: 60,
        useScrapeDo: false,
        useScrapeDoApi: false
    },
    {
        name: 'bhw',
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer.76/',
        containerSelector: '.structItem--thread, .structItem',
        threadSelector: '.structItem-title a, .block-row .contentRow-title a',
        contentSelector: CONTENT_SELECTORS.bhw,
        maxThreads: 20,
        minContentLength: 40,
        useScrapeDo: true,
        useScrapeDoApi: true
    }
];

const FEED_SOURCES = [
    {
        name: 'r10-sitemap',
        type: 'sitemap',
        url: 'https://www.r10.net/sitemap.xml',
        allowViewSource: false,
        contentSelector: CONTENT_SELECTORS.r10,
        emitAs: 'r10',
        prefilter: 'title',
        maxThreads: 20,
        maxItems: 20,
        minContentLength: 40,
        useScrapeDo: true
    },
    {
        name: 'wmaraci-sitemap',
        type: 'sitemap',
        url: 'https://wmaraci.com/sitemap/forum.xml',
        contentSelector: CONTENT_SELECTORS.wmaraci,
        emitAs: 'wmaraci',
        prefilter: 'title',
        maxThreads: 20,
        maxItems: 20,
        minContentLength: 60,
        useScrapeDo: false
    },
    {
        name: 'bhw-rss',
        type: 'rss',
        url: 'https://www.blackhatworld.com/forums/hire-a-freelancer.76/index.rss',
        contentSelector: CONTENT_SELECTORS.bhw,
        emitAs: 'bhw',
        resolveUrl: resolveBhwRssUrl,
        prefilter: 'title',
        maxThreads: 20,
        maxItems: 20,
        minContentLength: 40,
        useScrapeDo: true
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

function countHit(patterns, text) {
    let count = 0;
    for (const pattern of patterns) {
        if (pattern.test(text)) {
            count += 1;
        }
    }
    return count;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function withJitter(ms, spread = 150) {
    if (!ms || ms <= 0) return 0;
    const extra = Math.floor(Math.random() * spread);
    return ms + extra;
}

async function mapWithConcurrency(items, limit, mapper) {
    const results = new Array(items.length);
    let nextIndex = 0;
    let running = 0;

    return new Promise((resolve) => {
        const runNext = () => {
            if (nextIndex >= items.length && running === 0) {
                resolve(results);
                return;
            }
            while (running < limit && nextIndex < items.length) {
                const currentIndex = nextIndex++;
                running++;
                Promise.resolve(mapper(items[currentIndex], currentIndex))
                    .then(result => {
                        results[currentIndex] = result;
                    })
                    .catch(() => {
                        results[currentIndex] = null;
                    })
                    .finally(() => {
                        running--;
                        runNext();
                    });
            }
        };
        runNext();
    });
}

function toArray(value) {
    if (!value) return [];
    return Array.isArray(value) ? value : [value];
}

function parseThreadDate(dateText) {
    if (!dateText) return null;
    const text = String(dateText).trim();
    if (!text) return null;

    const lower = text.toLowerCase();
    let base = new Date();
    if (lower.includes('bugün') || lower.includes('today')) {
        // today
    } else if (lower.includes('dün') || lower.includes('yesterday')) {
        base = new Date(Date.now() - 24 * 3600 * 1000);
    } else {
        const m = /(\d{1,2})[.\/-](\d{1,2})[.\/-](\d{2,4})/.exec(text);
        if (m) {
            const day = Number(m[1]);
            const month = Number(m[2]) - 1;
            let year = Number(m[3]);
            if (year < 100) year += 2000;
            base = new Date(year, month, day);
        } else {
            const parsed = new Date(text);
            if (!isNaN(parsed)) return parsed;
            return null;
        }
    }

    const tm = /(\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(text);
    if (tm) {
        base.setHours(Number(tm[1]), Number(tm[2]), Number(tm[3] || 0), 0);
    } else {
        base.setHours(0, 0, 0, 0);
    }
    return base;
}

function isTooOld(dateText) {
    if (!MAX_THREAD_AGE_HOURS || MAX_THREAD_AGE_HOURS <= 0) return false;
    const d = parseThreadDate(dateText);
    if (!d) return STRICT_DATE;
    const diffMs = Date.now() - d.getTime();
    return diffMs > MAX_THREAD_AGE_HOURS * 3600 * 1000;
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
            'no_restriction': 'None',
            '0': 'None',
            '1': 'Lax',
            '2': 'Strict',
            '-1': undefined
        };
        const key = String(c.sameSite).toLowerCase();
        const mapped = map[key];
        if (mapped) {
            c.sameSite = mapped;
        } else if (mapped === undefined) {
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
    if (!preserveViewSource && cleaned.toLowerCase().startsWith('view-source:')) {
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

function deriveTitleFromUrl(url) {
    if (!url) return '';
    try {
        const u = new URL(url);
        let segment = u.pathname.split('/').filter(Boolean).pop() || '';
        segment = segment.replace(/\.html?$/i, '');
        segment = segment.replace(/-new-post$/i, '');
        segment = segment.replace(/^\d+-/, '');
        segment = segment.replace(/-\d+$/i, '');
        segment = segment.replace(/[-_]+/g, ' ');
        try {
            segment = decodeURIComponent(segment);
        } catch {
            // ignore decoding issues
        }
        segment = segment.replace(/\s+/g, ' ').trim();
        if (segment.length < 4) return '';
        return segment;
    } catch {
        return '';
    }
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

function detectCharsetFromHeaders(headers) {
    if (!headers) return '';
    const ct = headers['content-type'] || headers['Content-Type'] || '';
    const m = /charset=([^;]+)/i.exec(ct);
    return m ? m[1].trim().toLowerCase() : '';
}

function detectCharsetFromMeta(text) {
    if (!text) return '';
    let m = /<meta[^>]+charset=["']?([^"'>\s;]+)/i.exec(text);
    if (m && m[1]) return m[1].trim().toLowerCase();
    m = /<meta[^>]+http-equiv=["']content-type["'][^>]+content=["'][^"']*charset=([^"'>\s;]+)/i.exec(text);
    return m && m[1] ? m[1].trim().toLowerCase() : '';
}

function normalizeCharset(charset) {
    if (!charset) return '';
    const c = charset.toLowerCase();
    if (c === 'utf8') return 'utf-8';
    if (c === 'iso-8859-9' || c === 'latin5') return 'windows-1254';
    return c;
}

function decodeResponseBody(data, headers) {
    if (!data) return '';
    if (typeof data === 'string') return data;
    const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
    let charset = normalizeCharset(detectCharsetFromHeaders(headers));
    if (!charset) {
        const sniff = buffer.toString('latin1');
        charset = normalizeCharset(detectCharsetFromMeta(sniff));
    }
    if (!charset || !iconv.encodingExists(charset)) {
        charset = 'utf-8';
    }
    try {
        return iconv.decode(buffer, charset);
    } catch {
        return buffer.toString('utf8');
    }
}

function isUpworkBlockedHtml(html) {
    if (!html) return true;
    if (html.length < 5000) {
        const lower = html.toLowerCase();
        if (lower.includes('up-challenge-container')) return true;
        if (lower.includes('cf-chl') || lower.includes('cloudflare')) return true;
        if (lower.includes('enable javascript') && lower.includes('cookies')) return true;
        if (lower.includes('challenge - upwork')) return true;
        if (lower.includes('access denied')) return true;
    }
    // If it's a "Jobs" page but doesn't have any job tiles, it might be a soft block or empty
    if (html.includes('job-grid') && !html.includes('job-tile')) return true;
    return false;
}

async function fetchHtmlViaBrowser(url, context) {
    if (!context) return null;
    const page = await context.newPage();
    try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await handleAntiBot(page);
        await page.waitForTimeout(1500);
        return await page.content();
    } catch (e) {
        console.warn(`  [Upwork] Browser fetch failed: ${e.message.split('\n')[0]}`);
        return null;
    } finally {
        await page.close().catch(() => { });
    }
}

async function fetchUpworkHtml(url, context, options = {}) {
    const timeoutMs = Number(options.timeoutMs ?? UPWORK_TIMEOUT_MS);
    const retries = Number(options.retries ?? UPWORK_RETRIES);
    const backoffBaseMs = Number(options.backoffBaseMs ?? UPWORK_BACKOFF_MS);
    const useScrapeDo = options.useScrapeDo ?? UPWORK_USE_SCRAPE_DO;

    const headers = {
        'User-Agent': DEFAULT_UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,tr;q=0.8',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    };

    if (useScrapeDo && process.env.SCRAPE_DO_TOKEN) {
        // Upwork requires render and super mode for reliable results
        const isUpwork = url.includes('upwork.com');
        const html = await fetchHtmlViaScrapeDo(url, {
            render: isUpwork,
            super: isUpwork
        });
        if (html && !isUpworkBlockedHtml(html)) {
            return html;
        }
    }

    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            const reqConfig = {
                timeout: timeoutMs,
                headers,
                responseType: 'arraybuffer'
            };
            if (useScrapeDo && !USE_SCRAPE_DO_API) {
                reqConfig.proxy = AXIOS_PROXY_CONFIG;
            } else {
                reqConfig.proxy = false;
            }

            const response = await axios.get(url, reqConfig);
            const html = decodeResponseBody(response.data, response.headers);
            if (!html) throw new Error('empty response');
            if (isUpworkBlockedHtml(html)) throw new Error('blocked');
            return html;
        } catch (e) {
            const status = e?.response?.status;
            const retryable = status === 429 || (status >= 500 && status <= 599) || !status;
            if (attempt < retries && retryable) {
                const waitMs = backoffBaseMs * Math.pow(2, attempt);
                console.warn(`  [Upwork] Retry ${attempt + 1}/${retries} in ${waitMs}ms: ${e.message.split('\n')[0]}`);
                await sleep(waitMs);
                continue;
            }
            break;
        }
    }

    const html = await fetchHtmlViaBrowser(url, context);
    if (html && !isUpworkBlockedHtml(html)) {
        return html;
    }
    return null;
}

function normalizeUpworkSlug(raw) {
    if (!raw) return '';
    const text = String(raw).trim();
    if (!text) return '';

    try {
        const u = new URL(text);
        const m = u.pathname.match(/\/freelance-jobs\/([^/]+)\/?/i);
        if (m) return m[1].trim();
    } catch {
        // ignore
    }

    const m = text.match(/freelance-jobs\/([^/]+)\/?/i);
    if (m) return m[1].trim();
    return text.replace(/^\/+|\/+$/g, '').trim();
}

function parseUpworkCategorySlugs(html) {
    if (!html) return [];
    const $ = cheerio.load(html);
    const slugs = new Set();

    $('a[href^="/freelance-jobs/"], a[href^="https://www.upwork.com/freelance-jobs/"]').each((_, el) => {
        const href = ($(el).attr('href') || '').trim();
        if (!href) return;
        if (href.includes('/freelance-jobs/apply/')) return;

        let path = href;
        try {
            if (href.startsWith('http')) {
                const u = new URL(href);
                path = u.pathname || href;
            }
        } catch {
            // ignore
        }

        const m = path.match(/^\/freelance-jobs\/([^/]+)\/?$/i);
        if (m && m[1]) {
            slugs.add(m[1].trim());
        }
    });

    return Array.from(slugs);
}

function parseUpworkJobsFromCategoryHtml(html, categorySlug) {
    if (!html) return [];
    const $ = cheerio.load(html);
    const jobs = [];

    const cleanText = (value) => String(value || '').replace(/\s+/g, ' ').trim();
    const uniq = (arr) => Array.from(new Set(arr.filter(Boolean)));

    $('section[data-qa="job-tile"]').each((_, el) => {
        const tile = $(el);
        const titleEl = tile.find('a[data-qa="job-title"]').first();
        const title = cleanText(titleEl.text());
        let url = cleanText(titleEl.attr('href') || '');
        if (url && url.startsWith('/')) {
            url = `https://www.upwork.com${url}`;
        }

        const description = cleanText(tile.find('p[data-qa="job-description"]').first().text());
        const badgeText = cleanText(tile.find('.air3-badge-feature').first().text());
        const isNew = badgeText.toLowerCase() === 'new';

        const smallTexts = tile.find('small').map((i, s) => cleanText($(s).text())).get().filter(Boolean);
        const postedText = smallTexts.find(t => /posted/i.test(t)) || '';
        const jobType = smallTexts.find(t => /hourly|fixed/i.test(t)) || (smallTexts[0] || '');

        const hoursNeeded = cleanText(tile.find('div[data-qa="hours-needed"] strong[data-qa="value"]').first().text());
        const duration = cleanText(tile.find('div[data-qa="duration"] strong[data-qa="value"]').first().text());
        const experienceLevel = cleanText(tile.find('div[data-qa="expert-level"] strong[data-qa="value"]').first().text());

        const skills = uniq(tile.find('span[data-qa="ontology-skill"], span[data-qa="legacy-skill"]').map((i, s) => cleanText($(s).text())).get());

        let budget = '';
        const fixedPrice = tile.find('small[data-qa="fixed-price"]').first();
        if (fixedPrice.length > 0) {
            budget = cleanText(fixedPrice.parent().find('strong').first().text());
        }
        if (!budget) {
            // Check for strong tags containing dollar signs (common in new layout)
            const strongest = tile.find('strong').filter((i, el) => $(el).text().includes('$')).first();
            if (strongest.length > 0) {
                budget = cleanText(strongest.text());
            }
        }
        if (!budget) {
            const m = tile.text().match(/\$\s?[\d,]+(?:\.\d+)?/);
            if (m) budget = cleanText(m[0]);
        }

        let externalId = '';
        if (url) {
            const idMatch = url.match(/_~([0-9a-zA-Z]+)/) || url.match(/~([0-9a-zA-Z]+)/);
            externalId = idMatch ? idMatch[1] : '';
        }

        if (!title || !url) return;

        jobs.push({
            source: 'upwork',
            category: categorySlug || '',
            title,
            url,
            externalId,
            isNew,
            jobType,
            postedText,
            hoursNeeded,
            duration,
            experienceLevel,
            budget,
            description,
            skills
        });
    });

    return jobs;
}

function buildUpworkJobContent(job) {
    const parts = [];
    if (job.description) parts.push(job.description);
    if (job.jobType) parts.push(`Type: ${job.jobType}`);
    if (job.postedText) parts.push(`Posted: ${job.postedText}`);
    if (job.budget) parts.push(`Budget: ${job.budget}`);
    if (job.hoursNeeded) parts.push(`Hours: ${job.hoursNeeded}`);
    if (job.duration) parts.push(`Duration: ${job.duration}`);
    if (job.experienceLevel) parts.push(`Experience: ${job.experienceLevel}`);
    if (job.skills && job.skills.length > 0) parts.push(`Skills: ${job.skills.join(', ')}`);
    if (job.category) parts.push(`Category: ${job.category}`);
    return parts.join('\n').trim();
}

async function scrapeUpwork(context) {
    if (!UPWORK_ENABLED) return;
    console.log('\n=== UPWORK ===');

    const maxCategories = Number.isFinite(UPWORK_MAX_CATEGORIES) && UPWORK_MAX_CATEGORIES > 0 ? UPWORK_MAX_CATEGORIES : 10;
    const maxJobsPerCategory = Number.isFinite(UPWORK_MAX_JOBS_PER_CATEGORY) && UPWORK_MAX_JOBS_PER_CATEGORY > 0 ? UPWORK_MAX_JOBS_PER_CATEGORY : 50;
    const delayMs = Number.isFinite(UPWORK_DELAY_MS) && UPWORK_DELAY_MS >= 0 ? UPWORK_DELAY_MS : 1200;

    let slugs = [];
    if (UPWORK_CATEGORY_SLUGS.length > 0) {
        slugs = UPWORK_CATEGORY_SLUGS.map(normalizeUpworkSlug).filter(Boolean);
        console.log(`Using UPWORK_CATEGORY_SLUGS (${slugs.length})`);
    } else if (UPWORK_STATIC_CATEGORIES.length > 0) {
        slugs = UPWORK_STATIC_CATEGORIES.slice();
        console.log(`Using static Upwork categories (${slugs.length})`);
    } else {
        const categoriesUrl = 'https://www.upwork.com/freelance-jobs/';
        console.log(`Fetching categories: ${categoriesUrl}`);
        const html = await fetchUpworkHtml(categoriesUrl, context, {
            retries: UPWORK_RETRIES,
            backoffBaseMs: UPWORK_BACKOFF_MS,
            timeoutMs: UPWORK_TIMEOUT_MS,
            useScrapeDo: UPWORK_USE_SCRAPE_DO
        });
        if (!html) {
            console.warn('  [Upwork] Categories page blocked or empty.');
            return;
        }
        slugs = parseUpworkCategorySlugs(html);
    }

    const categoriesFound = slugs.length;
    if (categoriesFound === 0) {
        console.warn('  [Upwork] No categories found.');
        return;
    }

    const targets = slugs.slice(0, maxCategories);
    console.log(`Categories found: ${categoriesFound}. Crawling: ${targets.length}`);

    const jobsToSend = [];
    const seenLocal = new Set();
    let jobsParsed = 0;
    let duplicatesSkipped = 0;
    let blockedPages = 0;

    const catConcurrency = Math.max(1, UPWORK_CATEGORY_CONCURRENCY);
    await mapWithConcurrency(targets, catConcurrency, async (slug) => {
        if (delayMs > 0) {
            await sleep(withJitter(delayMs, 200));
        }
        const categoryUrl = `https://www.upwork.com/freelance-jobs/${slug}/`;
        console.log(`  [Upwork] Fetching: ${categoryUrl}`);

        const html = await fetchUpworkHtml(categoryUrl, context, {
            retries: UPWORK_RETRIES,
            backoffBaseMs: UPWORK_BACKOFF_MS,
            timeoutMs: UPWORK_TIMEOUT_MS,
            useScrapeDo: UPWORK_USE_SCRAPE_DO
        });
        if (!html) {
            blockedPages += 1;
            console.warn(`  [Upwork] Blocked/empty: ${slug}`);
            return null;
        }

        const parsed = parseUpworkJobsFromCategoryHtml(html, slug);
        jobsParsed += parsed.length;

        const limited = parsed.slice(0, maxJobsPerCategory);
        for (const job of limited) {
            if (!job.url || !job.title) continue;
            if (SEEN_URLS.has(job.url) || seenLocal.has(job.url)) {
                duplicatesSkipped += 1;
                continue;
            }
            seenLocal.add(job.url);
            SEEN_URLS.add(job.url);

            let content = buildUpworkJobContent(job);
            if (content.length > MAX_CONTENT_LENGTH) {
                content = content.slice(0, MAX_CONTENT_LENGTH);
            }

            jobsToSend.push({
                title: job.title,
                url: job.url,
                original_content: content,
                source: 'upwork',
                category: job.category,
                meta: {
                    externalId: job.externalId,
                    isNew: job.isNew,
                    jobType: job.jobType,
                    postedText: job.postedText,
                    hoursNeeded: job.hoursNeeded,
                    duration: job.duration,
                    experienceLevel: job.experienceLevel,
                    budget: job.budget,
                    skills: job.skills
                }
            });
        }
        return true;
    });

    console.log(`Upwork parsed: ${jobsParsed}, ready: ${jobsToSend.length}, duplicates: ${duplicatesSkipped}, blocked: ${blockedPages}`);

    if (jobsToSend.length > 0) {
        await axios.post(WEBHOOK_URL, {
            type: 'external_crawl',
            token: SCRAPER_TOKEN,
            source: 'upwork',
            data: jobsToSend
        });
        console.log(`Pushed ${jobsToSend.length} Upwork items to webhook.`);
    } else {
        console.log('No Upwork items to push.');
    }
}

async function fetchXmlViaBrowser(url, context) {
    if (!context) return null;
    const normalizedUrl = (url || '').trim();
    const isViewSource = normalizedUrl.toLowerCase().startsWith('view-source:');
    const actualUrl = isViewSource ? normalizedUrl.replace(/^view-source:/i, '') : normalizedUrl;
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
        try {
            await page.goto(isViewSource ? normalizedUrl : actualUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
        } catch (e) {
            if (isViewSource) {
                console.warn(`  [!] view-source navigation failed, falling back to direct URL`);
                await page.goto(actualUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
            } else {
                throw e;
            }
        }

        // Handle Cloudflare challenge check if present
        try {
            const challenge = await page.$('#challenge-running');
            if (challenge) {
                await page.waitForTimeout(5000);
            }
        } catch { }

        await page.waitForTimeout(2000);

        const rawText = await page.evaluate(() => {
            const pre = document.querySelector('pre');
            if (pre && pre.innerText) {
                return pre.innerText;
            }
            return document.documentElement.innerText || '';
        });
        const payload = extractXmlPayload(rawText);
        if (payload && (payload.includes('<urlset') || payload.includes('<rss') || payload.includes('<sitemapindex') || payload.includes('<feed'))) {
            return payload;
        }
        const html = await page.content();
        return html || rawText;
    } catch (e) {
        console.warn(`  [!] Browser XML page fallback failed: ${e.message.split('\n')[0]}`);
    } finally {
        await page.close().catch(() => { });
    }
    return null;
}

async function fetchXml(url, context, options = {}) {
    const normalizedUrl = (url || '').trim();
    const useScrapeDo = options.useScrapeDo ?? USE_SCRAPE_DO_API;
    if (normalizedUrl.toLowerCase().startsWith('view-source:')) {
        return await fetchXmlViaBrowser(normalizedUrl, context);
    }

    // Force browser for BHW RSS which consistently blocks axios (UNLESS Scrape.do is active)
    if (!useScrapeDo && normalizedUrl.includes('blackhatworld.com') && normalizedUrl.includes('rss')) {
        console.log('  [BHW-RSS] Enforcing browser fetch (No Scrape.do)...');
        return await fetchXmlViaBrowser(normalizedUrl, context);
    }

    try {
        let reqConfig = {
            timeout: options.timeoutMs || 60000,
            headers: {
                'User-Agent': DEFAULT_UA,
                'Accept': 'application/xml,text/xml,application/rss+xml,application/atom+xml',
                'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': new URL(normalizedUrl).origin + '/',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }
        };

        let targetUrl = normalizedUrl;

        if (useScrapeDo && process.env.SCRAPE_DO_TOKEN) {
            // Use Scrape.do API Gateway
            targetUrl = `http://api.scrape.do?url=${encodeURIComponent(normalizedUrl)}&token=${process.env.SCRAPE_DO_TOKEN}`;
            // Remove proxy config if using Gateway API
            reqConfig.proxy = false;
        } else if (useScrapeDo) {
            // Use standard proxy if configured
            reqConfig.proxy = AXIOS_PROXY_CONFIG;
        } else {
            // Explicitly disable proxy for direct sources
            reqConfig.proxy = false;
        }

        reqConfig.responseType = 'arraybuffer';
        const response = await axios.get(targetUrl, reqConfig);
        const text = decodeResponseBody(response.data, response.headers);
        return extractXmlPayload(text);
    } catch (e) {
        console.warn(`  [!] XML fetch failed: ${normalizedUrl} -> ${e.message.split('\n')[0]}`);
        return await fetchXmlViaBrowser(normalizedUrl, context);
    }
}

async function fetchHtmlViaScrapeDo(url, params = {}) {
    if (!process.env.SCRAPE_DO_TOKEN) return null;
    let targetUrl = `http://api.scrape.do?url=${encodeURIComponent(url)}&token=${process.env.SCRAPE_DO_TOKEN}`;

    if (params.render) targetUrl += '&render=true';
    if (params.super) targetUrl += '&super=true';
    if (params.geoCode) targetUrl += `&geoCode=${params.geoCode}`;

    for (let attempt = 1; attempt <= 2; attempt++) {
        try {
            const response = await axios.get(targetUrl, {
                timeout: 90000, // rendering takes longer
                responseType: 'arraybuffer',
                headers: { 'User-Agent': DEFAULT_UA }
            });
            return decodeResponseBody(response.data, response.headers);
        } catch (e) {
            const status = e?.response?.status;
            if (attempt < 2 && status && status >= 500) {
                await new Promise(r => setTimeout(r, 2000));
                continue;
            }
            console.warn(`  [!] Scrape.do HTML fetch failed: ${url} -> ${e.message}`);
            return null;
        }
    }
    return null;
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

            const pubDate = item.pubDate || item.published || item.updated || item.date || '';
            return {
                title: (title || '').trim(),
                url: (link || '').trim(),
                published: (pubDate || '').trim()
            };
        });

        return mapped.filter(item => item.url).slice(0, maxItems);
    } catch (e) {
        console.warn(`  [!] RSS parse failed: ${e.message.split('\n')[0]}`);
        return [];
    }
}

function extractUrlsFromHtmlSitemap(text) {
    if (!text) return [];
    const matches = text.match(/https?:\/\/[^\s"'<>]+/gi) || [];
    const cleaned = matches
        .map(u => u.replace(/&amp;/g, '&'))
        .filter(u => !u.toLowerCase().includes('sitemap') && !u.toLowerCase().endsWith('.xml'));
    return Array.from(new Set(cleaned));
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

    const htmlUrls = extractUrlsFromHtmlSitemap(xmlText);
    if (htmlUrls.length > 0) {
        return { type: 'urlset', urls: htmlUrls.map(loc => ({ loc, lastmod: '' })) };
    }

    return { type: 'unknown', urls: [] };
}

async function fetchSitemapUrls(url, maxUrls = 50, depth = 0, context = null, options = {}) {
    if (depth > 2) return [];
    const xml = await fetchXml(url, context, options);
    if (!xml) return [];

    const parsed = extractSitemapUrls(xml);
    if (parsed.type === 'index') {
        const sorted = parsed.sitemaps
            .filter(sm => sm.loc)
            .sort((a, b) => new Date(b.lastmod || 0) - new Date(a.lastmod || 0));
        const pick = sorted.length > 0 ? sorted.slice(0, 3) : [];
        let urls = [];
        for (const sm of pick) {
            const childUrls = await fetchSitemapUrls(sm.loc, maxUrls - urls.length, depth + 1, context, options);
            urls = urls.concat(childUrls);
            if (urls.length >= maxUrls) break;
        }
        return urls.slice(0, maxUrls);
    }

    if (parsed.type === 'urlset') {
        const urls = parsed.urls.filter(u => u.loc);
        const hasLastmod = urls.some(u => u.lastmod);
        const ordered = hasLastmod
            ? urls.sort((a, b) => new Date(b.lastmod || 0) - new Date(a.lastmod || 0))
            : urls;
        return ordered.map(u => u.loc).slice(0, maxUrls);
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

function classifyTopic({ title, content, url, prefix, forum, sourceName }) {
    const s = analyzeJobSignals({ title, content, url, prefix, forum });
    const text = s.combined;
    const norm = s.normalized;

    const reqHits = countHit(REQ_TR, norm) + countHit(REQ_EN, text);
    const offHits = countHit(OFF_TR, norm) + countHit(OFF_EN, text);

    let score = 0;
    score += s.posStrong * 3;
    score += s.posWeak * 1;
    score += reqHits * 3;

    score -= s.negStrong * 3;
    score -= s.negWeak * 1;
    score -= offHits * 3;

    if (s.urlHit) score += 1;
    if (s.prefixDemand) score += 2;
    if (s.prefixSupply) score -= 2;

    if (s.hasCurrency) score += 0.5;
    if (s.hasContact) score += 0.5;

    if (sourceName === 'bhw' && offHits > 0) score -= 2;

    if (score >= 4) return { label: 'JOB_REQUEST', score };
    if (score <= -4) return { label: 'SERVICE_OFFER', score };

    return { label: 'UNKNOWN', score };
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

        if (ALLOW_MARKETPLACE && (s.posWeak > 0 || s.urlHit || s.hasCurrency || s.hasContact)) {
            return {
                isJob: true,
                reason: `marketplace:${supplyScore}`
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
        return false;
    }

    if (supplyScore > 0) {
        return !ALLOW_MARKETPLACE;
    }

    if (s.negWeak >= 2 && s.posWeak === 0) {
        return true;
    }

    return false;
}

function shouldPrefilterSkipSmart(input) {
    const c = classifyTopic(input);
    if (c.label === 'SERVICE_OFFER' && c.score <= -6) {
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

async function preflightHtml(context, url, timeoutMs) {
    if (!context || !url) return { ok: true };
    try {
        const response = await context.request.get(url, {
            headers: {
                'User-Agent': DEFAULT_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            },
            timeout: timeoutMs
        });
        const status = response.status();
        if (status >= 400) {
            return { ok: false, status };
        }
        return { ok: true, status };
    } catch (e) {
        return { ok: false, error: e.message.split('\n')[0] };
    }
}

async function fetchThreadDetails(context, url, contentSelector, titleSelector, options = {}) {
    const page = await context.newPage();
    try {
        if (options.prefilter) {
            const pre = await preflightHtml(context, url, options.prefilterTimeoutMs || SCRAPER_PREFLIGHT_TIMEOUT_MS);
            if (!pre.ok) {
                console.log(`  [SKIP:PREFLIGHT] ${url} (${pre.status || pre.error || 'error'})`);
                return null;
            }
        }
        const useScrapeDo = options.useScrapeDo ?? USE_SCRAPE_DO_API;
        const minLen = Number(options.minContentLength || MIN_CONTENT_LENGTH);
        const maxLen = Number(options.maxContentLength || MAX_CONTENT_LENGTH);
        let usedContent = false;
        if (useScrapeDo && USE_SCRAPE_DO_API) {
            const html = await fetchHtmlViaScrapeDo(url);
            if (html && html.length > 1000) {
                await page.setContent(html, { waitUntil: 'domcontentloaded' });
                await page.evaluate((baseUrl) => {
                    const base = document.createElement('base');
                    base.href = baseUrl;
                    document.head.prepend(base);
                }, url);
                usedContent = true;
            }
        }
        if (!usedContent) {
            try {
                await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
            } catch (e) {
                console.warn(`  [!] Fetch navigation warning: ${e.message.split('\n')[0]}`);
            }
        }
        let found = await waitForAnySelector(page, contentSelector, 8000);
        if (!found) {
            await page.waitForLoadState('networkidle', { timeout: 6000 }).catch(() => { });
            await waitForAnySelector(page, contentSelector, 8000);
        }

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

            const extractSignatureText = (rootEl) => {
                const sigSelectors = [
                    '.userSignature',
                    '.postContent.userSignature',
                    '.signature',
                    '.post-signature',
                    '.message-signature',
                    '.signatureContainer'
                ];
                const root = rootEl || document;
                const parts = [];
                sigSelectors.forEach((sel) => {
                    root.querySelectorAll(sel).forEach((el) => {
                        const text = cleanText(el);
                        if (text) parts.push(text);
                    });
                });
                if (parts.length === 0) return '';
                const uniq = Array.from(new Set(parts));
                const merged = uniq.join(' | ').trim();
                if (merged.length > 500) {
                    return merged.slice(0, 500);
                }
                return merged;
            };

            const extractDateText = () => {
                const metaSelectors = [
                    'meta[property="article:published_time"]',
                    'meta[name="date"]',
                    'meta[itemprop="datePublished"]'
                ];
                for (const sel of metaSelectors) {
                    const el = document.querySelector(sel);
                    const val = el ? (el.getAttribute('content') || '').trim() : '';
                    if (val) return val;
                }

                const timeEl = document.querySelector('time[datetime]');
                if (timeEl) {
                    const val = (timeEl.getAttribute('datetime') || timeEl.innerText || '').trim();
                    if (val) return val;
                }
                const timeText = document.querySelector('time');
                if (timeText && timeText.innerText) {
                    const val = timeText.innerText.trim();
                    if (val) return val;
                }

                const candidates = [
                    '.message-attribution-main time',
                    '.message-attribution time',
                    '.postDate',
                    '.post-date',
                    '.thread-date',
                    '.message-date',
                    '.date',
                    '.head .left'
                ];
                for (const sel of candidates) {
                    const el = document.querySelector(sel);
                    const val = el ? (el.innerText || '').trim() : '';
                    if (val) return val;
                }

                return '';
            };

            const candidates = [];

            for (const sel of selectors) {
                const els = document.querySelectorAll(sel);
                if (els && els.length > 0) {
                    const text = cleanText(els[0]);
                    if (text) candidates.push({ text, el: els[0] });
                }
            }

            if (!candidates.some(c => c.text.length >= minLen)) {
                for (const sel of selectors) {
                    document.querySelectorAll(sel).forEach(el => {
                        const text = cleanText(el);
                        if (text) candidates.push({ text, el });
                    });
                }
            }

            if (candidates.length === 0) {
                const fallback = document.querySelector('article, .post, .thread-content, main');
                const text = cleanText(fallback);
                if (text) candidates.push({ text, el: fallback });
            }

            if (candidates.length === 0) {
                const meta = document.querySelector('meta[name="description"], meta[property="og:description"], meta[name="twitter:description"]');
                const metaText = meta ? (meta.getAttribute('content') || '').trim() : '';
                if (metaText) candidates.push({ text: metaText, el: null });
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

            let text = best.text;
            let signatureRoot = null;
            if (best.el) {
                signatureRoot = best.el.closest('.message, .message--post, .post, .postContainer, article') || best.el;
            }
            const signatureText = extractSignatureText(signatureRoot);
            if (signatureText) {
                text = text + '\n\nSignature: ' + signatureText;
            }
            const dateText = extractDateText();
            return {
                content: text.length > maxLen ? text.slice(0, maxLen) : text,
                title: pageTitle,
                publishedAt: dateText
            };
        }, {
            selector: contentSelector,
            titleSelector,
            minLen,
            maxLen
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
    const useScrapeDo = opts.useScrapeDo ?? USE_SCRAPE_DO_API;

    const page = await context.newPage();
    try {
        let usedContent = false;
        if (useScrapeDo && USE_SCRAPE_DO_API) {
            const html = await fetchHtmlViaScrapeDo(url);
            if (html && html.length > 1000) {
                await page.setContent(html, { waitUntil: 'domcontentloaded' });
                await page.evaluate((baseUrl) => {
                    const base = document.createElement('base');
                    base.href = baseUrl;
                    document.head.prepend(base);
                }, url);
                usedContent = true;
            }
        }
        if (!usedContent) {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        }

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

async function enrichThreads(prefiltered, source, context) {
    const enrichedThreads = [];
    const concurrency = Math.max(1, SCRAPER_DETAIL_CONCURRENCY);
    const minLen = Number(source.minContentLength || MIN_CONTENT_LENGTH);

    await mapWithConcurrency(prefiltered, concurrency, async (thread) => {
        if (SCRAPER_DETAIL_DELAY_MS > 0) {
            await sleep(withJitter(SCRAPER_DETAIL_DELAY_MS));
        }

        const details = await fetchThreadDetails(
            context,
            thread.url,
            source.contentSelector,
            source.titleSelector,
            {
                useScrapeDo: source.useScrapeDo,
                minContentLength: source.minContentLength,
                prefilter: SCRAPER_PREFLIGHT && !(source.useScrapeDo && USE_SCRAPE_DO_API),
                prefilterTimeoutMs: SCRAPER_PREFLIGHT_TIMEOUT_MS
            }
        );

        const content = details?.content || '';
        const publishedAt = details?.publishedAt || thread.published || '';
        if (publishedAt && isTooOld(publishedAt)) {
            console.log(`  [SKIP:OLD] ${(thread.title || thread.url).substring(0, 60)}...`);
            return null;
        }

        let finalTitle = (thread.title || '').trim() || (details?.title || '').trim();
        if (!finalTitle) {
            finalTitle = deriveTitleFromUrl(thread.url);
        }

        if (!finalTitle) {
            console.log(`  [SKIP:NOTITLE] ${thread.url.substring(0, 60)}...`);
            return null;
        }

        if (content && content.length >= minLen) {
            if (!DISABLE_JOB_FILTER) {
                const cls = classifyTopic({
                    title: finalTitle,
                    content,
                    url: thread.url,
                    prefix: thread.prefix,
                    forum: thread.forum,
                    sourceName: source.emitAs || source.name
                });
                if (cls.label === 'SERVICE_OFFER') {
                    console.log(`  [SKIP:${cls.label}] ${finalTitle.substring(0, 60)}... score:${cls.score}`);
                    return null;
                }
            }
            enrichedThreads.push({ ...thread, title: finalTitle, original_content: content });
            console.log(`  [OK] ${finalTitle.substring(0, 60)}...`);
            SEEN_URLS.add(thread.url);
            return true;
        }

        console.log(`  [SKIP:NOCONTENT] ${finalTitle.substring(0, 60)}...`);
        return null;
    });

    return enrichedThreads;
}

async function processThreadsForSource(source, threads, context) {
    const maxThreads = source.maxThreads || MAX_THREADS_PER_SOURCE;
    const prefilterMode = source.prefilter || 'smart';

    const prefiltered = [];
    const pickedUrls = new Set();
    for (const thread of threads) {
        if (!thread.url) continue;
        if (SEEN_URLS.has(thread.url) || pickedUrls.has(thread.url)) {
            continue;
        }
        if (thread.published && isTooOld(thread.published)) {
            console.log(`  [SKIP:OLD] ${(thread.title || thread.url).substring(0, 60)}...`);
            continue;
        }

        if (!DISABLE_JOB_FILTER && prefilterMode === 'strict') {
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
        } else if (!DISABLE_JOB_FILTER && prefilterMode === 'smart') {
            if (shouldPrefilterSkipSmart({
                title: thread.title,
                content: '',
                url: thread.url,
                prefix: thread.prefix,
                forum: thread.forum,
                sourceName: source.emitAs || source.name
            })) {
                console.log(`  [SKIP:OFFER] ${(thread.title || thread.url).substring(0, 60)}...`);
                continue;
            }
        }

        prefiltered.push(thread);
        pickedUrls.add(thread.url);

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
    const enrichedThreads = await enrichThreads(prefiltered, source, context);

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
    const useScrapeDo = feed.useScrapeDo ?? USE_SCRAPE_DO_API;

    if (feed.type === 'rss') {
        const feedUrl = sanitizeFeedUrl(
            feed.resolveUrl ? await feed.resolveUrl(feed.url, context) : feed.url,
            feed.allowViewSource
        );
        console.log(`Fetching RSS: ${feedUrl}`);
        const xml = await fetchXml(feedUrl, context, { useScrapeDo, timeoutMs: 60000 });
        if (!xml) return;
        threads = extractRssItems(xml, feed.maxItems || 60);
        threads = cleanThreadList(threads, feedUrl);
    } else if (feed.type === 'sitemap') {
        const sitemapUrl = sanitizeFeedUrl(feed.url, feed.allowViewSource);
        console.log(`Fetching Sitemap: ${sitemapUrl}`);
        const urls = await fetchSitemapUrls(sitemapUrl, feed.maxItems || 60, 0, context, { useScrapeDo, timeoutMs: 60000 });
        const filtered = feed.urlAllow
            ? urls.filter(u => feed.urlAllow.some(r => r.test(u)))
            : urls;
        threads = cleanThreadList(filtered.map(url => ({ title: '', url })), sitemapUrl);
    } else if (feed.type === 'html') {
        const feedUrl = sanitizeFeedUrl(feed.url);
        console.log(`Fetching HTML feed: ${feedUrl}`);
        const rawLinks = await collectLinksFromPage(context, feedUrl, { ...feed, useScrapeDo });
        threads = cleanThreadList(rawLinks, feedUrl);
    }

    if (threads.length === 0) {
        console.log('No threads found.');
        return;
    }

    console.log(`Found ${threads.length} topics. Prefiltering...`);
    await processThreadsForSource(feed, threads, context);
}

async function scrapeFeedSources(context, directContext) {
    for (const feed of FEED_SOURCES) {
        try {
            const feedContext = feed.useScrapeDo === false ? (directContext || context) : context;
            await processFeedSource(feed, feedContext);
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

    const needsDirectContext = SOURCES.some(s => s.useScrapeDo === false) || FEED_SOURCES.some(f => f.useScrapeDo === false);
    const hasProxy = Boolean(PLAYWRIGHT_PROXY_CONFIG);

    const browser = await chromium.launch({
        headless: true,
        args: browserArgs,
        proxy: hasProxy ? PLAYWRIGHT_PROXY_CONFIG : undefined
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
    let directBrowser = null;
    let directContext = context;

    if (needsDirectContext && hasProxy) {
        directBrowser = await chromium.launch({
            headless: true,
            args: browserArgs
        });
        directContext = await directBrowser.newContext({
            userAgent: DEFAULT_UA,
            viewport: { width: 1280, height: 800 },
            locale: 'tr-TR',
            extraHTTPHeaders: {
                'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7'
            }
        });
    }

    const setupContext = async (ctx) => {
        await applyCookiesFromEnv(ctx);
        ctx.setDefaultTimeout(60000);
        ctx.setDefaultNavigationTimeout(60000);

        await ctx.route('**/*', route => {
            const type = route.request().resourceType();
            if (['image', 'media', 'font'].includes(type)) {
                return route.abort();
            }
            return route.continue();
        });
    };

    await setupContext(context);
    if (directContext !== context) {
        await setupContext(directContext);
    }

    for (const source of SOURCES) {
        const activeContext = source.useScrapeDo === false ? (directContext || context) : context;
        const mainPage = await activeContext.newPage();
        try {
            console.log(`\n=== ${source.name.toUpperCase()} ===`);
            console.log(`Navigating to: ${source.url}`);

            const useScrapeDoApi = USE_SCRAPE_DO_API && source.useScrapeDo !== false && source.useScrapeDoApi !== false;
            if (useScrapeDoApi) {
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

            const safeName = source.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
            await mainPage.screenshot({ path: `nav_${safeName}.png`, fullPage: false });

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
                return results.slice(0, s.maxThreads || 50);
            }, source);

            const threads = cleanThreadList(rawThreads, source.url);

            console.log(`Found ${threads.length} topics. Prefiltering...`);
            await mainPage.close();

            const prefilteredThreads = [];
            const pickedUrls = new Set();
            let skippedCount = 0;
            const maxThreads = source.maxThreads || MAX_THREADS_PER_SOURCE;
            for (const thread of threads) {
                // Stop if we have seen 10 consecutive old topics
                if (skippedCount >= 10) {
                    console.log(`[${source.name}] Stopped early: 10 consecutive old topics found.`);
                    break;
                }

                if (SEEN_URLS.has(thread.url) || pickedUrls.has(thread.url)) {
                    skippedCount++;
                    continue;
                }
                skippedCount = 0; // reset if we find a new one

                if (!DISABLE_JOB_FILTER) {
                    if (shouldPrefilterSkipSmart({
                        title: thread.title,
                        content: '',
                        url: thread.url,
                        prefix: thread.prefix,
                        forum: thread.forum,
                        sourceName: source.emitAs || source.name
                    })) {
                        console.log(`  [SKIP:OFFER] ${thread.title}`);
                        continue;
                    }
                }
                prefilteredThreads.push(thread);
                pickedUrls.add(thread.url);
                if (prefilteredThreads.length >= maxThreads) {
                    break;
                }
            }

            console.log(`Fetching content for ${prefilteredThreads.length} topics...`);
            const enrichedThreads = await enrichThreads(prefilteredThreads, source, activeContext);

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

    await scrapeFeedSources(context, directContext);
    if (UPWORK_ENABLED) {
        try {
            await scrapeUpwork(context);
        } catch (e) {
            console.error(`[UPWORK] Error: ${e.message.split('\n')[0]}`);
        }
    }

    saveSeenUrls();
    if (directBrowser) {
        await directBrowser.close();
    }
    await browser.close();
    console.log("\n=== Scrape finished ===");
}

if (require.main === module) {
    scrape();
}

module.exports = {
    parseUpworkCategorySlugs,
    parseUpworkJobsFromCategoryHtml
};
