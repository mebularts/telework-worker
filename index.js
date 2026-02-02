/**
 * Full-recall forum job scraper (R10 + WMAraci + BlackHatWorld)
 * - Multi-discovery (global feeds + homepage/forum index)
 * - Stateful dedup + retry (avoid missing jobs on transient failures)
 * - Safe concurrency + timeouts
 *
 * IMPORTANT:
 * - Respect target sites' ToS/robots and rate-limit. This code is designed to be polite.
 */

const fs = require("fs");
const path = require("path");
const axios = require("axios");

const { chromium } = require("playwright-extra");
const stealth = require("puppeteer-extra-plugin-stealth")();

if (process.env.USE_STEALTH === "1") {
  chromium.use(stealth);
}

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SCRAPER_TOKEN = process.env.SCRAPER_TOKEN;

const STATE_PATH = process.env.STATE_PATH || path.join(process.cwd(), "state.json");
const HEADLESS = (process.env.HEADLESS ?? "1") !== "0";
const MAX_CONCURRENCY = Math.max(1, parseInt(process.env.MAX_CONCURRENCY || "4", 10));

const NAV_TIMEOUT = 45_000;
const DEFAULT_TIMEOUT = 30_000;

const DISCOVERY_LIMIT_PER_LISTING = Math.max(50, parseInt(process.env.DISCOVERY_LIMIT || "200", 10));
const MIN_CONTENT_LENGTH = 80;
const MAX_CONTENT_LENGTH = 4000;

const MAX_RETRIES_PER_THREAD = 3;
const RETRY_BACKOFF_MS = 800; // base

// ---------------------------
// Text normalization & scoring
// ---------------------------

function normalizeText(input) {
  if (!input) return "";
  return input
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\u0131/g, "i")
    .replace(/\s+/g, " ")
    .trim();
}

function countMatches(patterns, text) {
  let count = 0;
  for (const pattern of patterns) if (pattern.test(text)) count++;
  return count;
}

// TR + EN patterns (high recall)
const POS_STRONG = [
  // TR
  /\baranir\b/i, /\baraniyor\b/i, /\bihtiyac\b/i, /\bgerekli\b/i, /\blazim\b/i,
  /\btalep\b/i, /\bteklif\b/i, /\bfiyat teklifi\b/i, /\bbutce\b/i,
  /\byaptirmak istiyorum\b/i, /\byaptirilacak\b/i, /\bgelistirilecek\b/i,
  /\bfreelance\b/i, /\bfreelancer\b/i, /\bis araniyor\b/i, /\bhizmet alimi\b/i,
  // EN
  /\blooking for\b/i, /\bneed (a|an)\b/i, /\bseeking\b/i, /\bhiring\b/i,
  /\bfor hire\b/i, /\bbudget\b/i, /\bquotation\b/i, /\bproposal\b/i
];

const POS_WEAK = [
  /\bproje\b/i, /\bproject\b/i, /\bdeveloper\b/i, /\byazilim\b/i, /\btasarim\b/i,
  /\bdesign\b/i, /\bseo\b/i, /\breklam\b/i, /\bads\b/i, /\bentegrasyon\b/i,
  /\botomasyon\b/i, /\bbot\b/i, /\bscraper\b/i, /\bapi\b/i
];

// Strong negatives: "satılık/selling" style
const NEG_STRONG = [
  /\bsatilik\b/i, /\bsatiyorum\b/i, /\bsatilir\b/i, /\bfor sale\b/i, /\bselling\b/i
];

// Weak negatives: promos/marketplace noise (kept weaker to avoid missing)
const NEG_WEAK = [
  /\bkampanya\b/i, /\bindirim\b/i, /\bpromo\b/i, /\bpromosyon\b/i, /\bkupon\b/i,
  /\bpaket\b/i, /\blisans\b/i, /\baccount selling\b/i
];

const CURRENCY_RE = /(?:\u20BA|\$|\u20AC|\btl\b|\btry\b|\busd\b|\beur\b|\bgbp\b)/i;
const CONTACT_RE = /\b(?:pm|dm|whatsapp|telegram|tel|telefon|iletisim|contact)\b/i;

// URL hints (weak, only boosts)
const URL_HINTS = [
  "/is-ilan", "/is-ilani", "/is-ilanlari",
  "/is-arayan", "/is-veren", "/isveren",
  "/freelance", "/freelancer",
  "/proje", "/project",
  "/hizmet-alim", "/hizmet-alimi",
  "/talep", "/job", "/jobs", "/hiring"
];

function jobScore({ title, content, url, forumLabel }) {
  const combined = [title, content, url, forumLabel].filter(Boolean).join(" ");
  const norm = normalizeText(combined);

  const urlHit = URL_HINTS.some(h => norm.includes(h));
  const posS = countMatches(POS_STRONG, norm);
  const posW = countMatches(POS_WEAK, norm);
  const negS = countMatches(NEG_STRONG, norm);
  const negW = countMatches(NEG_WEAK, norm);

  const hasCurrency = CURRENCY_RE.test(combined);
  const hasContact = CONTACT_RE.test(norm);

  // If forum label explicitly indicates hiring, boost heavily (BHW: "Hire a Freelancer")
  const forumBoost = forumLabel && /hire a freelancer/i.test(forumLabel) ? 8 : 0;

  // Scoring tuned for high recall (avoid missing):
  // - strong positives dominate
  // - strong negatives can still be overridden if clearly hiring (forumBoost/posS)
  let score =
    (posS * 5) +
    (posW * 2) +
    (urlHit ? 2 : 0) +
    (hasCurrency ? 1 : 0) +
    (hasContact ? 1 : 0) +
    forumBoost -
    (negS * 6) -
    (negW * 2);

  const reasons = { posS, posW, negS, negW, urlHit, hasCurrency, hasContact, forumLabel };

  // Threshold: keep it permissive, because “kaçırmama” > “az bildirim”
  const isJob = score >= 6 || (forumBoost >= 8 && score >= 2) || (posS >= 1 && negS === 0);

  return { isJob, score, reasons };
}

// ---------------------------
// State (dedup + retry)
// ---------------------------

function loadState() {
  try {
    if (!fs.existsSync(STATE_PATH)) return { items: {} };
    const raw = fs.readFileSync(STATE_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    if (!parsed.items) parsed.items = {};
    return parsed;
  } catch {
    return { items: {} };
  }
}

function saveState(state) {
  try {
    fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
  } catch (e) {
    console.warn(`[state] failed to save: ${e.message}`);
  }
}

function pruneState(state, maxItems = 50_000) {
  const keys = Object.keys(state.items || {});
  if (keys.length <= maxItems) return;

  keys.sort((a, b) => (state.items[a]?.lastSeen || 0) - (state.items[b]?.lastSeen || 0));
  const removeCount = keys.length - maxItems;
  for (let i = 0; i < removeCount; i++) delete state.items[keys[i]];
}

// ---------------------------
// Helpers
// ---------------------------

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function withRetries(fn, retries, label) {
  let lastErr = null;
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn(i);
    } catch (e) {
      lastErr = e;
      const wait = RETRY_BACKOFF_MS * Math.pow(2, i);
      console.warn(`[retry] ${label} attempt=${i + 1}/${retries + 1} err=${e.message.split("\n")[0]} wait=${wait}ms`);
      await sleep(wait);
    }
  }
  throw lastErr;
}

function absUrl(href, base) {
  try {
    if (!href) return null;
    if (href.startsWith("http")) return href;
    return new URL(href, base).href;
  } catch {
    return null;
  }
}

// Extract stable per-site thread key (for dedup)
function threadKey(sourceName, url) {
  if (!url) return null;

  if (sourceName === "bhw") {
    // example: https://www.blackhatworld.com/seo/buy-trustpilot-reviews.1789971/
    const m = url.match(/\.([0-9]+)\/?($|\?)/);
    return m ? `bhw:${m[1]}` : `bhw:${url}`;
  }

  // wmaraci: ...-711822.html
  if (sourceName === "wmaraci") {
    const m = url.match(/-([0-9]+)\.html/i);
    return m ? `wmaraci:${m[1]}` : `wmaraci:${url}`;
  }

  // r10: either -123456.html or showthread.php?t=123456
  if (sourceName === "r10") {
    const m1 = url.match(/-([0-9]+)\.html/i);
    if (m1) return `r10:${m1[1]}`;
    const m2 = url.match(/[?&]t=([0-9]+)/i);
    if (m2) return `r10:${m2[1]}`;
    return `r10:${url}`;
  }

  return `${sourceName}:${url}`;
}

// Generic link filter per domain (keeps discovery broad but not insane)
function looksLikeThread(sourceName, url) {
  if (!url) return false;

  if (sourceName === "bhw") {
    // XenForo thread pattern: ".12345/" somewhere
    return /\.([0-9]+)\//.test(url) && url.includes("blackhatworld.com/");
  }

  if (sourceName === "wmaraci") {
    return /-([0-9]+)\.html/i.test(url) && url.includes("wmaraci.com/forum/");
  }

  if (sourceName === "r10") {
    return (
      url.includes("r10.net/") &&
      (/-([0-9]+)\.html/i.test(url) || /showthread\.php\?t=([0-9]+)/i.test(url))
    );
  }

  return false;
}

// ---------------------------
// Source definitions (multi-discovery)
// ---------------------------

const SOURCES = [
  {
    name: "r10",
    base: "https://www.r10.net/",
    listings: [
      // Global "new posts" feed (highest recall)
      { name: "getnew", url: "https://www.r10.net/search.php?do=getnew" },
      // Homepage fallback (what you had)
      { name: "home", url: "https://www.r10.net/" }
    ],
    contentSelectors: [
      ".postContent.userMessageSize",
      ".postContent",
      ".post_message",
      "#post_message_",
      "article .content"
    ].join(",")
  },
  {
    name: "wmaraci",
    base: "https://wmaraci.com/forum",
    listings: [
      // Guaranteed SSR directory (fallback, broad)
      { name: "forum_index", url: "https://wmaraci.com/forum" },
      // Optional: JS-driven aggregations (enable if they work in your environment)
      { name: "yeni_konular", url: "https://wmaraci.com/yeni-konular", optional: true },
      { name: "yeni_ilanlar", url: "https://wmaraci.com/yeni-ilanlar", optional: true }
    ],
    contentSelectors: [
      ".message-body",
      ".postMessage",
      ".post-content",
      ".forumPost .content",
      "article",
      "main"
    ].join(",")
  },
  {
    name: "bhw",
    base: "https://www.blackhatworld.com/",
    listings: [
      { name: "whats_new", url: "https://www.blackhatworld.com/whats-new/" },
      { name: "new_posts", url: "https://www.blackhatworld.com/whats-new/posts/" }
    ],
    contentSelectors: [
      ".message-body .bbWrapper",
      ".message-content .bbWrapper",
      "article .bbWrapper",
      "main"
    ].join(",")
  }
];

// ---------------------------
// Scrape primitives
// ---------------------------

async function waitForAnySelector(page, selectorList, timeoutMs) {
  const selectors = selectorList
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);

  const perSelector = Math.max(500, Math.floor(timeoutMs / Math.max(1, selectors.length)));
  for (const sel of selectors) {
    try {
      await page.waitForSelector(sel, { timeout: perSelector });
      return sel;
    } catch {}
  }
  return null;
}

async function extractThreadsFromPage(page, source) {
  const base = source.base;

  // Broad extraction: scan all anchors, keep only likely thread URLs
  const items = await page.evaluate(({ baseUrl }) => {
    const out = [];
    const seen = new Set();

    const anchors = Array.from(document.querySelectorAll("a[href]"));
    for (const a of anchors) {
      const title = (a.innerText || "").trim();
      const href = a.getAttribute("href");
      if (!href) continue;

      // skip empty/unhelpful
      if (title && title.length < 4) continue;

      let url = href;
      try {
        if (!url.startsWith("http")) url = new URL(url, window.location.origin).href;
      } catch {
        continue;
      }

      if (!seen.has(url)) {
        seen.add(url);
        out.push({ title, url });
      }
    }

    return out;
  }, { baseUrl: base });

  // filter by per-site patterns
  const filtered = items
    .map(it => ({ ...it, url: absUrl(it.url, base) }))
    .filter(it => it.url && looksLikeThread(source.name, it.url));

  // de-dup by URL
  const uniq = [];
  const urlSet = new Set();
  for (const it of filtered) {
    if (urlSet.has(it.url)) continue;
    urlSet.add(it.url);
    uniq.push(it);
    if (uniq.length >= DISCOVERY_LIMIT_PER_LISTING) break;
  }
  return uniq;
}

async function fetchThreadContent(context, url, contentSelector) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
    await page.waitForTimeout(500);

    await waitForAnySelector(page, contentSelector, 8000);

    const content = await page.evaluate(({ selector, minLen, maxLen }) => {
      const selectors = selector.split(",").map(s => s.trim()).filter(Boolean);

      const cleanText = (el) => {
        if (!el) return "";
        const clone = el.cloneNode(true);
        const removeSelectors = [
          "script", "style", "noscript",
          "blockquote", ".quote",
          ".bbCodeBlock", ".bbCodeBlock--quote",
          ".signature", ".postLike", ".postButtons", ".share"
        ];
        removeSelectors.forEach(sel => {
          clone.querySelectorAll(sel).forEach(n => n.remove());
        });
        const text = clone.innerText || "";
        return text.replace(/\s+/g, " ").trim();
      };

      const candidates = [];

      // Prefer the first match (usually OP / first post wrapper)
      for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el) {
          const t = cleanText(el);
          if (t) candidates.push(t);
        }
      }

      if (candidates.length === 0) {
        const fallback = document.querySelector("article, main, .post, .thread-content");
        const t = cleanText(fallback);
        if (t) candidates.push(t);
      }

      if (candidates.length === 0) {
        const meta = document.querySelector('meta[name="description"], meta[property="og:description"], meta[name="twitter:description"]');
        const t = meta ? (meta.getAttribute("content") || "").trim() : "";
        if (t) candidates.push(t);
      }

      if (candidates.length === 0) return null;

      let best = candidates.find(t => t.length >= minLen) || candidates.reduce((a, b) => (b.length > a.length ? b : a), candidates[0]);
      if (!best) return null;

      if (best.length > maxLen) best = best.slice(0, maxLen);
      return best;
    }, { selector: contentSelector, minLen: MIN_CONTENT_LENGTH, maxLen: MAX_CONTENT_LENGTH });

    return content;
  } finally {
    await page.close().catch(() => {});
  }
}

async function postWebhook(payload) {
  if (!WEBHOOK_URL || !SCRAPER_TOKEN) {
    throw new Error("Missing WEBHOOK_URL or SCRAPER_TOKEN");
  }

  await withRetries(async () => {
    const res = await axios.post(WEBHOOK_URL, payload, { timeout: 25_000 });
    return res.data;
  }, 2, "webhook");
}

// Simple concurrency pool
async function runPool(items, worker, concurrency) {
  const results = [];
  let idx = 0;

  async function next() {
    while (idx < items.length) {
      const cur = items[idx++];
      results.push(await worker(cur));
    }
  }

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, () => next());
  await Promise.all(runners);
  return results;
}

// ---------------------------
// Main scrape
// ---------------------------

async function scrape() {
  console.log(`[${new Date().toISOString()}] Starting scrape (headless=${HEADLESS}, conc=${MAX_CONCURRENCY})`);

  const state = loadState();
  pruneState(state);

  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    viewport: { width: 1920, height: 1080 },
    locale: "tr-TR",
    extraHTTPHeaders: { "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7" }
  });

  context.setDefaultTimeout(DEFAULT_TIMEOUT);
  context.setDefaultNavigationTimeout(NAV_TIMEOUT);

  // Be polite: block heavy assets
  await context.route("**/*", (route) => {
    const type = route.request().resourceType();
    if (["image", "media", "font"].includes(type)) return route.abort();
    return route.continue();
  });

  for (const source of SOURCES) {
    console.log(`\n=== ${source.name.toUpperCase()} ===`);
    let discovered = [];

    for (const listing of source.listings) {
      const page = await context.newPage();
      try {
        console.log(`[listing] ${listing.name} -> ${listing.url}`);
        await page.goto(listing.url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
        await page.waitForTimeout(800);

        // Optional listings might fail (login/js). We don't die.
        const links = await extractThreadsFromPage(page, source);
        console.log(`  found=${links.length}`);
        discovered.push(...links);
      } catch (e) {
        console.warn(`  [listing_fail] ${listing.name}: ${e.message.split("\n")[0]}`);
        if (!listing.optional) {
          // Keep going; other listings may still cover.
        }
      } finally {
        await page.close().catch(() => {});
      }
    }

    // Dedup discovered by threadKey
    const uniqMap = new Map();
    for (const it of discovered) {
      const key = threadKey(source.name, it.url);
      if (!key) continue;
      if (!uniqMap.has(key)) uniqMap.set(key, { ...it, key, source: source.name });
    }
    discovered = Array.from(uniqMap.values());

    // Filter already sent (but keep retry-able failures)
    const candidates = discovered.filter(it => {
      const st = state.items[it.key];
      if (!st) return true;
      if (st.status === "sent") return false;
      if ((st.attempts || 0) >= MAX_RETRIES_PER_THREAD) return false;
      return true;
    });

    console.log(`candidates(after_state)=${candidates.length}`);

    // Enrich only what is likely job OR unknown (high recall strategy):
    // - Do a LIGHT pre-score on title+url; if score is very negative, skip enrichment.
    const toEnrich = [];
    for (const it of candidates) {
      const pre = jobScore({ title: it.title, content: "", url: it.url, forumLabel: "" });
      // Avoid enriching obvious "selling/satılık" spam unless it's still ambiguous
      if (!pre.isJob && pre.score < -6) continue;
      toEnrich.push(it);
    }

    console.log(`enrich_queue=${toEnrich.length}`);

    const enriched = [];
    await runPool(toEnrich, async (it) => {
      const st = state.items[it.key] || { firstSeen: Date.now(), attempts: 0, status: "new" };
      st.lastSeen = Date.now();

      // attempt + backoff (per-thread)
      st.attempts = (st.attempts || 0) + 1;
      st.lastAttempt = Date.now();
      state.items[it.key] = st;
      saveState(state);

      // Small spacing per request
      await sleep(350);

      try {
        const content = await fetchThreadContent(context, it.url, source.contentSelectors);
        if (!content || content.length < MIN_CONTENT_LENGTH) {
          throw new Error("no_content");
        }

        const finalCheck = jobScore({ title: it.title, content, url: it.url, forumLabel: "" });
        if (!finalCheck.isJob) {
          st.status = "not_job";
          saveState(state);
          return;
        }

        enriched.push({
          title: it.title,
          url: it.url,
          original_content: content,
          score: finalCheck.score,
          reasons: finalCheck.reasons
        });

        st.status = "ready";
        saveState(state);
      } catch (e) {
        // Do NOT mark as sent; will retry on next run until attempts exhausted
        st.status = "failed";
        st.lastError = e.message;
        saveState(state);
      }
    }, MAX_CONCURRENCY);

    console.log(`enriched_jobs=${enriched.length}`);

    if (enriched.length > 0) {
      try {
        await postWebhook({
          type: "external_crawl",
          token: SCRAPER_TOKEN,
          source: source.name,
          data: enriched
        });

        // Mark as sent
        for (const item of enriched) {
          const k = threadKey(source.name, item.url);
          if (!k) continue;
          const st = state.items[k] || {};
          st.status = "sent";
          st.sentAt = Date.now();
          state.items[k] = st;
        }
        pruneState(state);
        saveState(state);

        console.log(`pushed=${enriched.length}`);
      } catch (e) {
        console.warn(`[webhook_fail] ${e.message.split("\n")[0]} (items stay retryable)`);
      }
    }
  }

  await browser.close();
  console.log("\n=== Scrape finished ===");
}

scrape().catch((e) => {
  console.error(`[fatal] ${e.message}`);
  process.exit(1);
});
