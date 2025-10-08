// server.js — Brightcove tools with precise search + ALWAYS-on sitemap scan (embeds column)

require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');
const http = require('http');
const https = require('https');
const zlib = require('zlib');

const app = express();
const PORT = process.env.PORT || 3000;

/* ------------ global error visibility ------------ */
process.on('unhandledRejection', err => console.error('UNHANDLED REJECTION:', err?.stack || err));
process.on('uncaughtException', err => console.error('UNCAUGHT EXCEPTION:', err?.stack || err));

/* ------------ env checks ------------ */
const MUST = ['BRIGHTCOVE_ACCOUNT_ID','BRIGHTCOVE_CLIENT_ID','BRIGHTCOVE_CLIENT_SECRET','BRIGHTCOVE_PLAYER_ID'];
const miss = MUST.filter(k => !process.env[k]);
if (miss.length) { console.error('Missing .env keys:', miss.join(', ')); process.exit(1); }

/* ------------ config ------------ */
const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;
const PLAYER_ID = process.env.BRIGHTCOVE_PLAYER_ID;

const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 9);
const DOWNLOAD_MAX_VIDEOS = Number(process.env.DOWNLOAD_MAX_VIDEOS || 400);

const SCAN_DOMAINS = String(process.env.SCAN_DOMAINS || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const SCAN_MAX_PAGES = Number(process.env.SCAN_MAX_PAGES || 2000);
const SCAN_CONCURRENCY = Number(process.env.SCAN_CONCURRENCY || 8);
const SCAN_TIMEOUT_MS = Number(process.env.SCAN_TIMEOUT_MS || 12000);
const SCAN_USER_AGENT = process.env.SCAN_USER_AGENT || 'Brightcove-Embed-Scanner/1.0 (+contact site admin)';

const CMS_PAGE_LIMIT = 100;

/* ------------ axios (keep-alive) ------------ */
const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const axiosHttp  = axios.create({ timeout: 15000, httpAgent, httpsAgent });

/* ------------ middleware ------------ */
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));

/* ------------ helpers ------------ */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function withRetry(fn, { tries = 3, baseDelay = 400 } = {}) {
  let last;
  for (let i=0;i<tries;i++){
    try { return await fn(); }
    catch(err){
      last = err;
      const s = err?.response?.status;
      const retriable = s===429 || (s>=500&&s<600) || err.code==='ECONNABORTED';
      if (!retriable || i===tries-1) throw err;
      await sleep(baseDelay * Math.pow(2, i));
    }
  }
  throw last;
}
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const esc = s => String(s).replace(/"/g, '\\"');
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());

/* ------------ token cache ------------ */
let tokenCache = { access_token: null, expires_at: 0 };
async function getAccessToken() {
  const now = Date.now();
  if (tokenCache.access_token && now < tokenCache.expires_at - 30000) return tokenCache.access_token;
  const r = await withRetry(() =>
    axiosHttp.post('https://oauth.brightcove.com/v4/access_token','grant_type=client_credentials',{
      auth: { username: process.env.BRIGHTCOVE_CLIENT_ID, password: process.env.BRIGHTCOVE_CLIENT_SECRET },
      headers: { 'Content-Type':'application/x-www-form-urlencoded' }
    })
  );
  const ttl = (r.data?.expires_in ?? 300)*1000;
  tokenCache = { access_token: r.data.access_token, expires_at: Date.now() + ttl };
  return tokenCache.access_token;
}

/* ------------ CMS helpers ------------ */
async function cmsSearch(q, token, { limit = CMS_PAGE_LIMIT, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const { data } = await withRetry(() =>
    axiosHttp.get(url, { headers:{ Authorization:`Bearer ${token}` }, params:{ q, fields, sort, limit, offset } })
  );
  return data || [];
}
async function fetchAllPages(q, token) {
  const out = []; let offset = 0;
  while (true) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < CMS_PAGE_LIMIT) break;
    offset += CMS_PAGE_LIMIT;
    if (out.length > 20000) break; // safety
  }
  return out;
}
async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const { data } = await withRetry(() =>
    axiosHttp.get(url, { headers:{ Authorization:`Bearer ${token}` } })
  );
  return data;
}

/* ------------ PRECISE QUERY PARSING ------------ */
function parseQuery(input) {
  // split by commas; keep quoted chunks intact
  const parts = String(input || '')
    .split(',')
    .map(s => s.trim().replace(/^"(.*)"$/,'$1').replace(/^'(.*)'$/,'$1'))
    .filter(Boolean);

  const ids = [], tagTerms = [], titleTerms = [];

  for (const tok of parts) {
    const m = tok.match(/^(id|tag|title)\s*:(.*)$/i);
    if (m) {
      const key = m[1].toLowerCase();
      const val = m[2].trim().replace(/^"(.*)"$/,'$1').replace(/^'(.*)'$/,'$1');
      if (!val) continue;

      if (key === 'id') {
        for (const x of val.split(/\s+/).filter(Boolean)) if (looksLikeId(x)) ids.push(x);
      } else if (key === 'tag') {
        tagTerms.push(val);
      } else if (key === 'title') {
        for (const t of val.split(/\s+/).filter(Boolean)) titleTerms.push(t);
      }
      continue;
    }

    // bare numeric → IDs
    if (looksLikeId(tok)) { ids.push(tok); continue; }

    // otherwise treat as title terms
    for (const t of tok.split(/\s+/).filter(Boolean)) titleTerms.push(t);
  }

  return { ids, tagTerms, titleTerms };
}

/* ------------ PRECISE UNIFIED SEARCH ------------ */
async function unifiedSearch(input, token) {
  const { ids, tagTerms, titleTerms } = parseQuery(input);

  // 1) If explicit IDs present, fetch exactly those
  if (ids.length) {
    const out = [];
    await Promise.allSettled(ids.map(id =>
      fetchVideoById(id, token).then(v => { if (v && v.state === 'ACTIVE') out.push(v); })
    ));
    const seen = new Set();
    return out
      .filter(v => v && v.id && v.state==='ACTIVE' && !seen.has(v.id) && seen.add(v.id))
      .map(v => ({
        id: v.id,
        name: v.name || 'Untitled',
        tags: v.tags || [],
        thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail',
        created_at: v.created_at
      }))
      .sort((a,b)=>new Date(b.created_at)-new Date(a.created_at));
  }

  // 2) Build a single ANDed CMS query:
  //    state:ACTIVE AND tags:"t1" AND ... AND name:*w1* AND name:*w2* ...
  const parts = ['state:ACTIVE'];
  for (const t of tagTerms)  parts.push(`tags:"${esc(t)}"`);
  for (const w of titleTerms) parts.push(`name:*${esc(w)}*`);
  if (parts.length === 1) return []; // no constraints → don't dump catalog
  const q = parts.join(' ').trim();

  // 3) Fetch all pages for the ANDed query
  const rows = await fetchAllPages(q, token);

  // 4) Normalize, de-dupe, newest first
  const seen = new Set(); const list = [];
  for (const v of rows) {
    if (!v || !v.id || v.state !== 'ACTIVE' || seen.has(v.id)) continue;
    seen.add(v.id);
    list.push({
      id: v.id,
      name: v.name || 'Untitled',
      tags: v.tags || [],
      thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail',
      created_at: v.created_at
    });
  }
  list.sort((a,b)=>new Date(b.created_at)-new Date(a.created_at));
  return list;
}

/* ------------ analytics (all-time + metrics) ------------ */
async function getAnalyticsForVideo(videoId, token) {
  const infoUrl = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${videoId}`;
  const alltimeViewsUrl = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;
  const metricsUrl = `https://analytics.api.brightcove.com/v1/data?accounts=${AID}&dimensions=video&where=video==${videoId}&fields=video,engagement_score,play_rate,video_seconds_viewed,video_impression&from=alltime&to=now`;

  const [info, alltime, m] = await Promise.all([
    withRetry(() => axiosHttp.get(infoUrl, { headers:{ Authorization:`Bearer ${token}` } })),
    withRetry(() => axiosHttp.get(alltimeViewsUrl, { headers:{ Authorization:`Bearer ${token}` } })),
    withRetry(() => axiosHttp.get(metricsUrl, { headers:{ Authorization:`Bearer ${token}` } })),
  ]);

  const title = info.data?.name || 'Untitled';
  const tags = info.data?.tags || [];
  const publishedAt = info.data?.published_at || info.data?.created_at;
  const views = alltime.data?.alltime_video_views ?? alltime.data?.alltime_videos_views ?? 0;

  const it = (m.data?.items||[])[0] || {};
  const impressions = it.video_impression || 0;
  const engagement = it.engagement_score || 0;
  const playRate   = it.play_rate || 0;
  const secondsViewed = it.video_seconds_viewed || 0;

  let daysSince = 1;
  if (publishedAt) {
    const ts = new Date(publishedAt).getTime();
    if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((Date.now() - ts) / 86400000));
  }
  const dailyAvgViews = Number(((views || 0) / daysSince).toFixed(2));

  return { id: videoId, title, tags, views, dailyAvgViews, impressions, engagement, playRate, secondsViewed };
}

/* ------------ UI style (dark/light toggle) ------------ */
function themeHead(){ return `
  <style>
    :root{ --bg:#0b0b0d; --panel:#121217; --border:#262633; --text:#e9eef5; --muted:#9aa3af; --link:#7cc5ff; --btn:#14b8a6; --btnText:#031313; --btnHover:#10a195; }
    :root[data-theme="light"]{ --bg:#ffffff; --panel:#f8f9fa; --border:#e5e7eb; --text:#0b1220; --muted:#6b7280; --link:#0b63ce; --btn:#001f3f; --btnText:#ffffff; --btnHover:#003366; }
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:'Open Sans',system-ui,Arial,sans-serif}
    a{color:var(--link)} header{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid var(--border);background:var(--panel)}
    .toggle{border:1px solid var(--border);padding:8px 12px;border-radius:999px;background:transparent;color:var(--text);cursor:pointer}
    main{max-width:1100px;margin:24px auto;padding:0 20px}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:24px}
    input{width:100%;padding:12px;border:1px solid var(--border);background:transparent;color:var(--text);border-radius:10px}
    .btn{padding:12px 16px;background:var(--btn);color:var(--btnText);border:none;border-radius:10px;cursor:pointer;font-weight:700;margin-top:12px}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:18px;margin-top:12px}
    .vcard{border:1px solid var(--border);border-radius:10px;overflow:hidden}
    .vcard iframe{width:100%;aspect-ratio:16/9;border:0;background:#000}
    .meta{padding:12px 14px}.title{font-weight:700;font-size:15px}.id{color:var(--muted);font-size:12.5px}
  </style>
  <script>(function(){try{var s=localStorage.getItem('theme')||'dark';document.documentElement.setAttribute('data-theme',s);}catch(e){document.documentElement.setAttribute('data-theme','dark');}})();</script>
`; }
function themeToggle(){ return `
  <button class="toggle" id="themeToggle">Toggle Theme</button>
  <script>(function(){var b=document.getElementById('themeToggle');function cur(){return document.documentElement.getAttribute('data-theme')||'dark';}
  b.addEventListener('click',function(){var n=cur()==='dark'?'light':'dark';document.documentElement.setAttribute('data-theme',n);try{localStorage.setItem('theme',n);}catch(e){}});})();</script>
`; }

/* ------------ Health ------------ */
app.get('/healthz', (_req, res) => res.send('ok'));

/* ------------ Home (search + recent uploads) ------------ */
app.get('/', async (_req, res) => {
  let recent = [];
  try {
    const token = await getAccessToken();
    recent = await cmsSearch('state:ACTIVE', token, { limit: RECENT_LIMIT, sort:'-created_at' });
  } catch {}
  const cards = recent.map(v => `
    <div class="vcard">
      <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}" allow="encrypted-media" allowfullscreen loading="lazy" title="${stripHtml(v.name||'Untitled')}"></iframe>
      <div class="meta"><div class="title">${stripHtml(v.name||'Untitled')}</div><div class="id">ID: ${v.id}</div></div>
    </div>`).join('');
  res.send(`<!doctype html><html><head><meta charset="utf-8"/><title>Brightcove Tools</title>
    <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">${themeHead()}</head>
    <body><header><h1>Brightcove Video Tools</h1>${themeToggle()}</header>
    <main><div class="card"><h2>🔍 Search by ID, Tag(s), or Title</h2>
      <form action="/search" method="get"><input name="q" placeholder='Examples: id:637..., tag:"pega platform", title:"customer decision hub"' required><button class="btn" type="submit">Search</button></form>
    </div>
    <div class="card" style="margin-top:20px"><h2>🆕 Most Recent Uploads</h2><div class="grid">${cards || '<div class="id">No recent uploads.</div>'}</div></div>
    </main></body></html>`);
});

/* ------------ Search results page ------------ */
app.get('/search', async (req, res) => {
  const q = (req.query.q || '').trim(); if (!q) return res.redirect('/');
  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(q, token);
    const cards = videos.map(v => `
      <div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}" allowfullscreen loading="lazy"></iframe>
        <div class="meta"><div class="title">${stripHtml(v.name)}</div><div class="id">ID: ${v.id}</div></div>
      </div>`).join('');
    res.send(`<!doctype html><html><head><meta charset="utf-8"/><title>Results</title>
      <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">${themeHead()}</head>
      <body><header><a href="/">← Back</a>${themeToggle()}</header>
      <main><div class="card" style="margin-bottom:12px"><a class="btn" href="/download?q=${encodeURIComponent(q)}">Download Video Analytics Spreadsheet</a></div>
      <div class="card"><div class="grid">${cards || '<div class="id">No videos found.</div>'}</div></div></main></body></html>`);
  } catch (e) {
    console.error('Search error:', e?.response?.status, e?.response?.data || e.message);
    res.status(500).send('Error searching.');
  }
});

/* ------------ Sitemap scan utilities ------------ */
async function fetchText(url, timeoutMs = SCAN_TIMEOUT_MS) {
  const res = await axiosHttp.get(url, {
    timeout: timeoutMs,
    responseType: 'arraybuffer',
    headers: { 'User-Agent': SCAN_USER_AGENT, 'Accept-Encoding': 'gzip,deflate' },
    validateStatus: s => s>=200 && s<400
  });
  let buf = res.data;
  const enc = (res.headers['content-encoding'] || '').toLowerCase();
  if (enc.includes('gzip')) buf = zlib.gunzipSync(buf);
  else if (enc.includes('deflate')) buf = zlib.inflateSync(buf);
  else if (/\.gz(\?|$)/i.test(url)) buf = zlib.gunzipSync(buf);
  return buf.toString('utf8');
}
function parseSitemapLocs(xml) {
  const locs = []; const re = /<loc>\s*([^<\s]+)\s*<\/loc>/gi; let m;
  while ((m = re.exec(xml))) locs.push(m[1]);
  return locs;
}
function urlAllowed(u, allowed) {
  try {
    const x = new URL(u);
    if (!/^https?:$/.test(x.protocol)) return false;
    const h = x.hostname.toLowerCase();
    for (const d of allowed) if (h===d || h.endsWith('.'+d)) return true;
    return false;
  } catch { return false; }
}
async function discoverPagesFromSitemaps(domains, maxPagesTotal) {
  const allowed = domains.map(d => d.toLowerCase());
  const pages = new Set();
  async function processSitemap(url) {
    if (pages.size >= maxPagesTotal) return;
    try {
      const xml = await fetchText(url);
      const locs = parseSitemapLocs(xml);
      const subs = locs.filter(u => /\.xml(\.gz)?$/i.test(u));
      if (subs.length && subs.length >= locs.length * 0.5) {
        for (const u of subs) { if (pages.size >= maxPagesTotal) break; if (urlAllowed(u, allowed)) await processSitemap(u); }
      } else {
        for (const u of locs) { if (pages.size >= maxPagesTotal) break; if (urlAllowed(u, allowed)) pages.add(u); }
      }
    } catch (e) { console.warn('[sitemap] failed', url, e.message); }
  }
  await Promise.allSettled(domains.map(d => processSitemap(`https://${d}/sitemap.xml`)));
  return Array.from(pages);
}
function buildPatternsForId(vid) {
  return [
    new RegExp(`videoId=${vid}(?:[^0-9]|$)`,'i'),
    new RegExp(`data-video-id=["']${vid}["']`,'i'),
    new RegExp(`data-brightcove-video-id=["']${vid}["']`,'i'),
    new RegExp(`data-experience-video-id=["']${vid}["']`,'i'),
    new RegExp(`"videoId"\\s*:\\s*["']${vid}["']`,'i'),
    new RegExp(`"data-video-id"\\s*:\\s*["']${vid}["']`,'i'),
    new RegExp(`"brightcoveVideoId"\\s*:\\s*["']${vid}["']`,'i'),
    new RegExp(`"bcVideoId"\\s*:\\s*["']${vid}["']`,'i'),
    new RegExp(`\\bdata-video-id=${vid}\\b`,'i'),
    new RegExp(`videojs\$begin:math:text$[^)]+\\$end:math:text$[\\s\\S]{0,200}?["']${vid}["']`,'i'),
    new RegExp(`brightcove[\\s\\S]{0,200}?["']${vid}["']`,'i'),
    new RegExp(`players\\.brightcove\\.net\\/\\d+\\/[^\\s"'<>]+`,'i'),
  ];
}
async function scanPageForIds(pageUrl, ids) {
  try {
    const html = await fetchText(pageUrl);
    const hits = [];
    for (const vid of ids) {
      const pats = buildPatternsForId(vid);
      for (const rx of pats) {
        if (rx.test(html)) { hits.push({ id: String(vid), url: pageUrl }); break; }
      }
    }
    return hits;
  } catch { return []; }
}
async function runSitemapScan(ids, { domains = SCAN_DOMAINS, maxPages = SCAN_MAX_PAGES, concurrency = SCAN_CONCURRENCY } = {}) {
  if (!domains.length || !ids.length) return new Map();
  const pages = await discoverPagesFromSitemaps(domains, maxPages);
  let i = 0; const found = new Map();
  for (const id of ids) found.set(String(id), new Set());
  async function worker() {
    while (i < pages.length) {
      const idx = i++; const url = pages[idx];
      const hits = await scanPageForIds(url, ids);
      for (const h of hits) found.get(h.id).add(h.url);
    }
  }
  await Promise.all(Array.from({length: Math.min(concurrency, pages.length)}, worker));
  return found;
}

/* ------------ DOWNLOAD: always runs scan and adds column ------------ */
app.get('/download', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q) return res.status(400).send('Missing search terms');

  try {
    const token = await getAccessToken();
    let videos = await unifiedSearch(q, token);
    if (!videos.length) return res.status(404).send('No videos found for that query.');

    // cap for safety
    let truncated = false;
    if (videos.length > DOWNLOAD_MAX_VIDEOS) { videos = videos.slice(0, DOWNLOAD_MAX_VIDEOS); truncated = true; }

    const ids = videos.map(v => v.id);

    // analytics (resilient)
    const rows = [];
    for (const v of videos) {
      try {
        rows.push(await getAnalyticsForVideo(v.id, token));
      } catch (e1) {
        console.warn(`Retrying metrics for ${v.id}...`, e1.message);
        await sleep(600);
        try { rows.push(await getAnalyticsForVideo(v.id, token)); }
        catch (e2) {
          console.error(`Failed metrics for ${v.id}:`, e2.message);
          rows.push({ id: v.id, title: v.name || 'Error', tags: v.tags||[], views:'N/A', dailyAvgViews:'N/A', impressions:'N/A', engagement:'N/A', playRate:'N/A', secondsViewed:'N/A' });
        }
      }
    }

    // sitemap scan (ALWAYS)
    const embedsMap = await runSitemapScan(ids);

    // Excel
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('Video Metrics (All-Time)');
    ws.columns = [
      { header: 'Video ID', key: 'id', width: 20 },
      { header: 'Title', key: 'title', width: 40 },
      { header: 'All-Time Views', key: 'views', width: 18 },
      { header: 'Daily Avg Views', key: 'dailyAvgViews', width: 18 },
      { header: 'All-Time Impressions', key: 'impressions', width: 22 },
      { header: 'Engagement Score', key: 'engagement', width: 18 },
      { header: 'Play Rate', key: 'playRate', width: 12 },
      { header: 'Seconds Viewed', key: 'secondsViewed', width: 18 },
      { header: 'Tags', key: 'tags', width: 40 },
      { header: 'Embedded On (URLs)', key: 'embeddedOn', width: 80 },
    ];
    if (truncated) ws.addRow({ id:'NOTE', title:`Export capped at ${DOWNLOAD_MAX_VIDEOS} newest items.` });

    const titleById = new Map(videos.map(v => [String(v.id), v.name || 'Untitled']));
    const tagsById = new Map(videos.map(v => [String(v.id), v.tags || []]));
    for (const r of rows) {
      const urls = Array.from(embedsMap.get(String(r.id)) || []);
      ws.addRow({
        id: r.id,
        title: r.title || titleById.get(String(r.id)) || 'Untitled',
        views: r.views,
        dailyAvgViews: r.dailyAvgViews,
        impressions: r.impressions,
        engagement: r.engagement,
        playRate: r.playRate,
        secondsViewed: r.secondsViewed,
        tags: (r.tags && r.tags.length ? r.tags : tagsById.get(String(r.id)) || []).join(', '),
        embeddedOn: urls.join(' ; ')
      });
    }

    // Raw embeds sheet (audit)
    const wf = wb.addWorksheet('Embeds Found');
    wf.columns = [
      { header: 'Video ID', key: 'id', width: 20 },
      { header: 'Page URL', key: 'url', width: 90 },
    ];
    for (const [vid, set] of embedsMap.entries()) {
      for (const u of set) wf.addRow({ id: vid, url: u });
    }
    if (!embedsMap.size) wf.addRow({ id:'INFO', url:'No embeds found via sitemaps; check SCAN_DOMAINS or increase SCAN_MAX_PAGES.' });

    const buf = await wb.xlsx.writeBuffer();
    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_alltime.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Length', buf.byteLength);
    return res.end(Buffer.from(buf));
  } catch (e) {
    console.error('Download error:', e?.response?.status, e?.response?.data || e.message);
    return res.status(500).send('Error generating spreadsheet.');
  }
});

/* ------------ 404 + start ------------ */
app.use((req, res) => res.status(404).send('Not found'));
const server = app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
server.keepAliveTimeout = 120000;
server.headersTimeout   = 125000;
server.requestTimeout   = 0;
