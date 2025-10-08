// server.js — Brightcove tools + ALWAYS scan sitemaps to list exact page URLs where video IDs are embedded
// Requires .env (set in Render Environment):
//   BRIGHTCOVE_ACCOUNT_ID, BRIGHTCOVE_CLIENT_ID, BRIGHTCOVE_CLIENT_SECRET, BRIGHTCOVE_PLAYER_ID
//   SCAN_DOMAINS=www.pega.com,community.pega.com,academy.pega.com,support.pega.com
// Optional tuning (with safe defaults):
//   PORT=3000
//   RECENT_LIMIT=9
//   DOWNLOAD_MAX_VIDEOS=400
//   SCAN_MAX_PAGES=2000
//   SCAN_CONCURRENCY=8
//   SCAN_TIMEOUT_MS=12000
//   SCAN_USER_AGENT=Brightcove-Embed-Scanner/1.0

require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');
const http = require('http');
const https = require('https');
const zlib = require('zlib');

const app = express();
const PORT = process.env.PORT || 3000;

/* ------------ global error visibility (helps diagnose Render issues) ------------ */
process.on('unhandledRejection', err => console.error('UNHANDLED REJECTION:', err?.stack || err));
process.on('uncaughtException', err => console.error('UNCAUGHT EXCEPTION:', err?.stack || err));

/* ------------ env checks ------------ */
const MUST = ['BRIGHTCOVE_ACCOUNT_ID','BRIGHTCOVE_CLIENT_ID','BRIGHTCOVE_CLIENT_SECRET','BRIGHTCOVE_PLAYER_ID'];
const missing = MUST.filter(k => !process.env[k]);
if (missing.length) { console.error('Missing .env keys:', missing.join(', ')); process.exit(1); }

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
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const esc = s => String(s).replace(/"/g, '\\"');

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

/* ------------ CMS search ------------ */
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
    if (out.length > 20000) break; // hard safety
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

/* ------------ unified search: IDs + tags AND + title AND ------------ */
function parseQuery(input) {
  const raw = String(input || '').split(',').map(s => s.trim().replace(/^"(.*)"$/,'$1').replace(/^'(.*)'$/,'$1')).filter(Boolean);
  const ids = [], tagTerms = [], titleTerms = [];
  for (let tok of raw) {
    const m = tok.match(/^(id|tag|title)\s*:(.*)$/i);
    if (m) {
      const key = m[1].toLowerCase(); const val = m[2].trim().replace(/^"(.*)"$/,'$1').replace(/^'(.*)'$/,'$1');
      if (!val) continue;
      if (key==='id' && looksLikeId(val)) ids.push(val);
      else if (key==='tag') tagTerms.push(val);
      else if (key==='title') titleTerms.push(val);
      continue;
    }
    if (looksLikeId(tok)) { ids.push(tok); continue; }
    tagTerms.push(tok); // bare tokens treated as tags
  }
  return { ids, tagTerms, titleTerms };
}
async function unifiedSearch(input, token) {
  const { ids, tagTerms, titleTerms } = parseQuery(input);
  const pool = [];

  // exact IDs
  await Promise.allSettled(ids.map(id => fetchVideoById(id, token).then(v => v && pool.push(v))));

  // tags AND
  if (tagTerms.length) {
    const qTags = ['state:ACTIVE', ...tagTerms.map(t => `tags:"${esc(t)}"`)].join(' ');
    const rows = await fetchAllPages(qTags, token);
    pool.push(...rows);
  }

  // title AND (name:*term*) intersection
  if (titleTerms.length) {
    const perTerm = await Promise.allSettled(
      titleTerms.map(t => fetchAllPages(`state:ACTIVE name:*${esc(t)}*`, token))
    );
    const buckets = perTerm.map(r => r.status==='fulfilled' ? r.value : []).map(arr => new Map(arr.map(v => [v.id, v])));
    if (buckets.length) {
      const counts = new Map();
      for (const b of buckets) for (const id of b.keys()) counts.set(id, (counts.get(id)||0)+1);
      const andIds = [...counts.entries()].filter(([,c]) => c===buckets.length).map(([id]) => id);
      const first = buckets[0];
      pool.push(...andIds.map(id => first.get(id)).filter(Boolean));
    }
  }

  // de-dupe + normalize + newest first
  const seen = new Set(); const list = [];
  for (const v of pool) {
    if (!v || !v.id || v.state!=='ACTIVE' || seen.has(v.id)) continue;
    seen.add(v.id);
    list.push({
      id: v.id,
      name: v.name || 'Untitled',
      tags: v.tags || [],
      created_at: v.created_at,
      published_at: v.published_at,
      thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'
    });
  }
  list.sort((a,b)=>new Date(b.created_at||0)-new Date(a.created_at||0));
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

/* ------------ UI: theme css (simple) ------------ */
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

/* ------------ UI: home ------------ */
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

/* ------------ UI: results ------------ */
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

/* ------------ Sitemap scanner (ALWAYS used in /download) ------------ */
// fetch text with gzip/deflate and .gz fallback
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
  else if (/\.gz(\?|$)/i.test(url)) buf = zlib.gunzipSync(buf); // filename-based fallback
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
    for (const d of allowed) { if (h===d || h.endsWith('.'+d)) return true; }
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
      const sub = locs.filter(u => /\.xml(\.gz)?$/i.test(u));
      if (sub.length && sub.length >= locs.length * 0.5) {
        for (const u of sub) { if (pages.size >= maxPagesTotal) break; if (urlAllowed(u, allowed)) await processSitemap(u); }
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
    new RegExp(`players\\.brightcove\\.net\\/\\d+\\/[^\\s"'<>]+`,'i')
  ];
}
async function scanPageForIds(pageUrl, ids) {
  try {
    const html = await fetchText(pageUrl);
    const out = [];
    for (const vid of ids) {
      const patterns = buildPatternsForId(vid);
      for (const rx of patterns) {
        const m = html.match(rx);
        if (m) { out.push({ id: String(vid), url: pageUrl }); break; }
      }
    }
    return out;
  } catch { return []; }
}
async function runSitemapScan(ids, { domains = SCAN_DOMAINS, maxPages = SCAN_MAX_PAGES, concurrency = SCAN_CONCURRENCY } = {}) {
  if (!domains.length || !ids.length) return new Map();
  const pages = await discoverPagesFromSitemaps(domains, maxPages);
  let i = 0; const found = new Map(); // id -> Set(url)
  for (const id of ids) found.set(String(id), new Set());
  async function worker() {
    while (i < pages.length) {
      const idx = i++; const url = pages[idx];
      const hits = await scanPageForIds(url, ids);
      for (const h of hits) found.get(h.id).add(h.url);
    }
  }
  await Promise.all(Array.from({length: Math.min(concurrency, pages.length)}, worker));
  return found; // Map(id -> Set(url))
}

/* ------------ DOWNLOAD: always runs scan and adds column ------------ */
app.get('/download', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q) return res.status(400).send('Missing search terms');

  const errors = [];
  try {
    const token = await getAccessToken();
    let videos = await unifiedSearch(q, token);
    if (!videos.length) return res.status(404).send('No videos found for that query.');

    // cap for safety
    let truncated = false;
    if (videos.length > DOWNLOAD_MAX_VIDEOS) { videos = videos.slice(0, DOWNLOAD_MAX_VIDEOS); truncated = true; }

    const ids = videos.map(v => v.id);

    // analytics per video with retry (keeps whole export resilient)
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

    // ALWAYS run sitemap scan to find page URLs for each ID
    const embedsMap = await runSitemapScan(ids); // Map(id -> Set(url))

    // build Excel
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

    // separate sheet with raw embed hits (optional, useful for auditing)
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
    if (errors.length) console.error('Errors:', errors);
    return res.status(500).send('Error generating spreadsheet.');
  }
});

/* ------------ misc ------------ */
app.get('/healthz', (_req, res) => res.send('ok'));
app.use((req, res) => res.status(404).send('Not found'));

/* ------------ start ------------ */
const server = app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
// Render keep-alive friendliness
server.keepAliveTimeout = 120000;
server.headersTimeout   = 125000;
server.requestTimeout   = 0;
