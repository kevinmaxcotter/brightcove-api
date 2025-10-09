// server.js — Brightcove tools with Destination Path view sources (all-time)
// - Home: search form + most recent uploads
// - Results: playable cards, tags, "Download Video Analytics Spreadsheet"
// - Spreadsheet: all-time metrics + "View Sources (URLs & Views)" from Analytics
// - Light/Dark toggle with emojis
// - /healthz for platform health checks
//
// Env required:
//   BRIGHTCOVE_ACCOUNT_ID, BRIGHTCOVE_CLIENT_ID, BRIGHTCOVE_CLIENT_SECRET, BRIGHTCOVE_PLAYER_ID
// Optional:
//   RECENT_LIMIT, DOWNLOAD_MAX_VIDEOS, METRICS_CONCURRENCY, EMBED_CONCURRENCY, DOWNLOAD_TIME_BUDGET_MS

require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');
const http = require('http');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

/* ---------- visibility ---------- */
process.on('unhandledRejection', err => console.error('UNHANDLED REJECTION:', err?.stack || err));
process.on('uncaughtException', err => console.error('UNCAUGHT EXCEPTION:', err?.stack || err));

/* ---------- env checks ---------- */
const MUST = ['BRIGHTCOVE_ACCOUNT_ID','BRIGHTCOVE_CLIENT_ID','BRIGHTCOVE_CLIENT_SECRET','BRIGHTCOVE_PLAYER_ID'];
const missing = MUST.filter(k => !process.env[k]);
if (missing.length) console.error('Missing .env keys:', missing.join(', '));

/* ---------- config ---------- */
const AID = process.env.BRIGHTCOVE_ACCOUNT_ID || '';
const PLAYER_ID = process.env.BRIGHTCOVE_PLAYER_ID || '';

const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 9);
const DOWNLOAD_MAX_VIDEOS = Number(process.env.DOWNLOAD_MAX_VIDEOS || 400);
const DOWNLOAD_TIME_BUDGET_MS = Number(process.env.DOWNLOAD_TIME_BUDGET_MS || 60000);
const METRICS_CONCURRENCY = Number(process.env.METRICS_CONCURRENCY || 6);
const EMBED_CONCURRENCY = Number(process.env.EMBED_CONCURRENCY || 6); // reused for destination sources

const CMS_PAGE_LIMIT = 100;

/* ---------- axios keep-alive ---------- */
const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });
const axiosHttp  = axios.create({ timeout: 15000, httpAgent, httpsAgent });

/* ---------- middleware ---------- */
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));

/* ---------- helpers ---------- */
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
const esc = s => String(s).replace(/"/g, '\\"');
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());

function titleContainsAll(video, terms) {
  if (!terms.length) return true;
  const name = (video.name || '').toLowerCase();
  return terms.every(t => name.includes(String(t).toLowerCase()));
}
function hasAllTags(video, terms) {
  if (!terms.length) return true;
  const vt = (video.tags || []).map(t => String(t).toLowerCase());
  return terms.every(t => vt.includes(String(t).toLowerCase()));
}

/* ---------- auth ---------- */
let tokenCache = { access_token: null, expires_at: 0 };
async function getAccessToken() {
  if (!AID) throw new Error('Missing BRIGHTCOVE_ACCOUNT_ID');
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

/* ---------- CMS ---------- */
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
    if (out.length > 20000) break;
  }
  return out;
}
async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const { data } = await withRetry(() =>
    axiosHttp.get(url, { headers: { Authorization: `Bearer ${token}` } })
  );
  return data;
}

/* ---------- precise query parsing ---------- */
function parseQuery(input) {
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
    if (looksLikeId(tok)) { ids.push(tok); continue; }
    tagTerms.push(tok); // bare terms as tag terms (AND)
  }
  return { ids, tagTerms, titleTerms };
}

/* ---------- unified search (ID or Tag AND + Title AND) ---------- */
async function unifiedSearch(input, token) {
  const { ids, tagTerms, titleTerms } = parseQuery(input);

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

  const parts = ['state:ACTIVE'];
  for (const t of tagTerms)  parts.push(`tags:"${esc(t)}"`);
  for (const w of titleTerms) parts.push(`name:*${esc(w)}*`);
  if (parts.length === 1) return [];
  const q = parts.join(' ').trim();

  const rows = await fetchAllPages(q, token);

  const filtered = rows.filter(v =>
    v && v.state === 'ACTIVE' &&
    hasAllTags(v, tagTerms) &&
    titleContainsAll(v, titleTerms)
  );

  const seen = new Set(); const list = [];
  for (const v of filtered) {
    if (!v.id || seen.has(v.id)) continue;
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

/* ---------- analytics (all-time) ---------- */
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
  const tags  = info.data?.tags || [];
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

/* ---------- NEW: Destination Path view sources (all-time) ---------- */
async function getViewSources(videoId, token) {
  // Returns array of { url: 'https://domain/path', views: number }
  const base = 'https://analytics.api.brightcove.com/v1/data';
  const params = new URLSearchParams({
    accounts: AID,
    dimensions: 'destination_domain,destination_path',
    where: `video==${videoId}`,
    from: 'alltime',
    to: 'now',
    fields: 'destination_domain,destination_path,video_view',
    limit: '10000'
  });
  const url = `${base}?${params.toString()}`;

  const { data } = await withRetry(() =>
    axiosHttp.get(url, { headers: { Authorization: `Bearer ${token}` } })
  );

  const items = data?.items || [];
  const out = [];
  for (const it of items) {
    const dom = (it.destination_domain || '').trim();
    let path = (it.destination_path || '').trim();
    if (!dom) continue;
    if (!path) path = '/';
    if (!path.startsWith('/')) path = '/' + path;
    const views = Number(it.video_view || 0);
    out.push({ url: `https://${dom}${path}`, views });
  }
  out.sort((a,b) => b.views - a.views);
  return out;
}

/* ---------- UI (unchanged) ---------- */
function themeHead(){ return `
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root { --bg:#ffffff; --text:#001f3f; --border:#e5e7eb; --muted:#6b7280; --chip:#eef2f7; --chipBorder:#c7ccd3; --btn:#001f3f; --btnHover:#003366; --btnText:#fff; }
  :root[data-theme="dark"] { --bg:#0b0c10; --text:#eaeaea; --border:#2a2f3a; --muted:#9aa3af; --chip:#1a1f29; --chipBorder:#2a2f3a; --btn:#14b8a6; --btnHover:#10a195; --btnText:#031313; }
  *{box-sizing:border-box}
  body{font-family:'Open Sans',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--text);margin:0}
  header{display:flex;align-items:center;justify-content:space-between;padding:20px;border-bottom:1px solid var(--border)}
  main{max-width:980px;margin:20px auto;padding:0 20px}
  .card{background:rgba(0,0,0,0.0);border:1px solid var(--border);border-radius:12px;padding:24px}
  input{width:100%;padding:12px 14px;border:1px solid var(--chipBorder);border-radius:10px;background:transparent;color:var(--text)}
  .btn{display:inline-block;padding:10px 14px;background:var(--btn);color:var(--btnText);border-radius:10px;text-decoration:none;font-weight:700}
  .btn:hover{background:var(--btnHover)}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:18px}
  .vcard{background:transparent;border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .vcard iframe{width:100%;aspect-ratio:16/9;border:0;background:#000}
  .meta{padding:12px 14px}
  .title{font-weight:700;font-size:15px;margin-bottom:4px}
  .id{color:var(--muted);font-size:13px;margin-top:4px}
  .topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
  .toggle{cursor:pointer;padding:6px 10px;border:1px solid var(--chipBorder);border-radius:8px;font-size:.9rem;background:transparent;color:var(--text)}
</style>
<script>(function(){try{var s=localStorage.getItem('theme')||'light';document.documentElement.setAttribute('data-theme',s);}catch(e){}})();</script>
`; }
function themeToggle(){ return `
  <button class="toggle" id="modeToggle">🌙 Dark Mode</button>
  <script>(function(){
    function isDark(){ return (document.documentElement.getAttribute('data-theme')||'light')==='dark'; }
    function setLabel(btn){ btn.textContent = isDark() ? '☀️ Light Mode' : '🌙 Dark Mode'; }
    var b=document.getElementById('modeToggle'); setLabel(b);
    b.addEventListener('click',function(){
      var next = isDark() ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      try{ localStorage.setItem('theme', next); }catch(e){}
      setLabel(b);
    });
  })();</script>
`; }

/* ---------- Health ---------- */
app.get('/healthz', (_req, res) => res.send('ok'));

/* ---------- Home ---------- */
app.get('/', async (req, res) => {
  const qPrefill = (req.query.q || '').replace(/`/g, '\\`');

  const warn = missing.length
    ? `<div style="background:#ffefef;border:1px solid #f5b5b5;padding:10px;border-radius:8px;color:#8b0000;margin-bottom:10px">Missing .env keys: ${missing.join(', ')}</div>`
    : '';

  let recentHTML = '';
  try {
    const token = await getAccessToken();
    const recent = await cmsSearch('state:ACTIVE', token, { limit: RECENT_LIMIT, sort: '-created_at' });
    recentHTML = recent.map(v => `
      <div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}"
                allow="encrypted-media" allowfullscreen loading="lazy"
                title="${stripHtml(v.name || 'Untitled')}"></iframe>
        <div class="meta">
          <div class="title">${stripHtml(v.name || 'Untitled')}</div>
          <div class="id">ID: ${v.id}</div>
        </div>
      </div>
    `).join('');
  } catch (e) {
    console.error('Recent fetch error:', e?.response?.data || e.message);
    recentHTML = '<div class="id">Error fetching recent videos.</div>';
  }

  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Brightcove Video Tools</title>
  ${themeHead()}
</head>
<body>
  <header>
    <h1>Brightcove Video Tools</h1>
    ${themeToggle()}
  </header>
  <main>
    ${warn}
    <div class="card" style="max-width:520px;margin:0 auto 20px">
      <h2>🔍 Search by ID, Tag(s), or Title</h2>
      <form action="/search" method="get">
        <input id="q" name="q" placeholder='Examples: 6376653485112, tag:"pega platform", title:"customer decision hub"' required />
        <button class="btn" type="submit" style="width:100%;margin-top:12px">Search</button>
        <div class="id" style="margin-top:8px">IDs → exact match. Multiple tags → AND. Titles → must contain all terms.</div>
      </form>
    </div>

    <div class="card" style="margin-top:20px">
      <h2>🆕 Most Recent Uploads</h2>
      <div class="grid" style="margin-top:12px">
        ${recentHTML}
      </div>
    </div>
  </main>
  <script>(function(){var v=${JSON.stringify(qPrefill)}; if(v) document.getElementById('q').value=v;})();</script>
</body>
</html>`);
});

/* ---------- Results page ---------- */
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.redirect('/');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;

    const cards = videos.map(v => `
      <div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}"
                allow="encrypted-media" allowfullscreen loading="lazy"
                title="${stripHtml(v.name)}"></iframe>
        <div class="meta">
          <div class="title">${stripHtml(v.name)}</div>
          <div class="id">ID: ${v.id}</div>
          <div class="id"><strong>Tags:</strong> ${ (v.tags && v.tags.length ? v.tags.map(stripHtml).join(', ') : 'None') }</div>
        </div>
      </div>
    `).join('');

    res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Results for: ${stripHtml(qInput)}</title>
  ${themeHead()}
</head>
<body>
  <header>
    <a href="/" style="text-decoration:none;color:var(--text)">← Back to search</a>
    ${themeToggle()}
  </header>
  <main>
    <div class="topbar">
      <div></div>
      <a class="btn" href="${downloadUrl}">Download Video Analytics Spreadsheet</a>
    </div>
    <div class="card">
      <div class="grid" style="margin-top:12px">
        ${cards || '<div>No videos found.</div>'}
      </div>
    </div>
  </main>
</body>
</html>`);
  } catch (err) {
    console.error('Search error:', err?.response?.status, err?.response?.data || err.message);
    res.status(500).send('Error searching.');
  }
});

/* ---------- Download (all-time analytics + destination paths) ---------- */
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.status(400).send('Missing search terms');

  const dlDeadline = Date.now() + DOWNLOAD_TIME_BUDGET_MS;

  try {
    const token  = await getAccessToken();
    let videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found for that search.');

    // cap for safety
    let truncated = false;
    if (videos.length > DOWNLOAD_MAX_VIDEOS) { videos = videos.slice(0, DOWNLOAD_MAX_VIDEOS); truncated = true; }

    // ---- metrics (concurrent, deadline-guarded) ----
    const rows = new Array(videos.length);
    let idxA = 0;

    async function metricsWorker() {
      while (Date.now() < dlDeadline && idxA < videos.length) {
        const i = idxA++; const v = videos[i];
        try {
          rows[i] = await getAnalyticsForVideo(v.id, token);
        } catch (e1) {
          await sleep(300);
          try { rows[i] = await getAnalyticsForVideo(v.id, token); }
          catch (e2) {
            console.error('metrics error for', v.id, e2?.response?.data || e2.message);
            rows[i] = { id: v.id, title: v.name || 'Error', tags: v.tags||[], views:'N/A', dailyAvgViews:'N/A', impressions:'N/A', engagement:'N/A', playRate:'N/A', secondsViewed:'N/A' };
          }
        }
      }
    }
    const metricsWorkers = Array.from({length: Math.min(METRICS_CONCURRENCY, videos.length)}, metricsWorker);
    await Promise.race([
      Promise.all(metricsWorkers),
      (async()=>{ while(Date.now()<dlDeadline) await sleep(100); })()
    ]);
    for (let i=0;i<videos.length;i++){
      if (!rows[i]) {
        const v = videos[i];
        rows[i] = { id: v.id, title: v.name || 'Timeout', tags: v.tags||[], views:'N/A', dailyAvgViews:'N/A', impressions:'N/A', engagement:'N/A', playRate:'N/A', secondsViewed:'N/A' };
      }
    }

    // ---- destination paths (concurrent) ----
    const sourcesMap = new Map(); // id -> [{url,views}, ...]
    let idxE = 0;
    async function embedWorker() {
      while (idxE < videos.length) {
        const i = idxE++; const v = videos[i];
        try {
          const sources = await getViewSources(v.id, token);
          sourcesMap.set(String(v.id), sources);
        } catch (e) {
          console.error('view sources error for', v.id, e?.response?.data || e.message);
          sourcesMap.set(String(v.id), []);
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(EMBED_CONCURRENCY, videos.length) }, embedWorker));

    // ---- build workbook ----
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
      { header: 'View Sources (URLs & Views)', key: 'viewSources', width: 90 },
    ];
    if (truncated) ws.addRow({ id:'NOTE', title:`Export capped at ${DOWNLOAD_MAX_VIDEOS} newest items.` });
    if (Date.now() >= dlDeadline) ws.addRow({ id:'NOTE', title:`Export reached time budget; some rows may show N/A.` });

    const titleById = new Map(videos.map(v => [String(v.id), v.name || 'Untitled']));
    const tagsById  = new Map(videos.map(v => [String(v.id), v.tags || []]));

    for (const r of rows) {
      const sources = sourcesMap.get(String(r.id)) || [];
      // show top 10 in main sheet
      const top = sources.slice(0, 10).map(s => `${s.url} (${s.views})`).join(' ; ');
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
        viewSources: top
      });
    }

    // Optional detail sheet: one row per destination path with counts
    const wf = wb.addWorksheet('View Sources Detail');
    wf.columns = [
      { header: 'Video ID', key: 'id', width: 20 },
      { header: 'Page URL', key: 'url', width: 90 },
      { header: 'Views (All-Time)', key: 'views', width: 20 },
    ];
    for (const v of videos) {
      const list = sourcesMap.get(String(v.id)) || [];
      for (const s of list) wf.addRow({ id: v.id, url: s.url, views: s.views });
      if (!list.length) wf.addRow({ id: v.id, url: '(no destinations reported)', views: 0 });
    }

    // ---- stream to client ----
    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_alltime.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Download error (top-level):', err?.response?.status, err?.response?.data || err.message);
    res.status(500).send('Error generating spreadsheet.');
  }
});

/* ---------- 404 + start ---------- */
app.get('/healthz', (_req, res) => res.send('ok'));
app.use((req, res) => res.status(404).send('Not found'));
const server = app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
server.keepAliveTimeout = 120000;
server.headersTimeout   = 125000;
server.requestTimeout   = 0;
