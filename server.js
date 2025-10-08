// server.js — Brightcove Tools + Sitemap-based Embed Scanner
// Features:
// - Search (id:, tag:, title:), recent uploads, light/dark toggle
// - All-time analytics export (hybrid: /v1/data + /v1/alltime fallback for views)
// - Optional placements (off by default)
// - Streaming XLSX, keep-alive, 502 hardening, never-500 export (errors sheet)
// - NEW: Sitemap-based scanner finds page URLs where VIDEO_ID appears (no view counts)
//
// .env knobs (with safe defaults):
//   BRIGHTCOVE_ACCOUNT_ID=...
//   BRIGHTCOVE_CLIENT_ID=...
//   BRIGHTCOVE_CLIENT_SECRET=...
//   BRIGHTCOVE_PLAYER_ID=...
//   PORT=3000
//   RECENT_LIMIT=9
//   DOWNLOAD_MAX_VIDEOS=400
//   PLACEMENTS_ENABLED=false
//   PLACEMENTS_WINDOW=alltime
//   SEARCH_ACTIVE_ONLY=false
//   CMS_HARD_CAP_ALLPAGES=20000
//   NAME_MAX_PAGES=5
//   NAME_TIME_BUDGET_MS=8000
//   SCAN_ENABLED=true
//   SCAN_DOMAINS=www.pega.com,community.pega.com,academy.pega.com,support.pega.com
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

/* -------------------- ENV CHECKS -------------------- */
const MUST = ['BRIGHTCOVE_ACCOUNT_ID','BRIGHTCOVE_CLIENT_ID','BRIGHTCOVE_CLIENT_SECRET','BRIGHTCOVE_PLAYER_ID'];
const missing = MUST.filter(k => !process.env[k]);
if (missing.length) { console.error('Missing .env keys:', missing.join(', ')); process.exit(1); }

/* -------------------- CONFIG -------------------- */
const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;
const PLAYER_ID = process.env.BRIGHTCOVE_PLAYER_ID;
const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 9);

const DOWNLOAD_MAX_VIDEOS = Number(process.env.DOWNLOAD_MAX_VIDEOS || 400);

const PLACEMENTS_ENABLED = String(process.env.PLACEMENTS_ENABLED || 'false').toLowerCase() === 'true';
const PLACEMENTS_WINDOW = process.env.PLACEMENTS_WINDOW || 'alltime';

const SEARCH_ACTIVE_ONLY = String(process.env.SEARCH_ACTIVE_ONLY || 'false').toLowerCase() === 'true';

const CMS_PAGE_LIMIT = 100;
const CMS_HARD_CAP_ALLPAGES = Number(process.env.CMS_HARD_CAP_ALLPAGES || 20000);

const NAME_MAX_PAGES = Number(process.env.NAME_MAX_PAGES || 5);
const NAME_TIME_BUDGET_MS = Number(process.env.NAME_TIME_BUDGET_MS || 8000);

/* ---- Scanner knobs (sitemap-based) ---- */
const SCAN_ENABLED = String(process.env.SCAN_ENABLED || 'true').toLowerCase() === 'true';
const SCAN_DOMAINS = String(process.env.SCAN_DOMAINS || 'www.pega.com,community.pega.com,academy.pega.com,support.pega.com')
  .split(',').map(s => s.trim()).filter(Boolean);
const SCAN_MAX_PAGES = Number(process.env.SCAN_MAX_PAGES || 2000);
const SCAN_CONCURRENCY = Number(process.env.SCAN_CONCURRENCY || 8);
const SCAN_TIMEOUT_MS = Number(process.env.SCAN_TIMEOUT_MS || 12000);
const SCAN_USER_AGENT = process.env.SCAN_USER_AGENT || 'Brightcove-Embed-Scanner/1.0 (+contact site admin)';

/* -------------------- MIDDLEWARE -------------------- */
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));

/* -------------------- AXIOS (keep-alive) -------------------- */
const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 50, maxFreeSockets: 10 });
const axiosInstance = axios.create({ timeout: 15000, httpAgent, httpsAgent });

/* -------------------- HELPERS -------------------- */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function withRetry(fn, { tries = 3, baseDelay = 400 } = {}) {
  let last; for (let i=0;i<tries;i++){ try { return await fn(); }
    catch(err){ last = err; const s = err.response?.status;
      const retriable = s===429 || (s>=500&&s<600) || err.code==='ECONNABORTED';
      if (!retriable || i===tries-1) throw err; await sleep(baseDelay*Math.pow(2,i)); } }
  throw last;
}
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());
const esc = s => String(s).replace(/"/g, '\\"');

/* -------------------- TOKEN CACHE -------------------- */
let tokenCache = { access_token: null, expires_at: 0 };
async function getAccessToken() {
  const now = Date.now();
  if (tokenCache.access_token && now < tokenCache.expires_at - 30000) return tokenCache.access_token;
  const r = await withRetry(() =>
    axiosInstance.post('https://oauth.brightcove.com/v4/access_token','grant_type=client_credentials',{
      auth:{ username:process.env.BRIGHTCOVE_CLIENT_ID, password:process.env.BRIGHTCOVE_CLIENT_SECRET },
      headers:{ 'Content-Type':'application/x-www-form-urlencoded' }
    })
  );
  const ttl = (r.data?.expires_in ?? 300)*1000;
  tokenCache = { access_token:r.data.access_token, expires_at:Date.now()+ttl };
  return tokenCache.access_token;
}

/* -------------------- QUERY PARSER -------------------- */
function parseQuery(input) {
  const raw = String(input || '').split(',').map(s=>s.trim()).filter(Boolean);
  const ids = [], tagTerms = [], titleTerms = [];
  for (let tok of raw) {
    tok = tok.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');
    const m = tok.match(/^(id|tag|title)\s*:(.*)$/i);
    if (m) {
      const key = m[1].toLowerCase(); const val = m[2].trim().replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');
      if (!val) continue;
      if (key==='id') { if (looksLikeId(val)) ids.push(val); }
      else if (key==='tag') tagTerms.push(val);
      else if (key==='title') titleTerms.push(val);
      continue;
    }
    if (looksLikeId(tok)) { ids.push(tok); continue; }
    tagTerms.push(tok); // bare tokens = tags
  }
  return { ids, tagTerms, titleTerms };
}

/* -------------------- CMS HELPERS -------------------- */
async function cmsSearch(q, token, { limit = CMS_PAGE_LIMIT, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const { data } = await withRetry(() =>
    axiosInstance.get(url, { headers:{ Authorization:`Bearer ${token}` }, params:{ q, fields, sort, limit, offset } })
  );
  return data || [];
}
async function fetchAllPagesUnlimited(q, token) {
  const out = []; let offset = 0;
  while (true) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < CMS_PAGE_LIMIT) break;
    offset += CMS_PAGE_LIMIT;
    if (out.length >= CMS_HARD_CAP_ALLPAGES) break;
  }
  return out;
}
async function fetchAllPagesCapped(q, token) {
  const out = []; let offset = 0; let page = 0; const start = Date.now();
  while (page < NAME_MAX_PAGES && (Date.now()-start) < NAME_TIME_BUDGET_MS) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < CMS_PAGE_LIMIT) break;
    offset += CMS_PAGE_LIMIT; page += 1;
  }
  return out;
}
async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const { data } = await withRetry(() => axiosInstance.get(url, { headers:{ Authorization:`Bearer ${token}` } }));
  return data;
}

/* -------------------- RECENT UPLOADS -------------------- */
async function fetchRecentUploads(token, limit = RECENT_LIMIT) {
  const scope = SEARCH_ACTIVE_ONLY ? 'state:ACTIVE' : '';
  const list = await cmsSearch(scope, token, { limit, sort:'-created_at', offset:0 });
  return (list||[]).map(v => ({
    id:v.id, name:v.name||'Untitled', tags:v.tags||[], created_at:v.created_at, published_at:v.published_at,
    thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'
  }));
}

/* -------------------- UNIFIED SEARCH -------------------- */
async function unifiedSearch(input, token) {
  const { ids, tagTerms, titleTerms } = parseQuery(input);
  const pool = [];

  const idFetches = ids.map(id =>
    fetchVideoById(id, token).then(v => { if (v && v.id) pool.push(v); }).catch(()=>{})
  );

  if (tagTerms.length) {
    const parts = [...tagTerms.map(t => `tags:"${esc(t)}"`)];
    if (SEARCH_ACTIVE_ONLY) parts.unshift('state:ACTIVE');
    const qTags = parts.join(' ');
    const rows = await fetchAllPagesUnlimited(qTags, token);
    pool.push(...rows);
    console.log(`[search] TAG AND q="${qTags}" -> ${rows.length}`);
  }

  if (titleTerms.length) {
    const perTerm = await Promise.allSettled(
      titleTerms.map(t => {
        const parts = [`name:*${esc(t)}*`];
        if (SEARCH_ACTIVE_ONLY) parts.unshift('state:ACTIVE');
        return fetchAllPagesCapped(parts.join(' '), token);
      })
    );
    const buckets = perTerm.map(r => (r.status==='fulfilled'? r.value : [])).map(arr => new Map(arr.map(v => [v.id, v])));
    if (buckets.length) {
      const idCounts = new Map();
      for (const b of buckets) for (const id of b.keys()) idCounts.set(id,(idCounts.get(id)||0)+1);
      const andIds = [...idCounts.entries()].filter(([,c])=>c===buckets.length).map(([id])=>id);
      const first = buckets[0];
      pool.push(...andIds.map(id => first.get(id)).filter(Boolean));
    }
  }
  await Promise.allSettled(idFetches);

  const seen = new Set(); const list = [];
  for (const v of pool) {
    if (!v || !v.id || seen.has(v.id)) continue;
    seen.add(v.id);
    list.push({
      id:v.id, name:v.name||'Untitled', tags:v.tags||[],
      created_at:v.created_at, published_at:v.published_at,
      thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'
    });
  }
  list.sort((a,b)=>new Date(b.created_at||0)-new Date(a.created_at||0));
  return list;
}

/* -------------------- ANALYTICS (HYBRID ALL-TIME) -------------------- */
async function getAnalyticsForVideos(videoIds, token, errors) {
  if (!Array.isArray(videoIds) || !videoIds.length) return [];
  const endpoint = 'https://analytics.api.brightcove.com/v1/data';
  const fields = ['video','video_name','video_view','video_impression','play_rate','engagement_score','video_seconds_viewed'].join(',');

  // batch /v1/data
  const chunks = []; for (let i=0;i<videoIds.length;i+=100) chunks.push(videoIds.slice(i,i+100));
  const out = []; const CONCURRENCY = 5; let idx = 0;
  async function worker() {
    while (idx < chunks.length) {
      const my = idx++; const batch = chunks[my];
      const params = new URLSearchParams({
        accounts: AID, dimensions: 'video', fields, from: 'alltime', to: 'now', reconciled: 'true',
        where: `video==${batch.join(',')}`
      });
      try {
        const data = await withRetry(() =>
          axiosInstance.get(`${endpoint}?${params.toString()}`, { headers:{ Authorization:`Bearer ${token}` } })
            .then(r=>r.data)
        );
        out.push(...(data?.items || []));
      } catch (e) {
        errors && errors.push({ step: 'analytics-batch', ids: batch.join(','), status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
      }
    }
  }
  await Promise.all(Array.from({length:Math.min(CONCURRENCY,chunks.length)}, worker));
  const byId = new Map(out.map(i => [String(i.video), i]));

  // fallback /v1/alltime if views missing/0
  const needFallback = videoIds.filter(id => {
    const row = byId.get(String(id));
    return !row || !row.video_view || row.video_view === 0;
  });

  const FALLBACK_CONC = 8; let j = 0;
  async function fallbackWorker() {
    while (j < needFallback.length) {
      const vid = needFallback[j++]; const url = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${vid}`;
      try {
        const data = await withRetry(() =>
          axiosInstance.get(url, { headers:{ Authorization:`Bearer ${token}` } }).then(r=>r.data)
        );
        const views = data?.alltime_video_views ?? data?.alltime_videos_views ?? 0;
        const key = String(vid);
        const existing = byId.get(key) || { video: key };
        existing.video_view = Math.max(Number(existing.video_view || 0), Number(views || 0));
        byId.set(key, existing);
      } catch (e) {
        errors && errors.push({ step: 'analytics-fallback-alltime', id: String(vid), status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
      }
    }
  }
  await Promise.all(Array.from({length:Math.min(FALLBACK_CONC, needFallback.length)}, fallbackWorker));

  return videoIds.map(id => byId.get(String(id)) || { video: String(id) });
}

/* -------------------- PLACEMENTS (OPTIONAL) -------------------- */
let DEST_CAPABILITY = null;
async function detectDestinationCapability(token) {
  if (DEST_CAPABILITY) return DEST_CAPABILITY;
  const endpoint='https://analytics.api.brightcove.com/v1/data';
  const params = new URLSearchParams({ accounts:AID, dimensions:'destination_domain,destination_path', fields:'video_view', from:PLACEMENTS_WINDOW, to:'now', limit:'1' });
  try { await axiosInstance.get(`${endpoint}?${params.toString()}`, { headers:{ Authorization:`Bearer ${token}` } }); DEST_CAPABILITY='full'; }
  catch { DEST_CAPABILITY='playerOnly'; }
  return DEST_CAPABILITY;
}
async function getPlacementsForVideos(videoIds, token, { from = PLACEMENTS_WINDOW, to = 'now' } = {}, errors) {
  const out = { mode: 'playerOnly', map: new Map() };
  if (!Array.isArray(videoIds) || !videoIds.length) return out;

  const mode = await detectDestinationCapability(token);
  out.mode = mode;

  const endpoint='https://analytics.api.brightcove.com/v1/data';
  const chunks=[]; for (let i=0;i<videoIds.length;i+=100) chunks.push(videoIds.slice(i,i+100));
  const CONCURRENCY = 4; let idx=0;

  async function worker(){
    while (idx<chunks.length){
      const my = idx++; const batch = chunks[my];
      const base = { accounts:AID, from, to, where:`video==${batch.join(',')}` };
      const dimensions = mode==='full' ? 'video,player,destination_domain,destination_path' : 'video,player';
      const fields = mode==='full' ? 'video,player,destination_domain,destination_path,video_view' : 'video,player,video_view';
      const params = new URLSearchParams({ ...base, dimensions, fields });
      try {
        const data = await withRetry(() => axiosInstance.get(`${endpoint}?${params.toString()}`, { headers:{ Authorization:`Bearer ${token}` } }).then(r=>r.data));
        const items = (data && data.items) || [];
        for (const row of items) {
          const vid = String(row.video); if (!out.map.has(vid)) out.map.set(vid, []);
          if (mode==='full') {
            const domain = (row.destination_domain||'').trim();
            const path = (row.destination_path||'').trim();
            const url = domain ? `//${domain}${path.startsWith('/')?path:(path?'/'+path:'')}` : '(unknown)';
            out.map.get(vid).push({ player:(row.player||'').trim(), domain, path, url, views:row.video_view||0 });
          } else {
            out.map.get(vid).push({ player:(row.player||'').trim(), views:row.video_view||0 });
          }
        }
      } catch (e) {
        errors && errors.push({ step: 'placements-batch', ids: batch.join(','), status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
      }
    }
  }
  await Promise.all(Array.from({length:Math.min(CONCURRENCY,chunks.length)}, worker));

  // compact + sort
  if (mode==='full'){
    for (const [vid, rows] of out.map.entries()){
      const keyMap=new Map();
      for (const r of rows){ const k=`${r.player}|${r.url}`; keyMap.set(k,(keyMap.get(k)||0)+(r.views||0)); }
      const merged = Array.from(keyMap.entries()).map(([k,views])=>{
        const [player,url]=k.split('|'); const domain=url.startsWith('//')?url.slice(2).split('/')[0]:''; const path=url.startsWith('//')?url.slice(2).slice(domain.length)||'/':'';
        return { player, domain, path, url, views };
      }).sort((a,b)=>b.views-a.views);
      out.map.set(vid, merged);
    }
  } else {
    for (const [vid, rows] of out.map.entries()){
      const byPlayer=new Map(); for (const r of rows) byPlayer.set(r.player,(byPlayer.get(r.player)||0)+(r.views||0));
      const merged = Array.from(byPlayer.entries()).map(([player,views])=>({player,views})).sort((a,b)=>b.views-a.views);
      out.map.set(vid, merged);
    }
  }

  return out;
}

/* -------------------- THEME -------------------- */
function themeHead(){ return `
  <style>
    :root{ --bg:#0b0b0d; --panel:#121217; --border:#262633; --text:#e9eef5; --muted:#9aa3af; --chip:#1a1a22; --chipBorder:#2a2a3a; --link:#7cc5ff; --btn:#14b8a6; --btnText:#031313; --btnHover:#10a195; }
    :root[data-theme="light"]{ --bg:#ffffff; --panel:#f8f9fa; --border:#e5e7eb; --text:#0b1220; --muted:#6b7280; --chip:#eef2f7; --chipBorder:#c7ccd3; --link:#0b63ce; --btn:#001f3f; --btnText:#ffffff; --btnHover:#003366; }
    *{box-sizing:border-box} html,body{height:100%} body{margin:0;background:var(--bg);color:var(--text);font-family:'Open Sans',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;}
    a{color:var(--link);text-decoration:none} a:hover{text-decoration:underline}
    header{display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid var(--border);background:var(--panel);}
    header h1{margin:0;font-size:1.3rem}
    .toggle{display:inline-flex;align-items:center;gap:8px;background:transparent;border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:999px;cursor:pointer;}
    .toggle:hover{background:var(--chip)}
    main{max-width:1100px;margin:24px auto;padding:0 20px}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,.25);}
    h2{margin:0 0 12px;font-size:1.2rem} label{font-weight:600;display:block;margin:10px 0 6px}
    input{width:100%;padding:12px 14px;border:1px solid var(--border);background:transparent;color:var(--text);border-radius:10px;outline:none;} input::placeholder{color:var(--muted)}
    .btn{display:inline-block;padding:12px 16px;background:var(--btn);color:var(--btnText);border:none;border-radius:10px;cursor:pointer;font-weight:700;margin-top:12px;}
    .btn:hover{background:var(--btnHover)}
    .note,.topnote{color:var(--muted);font-size:.9rem;margin-top:8px}
    .section{margin-top:24px}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:18px;margin-top:12px}
    .vcard{background:transparent;border:1px solid var(--border);border-radius:10px;overflow:hidden}
    .vcard iframe{width:100%;aspect-ratio:16/9;border:0;background:#000}
    .meta{padding:12px 14px}.title{font-weight:700;font-size:15px;margin-bottom:4px}.id,.date{color:var(--muted);font-size:12.5px;margin-top:2px}
    .tag{display:inline-block;margin:4px 6px 0 0;padding:4px 8px;border-radius:999px;background:var(--chip);border:1px solid var(--chipBorder);color:var(--text);font-size:12px}
    .topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px;flex-wrap:wrap}
    .btn-dl{display:inline-block;padding:10px 14px;background:var(--btn);color:var(--btnText);border-radius:10px;text-decoration:none;font-weight:700}
    .btn-dl:hover{background:var(--btnHover)}
  </style>
  <script>(function(){try{var s=localStorage.getItem('theme');if(!s){s=(window.matchMedia&&window.matchMedia('(prefers-color-scheme: light)').matches)?'light':'dark';}document.documentElement.setAttribute('data-theme',s);}catch(e){document.documentElement.setAttribute('data-theme','dark');}})();</script>
`; }
function themeToggleButton(){ return `
  <button class="toggle" id="themeToggle" aria-label="Toggle light/dark"><span id="themeIcon">🌙</span><span id="themeText">Dark</span></button>
  <script>(function(){var b=document.getElementById('themeToggle'),i=document.getElementById('themeIcon'),t=document.getElementById('themeText');
    function cur(){return document.documentElement.getAttribute('data-theme')||'dark';}
    function render(m){ if(m==='light'){i.textContent='🌞'; t.textContent='Light';} else {i.textContent='🌙'; t.textContent='Dark';}}
    render(cur()); b.addEventListener('click',function(){var n=cur()==='dark'?'light':'dark';document.documentElement.setAttribute('data-theme',n);try{localStorage.setItem('theme',n);}catch(e){}render(n);}); })();</script>
`; }

/* -------------------- UI: HOME -------------------- */
app.get('/', async (req, res) => {
  const qPrefill = (req.query.q || '').replace(/`/g, '\\`');
  try {
    const token = await getAccessToken();
    const recent = await fetchRecentUploads(token, RECENT_LIMIT);
    const recentCards = recent.map(v => `
      <div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}" allow="encrypted-media" allowfullscreen loading="lazy" title="${stripHtml(v.name)}"></iframe>
        <div class="meta"><div class="title">${stripHtml(v.name)}</div><div class="id">ID: ${v.id}</div><div class="date">Created: ${new Date(v.created_at).toLocaleString()}</div></div>
      </div>`).join('');
    res.send(`<!doctype html><html><head><meta charset="utf-8"/><title>Brightcove Video Tools</title>
      <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">${themeHead()}</head>
      <body><header><h1>Brightcove Video Tools</h1>${themeToggleButton()}</header>
      <main><div class="card"><h2>🔍 Search by ID, Tag(s), or Title</h2>
      <form action="/search" method="get"><label for="q">Enter terms (comma-separated)</label>
      <input id="q" name="q" placeholder='Examples: id:6376653485112, tag:"pega platform", title:"customer decision hub"' required />
      <button class="btn" type="submit">Search & Watch</button>
      <div class="topnote">Use prefixes id:, tag:, title:. Bare terms are treated as tags. Multiple tokens are ANDed per prefix.</div></form>
      <div class="section"><h2>🆕 Most Recent Uploads</h2><div class="grid">${recentCards || '<div class="note">No recent uploads.</div>'}</div></div></div></main>
      <script>(function(){var v=${JSON.stringify(qPrefill)}; if(v) document.getElementById('q').value=v;})();</script></body></html>`);
  } catch (e) {
    console.error('Home error:', e.response?.status, e.response?.data || e.message);
    res.status(200).send(`<!doctype html><meta charset="utf-8"><title>Brightcove Video Tools</title>${themeHead()}
      <body style="font-family:system-ui;padding:24px;color:var(--text);background:var(--bg)">
      <h1>Brightcove Video Tools</h1>
      <p>We couldn't load recent uploads right now, but search still works.</p>
      <form action="/search" method="get"><input style="padding:10px;border:1px solid var(--border);background:transparent;color:var(--text);border-radius:8px" name="q" placeholder='id:..., tag:"...", title:"..."' required />
      <button class="btn" type="submit" style="margin-left:8px">Search</button></form></body>`);
  }
});

/* -------------------- UI: SEARCH -------------------- */
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim(); if (!qInput) return res.redirect('/');
  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;
    const cards = (videos||[]).map(v => {
      const tags = (v.tags||[]).map(t=>`<span class="tag">${stripHtml(t)}</span>`).join('');
      return `<div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}" allow="encrypted-media" allowfullscreen loading="lazy" title="${stripHtml(v.name)}"></iframe>
        <div class="meta"><div class="title">${stripHtml(v.name)}</div><div class="id">ID: ${v.id}</div>
        <div class="tags"><strong>Tags:</strong> ${tags || '<em class="id">None</em>'}</div></div></div>`;
    }).join('');
    res.status(200).send(`<!doctype html><html><head><meta charset="utf-8"/><title>Results for: ${stripHtml(qInput)}</title>
      <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">${themeHead()}</head>
      <body><header><h1>Search Results</h1>${themeToggleButton()}</header>
      <main><div class="topbar"><a href="/?q=${encodeURIComponent(qInput)}">← Back to search</a>
      <a class="btn-dl" href="${downloadUrl}">Download Video Analytics Spreadsheet</a></div>
      <div class="card">${videos && videos.length ? '<div class="grid">'+cards+'</div>' : '<div class="note">No videos found for that query.</div>'}</div></main></body></html>`);
  } catch (err) {
    console.error('Search error:', err.response?.status, err.response?.data || err.message);
    res.status(200).send(`<!doctype html><meta charset="utf-8"><title>Search</title>${themeHead()}
      <body style="font-family:system-ui;padding:24px;color:var(--text);background:var(--bg)">
      <h1>Search Results</h1><p class="note">We couldn’t complete the search right now. Please try again.</p>
      <p><a href="/" style="color:var(--link)">← Back</a></p></body>`);
  }
});

/* -------------------- SITEMAP SCANNER -------------------- */
// fetch text (with gzip/deflate support)
async function fetchText(url, timeoutMs = SCAN_TIMEOUT_MS) {
  const res = await axios.get(url, {
    timeout: timeoutMs,
    responseType: 'arraybuffer',
    headers: { 'User-Agent': SCAN_USER_AGENT, 'Accept-Encoding': 'gzip,deflate' },
    httpAgent, httpsAgent, validateStatus: s => s >= 200 && s < 400
  });
  let buf = res.data;
  const enc = (res.headers['content-encoding'] || '').toLowerCase();
  if (enc.includes('gzip')) buf = zlib.gunzipSync(buf);
  else if (enc.includes('deflate')) buf = zlib.inflateSync(buf);
  return buf.toString('utf8');
}

// parse <loc>...</loc> from a sitemap or sitemap-index (very lightweight)
function parseSitemapLocs(xmlText) {
  const locs = [];
  const re = /<loc>\s*([^<\s]+)\s*<\/loc>/gi;
  let m; while ((m = re.exec(xmlText))) { locs.push(m[1]); }
  return locs;
}

// normalize/keep only http(s) and limit to our domains
function urlAllowed(u, domainsSet) {
  try {
    const x = new URL(u);
    if (!/^https?:$/.test(x.protocol)) return false;
    const host = x.hostname.toLowerCase();
    for (const d of domainsSet) {
      if (host === d || host.endsWith('.' + d)) return true;
    }
    return false;
  } catch { return false; }
}

// Discover pages via sitemap.xml (+ handle index files)
async function discoverPagesFromSitemaps(domains, maxPagesTotal) {
  const domainsSet = new Set(domains.map(d => d.toLowerCase()));
  const seenSitemaps = new Set();
  const pages = new Set();
  async function fetchSite(domain) {
    const base = `https://${domain}`;
    const main = `${base}/sitemap.xml`;
    try {
      const xml = await fetchText(main);
      await processSitemap(main, xml);
    } catch (e) {
      console.warn('[sitemap] failed', main, e.message);
    }
  }
  async function processSitemap(sitemapUrl, xml) {
    if (seenSitemaps.has(sitemapUrl) || pages.size >= maxPagesTotal) return;
    seenSitemaps.add(sitemapUrl);
    const locs = parseSitemapLocs(xml);
    // Heuristic: if many locs end with .xml => it's an index
    const xmlLocs = locs.filter(u => /\.xml(\.gz)?$/i.test(u));
    if (xmlLocs.length && xmlLocs.length >= locs.length * 0.5) {
      for (const u of xmlLocs) {
        if (pages.size >= maxPagesTotal) break;
        if (!urlAllowed(u, domainsSet)) continue;
        try {
          const xml2 = await fetchText(u);
          await processSitemap(u, xml2);
        } catch (e) {
          console.warn('[sitemap] sub failed', u, e.message);
        }
      }
    } else {
      for (const u of locs) {
        if (pages.size >= maxPagesTotal) break;
        if (!urlAllowed(u, domainsSet)) continue;
        pages.add(u);
      }
    }
  }

  await Promise.allSettled(domains.map(fetchSite));
  return Array.from(pages);
}

// Scan a single page for given video IDs; return matches [{url, id, snippet}]
async function scanPageForIds(pageUrl, videoIds) {
  try {
    const html = await fetchText(pageUrl);
    const matches = [];
    for (const vid of videoIds) {
      // common patterns: iframe player URL query, data attributes, serialized json
      const patterns = [
        new RegExp(`videoId=${vid}(?:[^0-9]|$)`,'i'),
        new RegExp(`data-video-id=["']${vid}["']`,'i'),
        new RegExp(`data-brightcove-video-id=["']${vid}["']`,'i'),
        new RegExp(`"videoId"\\s*:\\s*["']${vid}["']`,'i'),
        new RegExp(`data-experience-video-id=["']${vid}["']`,'i'),
      ];
      for (const rx of patterns) {
        const m = html.match(rx);
        if (m) {
          // build tiny snippet around match
          const idx = m.index || 0;
          const start = Math.max(0, idx - 60);
          const end = Math.min(html.length, idx + 60);
          const snippet = html.slice(start, end).replace(/\s+/g,' ').slice(0,120);
          matches.push({ url: pageUrl, id: String(vid), snippet });
          break; // one match per vid per page is enough
        }
      }
    }
    return matches;
  } catch {
    return [];
  }
}

// Run the sitemap-based scan (polite concurrency)
async function runSitemapScan(videoIds, { domains = SCAN_DOMAINS, maxPages = SCAN_MAX_PAGES, concurrency = SCAN_CONCURRENCY } = {}) {
  if (!domains.length || !videoIds.length) return { matches: [], pagesTotal: 0 };
  const pages = await discoverPagesFromSitemaps(domains, maxPages);
  let i = 0;
  const out = [];
  async function worker() {
    while (i < pages.length) {
      const my = i++;
      const pageUrl = pages[my];
      const found = await scanPageForIds(pageUrl, videoIds);
      if (found.length) out.push(...found);
    }
  }
  const workers = Array.from({length: Math.min(concurrency, pages.length)}, worker);
  await Promise.all(workers);
  return { matches: out, pagesTotal: pages.length };
}

/* -------------------- API: /scan (JSON) -------------------- */
app.get('/scan', async (req, res) => {
  if (!SCAN_ENABLED) return res.status(403).json({ error: 'Scanning disabled by server.' });
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.status(400).json({ error: 'Missing q' });

  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const ids = videos.map(v => v.id);
    if (!ids.length) return res.json({ pages: 0, results: {} });

    const { matches, pagesTotal } = await runSitemapScan(ids);
    const map = {};
    for (const m of matches) {
      if (!map[m.id]) map[m.id] = [];
      map[m.id].push({ url: m.url, snippet: m.snippet });
    }
    // de-dupe urls per id
    for (const k of Object.keys(map)) {
      const seen = new Set();
      map[k] = map[k].filter(x => { const key = x.url; if (seen.has(key)) return false; seen.add(key); return true; });
    }
    res.json({ pages: pagesTotal, results: map });
  } catch (e) {
    console.error('scan error', e.message);
    res.status(500).json({ error: 'scan failed' });
  }
});

/* -------------------- DOWNLOAD (ALWAYS returns a file) -------------------- */
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  const wantPlacements = req.query.placements ? req.query.placements === '1' : PLACEMENTS_ENABLED;
  const wantScan = req.query.scan ? req.query.scan === '1' : SCAN_ENABLED;
  if (!qInput) return res.status(400).send('Missing search terms');

  const errors = [];
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
    { header: wantPlacements ? `Top Destinations/Players (${PLACEMENTS_WINDOW})` : 'Top Destinations/Players (disabled)', key: 'placementsSummary', width: 70 },
  ];

  let videos = [];
  let ids = [];
  let placementsMode = 'playerOnly';
  let placementsMap = new Map();

  try {
    const token  = await getAccessToken();
    try {
      videos = await unifiedSearch(qInput, token);
    } catch (e) {
      errors.push({ step: 'search', status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
      videos = [];
    }

    if (!videos.length) {
      ws.addRow({ id: 'INFO', title: 'No videos matched the query. See Errors sheet if present.' });
    } else {
      let truncated = false;
      if (videos.length > DOWNLOAD_MAX_VIDEOS) { videos = videos.slice(0, DOWNLOAD_MAX_VIDEOS); truncated = true; }
      if (truncated) { ws.addRow({ id: 'NOTE', title: `Export capped at ${DOWNLOAD_MAX_VIDEOS} newest items.` }); ws.addRow({}); }

      ids = videos.map(v => v.id);

      // analytics (hybrid)
      let analytics = [];
      try {
        analytics = await getAnalyticsForVideos(ids, token, errors);
      } catch (e) {
        errors.push({ step: 'analytics', status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
        analytics = [];
      }
      const aMap = new Map(analytics.map(a => [String(a.video), a]));

      // placements (optional)
      if (wantPlacements) {
        try {
          const plac = await getPlacementsForVideos(ids, token, { from: PLACEMENTS_WINDOW, to: 'now' }, errors);
          placementsMode = plac.mode;
          placementsMap = plac.map;
        } catch (e) {
          errors.push({ step: 'placements', status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
        }
      }

      // top summary
      const topSummaryByVideo = new Map();
      for (const [vid, rows] of placementsMap.entries()) {
        if (placementsMode === 'full') {
          const byUrl = new Map();
          for (const r of rows) byUrl.set(r.url, (byUrl.get(r.url) || 0) + (r.views || 0));
          const top = Array.from(byUrl.entries()).map(([url, views]) => ({ url, views }))
            .sort((a,b)=>b.views-a.views).slice(0,5);
          topSummaryByVideo.set(String(vid), top);
        } else {
          const byPlayer = new Map();
          for (const r of rows) byPlayer.set(r.player, (byPlayer.get(r.player) || 0) + (r.views || 0));
          const top = Array.from(byPlayer.entries()).map(([player, views]) => ({ player, views }))
            .sort((a,b)=>b.views-a.views).slice(0,5);
          topSummaryByVideo.set(String(vid), top);
        }
      }

      // summary rows
      const now = Date.now();
      for (const v of videos) {
        const a = aMap.get(String(v.id)) || {};
        const title = v.name || a.video_name || 'Untitled';
        const views = a.video_view || 0;
        const basis = v.published_at || v.created_at;
        let daysSince = 1;
        if (basis) {
          const ts = new Date(basis).getTime();
          if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((now - ts) / 86400000));
        }
        const dailyAvgViews = Number(((views || 0) / daysSince).toFixed(2));

        const top = topSummaryByVideo.get(String(v.id)) || [];
        const placementsCell = (wantPlacements && top.length)
          ? (placementsMode === 'full'
              ? top.map(d => `${d.url} · ${d.views}`).join('; ')
              : top.map(d => `${d.player} · ${d.views}`).join('; '))
          : (wantPlacements ? '—' : '(disabled)');

        ws.addRow({
          id: v.id,
          title,
          views,
          dailyAvgViews,
          impressions: a.video_impression || 0,
          engagement: a.engagement_score || 0,
          playRate: a.play_rate || 0,
          secondsViewed: a.video_seconds_viewed || 0,
          tags: (v.tags || []).join(', '),
          placementsSummary: placementsCell
        });
      }

      // ---- Embeds Found sheet (sitemap scan) ----
      if (wantScan) {
        try {
          const { matches, pagesTotal } = await runSitemapScan(ids);
          const wf = wb.addWorksheet('Embeds Found');
          wf.columns = [
            { header: 'Video ID', key: 'id', width: 20 },
            { header: 'Page URL', key: 'url', width: 80 },
            { header: 'Match Snippet', key: 'snippet', width: 60 },
          ];
          // de-dupe (id,url)
          const dedupe = new Set();
          for (const m of matches) {
            const key = `${m.id}|${m.url}`;
            if (dedupe.has(key)) continue;
            dedupe.add(key);
            wf.addRow({ id: m.id, url: m.url, snippet: m.snippet || '' });
          }
          // note row
          wf.addRow({});
          wf.addRow({ id: 'INFO', url: `Scanned ~${pagesTotal} pages across: ${SCAN_DOMAINS.join(', ')}` });
        } catch (e) {
          errors.push({ step: 'scan', status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
        }
      }
    }
  } catch (e) {
    errors.push({ step: 'fatal', status: e?.response?.status || 'ERR', detail: e?.response?.data || e.message });
  }

  if (errors.length) {
    const we = wb.addWorksheet('Errors');
    we.columns = [
      { header: 'Step', key: 'step', width: 28 },
      { header: 'IDs/ID', key: 'ids', width: 40 },
      { header: 'Status', key: 'status', width: 10 },
      { header: 'Detail', key: 'detail', width: 80 },
    ];
    for (const e of errors) we.addRow({ step: e.step, ids: e.ids || e.id || '', status: e.status, detail: typeof e.detail === 'string' ? e.detail : JSON.stringify(e.detail) });
  }

  res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_alltime.xlsx');
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  try { await wb.xlsx.write(res); } catch { /* fall through */ }
  return res.end();
});

/* -------------------- HEALTH + NOT FOUND -------------------- */
app.get('/healthz', (_req, res) => res.status(200).send('ok'));
app.use((req, res) => res.status(404).send('Not found'));

/* -------------------- START -------------------- */
const server = app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
server.keepAliveTimeout = 120000;
server.headersTimeout   = 125000;
server.requestTimeout   = 0;
