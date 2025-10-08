// server.js — Precise Tag/Title/ID search (returns ALL tag matches) + Light/Dark + Recent Uploads + Robust Export
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');

const app = express();
const PORT = process.env.PORT || 3000;

// ---- ENV CHECKS ----
const MUST = [
  'BRIGHTCOVE_ACCOUNT_ID',
  'BRIGHTCOVE_CLIENT_ID',
  'BRIGHTCOVE_CLIENT_SECRET',
  'BRIGHTCOVE_PLAYER_ID'
];
const missing = MUST.filter(k => !process.env[k]);
if (missing.length) {
  console.error('Missing .env keys:', missing.join(', '));
  process.exit(1);
}

// ---- CONFIG ----
const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;
const PLAYER_ID = process.env.BRIGHTCOVE_PLAYER_ID;

// Recent uploads on home
const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 9);

// Analytics placements window (e.g. "-90d" or "alltime")
const PLACEMENTS_WINDOW = process.env.PLACEMENTS_WINDOW || 'alltime';

// Search scope
const SEARCH_ACTIVE_ONLY = String(process.env.SEARCH_ACTIVE_ONLY || 'false').toLowerCase() === 'true';

// CMS paging knobs
const CMS_PAGE_LIMIT = 100;                           // Brightcove max
const CMS_HARD_CAP_ALLPAGES = Number(process.env.CMS_HARD_CAP_ALLPAGES || 20000); // big safety rail

// Title-search safety knobs (do NOT affect tag search)
const NAME_MAX_PAGES = Number(process.env.NAME_MAX_PAGES || 5);
const NAME_TIME_BUDGET_MS = Number(process.env.NAME_TIME_BUDGET_MS || 8000);

// ---- MIDDLEWARE ----
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public')); // optional assets

// ---- HTTP + RETRY HELPERS ----
const axiosInstance = axios.create({ timeout: 15000 });
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function withRetry(fn, { tries = 3, baseDelay = 400 } = {}) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const s = err.response?.status;
      const retriable = s === 429 || (s >= 500 && s < 600) || err.code === 'ECONNABORTED';
      if (!retriable || i === tries - 1) throw err;
      await sleep(baseDelay * Math.pow(2, i));
    }
  }
  throw lastErr;
}

// ---- TOKEN CACHE ----
let tokenCache = { access_token: null, expires_at: 0 };
async function getAccessToken() {
  const now = Date.now();
  if (tokenCache.access_token && now < tokenCache.expires_at - 30000) {
    return tokenCache.access_token;
  }
  const r = await withRetry(() =>
    axiosInstance.post(
      'https://oauth.brightcove.com/v4/access_token',
      'grant_type=client_credentials',
      {
        auth: {
          username: process.env.BRIGHTCOVE_CLIENT_ID,
          password: process.env.BRIGHTCOVE_CLIENT_SECRET
        },
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      }
    )
  );
  const ttl = (r.data?.expires_in ?? 300) * 1000;
  tokenCache = {
    access_token: r.data.access_token,
    expires_at: Date.now() + ttl
  };
  return tokenCache.access_token;
}

// ---- UTILS ----
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());
const esc = s => String(s).replace(/"/g, '\\"');

// Parse comma-separated search string into explicit buckets:
// - id:1234567890123  OR bare numeric -> ids
// - tag:foo (quoted allowed)          -> tagTerms
// - title:bar (quoted allowed)        -> titleTerms
// - bare tokens -> treated as tags (so "pega platform" == tag:"pega platform")
function parseQuery(input) {
  const raw = String(input || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  const ids = [];
  const tagTerms = [];
  const titleTerms = [];

  for (let tok of raw) {
    // unquote "..." or '...'
    tok = tok.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');

    // explicit prefixes
    const m = tok.match(/^(id|tag|title)\s*:(.*)$/i);
    if (m) {
      const key = m[1].toLowerCase();
      const val = m[2].trim().replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');
      if (!val) continue;
      if (key === 'id') { if (looksLikeId(val)) ids.push(val); }
      else if (key === 'tag') { tagTerms.push(val); }
      else if (key === 'title') { titleTerms.push(val); }
      continue;
    }

    // bare numeric -> id
    if (looksLikeId(tok)) { ids.push(tok); continue; }

    // otherwise treat as a tag token
    tagTerms.push(tok);
  }

  return { ids, tagTerms, titleTerms };
}

// ---- CMS HELPERS ----
async function cmsSearch(q, token, { limit = CMS_PAGE_LIMIT, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const { data } = await withRetry(() =>
    axiosInstance.get(url, {
      headers: { Authorization: `Bearer ${token}` },
      params: { q, fields, sort, limit, offset }
    })
  );
  return data || [];
}

// Fetch ALL pages for a query (authoritative for tag searches)
async function fetchAllPagesUnlimited(q, token) {
  const out = [];
  let offset = 0;
  while (true) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < CMS_PAGE_LIMIT) break;
    offset += CMS_PAGE_LIMIT;
    if (out.length >= CMS_HARD_CAP_ALLPAGES) break; // very large safety ceiling
  }
  return out;
}

// Fetch pages with a conservative budget (used for *title* queries only)
async function fetchAllPagesCapped(q, token) {
  const out = [];
  let offset = 0;
  let page = 0;
  const start = Date.now();
  while (page < NAME_MAX_PAGES && (Date.now() - start) < NAME_TIME_BUDGET_MS) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < CMS_PAGE_LIMIT) break;
    offset += CMS_PAGE_LIMIT;
    page += 1;
  }
  return out;
}

async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const { data } = await withRetry(() =>
    axiosInstance.get(url, { headers: { Authorization: `Bearer ${token}` } })
  );
  return data;
}

// ---- RECENT UPLOADS ----
async function fetchRecentUploads(token, limit = RECENT_LIMIT) {
  const scope = SEARCH_ACTIVE_ONLY ? 'state:ACTIVE' : ''; // empty → all states
  const list = await cmsSearch(scope, token, { limit, sort: '-created_at', offset: 0 });
  return (list || []).map(v => ({
    id: v.id,
    name: v.name || 'Untitled',
    tags: v.tags || [],
    created_at: v.created_at,
    thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'
  }));
}

// ---- UNIFIED SEARCH (ID + TAG + TITLE; full tag coverage) ----
async function unifiedSearch(input, token) {
  const { ids, tagTerms, titleTerms } = parseQuery(input);

  const pool = [];

  // 1) IDs (exact)
  const idFetches = ids.map(id =>
    fetchVideoById(id, token)
      .then(v => { if (v && v.id) pool.push(v); })
      .catch(() => {})
  );

  // 2) TAGS (AND): authoritative, fetch ALL pages
  let tagMatches = [];
  if (tagTerms.length) {
    // Build q: tags:"t1" tags:"t2" ... ; optionally constrain to ACTIVE if env set
    const parts = [...tagTerms.map(t => `tags:"${esc(t)}"`)];
    if (SEARCH_ACTIVE_ONLY) parts.unshift('state:ACTIVE'); // else all states
    const qTags = parts.join(' ');
    try {
      tagMatches = await fetchAllPagesUnlimited(qTags, token);
      pool.push(...tagMatches);
      console.log(`[search] tag AND q="${qTags}" -> ${tagMatches.length} items`);
    } catch (e) {
      console.error('[search][tags] failed', e.response?.status, e.response?.data || e.message);
    }
  }

  // 3) TITLE (AND): capped
  if (titleTerms.length) {
    // One query per term, union results locally, then require AND by filtering
    const resultsPerTerm = await Promise.allSettled(
      titleTerms.map(t => {
        const parts = [`name:*${esc(t)}*`];
        if (SEARCH_ACTIVE_ONLY) parts.unshift('state:ACTIVE');
        const qName = parts.join(' ');
        return fetchAllPagesCapped(qName, token);
      })
    );

    // Union then AND-filter: keep videos that appeared in all term result sets
    const buckets = resultsPerTerm
      .map(r => (r.status === 'fulfilled' ? r.value : []))
      .map(arr => new Map(arr.map(v => [v.id, v])));

    if (buckets.length) {
      const idCounts = new Map();
      for (const b of buckets) {
        for (const id of b.keys()) idCounts.set(id, (idCounts.get(id) || 0) + 1);
      }
      const andIds = [...idCounts.entries()].filter(([, c]) => c === buckets.length).map(([id]) => id);
      const firstBucket = buckets[0];
      const titleAndMatches = andIds.map(id => firstBucket.get(id)).filter(Boolean);
      pool.push(...titleAndMatches);
      console.log(`[search] title AND terms=${JSON.stringify(titleTerms)} -> ${titleAndMatches.length} items`);
    }
  }

  await Promise.allSettled(idFetches);

  // De-dupe + normalize
  const seen = new Set();
  const list = [];
  for (const v of pool) {
    if (!v || !v.id || seen.has(v.id)) continue;
    seen.add(v.id);
    list.push({
      id: v.id,
      name: v.name || 'Untitled',
      tags: v.tags || [],
      thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail',
      created_at: v.created_at
    });
  }

  // Newest first
  list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  console.log(`[search] final unique results: ${list.length}`);
  return list;
}

// ---- ANALYTICS (BATCHED) ----
async function getAnalyticsForVideos(videoIds, token) {
  if (!Array.isArray(videoIds) || videoIds.length === 0) return [];
  const endpoint = 'https://analytics.api.brightcove.com/v1/data';
  const fields = [
    'video',
    'video_name',
    'video_view',
    'video_impression',
    'play_rate',
    'engagement_score',
    'video_seconds_viewed'
  ].join(',');

  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 100) chunks.push(videoIds.slice(i, i + 100));

  const out = [];
  for (const batch of chunks) {
    const params = new URLSearchParams({
      accounts: AID,
      dimensions: 'video',
      fields,
      from: 'alltime',
      to: 'now',
      where: `video==${batch.join(',')}`
    });

    const data = await withRetry(() =>
      axiosInstance.get(`${endpoint}?${params.toString()}`, {
        headers: { Authorization: `Bearer ${token}` }
      }).then(r => r.data)
    );
    out.push(...(data?.items || []));
  }
  return out;
}

// ---- PLACEMENTS (auto-detect capability; full or player-only) ----
let DEST_CAPABILITY = null; // 'full' | 'playerOnly'
async function detectDestinationCapability(token) {
  if (DEST_CAPABILITY) return DEST_CAPABILITY;
  const endpoint = 'https://analytics.api.brightcove.com/v1/data';
  const params = new URLSearchParams({
    accounts: AID,
    dimensions: 'destination_domain,destination_path',
    fields: 'video_view',
    from: PLACEMENTS_WINDOW,
    to: 'now',
    limit: '1'
  });

  try {
    await axiosInstance.get(`${endpoint}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    DEST_CAPABILITY = 'full';
  } catch (e) {
    console.warn('[capability] destination dims not available:', e.response?.status, e.response?.data || e.message);
    DEST_CAPABILITY = 'playerOnly';
  }
  return DEST_CAPABILITY;
}

async function getPlacementsForVideos(videoIds, token, { from = PLACEMENTS_WINDOW, to = 'now' } = {}) {
  if (!Array.isArray(videoIds) || videoIds.length === 0) return { mode: 'playerOnly', map: new Map() };

  const mode = await detectDestinationCapability(token);
  const endpoint = 'https://analytics.api.brightcove.com/v1/data';

  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 100) chunks.push(videoIds.slice(i, i + 100));

  const resultMap = new Map();

  for (const batch of chunks) {
    const base = { accounts: AID, from, to, where: `video==${batch.join(',')}` };
    const dimensions = mode === 'full'
      ? 'video,player,destination_domain,destination_path'
      : 'video,player';
    const fields = mode === 'full'
      ? 'video,player,destination_domain,destination_path,video_view'
      : 'video,player,video_view';

    const params = new URLSearchParams({ ...base, dimensions, fields });

    const data = await withRetry(() =>
      axiosInstance.get(`${endpoint}?${params.toString()}`, {
        headers: { Authorization: `Bearer ${token}` }
      }).then(r => r.data)
    );

    const items = (data && data.items) || [];
    for (const row of items) {
      const vid = String(row.video);
      if (!resultMap.has(vid)) resultMap.set(vid, []);

      if (mode === 'full') {
        const domain = (row.destination_domain || '').trim();
        const path = (row.destination_path || '').trim();
        const url = domain ? `//${domain}${path.startsWith('/') ? path : (path ? '/' + path : '')}` : '(unknown)';
        resultMap.get(vid).push({
          player: (row.player || '').trim(),
          domain,
          path,
          url,
          views: row.video_view || 0
        });
      } else {
        resultMap.get(vid).push({
          player: (row.player || '').trim(),
          views: row.video_view || 0
        });
      }
    }
  }

  // compact + sort
  for (const [vid, rows] of resultMap.entries()) {
    if (DEST_CAPABILITY === 'full') {
      const keyMap = new Map();
      for (const r of rows) {
        const key = `${r.player}|${r.url}`;
        keyMap.set(key, (keyMap.get(key) || 0) + (r.views || 0));
      }
      const merged = Array.from(keyMap.entries()).map(([k, v]) => {
        const [player, url] = k.split('|');
        const domain = url.startsWith('//') ? url.slice(2).split('/')[0] : '';
        const path = url.startsWith('//') ? url.slice(2).slice(domain.length) || '/' : '';
        return { player, domain, path, url, views: v };
      }).sort((a, b) => b.views - a.views);
      resultMap.set(vid, merged);
    } else {
      const byPlayer = new Map();
      for (const r of rows) {
        byPlayer.set(r.player, (byPlayer.get(r.player) || 0) + (r.views || 0));
      }
      const merged = Array.from(byPlayer.entries())
        .map(([player, views]) => ({ player, views }))
        .sort((a, b) => b.views - a.views);
      resultMap.set(vid, merged);
    }
  }

  return { mode: DEST_CAPABILITY, map: resultMap };
}

// ---- THEME (shared CSS + JS) ----
function themeHead() {
  return `
  <style>
    :root{
      --bg:#0b0b0d;
      --panel:#121217;
      --border:#262633;
      --text:#e9eef5;
      --muted:#9aa3af;
      --chip:#1a1a22;
      --chipBorder:#2a2a3a;
      --link:#7cc5ff;
      --btn:#14b8a6;
      --btnText:#031313;
      --btnHover:#10a195;
      --accent:#60a5fa;
    }
    :root[data-theme="light"]{
      --bg:#ffffff;
      --panel:#f8f9fa;
      --border:#e5e7eb;
      --text:#0b1220;
      --muted:#6b7280;
      --chip:#eef2f7;
      --chipBorder:#c7ccd3;
      --link:#0b63ce;
      --btn:#001f3f;
      --btnText:#ffffff;
      --btnHover:#003366;
      --accent:#1d4ed8;
    }

    *{box-sizing:border-box}
    html,body{height:100%}
    body{
      margin:0;
      background:var(--bg);
      color:var(--text);
      font-family:'Open Sans',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
    }
    a{color:var(--link); text-decoration:none}
    a:hover{text-decoration:underline}
    header{
      display:flex;align-items:center;justify-content:space-between;
      padding:16px 20px;border-bottom:1px solid var(--border);background:var(--panel);
    }
    header h1{margin:0;font-size:1.3rem}
    .toggle{
      display:inline-flex;align-items:center;gap:8px;
      background:transparent;border:1px solid var(--border);color:var(--text);
      padding:8px 12px;border-radius:999px;cursor:pointer;
    }
    .toggle:hover{background:var(--chip)}
    main{max-width:1100px;margin:24px auto;padding:0 20px}
    .card{
      background:var(--panel);border:1px solid var(--border);
      border-radius:12px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,.25);
    }
    h2{margin:0 0 12px;font-size:1.2rem}
    label{font-weight:600;display:block;margin:10px 0 6px}
    input{
      width:100%;padding:12px 14px;border:1px solid var(--border);
      background:transparent;color:var(--text);border-radius:10px;outline:none;
    }
    input::placeholder{color:var(--muted)}
    .btn{
      display:inline-block;padding:12px 16px;background:var(--btn);
      color:var(--btnText);border:none;border-radius:10px;cursor:pointer;
      font-weight:700;margin-top:12px;
    }
    .btn:hover{background:var(--btnHover)}
    .note,.topnote{color:var(--muted);font-size:.9rem;margin-top:8px}
    .section{margin-top:24px}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:18px;margin-top:12px}
    .vcard{background:transparent;border:1px solid var(--border);border-radius:10px;overflow:hidden}
    .vcard iframe{width:100%;aspect-ratio:16/9;border:0;background:#000}
    .meta{padding:12px 14px}
    .title{font-weight:700;font-size:15px;margin-bottom:4px}
    .id,.date{color:var(--muted);font-size:12.5px;margin-top:2px}
    .tag{display:inline-block;margin:4px 6px 0 0;padding:4px 8px;border-radius:999px;background:var(--chip);border:1px solid var(--chipBorder);color:var(--text);font-size:12px}
    .topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px;flex-wrap:wrap}
    .btn-dl{display:inline-block;padding:10px 14px;background:var(--btn);color:var(--btnText);border-radius:10px;text-decoration:none;font-weight:700}
    .btn-dl:hover{background:var(--btnHover)}
  </style>

  <!-- Apply saved/system theme *before* paint -->
  <script>
  (function(){
    try {
      var saved = localStorage.getItem('theme');
      if (!saved) {
        saved = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
      }
      document.documentElement.setAttribute('data-theme', saved);
    } catch (e) {
      document.documentElement.setAttribute('data-theme', 'dark');
    }
  })();
  </script>
  `;
}

function themeToggleButton() {
  return `
    <button class="toggle" id="themeToggle" aria-label="Toggle light/dark">
      <span id="themeIcon">🌙</span><span id="themeText">Dark</span>
    </button>
    <script>
      (function(){
        var btn = document.getElementById('themeToggle');
        var icon = document.getElementById('themeIcon');
        var txt = document.getElementById('themeText');
        function renderLabel(mode){
          if(mode === 'light'){ icon.textContent = '🌞'; txt.textContent = 'Light'; }
          else { icon.textContent = '🌙'; txt.textContent = 'Dark'; }
        }
        function current(){ return document.documentElement.getAttribute('data-theme') || 'dark'; }
        renderLabel(current());
        btn.addEventListener('click', function(){
          var now = current();
          var next = now === 'dark' ? 'light' : 'dark';
          document.documentElement.setAttribute('data-theme', next);
          try { localStorage.setItem('theme', next); } catch(e){}
          renderLabel(next);
        });
      })();
    </script>
  `;
}

// ---- UI: HOME ----
app.get('/', async (req, res) => {
  const qPrefill = (req.query.q || '').replace(/`/g, '\\`');

  try {
    const token = await getAccessToken();
    const recent = await fetchRecentUploads(token, RECENT_LIMIT);

    const recentCards = recent.map(v => `
      <div class="vcard">
        <iframe
          src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}"
          allow="encrypted-media" allowfullscreen loading="lazy"
          title="${stripHtml(v.name)}"></iframe>
        <div class="meta">
          <div class="title">${stripHtml(v.name)}</div>
          <div class="id">ID: ${v.id}</div>
          <div class="date">Created: ${new Date(v.created_at).toLocaleString()}</div>
        </div>
      </div>
    `).join('');

    res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Brightcove Video Tools</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
  ${themeHead()}
</head>
<body>
  <header>
    <h1>Brightcove Video Tools</h1>
    ${themeToggleButton()}
  </header>
  <main>
    <div class="card">
      <h2>🔍 Search by ID, Tag(s), or Title</h2>
      <form action="/search" method="get">
        <label for="q">Enter terms (comma-separated)</label>
        <input id="q" name="q" placeholder='Examples: id:6376653485112, tag:"pega platform", title:"customer decision hub"' required />
        <button class="btn" type="submit">Search & Watch</button>
        <div class="topnote">Use prefixes id:, tag:, title:. Bare terms are treated as tags. Multiple tokens are ANDed per prefix.</div>
      </form>

      <div class="section">
        <h2>🆕 Most Recent Uploads</h2>
        <div class="grid">
          ${recentCards || '<div class="note">No recent uploads.</div>'}
        </div>
      </div>
    </div>
  </main>
  <script>(function(){var v=${JSON.stringify(qPrefill)}; if(v) document.getElementById('q').value=v;})();</script>
</body>
</html>`);
  } catch (e) {
    console.error('Home error:', e.response?.status, e.response?.data || e.message);
    res.status(200).send(`<!doctype html><meta charset="utf-8"><title>Brightcove Video Tools</title>
      ${themeHead()}<body style="font-family:system-ui;padding:24px;color:var(--text);background:var(--bg)">
      <h1>Brightcove Video Tools</h1>
      <p>We couldn't load recent uploads right now, but search still works.</p>
      <form action="/search" method="get">
        <input style="padding:10px;border:1px solid var(--border);background:transparent;color:var(--text);border-radius:8px" name="q" placeholder='id:..., tag:"...", title:"..."' required />
        <button class="btn" type="submit" style="margin-left:8px">Search</button>
      </form>
      </body>`);
  }
});

// ---- UI: SEARCH RESULTS ----
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.redirect('/');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;

    const cards = (videos || []).map(v => {
      const tags = (v.tags || []).map(t => `<span class="tag">${stripHtml(t)}</span>`).join('');
      return `
        <div class="vcard">
          <iframe src="https://players.brightcove.net/${AID}/${PLAYER_ID}_default/index.html?videoId=${v.id}"
                  allow="encrypted-media" allowfullscreen loading="lazy"
                  title="${stripHtml(v.name)}"></iframe>
          <div class="meta">
            <div class="title">${stripHtml(v.name)}</div>
            <div class="id">ID: ${v.id}</div>
            <div class="tags"><strong>Tags:</strong> ${tags || '<em class="id">None</em>'}</div>
          </div>
        </div>`;
    }).join('');

    res.status(200).send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Results for: ${stripHtml(qInput)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
  ${themeHead()}
</head>
<body>
  <header>
    <h1>Search Results</h1>
    ${themeToggleButton()}
  </header>
  <main>
    <div class="topbar">
      <a href="/?q=${encodeURIComponent(qInput)}">← Back to search</a>
      <a class="btn-dl" href="${downloadUrl}">Download Video Analytics Spreadsheet</a>
    </div>
    <div class="card">
      ${videos && videos.length ? '<div class="grid">' + cards + '</div>' : `
        <div class="note">No videos found for that query.</div>
      `}
    </div>
  </main>
</body>
</html>`);
  } catch (err) {
    console.error('Search error:', err.response?.status, err.response?.data || err.message);
    res.status(200).send(`<!doctype html><meta charset="utf-8"><title>Search</title>
      ${themeHead()}<body style="font-family:system-ui;padding:24px;color:var(--text);background:var(--bg)">
      <h1>Search Results</h1>
      <p class="note">We couldn’t complete the search right now. Please try again.</p>
      <p><a href="/" style="color:var(--link)">← Back</a></p></body>`);
  }
});

// ---- ANALYTICS + PLACEMENTS EXPORT (auto fallback) ----
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  const debug = req.query.debug === '1';
  if (!qInput) return res.status(400).send('Missing search terms');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found for that search.');

    const ids = videos.map(v => v.id);

    // Core analytics
    let analytics = [];
    try {
      analytics = await getAnalyticsForVideos(ids, token);
    } catch (e) {
      console.error('[analytics] error', e.response?.status, e.response?.data || e.message);
      if (debug) return res.status(502).json({ step: 'analytics', status: e.response?.status, body: e.response?.data || e.message });
    }
    const aMap = new Map();
    for (const item of analytics) aMap.set(String(item.video), item);

    // Placements (auto-detect)
    let placementsMode = 'playerOnly';
    let placementsMap = new Map();
    try {
      const { mode, map } = await getPlacementsForVideos(ids, token, { from: PLACEMENTS_WINDOW, to: 'now' });
      placementsMode = mode;
      placementsMap = map;
    } catch (e) {
      console.error('[placements] failed completely', e.response?.status, e.response?.data || e.message);
      if (debug) return res.status(206).json({ step: 'placements', status: e.response?.status, body: e.response?.data || e.message });
    }

    // Build top-summary
    const topSummaryByVideo = new Map();
    for (const [vid, rows] of placementsMap.entries()) {
      if (placementsMode === 'full') {
        const byUrl = new Map();
        for (const r of rows) byUrl.set(r.url, (byUrl.get(r.url) || 0) + (r.views || 0));
        const top = Array.from(byUrl.entries()).map(([url, views]) => ({ url, views }))
          .sort((a, b) => b.views - a.views).slice(0, 5);
        topSummaryByVideo.set(String(vid), top);
      } else {
        const byPlayer = new Map();
        for (const r of rows) byPlayer.set(r.player, (byPlayer.get(r.player) || 0) + (r.views || 0));
        const top = Array.from(byPlayer.entries()).map(([player, views]) => ({ player, views }))
          .sort((a, b) => b.views - a.views).slice(0, 5);
        topSummaryByVideo.set(String(vid), top);
      }
    }

    // Workbook
    const wb = new ExcelJS.Workbook();

    // Sheet 1
    const ws = wb.addWorksheet('Video Metrics (All-Time)');
    const summaryHeader = placementsMode === 'full'
      ? `Top Destinations (${PLACEMENTS_WINDOW} · URL · views)`
      : `Top Players (${PLACEMENTS_WINDOW} · playerId · views)`;
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
      { header: summaryHeader, key: 'placementsSummary', width: 70 },
    ];

    const now = Date.now();
    for (const v of videos) {
      const a = aMap.get(String(v.id)) || {};
      const title = v.name || a.video_name || 'Untitled';
      const views = a.video_view || 0;

      let daysSince = 1;
      if (v.created_at) {
        const ts = new Date(v.created_at).getTime();
        if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((now - ts) / 86400000));
      }
      const dailyAvgViews = Number(((views || 0) / daysSince).toFixed(2));

      const top = topSummaryByVideo.get(String(v.id)) || [];
      const placementsCell = top.length
        ? (placementsMode === 'full'
            ? top.map(d => `${d.url} · ${d.views}`).join('; ')
            : top.map(d => `${d.player} · ${d.views}`).join('; '))
        : '—';

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

    // Sheet 2
    if (placementsMode === 'full') {
      const wp = wb.addWorksheet('Placements by Video');
      wp.columns = [
        { header: 'Video ID', key: 'video', width: 20 },
        { header: 'Player ID', key: 'player', width: 28 },
        { header: 'Destination Domain', key: 'domain', width: 34 },
        { header: 'Destination Path', key: 'path', width: 50 },
        { header: 'Full URL (protocol-relative)', key: 'url', width: 60 },
        { header: `Views (${PLACEMENTS_WINDOW})`, key: 'views', width: 18 },
      ];
      for (const vid of ids) {
        const rows = placementsMap.get(String(vid)) || [];
        for (const r of rows) {
          wp.addRow({
            video: vid,
            player: r.player || '(unknown)',
            domain: r.domain || '(none)',
            path: r.path || '(none)',
            url: r.url,
            views: r.views || 0
          });
        }
      }
    } else {
      const wp = wb.addWorksheet('Placements by Video (Players)');
      wp.columns = [
        { header: 'Video ID', key: 'video', width: 20 },
        { header: 'Player ID', key: 'player', width: 28 },
        { header: `Views (${PLACEMENTS_WINDOW})`, key: 'views', width: 18 },
      ];
      for (const vid of ids) {
        const rows = placementsMap.get(String(vid)) || [];
        for (const r of rows) {
          wp.addRow({
            video: vid,
            player: r.player || '(unknown)',
            views: r.views || 0
          });
        }
      }
    }

    const buffer = await wb.xlsx.writeBuffer();
    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_with_placements.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Length', buffer.length);
    return res.status(200).end(buffer);

  } catch (err) {
    console.error('[download] fatal', err.response?.status, err.response?.data || err.message);
    return res.status(500).send('Error generating spreadsheet.');
  }
});

// ---- HEALTH + NOT FOUND ----
app.get('/healthz', (_req, res) => res.status(200).send('ok'));
app.use((req, res) => res.status(404).send('Not found'));

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
