// server.js — Light/Dark Theme Toggle Edition
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
const RECENT_LIMIT = Number(process.env.RECENT_LIMIT || 9);

// ---- MIDDLEWARE ----
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public')); // optional assets

// ---- HTTP + TOKEN CACHE ----
const axiosInstance = axios.create({ timeout: 30000 });
let tokenCache = { access_token: null, expires_at: 0 };

async function getAccessToken() {
  const now = Date.now();
  if (tokenCache.access_token && now < tokenCache.expires_at - 30000) {
    return tokenCache.access_token;
  }
  const r = await axiosInstance.post(
    'https://oauth.brightcove.com/v4/access_token',
    'grant_type=client_credentials',
    {
      auth: {
        username: process.env.BRIGHTCOVE_CLIENT_ID,
        password: process.env.BRIGHTCOVE_CLIENT_SECRET
      },
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    }
  );
  const ttl = (r.data?.expires_in ?? 300) * 1000;
  tokenCache = {
    access_token: r.data.access_token,
    expires_at: Date.now() + ttl
  };
  return tokenCache.access_token;
}

// ---- UTILS ----
const looksLikeId = s => /^\d{9,}$/.test(String(s).trim());
const splitTerms = input => String(input || '')
  .split(',')
  .map(s => s.trim().replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1'))
  .filter(Boolean);
const esc = s => String(s).replace(/"/g, '\\"');
const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const titleContainsAll = (video, terms) => {
  const name = (video.name || '').toLowerCase();
  return terms.every(t => name.includes(t.toLowerCase()));
};
const hasAllTags = (video, terms) => {
  const vt = (video.tags || []).map(t => String(t).toLowerCase());
  return terms.every(t => vt.includes(t.toLowerCase()));
};

// ---- CMS HELPERS ----
async function cmsSearch(q, token, { limit = 100, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const r = await axiosInstance.get(url, {
    headers: { Authorization: `Bearer ${token}` },
    params: { q, fields, sort, limit, offset }
  });
  return r.data || [];
}

async function fetchAllPages(q, token) {
  const out = [];
  let offset = 0;
  while (true) {
    const batch = await cmsSearch(q, token, { offset });
    out.push(...batch);
    if (batch.length < 100) break;
    offset += 100;
    if (out.length > 5000) break; // safety
  }
  return out;
}

async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const r = await axiosInstance.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return r.data;
}

// ---- RECENT UPLOADS ----
async function fetchRecentUploads(token, limit = RECENT_LIMIT) {
  const list = await cmsSearch('state:ACTIVE', token, { limit, sort: '-created_at', offset: 0 });
  return (list || []).map(v => ({
    id: v.id,
    name: v.name || 'Untitled',
    tags: v.tags || [],
    created_at: v.created_at,
    thumb: v.images?.thumbnail?.src || v.images?.poster?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'
  }));
}

// ---- UNIFIED SEARCH ----
async function unifiedSearch(input, token) {
  const terms = splitTerms(input);
  if (!terms.length) return [];

  const idTerms = terms.filter(looksLikeId);
  const nonIds  = terms.filter(t => !looksLikeId(t));

  const pool = [];

  // IDs
  for (const id of idTerms) {
    try {
      const v = await fetchVideoById(id, token);
      if (v && v.state === 'ACTIVE') pool.push(v);
    } catch {}
  }

  // Tags AND (single query)
  if (nonIds.length) {
    const qTags = ['state:ACTIVE', ...nonIds.map(t => `tags:"${esc(t)}"`)].join(' ');
    const byTags = await fetchAllPages(qTags, token);
    pool.push(...byTags);
  }

  // Title contains terms (union then AND locally)
  for (const t of nonIds) {
    const qName = `state:ACTIVE name:*${esc(t)}*`;
    const chunk = await fetchAllPages(qName, token);
    pool.push(...chunk);
  }

  // Local filter for non-ID terms
  const filtered = nonIds.length ? pool.filter(v => hasAllTags(v, nonIds) || titleContainsAll(v, nonIds)) : pool;

  // De-dupe + normalize
  const seen = new Set();
  const list = [];
  for (const v of filtered) {
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

  list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
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

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const { data } = await axiosInstance.get(`${endpoint}?${params.toString()}`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        out.push(...(data?.items || []));
        break;
      } catch (err) {
        const status = err.response?.status;
        if (attempt < 2 && (status === 429 || (status >= 500 && status < 600))) {
          await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
          continue;
        }
        throw err;
      }
    }
  }
  return out;
}

async function getAlltimeViews(videoId, token) {
  const url = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;
  const { data } = await axiosInstance.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return data?.alltime_video_views ?? data?.alltime_videos_views ?? 0;
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
    /* Light mode vars override when data-theme="light" */
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

// ---- UI: HOME (Search + Recent Uploads; no spreadsheet button here) ----
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
        <input id="q" name="q" placeholder='Examples: 6376653485112, pega platform, customer decision hub' required />
        <button class="btn" type="submit">Search & Watch</button>
        <div class="topnote">IDs → exact match. Multiple tags → AND. Titles → must contain all terms.</div>
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
    res.status(500).send('Error loading home.');
  }
});

// ---- UI: SEARCH RESULTS (Spreadsheet button appears here) ----
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.redirect('/');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;

    const cards = videos.map(v => {
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

    res.send(`<!doctype html>
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
      <div class="grid">
        ${cards || '<div class="note">No videos found.</div>'}
      </div>
    </div>
  </main>
</body>
</html>`);
  } catch (err) {
    console.error('Search error:', err.response?.status, err.response?.data || err.message);
    res.status(500).send('Error searching.');
  }
});

// ---- SPREADSHEET EXPORT ----
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.status(400).send('Missing search terms');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found for that search.');

    const ids = videos.map(v => v.id);
    const analytics = await getAnalyticsForVideos(ids, token);

    const aMap = new Map();
    for (const item of analytics) aMap.set(String(item.video), item);

    const USE_ALLTIME_VIEWS = true;
    let viewsMap = new Map();
    if (USE_ALLTIME_VIEWS) {
      const limit = 6;
      let i = 0;
      async function worker() {
        while (i < ids.length) {
          const idx = i++;
          const id = ids[idx];
          try {
            const v = await getAlltimeViews(id, token);
            viewsMap.set(String(id), v);
          } catch (e) {
            console.error('alltime views error', id, e.response?.data || e.message);
            viewsMap.set(String(id), null);
          }
        }
      }
      await Promise.all(Array.from({ length: Math.min(limit, ids.length) }, worker));
    }

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
    ];

    const now = Date.now();
    for (const v of videos) {
      const a = aMap.get(String(v.id)) || {};
      const title = v.name || a.video_name || 'Untitled';
      const views = viewsMap.has(String(v.id))
        ? (viewsMap.get(String(v.id)) ?? a.video_view ?? 0)
        : (a.video_view ?? 0);

      let daysSince = 1;
      if (v.created_at) {
        const ts = new Date(v.created_at).getTime();
        if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((now - ts) / 86400000));
      }
      const dailyAvgViews = Number(((views || 0) / daysSince).toFixed(2));

      ws.addRow({
        id: v.id,
        title,
        views,
        dailyAvgViews,
        impressions: a.video_impression || 0,
        engagement: a.engagement_score || 0,
        playRate: a.play_rate || 0,
        secondsViewed: a.video_seconds_viewed || 0,
        tags: (v.tags || []).join(', ')
      });
    }

    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_alltime.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Download error:', err.response?.status, err.response?.data || err.message);
    res.status(500).send('Error generating spreadsheet.');
  }
});

// ---- START ----
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
