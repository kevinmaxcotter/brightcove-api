
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');

const app = express();
const PORT = process.env.PORT || 3000;

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

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));

const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;

// ---------------- helpers ----------------
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

// ---------------- auth ----------------
async function getAccessToken() {
  const r = await axios.post(
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
  return r.data.access_token;
}

// ---------------- CMS helpers ----------------
async function cmsSearch(q, token, { limit = 100, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at';
  const r = await axios.get(url, {
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
    if (out.length > 5000) break; // safety guard
  }
  console.log(`[CMS] q="${q}" -> ${out.length}`);
  return out;
}

async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const r = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return r.data;
}

// ---------------- unified search ----------------
async function unifiedSearch(input, token) {
  const terms = splitTerms(input);
  if (!terms.length) return [];

  const idTerms = terms.filter(looksLikeId);
  const nonIds  = terms.filter(t => !looksLikeId(t));

  const pool = [];

  // Exact IDs
  for (const id of idTerms) {
    try {
      const v = await fetchVideoById(id, token);
      if (v && v.state === 'ACTIVE') pool.push(v);
    } catch {}
  }

  // Candidates by tags (AND) in one query
  let byTags = [];
  if (nonIds.length) {
    const qTags = ['state:ACTIVE', ...nonIds.map(t => `tags:"${esc(t)}"`)].join(' ');
    byTags = await fetchAllPages(qTags, token);
    pool.push(...byTags);
  }

  // Candidates by name:*term* for EACH term (union), then locally require titleContainsAll
  let byNameUnion = [];
  for (const t of nonIds) {
    const qName = `state:ACTIVE name:*${esc(t)}*`;
    const chunk = await fetchAllPages(qName, token);
    byNameUnion.push(...chunk);
  }
  pool.push(...byNameUnion);

  // Local filter for non-ID terms:
  let filtered = pool;
  if (nonIds.length) {
    filtered = pool.filter(v => hasAllTags(v, nonIds) || titleContainsAll(v, nonIds));
  }

  // De-dupe and normalize
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

  // Newest first
  list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  return list;
}

// ---------------- metrics ----------------
async function getMetricsForVideo(videoId, token) {
  const alltimeViewsUrl = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;
  const metricsUrl =
    `https://analytics.api.brightcove.com/v1/data?accounts=${AID}` +
    `&dimensions=video&where=video==${videoId}` +
    `&fields=video,engagement_score,play_rate,video_seconds_viewed,video_impression` +
    `&from=alltime&to=now`;
  const infoUrl = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${videoId}`;

  const [alltimeResp, metricsResp, infoResp] = await Promise.all([
    axios.get(alltimeViewsUrl, { headers: { Authorization: `Bearer ${token}` } }),
    axios.get(metricsUrl,      { headers: { Authorization: `Bearer ${token}` } }),
    axios.get(infoUrl,         { headers: { Authorization: `Bearer ${token}` } }),
  ]);

  const title = infoResp.data?.name || 'Untitled';
  const tags  = infoResp.data?.tags || [];
  const publishedAt = infoResp.data?.published_at || infoResp.data?.created_at;

  const m = metricsResp.data.items?.[0] || {};
  const alltimeViews =
    alltimeResp.data?.alltime_video_views ??
    alltimeResp.data?.alltime_videos_views ?? 0;

  let daysSince = 1;
  if (publishedAt) {
    const ts = new Date(publishedAt).getTime();
    if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((Date.now() - ts) / 86400000));
  }
  const dailyAvgViews = Number((alltimeViews / daysSince).toFixed(2));

  return {
    id: videoId,
    title,
    tags,
    views: alltimeViews,
    dailyAvgViews,
    impressions: m.video_impression || 0,
    engagement: m.engagement_score || 0,
    playRate: m.play_rate || 0,
    secondsViewed: m.video_seconds_viewed || 0,
  };
}

// ---------------- UI: home ----------------
app.get('/', (req, res) => {
  const qPrefill = (req.query.q || '').replace(/`/g, '\\`');
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Brightcove Video Tools</title>
  <style>
    body { font-family:'Open Sans',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; background:#ffffff; color:#001f3f; margin:0; }
    header { display:flex; align-items:center; padding:20px; background:#fff; border-bottom:1px solid #e5e7eb; }
    header h1 { margin:0; font-size:1.8rem; font-weight:700; }
    main { display:flex; justify-content:center; align-items:center; flex-direction:column; padding:40px 20px; }
    .card { background:#f8f9fa; border:1px solid #e5e7eb; border-radius:12px; padding:24px; max-width:520px; width:100%; box-shadow:0 2px 8px rgba(0,0,0,.05); }
    h2 { margin:0 0 12px; font-size:1.3rem; }
    label { font-weight:600; display:block; margin:10px 0 6px; }
    input { width:100%; padding:12px 14px; border:1px solid #c7ccd3; background:#fff; color:#001f3f; border-radius:10px; outline:none; }
    input::placeholder { color:#6b7280; }
    .btn { display:inline-block; width:100%; padding:12px 16px; background:#001f3f; color:#fff; border:none; border-radius:10px; cursor:pointer; font-weight:700; margin-top:12px; }
    .btn:hover { background:#003366; }
    .note { color:#6b7280; font-size:.9rem; margin-top:8px; }
  </style>
</head>
<body>
  <header><h1>Brightcove Video Tools</h1></header>
  <main>
    <div class="card">
      <h2>🔍 Search by ID, Tag(s), or Title</h2>
      /search
        <label for="q">Enter terms (comma-separated)</label>
        <input id="q" name="q" placeholder='Examples: 6376653485112, pega platform, customer decision hub' required />
        <button class="btn" type="submit">Search & Watch</button>
        <div class="note">IDs → exact match. Multiple tags → AND. Titles → must contain all terms.</div>
      </form>
    </div>
  </main>
  <script>(function(){var v=${JSON.stringify(qPrefill)}; if(v) document.getElementById('q').value=v;})();</script>
</body>
</html>`);
});

// ---------------- UI: results with pagination ----------------
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  const page = parseInt(req.query.page) || 1;
  const limit = 100;

  if (!qInput) return res.redirect('/');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const playerId = process.env.BRIGHTCOVE_PLAYER_ID;
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;

    // Pagination logic
    const totalVideos = videos.length;
    const totalPages = Math.ceil(totalVideos / limit);
    const start = (page - 1) * limit;
    const end = start + limit;
    const paginatedVideos = videos.slice(start, end);

    // Pagination controls
    let paginationControls = '';
    if (totalPages > 1) {
      paginationControls += `<div style="margin:16px 0;">`;
      if (page > 1) {
        paginationControls += `/search?q=${encodeURIComponent(qInput)}&page=${page-1}&laquo; Previous</a> `;
      }
      paginationControls += `Page ${page} of ${totalPages}`;
      if (page < totalPages) {
        paginationControls += ` /search?q=${encodeURIComponent(qInput)}&page=${page+1}Next &raquo;</a>`;
      }
      paginationControls += `</div>`;
    }

    const cards = paginatedVideos.map(v => {
      const tags = (v.tags || []).map(t => `<span class="tag">${stripHtml(t)}</span>`).join('');
      return `
        <div class="vcard">
          <iframe src="https://players.brightcove.net/${AID}/${playerId}_default/index.html?videoId=${v.id}"
                  allow="encrypted-media" allowfullscreen loading="lazy"
                  title="${strip"id">ID: ${v.id}</div>
            <div class="tags"><strong>Tags:</strong> ${tags || '<em>None</em>'}</div>
          </div>
        </div>`;
    }).join('');

    res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Results for: ${stripHtml(qInput)}</title>
  <style>
    :root{--navy:#001f3f;--muted:#6b7280;--chip:#eef2f7;--chipBorder:#c7ccd3}
    *{box-sizing:border-box}
    body{font-family:'Open Sans',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#fff;color:var(--navy);margin:0}
    header{display:flex;align-items:center;padding:20px;border-bottom:1px solid #e5e7eb;max-width:980px;margin:0 auto}
    header h1{margin:0;font-size:1.2rem}
    main{max-width:980px;margin:20px auto;padding:0 20px}
    .topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
    a.back{color:#0b63ce;text-decoration:none}
    a.back:hover{text-decoration:underline}
    .btn-dl{display:inline-block;padding:10px 14px;background:#001f3f;color:#fff;border-radius:10px;text-decoration:none;font-weight:700}
    .btn-dl:hover{background:#003366}
    .card{background:#f8f9fa;border:1px solid #e5e7eb;border-radius:12px;padding:24px}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:18px;margin-top:12px}
    .vcard{background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden}
    .vcard iframe{width:100%;aspect-ratio:16/9;border:0}
    .meta{padding:12px 14px}
    .title{font-weight:700;font-size:15px;margin-bottom:4px}
    .id{color:var(--muted);font-size:13px;margin-bottom:6px}
    .tag{display:inline-block;margin:4px 6px 0 0;padding:4px 8px;border-radius:999px;background:var(--chip);border:1px solid var(--chipBorder);color:#1f2937;font-size:12px}
  </style>
</head>
<body>
  <header>
    <h1>Search results</h1>
  </header>
  <main>
    <div class="topbar">
      /?q=${encodeURIComponent(qInput)}&larr; Back to search</a>
      <a class="btn-dl" href="${downloadUrl}">Downloadet</a>
    </div>
    ${paginationControls}
    <div class="card">
      <div class="grid">
        ${cards || '<div>No videos found.</div>'}
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

// ---------------- Spreadsheet (metrics for these results) ----------------
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.status(400).send('Missing search terms');

  try {
    const token  = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found for that search.');

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

    for (const v of videos) {
      try {
        const row = await getMetricsForVideo(v.id, token);
        ws.addRow({ ...row, tags: (row.tags || []).join(', ') });
      } catch (e) {
        console.error(`Metrics error for ${v.id}:`, e.response?.data || e.message);
        ws.addRow({
          id: v.id, title: v.name || 'ERROR',
