/* eslint-disable no-console */
require('dotenv').config();

const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const REQUIRED_ENV = [
  'BRIGHTCOVE_ACCOUNT_ID',
  'BRIGHTCOVE_CLIENT_ID',
  'BRIGHTCOVE_CLIENT_SECRET',
  'BRIGHTCOVE_PLAYER_ID',
];
const missingEnv = REQUIRED_ENV.filter((k) => !process.env[k]);
if (missingEnv.length) {
  console.error('Missing .env keys:', missingEnv.join(', '));
  process.exit(1);
}

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1h',
}));

const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;

// ---------------- Helpers ----------------
const looksLikeId = (s) => /^\d{9,}$/.test(String(s).trim());
const splitTerms = (input) =>
  String(input || '')
    .split(',')
    .map((s) => s.trim())
    // remove wrapping single or double quotes if present
    .map((s) => s.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1'))
    .filter(Boolean);

// Escape only for Brightcove CMS query where we need to put text in quotes
const escapeForCmsQuery = (s) => String(s).replace(/(["\\])/g, '\\$1');

const escapeHtml = (s = '') =>
  String(s).replace(/[&<>"']/g, (m) =>
    ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
    }[m]),
  );

const titleContainsAll = (video, terms) => {
  const name = (video.name || '').toLowerCase();
  return terms.every((t) => name.includes(String(t).toLowerCase()));
};

const hasAllTags = (video, terms) => {
  const vt = (video.tags || []).map((t) => String(t).toLowerCase());
  return terms.every((t) => vt.includes(String(t).toLowerCase()));
};

const fmtDate = (iso) => {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? 'Unknown' : d.toISOString().slice(0, 10);
};

// ---------------- Auth (token cache) ----------------
let tokenCache = { token: null, expiresAt: 0 };

async function getAccessToken() {
  const now = Date.now();
  if (tokenCache.token && tokenCache.expiresAt > now) {
    return tokenCache.token;
  }
  const r = await axios.post(
    'https://oauth.brightcove.com/v4/access_token',
    'grant_type=client_credentials',
    {
      auth: {
        username: process.env.BRIGHTCOVE_CLIENT_ID,
        password: process.env.BRIGHTCOVE_CLIENT_SECRET,
      },
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 15000,
    },
  );
  const token = r.data.access_token;
  const ttl = Math.max(60, (r.data.expires_in || 300) - 30); // 30s safety buffer
  tokenCache = { token, expiresAt: now + ttl * 1000 };
  return token;
}

// Axios instance (sane defaults)
const http = axios.create({
  timeout: 20000,
});

// ---------------- CMS Helpers ----------------
async function cmsSearch(q, token, { limit = 100, offset = 0, sort = '-created_at' } = {}) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const r = await http.get(url, {
    headers: { Authorization: `Bearer ${token}` },
    params: { q, fields, sort, limit, offset },
  });
  return Array.isArray(r.data) ? r.data : [];
}

async function fetchAllPages(q, token) {
  const out = [];
  let offset = 0;
  const limit = 100;
  const CAP = 5000; // safety cap
  while (true) {
    const batch = await cmsSearch(q, token, { offset, limit });
    out.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
    if (out.length >= CAP) break;
  }
  return out;
}

async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const r = await http.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return r.data;
}

// Fetch N most recent videos
async function cmsRecentVideos(token, count = 20) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos`;
  const fields = 'id,name,images,tags,state,created_at,published_at';
  const params = {
    q: 'state:ACTIVE',
    fields,
    sort: '-created_at',
    limit: count,
  };
  const r = await http.get(url, {
    headers: { Authorization: `Bearer ${token}` },
    params,
  });
  const data = Array.isArray(r.data) ? r.data : [];
  return data
    .filter((v) => v && v.state === 'ACTIVE')
    .map((v) => ({
      id: v.id,
      name: v.name || 'Untitled',
      tags: v.tags || [],
      created_at: v.created_at,
      published_at: v.published_at,
      thumb:
        (v.images && (v.images.thumbnail?.src || v.images.poster?.src)) ||
        'https://via.placeholder.com/320x180.png?text=No+Thumbnail',
    }));
}

// ---------------- Unified Search ----------------
async function unifiedSearch(input, token) {
  const terms = splitTerms(input);
  if (!terms.length) return [];

  const idTerms = terms.filter(looksLikeId);
  const nonIds = terms.filter((t) => !looksLikeId(t));

  const pool = [];

  // Direct ID fetches (resilient to missing)
  for (const id of idTerms) {
    try {
      const v = await fetchVideoById(id, token);
      if (v && v.state === 'ACTIVE') pool.push(v);
    } catch {
      // ignore missing IDs
    }
  }

  // Tag and title searches
  if (nonIds.length) {
    // All non-ID terms as tags
    const qTags = ['state:ACTIVE', ...nonIds.map((t) => `tags:"${escapeForCmsQuery(t)}"`)].join(' ');
    pool.push(...(await fetchAllPages(qTags, token)));

    // Each term in name
    for (const t of nonIds) {
      const qName = `state:ACTIVE name:*${escapeForCmsQuery(t)}*`;
      pool.push(...(await fetchAllPages(qName, token)));
    }
  }

  // If non-ID terms were used, filter down to items that match *all* terms
  let filtered = pool;
  if (nonIds.length) {
    filtered = pool.filter((v) => hasAllTags(v, nonIds) || titleContainsAll(v, nonIds));
  }

  // De-dupe, map, sort by created
  const seen = new Set();
  const list = [];
  for (const v of filtered) {
    if (!v || !v.id || v.state !== 'ACTIVE' || seen.has(v.id)) continue;
    seen.add(v.id);
    list.push({
      id: v.id,
      name: v.name || 'Untitled',
      tags: v.tags || [],
      thumb:
        (v.images && (v.images.thumbnail?.src || v.images.poster?.src)) ||
        'https://via.placeholder.com/320x180.png?text=No+Thumbnail',
      created_at: v.created_at,
      published_at: v.published_at,
    });
  }

  list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  return list;
}

// ---------------- Analytics ----------------
async function getMetricsForVideo(videoId, token) {
  const alltimeViewsUrl = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;

  // Build analytics with params to let axios handle encoding cleanly.
  const analyticsUrl = 'https://analytics.api.brightcove.com/v1/data';

  const infoUrl = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${videoId}`;

  const [alltimeResp, metricsResp, infoResp] = await Promise.all([
    http.get(alltimeViewsUrl, { headers: { Authorization: `Bearer ${token}` } }),
    http.get(analyticsUrl, {
      headers: { Authorization: `Bearer ${token}` },
      params: {
        accounts: AID,
        dimensions: 'video',
        where: `video==${videoId}`,
        fields: 'video,engagement_score,play_rate,video_seconds_viewed,video_impression',
        from: 'alltime',
        to: 'now',
        limit: 1,
      },
    }),
    http.get(infoUrl, { headers: { Authorization: `Bearer ${token}` } }),
  ]);

  const title = infoResp.data?.name || 'Untitled';
  const tags = infoResp.data?.tags || [];
  const publishedAt = infoResp.data?.published_at || infoResp.data?.created_at;

  const m = metricsResp.data?.items?.[0] || {};
  const alltimeViews = alltimeResp.data?.alltime_video_views ?? 0;

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

// ---------------- Concurrency helper ----------------
async function mapConcurrent(items, limit, mapper) {
  const results = new Array(items.length);
  let i = 0;
  const workers = new Array(Math.min(limit, Math.max(1, items.length)))
    .fill(0)
    .map(async () => {
      while (true) {
        const idx = i++;
        if (idx >= items.length) break;
        try {
          results[idx] = await mapper(items[idx], idx);
        } catch (e) {
          results[idx] = e;
        }
      }
    });
  await Promise.all(workers);
  return results;
}

// ---------------- Home Page ----------------
app.get('/', async (req, res) => {
  const qPrefill = String(req.query.q || '').replace(/`/g, '\\`');
  let recent = [];
  try {
    const token = await getAccessToken();
    recent = await cmsRecentVideos(token, 20);
  } catch (e) {
    console.error('Recent videos error:', e.message);
  }

  const recentCards = recent
    .map(
      (v) => `
    <a class="r-card" href="/search?q=${}
      <div class="thumb-wrap">
        <img src="${escapeHtml(v.thumb)}" alt="${escapeHtml(v.name)}">
        <div class="r-title">${escapeHtml(v.name)}</div>
        <div class="r-sub">ID: ${v.id} â€¢ ${fmtDate(v.created_at)}</div>
      </div>
    </a>
  `,
    )
    .join('');

  res.type('html').send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Brightcove Video Tools</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="preconnect" href="https://fontsts.gstatic.com
  <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&displayle>
    body { font-family:'Open Sans',sans-serif; margin:0; background:#fff; color:#001f3f; }
    header { padding:20px; border-bottom:1px solid #e5e7eb; }
    header h1 { margin:0; font-size:1.8rem; font-weight:700; }
    main { max-width:1100px; margin:0 auto; padding:24px 16px; }
    .card { background:#f8f9fa; border:1px solid #e5e7eb; border-radius:12px; padding:24px; }
    input[type="text"] { width:100%; padding:12px; border:1px solid #ccc; border-radius:8px; }
    .btn { display:block; width:100%; padding:12px; background:#001f3f; color:#fff; border:none; border-radius:8px; margin-top:12px; font-weight:700; cursor:pointer; }
    .recent { margin-top:24px; }
    .recent-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; }
    @media(max-width:900px){ .recent-grid{grid-template-columns:repeat(2,1fr);} }
    @media(max-width:600px){ .recent-grid{grid-template-columns:1fr;} }
    .r-card { display:block; text-decoration:none; color:inherit; border:1px solid #e5e7eb; border-radius:10px; overflow:hidden; background:#fff; }
    .thumb-wrap { aspect-ratio:16/9; background:#eee; }
    .thumb-wrap img { width:100%; height:100%; object-fit:cover; display:block; }
    .r-meta { padding:10px; }
    .r-title { font-weight:700; font-size:14px; margin-bottom:4px; }
    .r-sub { font-size:12px; color:#6b7280; }
  </style>
</head>
<body>
  <header><h1>Brightcove Video Tools</h1></header>
  <main>
    <div class="card">
      <h2>Search by ID, Tag(s), or Title</h2>
      <form action="/search" methodtext" id="q" name="q" placeholder="Examples: 6376653485112, pega platform" required>
        <button class="btn" type="submit">Search &amp; Watch</button>
      </form>
      <div class="recent">
        <h3>20 Most Recent Uploads</h3>
        <div class="recent-grid">
          ${recentCards || '<p>No recent videos found.</p>'}
        </div>
      </div>
    </div>
  </main>
  <script>(function(){var v=\`${qPrefill}\`; if(v){var el=document.getElementById('q'); if(el) el.value=v;}})();</script>
</body>
</html>`);
});

// ---------------- Search Results ----------------
app.get('/search', async (req, res) => {
  const qInput = String(req.query.q || '').trim();
  if (!qInput) return res.redirect('/');

  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const playerId = process.env.BRIGHTCOVE_PLAYER_ID;
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;

    const cards = videos
      .map(
        (v) => `
      <div class="vcard">
        <iframe
          src="https://players.brightcove.net/${AID}/${playerId}_default/index.html?videoId=${v.id}"
          allow="autoplay; encrypted-media"
          allowe)}</div>
          <div class="id">ID: ${v.id} â€¢ ${fmtDate(v.created_at)}</div>
        </div>
      </div>
    `,
      )
      .join('');

    res.type('html').send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Results</title>
  <link rel="preconnectpis.com
  https://fonts.gstatic.com
  <link://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap
  <style>
    body{font-family:'Open Sans',sans-serif;margin:0;padding:20px;}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;}
    .vcard iframe{width:100%;aspect-ratio:16/9;border:0;}
    .meta{padding:8px;}
    .title{font-weight:700;}
    .id{font-size:12px;color:#6b7280;}
    a{color:#001f3f;text-decoration:none;font-weight:700;}
    a:hover{text-decoration:underline;}
  </style>
</head>
<body>
  <â† Back</a> | <{downloadUrl}Download Spreadsheet</a>
  <div class="grid">${cards || '<p>No videos found.</p>'}</div>
</body>
</html>`);
  } catch (err) {
    console.error('Search error:', err?.response?.data || err.message);
    res.status(500).send('Error searching.');
  }
});

// ---------------- Download Spreadsheet ----------------
app.get('/download', async (req, res) => {
  const qInput = String(req.query.q || '').trim();
  if (!qInput) return res.status(400).send('Missing search terms');
  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found.');

    const wb = new ExcelJS.Workbook();
    wb.created = new Date();
    const ws = wb.addWorksheet('Video Metrics');
    ws.columns = [
      { header: 'Video ID', key: 'id', width: 20 },
      { header: 'Title', key: 'title', width: 40 },
      { header: 'All-Time Views', key: 'views', width: 18 },
      { header: 'Daily Avg Views', key: 'dailyAvgViews', width: 18 },
      { header: 'Impressions', key: 'impressions', width: 18 },
      { header: 'Engagement', key: 'engagement', width: 18 },
      { header: 'Play Rate', key: 'playRate', width: 12 },
      { header: 'Seconds Viewed', key: 'secondsViewed', width: 18 },
      { header: 'Tags', key: 'tags', width: 40 },
    ];

    // Fetch metrics concurrently with a modest cap
    const rows = await mapConcurrent(videos, 4, async (v) => {
      const m = await getMetricsForVideo(v.id, token);
      return { ...m, tags: (m.tags || []).join(', ') };
    });

    for (const row of rows) {
      if (row instanceof Error) continue;
      ws.addRow(row);
    }

    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics.xlsx');
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Spreadsheet error:', err?.response?.data || err.message);
    res.status(500).send('Error generating spreadsheet.');
  }
});

// ---------------- Health ----------------
app.get('/healthz', (_req, res) => res.type('text').send('ok'));

// ---------------- Start ----------------
app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));