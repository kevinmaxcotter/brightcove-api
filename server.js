require('dotenv').config();
const express = require('express');
const axios = require('axios');
const ExcelJS = require('exceljs');

const app = express();
const PORT = process.env.PORT || 3000;

process.on('unhandledRejection', err => console.error('UNHANDLED REJECTION:', err));
process.on('uncaughtException', err => console.error('UNCAUGHT EXCEPTION:', err));

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

// ---------- Helpers ----------
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

// ---------- Auth ----------
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

// ---------- CMS ----------
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
    if (out.length > 5000) break;
  }
  console.log(`[CMS] q="${q}" -> ${out.length}`);
  return out;
}

async function fetchVideoById(id, token) {
  const url = `https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}`;
  const r = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return r.data;
}

// ---------- Unified Search ----------
async function unifiedSearch(input, token) {
  const terms = splitTerms(input);
  if (!terms.length) return [];

  const idTerms = terms.filter(looksLikeId);
  const nonIds = terms.filter(t => !looksLikeId(t));
  const pool = [];

  for (const id of idTerms) {
    try {
      const v = await fetchVideoById(id, token);
      if (v && v.state === 'ACTIVE') pool.push(v);
    } catch {}
  }

  let byTags = [];
  if (nonIds.length) {
    const qTags = ['state:ACTIVE', ...nonIds.map(t => `tags:"${esc(t)}"`)].join(' ');
    byTags = await fetchAllPages(qTags, token);
    pool.push(...byTags);
  }

  let byNameUnion = [];
  for (const t of nonIds) {
    const qName = `state:ACTIVE name:*${esc(t)}*`;
    const chunk = await fetchAllPages(qName, token);
    byNameUnion.push(...chunk);
  }
  pool.push(...byNameUnion);

  let filtered = pool;
  if (nonIds.length) {
    filtered = pool.filter(v => hasAllTags(v, nonIds) || titleContainsAll(v, nonIds));
  }

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

// ---------- Metrics ----------
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
    axios.get(metricsUrl, { headers: { Authorization: `Bearer ${token}` } }),
    axios.get(infoUrl, { headers: { Authorization: `Bearer ${token}` } }),
  ]);

  const title = infoResp.data?.name || 'Untitled';
  const tags = infoResp.data?.tags || [];
  const publishedAt = infoResp.data?.published_at || infoResp.data?.created_at;

  const m = metricsResp.data.items?.[0] || {};
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

// ---------- Health Check ----------
app.get('/healthz', (req, res) => res.send('ok'));

// ---------- Homepage ----------
app.get('/', async (req, res) => {
  const qPrefill = (req.query.q || '').replace(/`/g, '\\`');
  let recentHTML = '';
  try {
    const token = await getAccessToken();
    const recent = await cmsSearch('state:ACTIVE', token, { limit: 6, sort: '-created_at' });
    recentHTML = recent.map(v => `
      <div class="vcard">
        <img src="${v.images?.thumbnail?.src || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail'}" style="width:100%;border-radius:10px;"/>
        <div class="meta">
          <div class="title">${stripHtml(v.name || 'Untitled')}</div>
          <div class="id">ID: ${v.id}</div>
        </div>
      </div>
    `).join('');
  } catch (e) {
    recentHTML = '<div class="note">Error fetching recent videos.</div>';
  }

  res.send(`<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Brightcove Video Tools</title>
<link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
body{font-family:'Open Sans',sans-serif;margin:0;padding:0;background:#fff;color:#001f3f;transition:background .3s,color .3s}
header{display:flex;justify-content:space-between;align-items:center;padding:20px;border-bottom:1px solid #ccc}
.toggle{cursor:pointer;padding:6px 10px;border:1px solid #ccc;border-radius:8px;font-size:.9rem}
.dark{background:#0b0c10;color:#eaeaea}
.dark header{border-color:#444}
.card{background:#f8f9fa;border:1px solid #e5e7eb;border-radius:12px;padding:24px;max-width:520px;margin:40px auto;text-align:center}
input{width:100%;padding:12px;border:1px solid #c7ccd3;border-radius:10px}
.btn{width:100%;padding:12px;background:#001f3f;color:#fff;border:none;border-radius:10px;font-weight:700;margin-top:12px;cursor:pointer}
.btn:hover{background:#003366}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:20px;margin-top:30px;max-width:980px;margin-left:auto;margin-right:auto}
.vcard{background:#fff;border:1px solid #ccc;border-radius:10px;overflow:hidden;transition:transform .2s}
.vcard:hover{transform:scale(1.02)}
.dark .vcard{background:#1b1b1b;border-color:#444}
.dark .card{background:#1b1b1b;border-color:#444;color:#eaeaea}
</style>
</head>
<body>
<header>
  <h1>Brightcove Video Tools</h1>
  <div class="toggle" id="modeToggle">🌙 Dark Mode</div>
</header>
<main>
  <div class="card">
    <h2>Search Videos</h2>
    <form action="/search" method="get">
      <input id="q" name="q" placeholder='Enter ID, tags, or title keywords' required />
      <button class="btn" type="submit">Search</button>
    </form>
  </div>
  <section style="padding:20px">
    <h2 style="text-align:center">Recently Uploaded</h2>
    <div class="grid">${recentHTML}</div>
  </section>
</main>
<script>
document.getElementById('modeToggle').onclick=()=>{
  document.body.classList.toggle('dark');
  document.getElementById('modeToggle').textContent=
    document.body.classList.contains('dark')?'☀️ Light Mode':'🌙 Dark Mode';
};
</script>
</body>
</html>`);
});

// ---------- Search ----------
app.get('/search', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.redirect('/');
  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    const playerId = process.env.BRIGHTCOVE_PLAYER_ID;
    const downloadUrl = `/download?q=${encodeURIComponent(qInput)}`;
    const cards = videos.map(v => `
      <div class="vcard">
        <iframe src="https://players.brightcove.net/${AID}/${playerId}_default/index.html?videoId=${v.id}" allowfullscreen loading="lazy"></iframe>
        <div class="meta">
          <div class="title">${stripHtml(v.name)}</div>
          <div class="id">ID: ${v.id}</div>
        </div>
      </div>`).join('');

    res.send(`<!doctype html><html><head>
    <meta charset="utf-8"/><title>Results</title>
    <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet">
    <style>body{font-family:'Open Sans',sans-serif;background:#fff;color:#001f3f;margin:0}header{padding:20px;border-bottom:1px solid #ccc}a{color:#0b63ce;text-decoration:none}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:18px;padding:20px}.vcard{background:#fff;border:1px solid #ccc;border-radius:10px;overflow:hidden}iframe{width:100%;aspect-ratio:16/9;border:0}.meta{padding:10px}</style>
    </head><body>
    <header><a href="/">← Back</a> | <a href="${downloadUrl}" style="float:right;font-weight:700;">Download Spreadsheet</a></header>
    <main><div class="grid">${cards || '<p>No videos found.</p>'}</div></main></body></html>`);
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).send('Error searching.');
  }
});

// ---------- Download Spreadsheet ----------
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  if (!qInput) return res.status(400).send('Missing search terms');

  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found.');

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
      let row;
      try {
        row = await getMetricsForVideo(v.id, token);
      } catch (e1) {
        console.warn(`Retrying metrics for ${v.id}...`);
        await new Promise(r => setTimeout(r, 1000));
        try {
          row = await getMetricsForVideo(v.id, token);
        } catch (e2) {
          console.error(`Failed metrics for ${v.id}:`, e2.message);
          row = { id: v.id, title: v.name || 'Error', views: 'N/A', dailyAvgViews: 'N/A', impressions: 'N/A', engagement: 'N/A', playRate: 'N/A', secondsViewed: 'N/A', tags: v.tags || [] };
        }
      }
      ws.addRow({ ...row, tags: (row.tags || []).join(', ') });
    }

    const buf = await wb.xlsx.writeBuffer();
    res.setHeader('Content-Disposition', 'attachment; filename=video_metrics_alltime.xlsx');
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Length', buf.byteLength);
    res.end(Buffer.from(buf));
  } catch (err) {
    console.error('Download error:', err);
    res.status(500).send('Error generating spreadsheet.');
  }
});

app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
