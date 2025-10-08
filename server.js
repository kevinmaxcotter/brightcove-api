require('dotenv').config(); const express = require('express'); const axios = require('axios'); const ExcelJS = require('exceljs');

const app = express(); const PORT = process.env.PORT || 3000;

const MUST = [ 'BRIGHTCOVE_ACCOUNT_ID', 'BRIGHTCOVE_CLIENT_ID', 'BRIGHTCOVE_CLIENT_SECRET', 'BRIGHTCOVE_PLAYER_ID' ]; const missing = MUST.filter(k => !process.env[k]); if (missing.length) { console.error('Missing .env keys:', missing.join(', ')); process.exit(1); }

app.use(express.urlencoded({ extended: true })); app.use(express.json()); app.use(express.static('public'));

const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;

// ---------------- Helpers ---------------- const looksLikeId = s => /\d{9,}$/.test(String(s).trim()); const splitTerms = input => String(input || '') .split(',') .map(s => s.trim().replace(/"(.)"$/, '$1').replace(/^'(.)'$/, '$1')) .filter(Boolean); const esc = s => String(s).replace(/"/g, '\"'); const stripHtml = s => String(s).replace(/[&<>"']/g, m => ({ '&': '&', '<': '<', '>': '>', '"': '"', "'": ''' }[m])); const titleContainsAll = (video, terms) => { const name = (video.name || '').toLowerCase(); return terms.every(t => name.includes(t.toLowerCase())); }; const hasAllTags = (video, terms) => { const vt = (video.tags || []).map(t => String(t).toLowerCase()); return terms.every(t => vt.includes(t.toLowerCase())); }; const fmtDate = iso => { const d = new Date(iso); return isNaN(d) ? 'Unknown' : d.toISOString().slice(0, 10); };

// ---------------- Auth ---------------- async function getAccessToken() { const r = await axios.post( 'https://oauth.brightcove.com/v4/access_token', 'grant_type=client_credentials', { auth: { username: process.env.BRIGHTCOVE_CLIENT_ID, password: process.env.BRIGHTCOVE_CLIENT_SECRET }, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } } ); return r.data.access_token; }

// ---------------- CMS Helpers ---------------- async function cmsSearch(q, token, { limit = 100, offset = 0, sort = '-created_at' } = {}) { const url =
https://cms.api.brightcove.com/v1/accounts/${AID}/videos
; const fields = 'id,name,images,tags,state,created_at'; const r = await axios.get(url, { headers: { Authorization:
Bearer ${token}
}, params: { q, fields, sort, limit, offset } }); return r.data || []; }

async function fetchAllPages(q, token) { const out = []; let offset = 0; while (true) { const batch = await cmsSearch(q, token, { offset }); out.push(...batch); if (batch.length < 100) break; offset += 100; if (out.length > 5000) break; } return out; }

async function fetchVideoById(id, token) { const url =
https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${id}
; const r = await axios.get(url, { headers: { Authorization:
Bearer ${token}
} }); return r.data; }

// Fetch 20 most recent videos async function cmsRecentVideos(token, count = 20) { const url =
https://cms.api.brightcove.com/v1/accounts/${AID}/videos
; const fields = 'id,name,images,tags,state,created_at'; const params = { q: 'state:ACTIVE', fields, sort: '-created_at', limit: count }; const r = await axios.get(url, { headers: { Authorization:
Bearer ${token}
}, params }); const data = Array.isArray(r.data) ? r.data : []; return data .filter(v => v && v.state === 'ACTIVE') .map(v => ({ id: v.id, name: v.name || 'Untitled', tags: v.tags || [], created_at: v.created_at, thumb: (v.images && (v.images.thumbnail?.src || v.images.poster?.src)) || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail' })); }

// ---------------- Unified Search ---------------- async function unifiedSearch(input, token) { const terms = splitTerms(input); if (!terms.length) return [];

const idTerms = terms.filter(looksLikeId); const nonIds = terms.filter(t => !looksLikeId(t));

const pool = [];

for (const id of idTerms) { try { const v = await fetchVideoById(id, token); if (v && v.state === 'ACTIVE') pool.push(v); } catch {} }

if (nonIds.length) { const qTags = ['state:ACTIVE', ...nonIds.map(t =>
tags:"${esc(t)}"
)].join(' '); pool.push(...await fetchAllPages(qTags, token)); for (const t of nonIds) { const qName =
state:ACTIVE name:*${esc(t)}*
; pool.push(...await fetchAllPages(qName, token)); } }

let filtered = pool; if (nonIds.length) { filtered = pool.filter(v => hasAllTags(v, nonIds) || titleContainsAll(v, nonIds)); }

const seen = new Set(); const list = []; for (const v of filtered) { if (!v || !v.id || v.state !== 'ACTIVE' || seen.has(v.id)) continue; seen.add(v.id); list.push({ id: v.id, name: v.name || 'Untitled', tags: v.tags || [], thumb: (v.images && (v.images.thumbnail?.src || v.images.poster?.src)) || 'https://via.placeholder.com/320x180.png?text=No+Thumbnail', created_at: v.created_at }); }

list.sort((a, b) => new Date(b.created_at) - new Date(a.created_at)); return list; }

// ---------------- Metrics ---------------- async function getMetricsForVideo(videoId, token) { const alltimeViewsUrl =
https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}
; const metricsUrl =
https://analytics.api.brightcove.com/v1/data?accounts=${AID}
+
&dimensions=video&where=video==${videoId}
+
&fields=video,engagement_score,play_rate,video_seconds_viewed,video_impressions
+
&from=alltime&to=now
; const infoUrl =
https://cms.api.brightcove.com/v1/accounts/${AID}/videos/${videoId}
;

const [alltimeResp, metricsResp, infoResp] = await Promise.all([ axios.get(alltimeViewsUrl, { headers: { Authorization:
Bearer ${token}
} }), axios.get(metricsUrl, { headers: { Authorization:
Bearer ${token}
} }), axios.get(infoUrl, { headers: { Authorization:
Bearer ${token}
} }), ]);

const title = infoResp.data?.name || 'Untitled'; const tags = infoResp.data?.tags || []; const publishedAt = infoResp.data?.published_at || infoResp.data?.created_at;

const m = metricsResp.data.items?.[0] || {}; const alltimeViews = alltimeResp.data?.alltime_video_views ?? 0;

let daysSince = 1; if (publishedAt) { const ts = new Date(publishedAt).getTime(); if (!Number.isNaN(ts)) daysSince = Math.max(1, Math.ceil((Date.now() - ts) / 86400000)); } const dailyAvgViews = Number((alltimeViews / daysSince).toFixed(2));

return { id: videoId, title, tags, views: alltimeViews, dailyAvgViews, impressions: m.video_impressions || 0, engagement: m.engagement_score || 0, playRate: m.play_rate || 0, secondsViewed: m.video_seconds_viewed || 0, }; }

// ---------------- Home Page ---------------- app.get('/', async (req, res) => { const qPrefill = (req.query.q || '').replace(/
/g, '\\
'); let recent = []; try { const token = await getAccessToken(); recent = await cmsRecentVideos(token, 20); } catch (e) { console.error('Recent videos error:', e.message); }

const recentCards = recent.map(v =>
    <a class="r-card" href="/search?q=${encodeURIComponent(v.id)}">       <div class="thumb-wrap">         <img src="${stripHtml(v.thumb)}" alt="${stripHtml(v.name)}">       </div>       <div class="r-meta">         <div class="r-title">${stripHtml(v.name)}</div>         <div class="r-sub">ID: ${v.id} • ${fmtDate(v.created_at)}</div>       </div>     </a>  
).join('');

res.send(`<!doctype html>

<html> <head> <meta charset="utf-8" /> <title>Brightcove Video Tools</title> <meta name="viewport" content="width=device-width, initial-scale=1" /> <link rel="preconnect" href="https://fonts.googleapis.com"> <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin> <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap" rel="stylesheet"> <style> body { font-family:'Open Sans',sans-serif; margin:0; background:#fff; color:#001f3f; } header { padding:20px; border-bottom:1px solid #e5e7eb; } header h1 { margin:0; font-size:1.8rem; font-weight:700; } main { max-width:1100px; margin:0 auto; padding:24px 16px; } .card { background:#f8f9fa; border:1px solid #e5e7eb; border-radius:12px; padding:24px; } input[type="text"] { width:100%; padding:12px; border:1px solid #ccc; border-radius:8px; } .btn { display:block; width:100%; padding:12px; background:#001f3f; color:#fff; border:none; border-radius:8px; margin-top:12px; font-weight:700; cursor:pointer; } .recent { margin-top:24px; } .recent-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; } @media(max-width:900px){ .recent-grid{grid-template-columns:repeat(2,1fr);} } @media(max-width:600px){ .recent-grid{grid-template-columns:1fr;} } .r-card { display:block; text-decoration:none; color:inherit; border:1px solid #e5e7eb; border-radius:10px; overflow:hidden; background:#fff; } .thumb-wrap { aspect-ratio:16/9; background:#eee; } .thumb-wrap img { width:100%; height:100%; object-fit:cover; display:block; } .r-meta { padding:10px; } .r-title { font-weight:700; font-size:14px; margin-bottom:4px; } .r-sub { font-size:12px; color:#6b7280; } </style> </head> <body> <header><h1>Brightcove Video Tools</h1></header> <main> <div class="card"> <h2>🔍 Search by ID, Tag(s), or Title</h2> <form action="/search" method="GET"> <input type="text" id="q" name="q" placeholder="Examples: 6376653485112, pega platform" required> <button class="btn" type="submit">Search & Watch</button> </form> <div class="recent"> <h3>🆕 20 Most Recent Uploads</h3> <div class="recent-grid"> ${recentCards || '<p>No recent videos found.</p>'} </div> </div> </div> </main> <script>(function(){var v=\`${qPrefill}\`; if(v) document.getElementById('q').value=v;})();</script> </body> </html>`); });
// ---------------- Search Results ---------------- app.get('/search', async (req, res) => { const qInput = (req.query.q || '').trim(); if (!qInput) return res.redirect('/'); try { const token = await getAccessToken(); const videos = await unifiedSearch(qInput, token); const playerId = process.env.BRIGHTCOVE_PLAYER_ID; const downloadUrl =
/download?q=${encodeURIComponent(qInput)}
; const cards = videos.map(v =>
       <div class="vcard">         <iframe src="https://players.brightcove.net/${AID}/${playerId}_default/index.html?videoId=${v.id}" allow="autoplay; encrypted-media" allowfullscreen></iframe>         <div class="meta">           <div class="title">${stripHtml(v.name)}</div>           <div class="id">ID: ${v.id} • ${fmtDate(v.created_at)}</div>         </div>       </div>
).join(''); res.send(`<!doctype html>

<html><head><meta charset="utf-8"><title>Results</title> <style> body{font-family:'Open Sans',sans-serif;margin:0;padding:20px;} .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;} .vcard iframe{width:100%;aspect-ratio:16/9;border:0;} .meta{padding:8px;} .title{font-weight:700;} .id{font-size:12px;color:#6b7280;} a{color:#001f3f;text-decoration:none;font-weight:700;} a:hover{text-decoration:underline;} </style></head><body> <a href="/">← Back</a> | <a href="${downloadUrl}">Download Spreadsheet</a> <div class="grid">${cards || '<p>No videos found.</p>'}</div> </body></html>`); } catch (err) { res.status(500).send('Error searching.'); } });
// ---------------- Download Spreadsheet ---------------- app.get('/download', async (req, res) => { const qInput = (req.query.q || '').trim(); if (!qInput) return res.status(400).send('Missing search terms'); try { const token = await getAccessToken(); const videos = await unifiedSearch(qInput, token); if (!videos.length) return res.status(404).send('No videos found.'); const wb = new ExcelJS.Workbook(); const ws = wb.addWorksheet('Video Metrics'); ws.columns = [ { header: 'Video ID', key: 'id', width: 20 }, { header: 'Title', key: 'title', width: 40 }, { header: 'All-Time Views', key: 'views', width: 18 }, { header: 'Daily Avg Views', key: 'dailyAvgViews', width: 18 }, { header: 'Impressions', key: 'impressions', width: 18 }, { header: 'Engagement', key: 'engagement', width: 18 }, { header: 'Play Rate', key: 'playRate', width: 12 }, { header: 'Seconds Viewed', key: 'secondsViewed', width: 18 }, { header: 'Tags', key: 'tags', width: 40 }, ]; for (const v of videos) { const row = await getMetricsForVideo(v.id, token); ws.addRow({ ...row, tags: (row.tags || []).join(', ') }); } res.setHeader('Content-Disposition', 'attachment; filename=video_metrics.xlsx'); res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'); await wb.xlsx.write(res); res.end(); } catch (err) { res.status(500).send('Error generating spreadsheet.'); } });

app.listen(PORT, () => console.log(
Server running at http://localhost:${PORT}