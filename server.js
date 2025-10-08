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
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '1h' }));

const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;

// --- Helpers ---
const looksLikeId = (s) => /^\d{9,}$/.test(String(s).trim());
const splitTerms = (input) =>
  String(input || '')
    .split(',')
    .map((s) => s.trim())
    .map((s) => s.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1'))
    .filter(Boolean);

const escapeForCmsQuery = (s) => String(s).replace(/([\"\\])/g, '\\$1');
const escapeHtml = (s = '') =>
  String(s).replace(/[&<>"']/g, (m) =>
    ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
    }[m])
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

// --- Auth (token cache) ---
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
    }
  );
  const token = r.data.access_token;
  const ttl = Math.max(60, (r.data.expires_in || 300) - 30); // 30s safety buffer
  tokenCache = { token, expiresAt: now + ttl * 1000 };
  return token;
}

// Axios instance (sane defaults)
const http = axios.create({ timeout: 20000 });

// --- CMS Helpers ---
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

// --- Unified Search ---
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

// --- Analytics ---
async function getMetricsForVideo(videoId, token) {
  const alltimeViewsUrl = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;
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

// --- Concurrency helper ---
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
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '1h' }));

const AID = process.env.BRIGHTCOVE_ACCOUNT_ID;

// --- Helpers ---
const looksLikeId = (s) => /^\d{9,}$/.test(String(s).trim());
const splitTerms = (input) =>
  String(input || '')
    .split(',')
    .map((s) => s.trim())
    .map((s) => s.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1'))
    .filter(Boolean);

const escapeForCmsQuery = (s) => String(s).replace(/([\"\\])/g, '\\$1');
const escapeHtml = (s = '') =>
  String(s).replace(/[&<>"']/g, (m) =>
    ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
    }[m])
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

// --- Auth (token cache) ---
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
    }
  );
  const token = r.data.access_token;
  const ttl = Math.max(60, (r.data.expires_in || 300) - 30); // 30s safety buffer
  tokenCache = { token, expiresAt: now + ttl * 1000 };
  return token;
}

// Axios instance (sane defaults)
const http = axios.create({ timeout: 20000 });

// --- CMS Helpers ---
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

// --- Unified Search ---
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

// --- Analytics ---
async function getMetricsForVideo(videoId, token) {
  const alltimeViewsUrl = `https://analytics.api.brightcove.com/v1/alltime/accounts/${AID}/videos/${videoId}`;
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

// --- Concurrency helper ---
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