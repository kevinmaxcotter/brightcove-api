// ---- PLACEMENTS (per videoId: player + destination_domain/path) ----
async function getPlacementsForVideos(videoIds, token, { from = PLACEMENTS_WINDOW, to = 'now' } = {}) {
  // Returns Map<videoId, Array<{ player, domain, path, url, views }>>
  if (!Array.isArray(videoIds) || videoIds.length === 0) return new Map();

  const endpoint = 'https://analytics.api.brightcove.com/v1/data';
  const fields = ['video', 'player', 'destination_domain', 'destination_path', 'video_view'].join(',');
  const chunks = [];
  for (let i = 0; i < videoIds.length; i += 100) chunks.push(videoIds.slice(i, i + 100));

  const accum = new Map(); // Map<vid, Map<player|url key, {player, domain, path, url, views}>>

  for (const batch of chunks) {
    const params = new URLSearchParams({
      accounts: AID,
      dimensions: 'video,player,destination_domain,destination_path',
      fields,
      from,
      to,
      where: `video==${batch.join(',')}`
    });

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const { data } = await axiosInstance.get(`${endpoint}?${params.toString()}`, {
          headers: { Authorization: `Bearer ${token}` }
        });

        const items = (data && data.items) || [];
        for (const row of items) {
          const vid = String(row.video);
          const player = (row.player || '').trim();
          const domain = (row.destination_domain || '').trim();
          const path = (row.destination_path || '').trim();
          const url = domain ? `//${domain}${path.startsWith('/') ? path : (path ? '/' + path : '')}` : '(unknown)';
          const views = row.video_view || 0;

          if (!accum.has(vid)) accum.set(vid, new Map());
          const key = `${player}|${url}`;
          const cur = accum.get(vid).get(key) || { player, domain, path, url, views: 0 };
          cur.views += views;
          accum.get(vid).set(key, cur);
        }
        break;
      } catch (err) {
        // Log detailed info once, then rethrow on final attempt
        const s = err.response?.status;
        const body = err.response?.data;
        console.error('[placements] error', s, body || err.message);
        if (attempt < 2 && (s === 429 || (s >= 500 && s < 600))) {
          await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
          continue;
        }
        // If 400/403/etc., bubble up so caller can decide to fallback
        throw err;
      }
    }
  }

  const finalMap = new Map();
  for (const [vid, inner] of accum.entries()) {
    const rows = Array.from(inner.values()).sort((a, b) => b.views - a.views);
    finalMap.set(vid, rows);
  }
  return finalMap;
}

// ---- SPREADSHEET EXPORT (robust: falls back if placements fail; uses writeBuffer) ----
app.get('/download', async (req, res) => {
  const qInput = (req.query.q || '').trim();
  const debug = req.query.debug === '1';
  if (!qInput) return res.status(400).send('Missing search terms');

  try {
    const token = await getAccessToken();
    const videos = await unifiedSearch(qInput, token);
    if (!videos.length) return res.status(404).send('No videos found for that search.');

    const ids = videos.map(v => v.id);

    // Analytics (core metrics)
    let analytics = [];
    try {
      analytics = await getAnalyticsForVideos(ids, token);
    } catch (e) {
      console.error('[analytics] error', e.response?.status, e.response?.data || e.message);
      if (debug) return res.status(502).json({ step: 'analytics', status: e.response?.status, body: e.response?.data || e.message });
    }
    const aMap = new Map();
    for (const item of analytics) aMap.set(String(item.video), item);

    // Placements (may fail if account lacks destination_* dims or permissions)
    let placementsMap = new Map();
    let placementsFailed = false;
    try {
      placementsMap = await getPlacementsForVideos(ids, token, { from: PLACEMENTS_WINDOW, to: 'now' });
    } catch (e) {
      placementsFailed = true;
      console.error('[placements] giving up, proceeding without placements', e.response?.status, e.response?.data || e.message);
      // Continue; we’ll still deliver a metrics-only workbook.
      if (debug) return res.status(206).json({ step: 'placements', status: e.response?.status, body: e.response?.data || e.message });
    }

    // Build “top destinations” per video from placements (if available)
    const topDestByVideo = new Map();
    if (!placementsFailed) {
      for (const [vid, rows] of placementsMap.entries()) {
        const byUrl = new Map();
        for (const r of rows) {
          const cur = byUrl.get(r.url) || 0;
          byUrl.set(r.url, cur + (r.views || 0));
        }
        const top = Array.from(byUrl.entries())
          .map(([url, views]) => ({ url, views }))
          .sort((a, b) => b.views - a.views)
          .slice(0, 5);
        topDestByVideo.set(String(vid), top);
      }
    }

    // Create workbook
    const wb = new ExcelJS.Workbook();

    // Sheet 1: Summary metrics
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
      { header: placementsFailed
          ? 'Top Destinations (unavailable)'
          : `Top Destinations (${PLACEMENTS_WINDOW} · URL · views)`,
        key: 'destinations', width: 70
      },
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

      const topDest = placementsFailed ? [] : (topDestByVideo.get(String(v.id)) || []);
      const destinationsCell = topDest.length
        ? topDest.map(d => `${d.url} · ${d.views}`).join('; ')
        : (placementsFailed ? '— (placements unavailable)' : '—');

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
        destinations: destinationsCell
      });
    }

    // Sheet 2: Per-video placements (only if we got them)
    if (!placementsFailed) {
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
      const wx = wb.addWorksheet('Placements by Video');
      wx.addRow(['Placements unavailable', 'Your account may not have access to destination_* dimensions or the request failed.']);
    }

    // Send as buffer (more reliable than streaming in some environments)
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
