"use strict";
const { createSharedLimiter } = require('./request-protection');
const TABLES = new Set(['user_flights','profiles','user_settings','ticket_souvenirs','tracking_sessions','live_snapshots','notifications','friend_relationships','friend_invites']);
const RPCS = new Set(['runwy_remove_circle_member','runwy_set_circle_flights','runwy_update_circle_notifications','runwy_import_flighty_batch']);
function mountDataGateway(app, { query, supabaseURL, anonKey, secret, fetchImpl = (...args) => fetch(...args) }) {
  const userKey = req => req.auth.userId;
  const limit = (namespace, max) => createSharedLimiter({ query, namespace, limit: max, keyGenerator: userKey });
  app.use('/v1/data/rest/v1', limit('data-read-attempt', 300), (req,res,next) => {
    if (['GET','HEAD'].includes(req.method)) return next();
    return writeLimit(req,res,next);
  });
  const writeLimit = limit('data-write-attempt', 120);
  app.use('/v1/data/rest/v1', async (req, res) => {
    if (!query || !supabaseURL || !anonKey || !secret || secret.length < 32) return res.status(503).json({ error: 'Data gateway unavailable' });
    const path = req.path;
    const table = /^\/([a-z_]+)$/.exec(path)?.[1];
    const rpc = /^\/rpc\/([a-z_]+)$/.exec(path)?.[1];
    if (!TABLES.has(table) && !(req.method === 'POST' && RPCS.has(rpc))) return res.status(404).json({ error: 'Unknown data endpoint' });
    if (!['GET','HEAD','POST','PATCH','DELETE'].includes(req.method)) return res.sendStatus(405);
    const target = new URL('/rest/v1'+path, supabaseURL);
    const search = new URLSearchParams(req.url.split('?')[1] || '');
    if (['GET','HEAD'].includes(req.method)) {
      const max = Number(search.get('limit') || 500);
      if (!Number.isInteger(max) || max < 1 || max > 500) return res.status(400).json({ error: 'Page size must be 1–500' });
      search.set('limit', String(max));
    }
    target.search = search.toString();
    // Never forward arbitrary headers, service credentials, schemas or redirects.
    const headers = { authorization: req.get('authorization'), apikey: anonKey,
      'content-type': 'application/json', accept: 'application/json', 'x-runwy-data-gateway': secret };
    const preference = String(req.get('prefer') || '').split(',').filter(p => /^(resolution=(merge|ignore)-duplicates|return=(minimal|representation))$/.test(p.trim()));
    if (preference.length) headers.prefer = preference.join(',');
    const range = req.get('range');
    if (range) {
      const m = /^(\d+)-(\d+)$/.exec(range);
      if (!m || Number(m[2]) < Number(m[1]) || Number(m[2])-Number(m[1]) >= 500) return res.status(400).json({ error: 'Invalid page range' });
      headers.range = range;
    }
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 10000);
    const disconnected = () => { if (!res.writableEnded) controller.abort(); };
    res.on('close', disconnected);
    try {
      const prior = await query('select public.runwy_charge_data_bytes($1,0) as allowed', [req.auth.userId]);
      if (!prior.rows[0]?.allowed) { res.set('Retry-After','60'); return res.status(429).json({ error: 'Download allowance reached' }); }
      const response = await fetchImpl(target, { method: req.method, headers, redirect: 'error', signal: controller.signal,
        ...(['GET','HEAD'].includes(req.method) ? {} : { body: JSON.stringify(req.body || {}) }) });
      const reader = response.body?.getReader(), chunks = []; let bytes = 0;
      if (reader) {
        try { while (true) { const {done,value} = await reader.read(); if (done) break;
          bytes += value.length;
          if (bytes > 8*1024*1024) { await reader.cancel(); await query('select public.runwy_charge_data_bytes($1,$2)', [req.auth.userId,8*1024*1024]); return res.status(413).json({ error: 'Response too large; request a smaller page' }); }
          chunks.push(Buffer.from(value));
        } } finally { reader.releaseLock(); }
      }
      // This transaction is separate from PostgREST, including failed requests.
      const {rows} = await query('select public.runwy_charge_data_bytes($1,$2) as allowed', [req.auth.userId, bytes]);
      if (!rows[0]?.allowed) { res.set('Retry-After','60'); return res.status(429).json({ error: 'Download allowance reached' }); }
      res.set('Cache-Control','no-store');
      for (const key of ['content-range','range-unit','preference-applied']) if (response.headers.has(key)) res.set(key,response.headers.get(key));
      return res.status(response.status).type('application/json').send(Buffer.concat(chunks));
    } catch { return res.status(503).json({ error: 'Data service unavailable; retry later' }); }
    finally { clearTimeout(timer); res.off('close', disconnected); }
  });
}
module.exports = { mountDataGateway };
