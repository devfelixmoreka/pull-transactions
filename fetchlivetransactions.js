'use strict';

const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const CONFIG_PATH = path.join(__dirname, 'config.json');

function nowText() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function makeId(obj) {
  const candidates = [
    obj?.TransID,
    obj?.TransactionID,
    obj?.ReceiptNumber,
    obj?.MpesaReceiptNumber,
    obj?.transactionId,
    obj?.reference,
    obj?.billreference
  ].filter(Boolean);

  if (candidates.length) return String(candidates[0]);
  return crypto.createHash('sha1').update(JSON.stringify(obj)).digest('hex');
}

function collectTxLikeObjects(input, out = []) {
  if (!input) return out;

  if (Array.isArray(input)) {
    for (const item of input) collectTxLikeObjects(item, out);
    return out;
  }

  if (typeof input !== 'object') return out;

  const keys = Object.keys(input);
  const looksLikeTx =
    keys.includes('transactionId') ||
    keys.includes('TransID') ||
    keys.includes('TransactionID') ||
    keys.includes('ReceiptNumber') ||
    keys.includes('MpesaReceiptNumber') ||
    keys.includes('billreference') ||
    keys.includes('BillRefNumber') ||
    keys.includes('msisdn') ||
    keys.includes('MSISDN') ||
    keys.includes('amount') ||
    keys.includes('TransAmount');

  if (looksLikeTx) out.push(input);

  for (const value of Object.values(input)) {
    if (value && typeof value === 'object') collectTxLikeObjects(value, out);
  }

  return out;
}

function formatNairobi(date) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Africa/Nairobi',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).formatToParts(date);

  const m = {};
  for (const p of parts) {
    if (p.type !== 'literal') m[p.type] = p.value;
  }

  return `${m.year}-${m.month}-${m.day} ${m.hour}:${m.minute}:${m.second}`;
}

async function loadConfig() {
  const raw = await fs.readFile(CONFIG_PATH, 'utf8');
  const cfg = JSON.parse(raw);

  for (const key of ['consumerKey', 'consumerSecret', 'shortCode', 'oauthUrl', 'queryUrl']) {
    if (!cfg[key]) throw new Error(`Missing config field: ${key}`);
  }

  cfg.pollEveryMs = Number(cfg.pollEveryMs || 15000);
  cfg.windowMinutes = Number(cfg.windowMinutes || 5);
  cfg.storageFile = cfg.storageFile || './live-transactions.json';

  return cfg;
}

async function ensureStorage(filePath) {
  try {
    await fs.access(filePath);
  } catch {
    await fs.writeFile(filePath, JSON.stringify({ records: [] }, null, 2), 'utf8');
  }
}

async function readStorage(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    const data = JSON.parse(raw);
    if (!data.records) data.records = [];
    return data;
  } catch {
    return { records: [] };
  }
}

async function writeStorage(filePath, data) {
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
}

async function getAccessToken(cfg) {
  const basic = Buffer.from(`${cfg.consumerKey}:${cfg.consumerSecret}`).toString('base64');

  const res = await fetch(cfg.oauthUrl, {
    method: 'GET',
    headers: {
      Authorization: `Basic ${basic}`
    }
  });

  const text = await res.text();
  const data = safeJsonParse(text);

  if (!res.ok) {
    throw new Error(`OAuth failed (${res.status}): ${text}`);
  }

  if (!data.access_token) {
    throw new Error(`OAuth response missing access_token: ${text}`);
  }

  return data.access_token;
}

async function postJson(url, token, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  const data = safeJsonParse(text);

  if (!res.ok) {
    throw new Error(`Query failed (${res.status}): ${text}`);
  }

  return data;
}

async function main() {
  const cfg = await loadConfig();
  const storagePath = path.resolve(__dirname, cfg.storageFile);

  await ensureStorage(storagePath);

  const seen = new Set();
  const storage = await readStorage(storagePath);
  for (const record of storage.records) {
    for (const tx of collectTxLikeObjects(record.payload)) {
      seen.add(makeId(tx));
    }
  }

  console.log(`[${nowText()}] Starting live pull poller for shortcode ${cfg.shortCode}`);
  console.log(`[${nowText()}] Poll interval: ${cfg.pollEveryMs} ms`);
  console.log(`[${nowText()}] Query window: last ${cfg.windowMinutes} minute(s)`);

  let token = await getAccessToken(cfg);
  let tokenIssuedAt = Date.now();

  async function ensureToken() {
    if (!token || Date.now() - tokenIssuedAt > 50 * 60 * 1000) {
      token = await getAccessToken(cfg);
      tokenIssuedAt = Date.now();
    }
    return token;
  }

  while (true) {
    try {
      const accessToken = await ensureToken();

      const end = new Date();
      const start = new Date(end.getTime() - cfg.windowMinutes * 60 * 1000);

      const body = {
        ShortCode: String(cfg.shortCode),
        StartDate: formatNairobi(start),
        EndDate: formatNairobi(end),
        OffSetValue: 0
      };

      const response = await postJson(cfg.queryUrl, accessToken, body);

      console.log(`\n[${nowText()}] QUERY RESPONSE`);
      console.log(JSON.stringify(response, null, 2));

      const txs = collectTxLikeObjects(response);
      let newCount = 0;

      for (const tx of txs) {
        const id = makeId(tx);
        if (seen.has(id)) continue;
        seen.add(id);
        newCount += 1;

        console.log(`\n[${nowText()}] NEW TRANSACTION ${id}`);
        console.log(JSON.stringify(tx, null, 2));
      }

      const store = await readStorage(storagePath);
      store.records.push({
        at: nowText(),
        source: 'pull_query',
        request: body,
        payload: response
      });
      await writeStorage(storagePath, store);

      if (newCount === 0) {
        console.log(`[${nowText()}] No new transactions in this window.`);
      }
    } catch (err) {
      console.error(`[${nowText()}] ${err.message}`);
    }

    await sleep(cfg.pollEveryMs);
  }
}

main().catch((err) => {
  console.error(`[${nowText()}] Fatal: ${err.message}`);
  process.exit(1);
});