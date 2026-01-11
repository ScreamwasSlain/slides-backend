const dotenv = require('dotenv');

dotenv.config();

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const express = require('express');
const cors = require('cors');
const axios = require('axios');

const { Server } = require('socket.io');

const SPEED_API_BASE = 'https://api.tryspeed.com';
const SPEED_WALLET_API_BASE = 'https://api.tryspeed.com';

const SPEED_WALLET_SECRET_KEY = process.env.SPEED_WALLET_SECRET_KEY;
const SPEED_WALLET_PUBLISHABLE_KEY = process.env.SPEED_WALLET_PUBLISHABLE_KEY;
const SPEED_WALLET_WEBHOOK_SECRET = process.env.SPEED_WALLET_WEBHOOK_SECRET;
const SPEED_INVOICE_AUTH_MODE = (process.env.SPEED_INVOICE_AUTH_MODE || 'auto').toLowerCase();

const AUTH_HEADER = SPEED_WALLET_SECRET_KEY
  ? Buffer.from(`${SPEED_WALLET_SECRET_KEY}:`).toString('base64')
  : null;

const PUB_AUTH_HEADER = SPEED_WALLET_PUBLISHABLE_KEY
  ? Buffer.from(`${SPEED_WALLET_PUBLISHABLE_KEY}:`).toString('base64')
  : null;

const BET_OPTIONS = [20, 100, 300, 500, 1000, 5000, 10000];
const TOPUP_OPTIONS = [1000, 5000, 10000];

const walletsById = new Map();
const processedInvoices = new Map();

const WALLET_STORE_PATH = process.env.WALLET_STORE_PATH
  ? String(process.env.WALLET_STORE_PATH)
  : path.join(__dirname, 'wallet_store.json');

let walletStoreSaveTimer = null;

const LIABILITIES_REPORT_PATH = process.env.LIABILITIES_REPORT_PATH
  ? String(process.env.LIABILITIES_REPORT_PATH)
  : path.join(__dirname, 'wallet_liabilities.csv');

let liabilitiesReportTimer = null;

function serializeWallet(w) {
  return {
    walletId: w.walletId,
    balanceSats: Number(w.balanceSats) || 0,
    holdSats: Number(w.holdSats) || 0,
    lightningAddress: w.lightningAddress || null,
    createdAt: w.createdAt || null,
    updatedAt: w.updatedAt || null,
    lastActivityAt: w.lastActivityAt || null,
    boundAt: w.boundAt || null,
    secretSetAt: w.secretSetAt || null,
    pendingWithdrawal: w.pendingWithdrawal || null,
    lastWithdrawal: w.lastWithdrawal || null,
    walletSecretHash: Buffer.isBuffer(w.walletSecretHash) ? w.walletSecretHash.toString('hex') : (w.walletSecretHash || null)
  };
}

function writeLiabilitiesReport() {
  try {
    const rows = [];
    rows.push('walletId,lightningAddress,balanceSats,holdSats,totalSats,lastActivityAt,updatedAt,pendingWithdrawalId,pendingReason');

    const all = Array.from(walletsById.values())
      .map((w) => {
        const balance = Math.max(0, Number(w?.balanceSats) || 0);
        const hold = Math.max(0, Number(w?.holdSats) || 0);
        return {
          walletId: String(w?.walletId || ''),
          lightningAddress: String(w?.lightningAddress || ''),
          balanceSats: balance,
          holdSats: hold,
          totalSats: balance + hold,
          lastActivityAt: String(w?.lastActivityAt || ''),
          updatedAt: String(w?.updatedAt || ''),
          pendingWithdrawalId: String(w?.pendingWithdrawal?.withdrawalId || ''),
          pendingReason: String(w?.pendingWithdrawal?.reason || '')
        };
      })
      .filter((r) => r.totalSats > 0);

    all.sort((a, b) => b.totalSats - a.totalSats);

    for (const r of all) {
      const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
      rows.push([
        esc(r.walletId),
        esc(r.lightningAddress),
        r.balanceSats,
        r.holdSats,
        r.totalSats,
        esc(r.lastActivityAt),
        esc(r.updatedAt),
        esc(r.pendingWithdrawalId),
        esc(r.pendingReason)
      ].join(','));
    }

    const payload = `${rows.join('\n')}\n`;
    const tmp = `${LIABILITIES_REPORT_PATH}.tmp`;
    fs.writeFileSync(tmp, payload, 'utf8');
    fs.renameSync(tmp, LIABILITIES_REPORT_PATH);
  } catch (e) {
    console.warn(`Failed to write liabilities report: ${String(e.message || e)}`);
  }
}

function scheduleLiabilitiesReportWrite() {
  if (!LIABILITIES_REPORT_PATH) return;
  if (liabilitiesReportTimer) clearTimeout(liabilitiesReportTimer);
  liabilitiesReportTimer = setTimeout(() => {
    liabilitiesReportTimer = null;
    writeLiabilitiesReport();
  }, 800);
}

function loadWalletStore() {
  try {
    if (!WALLET_STORE_PATH) return;
    if (!fs.existsSync(WALLET_STORE_PATH)) return;
    const raw = fs.readFileSync(WALLET_STORE_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const arr = Array.isArray(parsed?.wallets) ? parsed.wallets : [];
    const processed = Array.isArray(parsed?.processedInvoices) ? parsed.processedInvoices : [];
    for (const item of arr) {
      const id = String(item?.walletId || '').trim();
      if (!id) continue;
      walletsById.set(id, {
        walletId: id,
        balanceSats: Math.max(0, Math.floor(Number(item?.balanceSats) || 0)),
        holdSats: Math.max(0, Math.floor(Number(item?.holdSats) || 0)),
        lightningAddress: item?.lightningAddress ? String(item.lightningAddress) : null,
        createdAt: item?.createdAt || null,
        updatedAt: item?.updatedAt || null,
        lastActivityAt: item?.lastActivityAt || null,
        boundAt: item?.boundAt || null,
        secretSetAt: item?.secretSetAt || null,
        pendingWithdrawal: item?.pendingWithdrawal || null,
        lastWithdrawal: item?.lastWithdrawal || null,
        walletSecretHash: item?.walletSecretHash || null
      });
    }

    for (const p of processed) {
      const invoiceId = String(p?.invoiceId || '').trim();
      if (!invoiceId) continue;
      processedInvoices.set(invoiceId, {
        purpose: String(p?.purpose || ''),
        walletId: p?.walletId ? String(p.walletId) : null,
        amountSats: Number(p?.amountSats) || 0,
        processedAt: p?.processedAt || null
      });
    }

    scheduleLiabilitiesReportWrite();
  } catch (e) {
    console.warn(`Failed to load wallet store: ${String(e.message || e)}`);
  }
}

function saveWalletStore() {
  try {
    if (!WALLET_STORE_PATH) return;
    const wallets = Array.from(walletsById.values()).map(serializeWallet);
    const processed = Array.from(processedInvoices.entries()).map(([invoiceId, rec]) => ({
      invoiceId,
      purpose: rec?.purpose || null,
      walletId: rec?.walletId || null,
      amountSats: Number(rec?.amountSats) || 0,
      processedAt: rec?.processedAt || null
    }));
    const payload = JSON.stringify({ wallets, processedInvoices: processed }, null, 2);
    const tmp = `${WALLET_STORE_PATH}.tmp`;
    fs.writeFileSync(tmp, payload, 'utf8');
    fs.renameSync(tmp, WALLET_STORE_PATH);
  } catch (e) {
    console.warn(`Failed to save wallet store: ${String(e.message || e)}`);
  }
}

function scheduleWalletStoreSave() {
  if (!WALLET_STORE_PATH) return;
  if (walletStoreSaveTimer) clearTimeout(walletStoreSaveTimer);
  walletStoreSaveTimer = setTimeout(() => {
    walletStoreSaveTimer = null;
    saveWalletStore();
  }, 600);
}

loadWalletStore();

const PAYOUT_TABLE = {
  20: [0, 20, 50, 80, 100],
  100: [0, 50, 120, 200, 300],
  300: [0, 100, 350, 500, 700],
  500: [0, 200, 400, 800, 1200],
  1000: [0, 300, 1000, 1500, 3000],
  5000: [0, 1000, 3000, 5000, 11000],
  10000: [0, 2000, 5000, 12000, 30000]
};

const PAYOUT_WEIGHTS = {
  20: [36, 50, 9, 4, 1],
  100: [36, 50, 9, 4, 1],
  300: [36, 50, 9, 4, 1],
  500: [36, 50, 9, 4, 1],
  1000: [36, 50, 9, 4, 1],
  5000: [36, 50, 9, 4, 1],
  10000: [2500, 2500, 2500, 249, 1]
};

function pickWeighted(options, weights) {
  const opts = Array.isArray(options) ? options : [];
  if (opts.length === 0) return { value: 0, weights: [] };

  const w = Array.isArray(weights) && weights.length === opts.length
    ? weights.map((n) => Math.max(0, Number(n) || 0))
    : opts.map(() => 1);

  const total = w.reduce((a, b) => a + b, 0);
  if (!Number.isFinite(total) || total <= 0) {
    const idx = crypto.randomInt(0, opts.length);
    return { value: opts[idx], weights: opts.map(() => 1) };
  }

  const r = (crypto.randomInt(0, 2 ** 32) / (2 ** 32)) * total;
  let acc = 0;
  for (let i = 0; i < opts.length; i += 1) {
    acc += w[i];
    if (r < acc) return { value: opts[i], weights: w };
  }
  return { value: opts[opts.length - 1], weights: w };
}

function getCorsOrigins() {
  const raw = (process.env.CORS_ORIGIN || '').trim();
  if (!raw) return true;
  if (raw === '*') return true;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

function formatLightningAddress(input) {
  const s = String(input || '').trim().toLowerCase();
  if (!s) throw new Error('Lightning address is required');
  if (s.includes('@')) return s;
  return `${s}@speed.app`;
}

function formatWalletId(input) {
  const s = String(input || '').trim();
  if (!s) throw new Error('Wallet ID is required');
  if (s.length < 32) throw new Error('Invalid wallet ID');
  return s;
}

function formatWalletSecret(input) {
  const s = String(input || '').trim();
  if (!s) throw new Error('Wallet secret is required');
  if (s.length < 32) throw new Error('Invalid wallet secret');
  return s;
}

function hashWalletSecret(secret) {
  return crypto.createHash('sha256').update(String(secret || '')).digest();
}

function ensureWalletAuth(walletId, walletSecret) {
  const w = getWallet(walletId);
  const s = formatWalletSecret(walletSecret);
  const h = hashWalletSecret(s);

  if (!w.walletSecretHash) {
    w.walletSecretHash = h;
    w.secretSetAt = new Date().toISOString();
    scheduleWalletStoreSave();
    scheduleLiabilitiesReportWrite();
    return w;
  }

  const existing = Buffer.isBuffer(w.walletSecretHash)
    ? w.walletSecretHash
    : Buffer.from(String(w.walletSecretHash), 'hex');

  if (existing.length !== h.length || !crypto.timingSafeEqual(existing, h)) {
    throw new Error('Invalid wallet secret');
  }

  return w;
}

function getWallet(walletId) {
  const id = formatWalletId(walletId);
  const w = walletsById.get(id);
  if (w && typeof w === 'object') return w;
  const now = new Date().toISOString();
  const created = {
    walletId: id,
    balanceSats: 0,
    holdSats: 0,
    lightningAddress: null,
    pendingWithdrawal: null,
    lastWithdrawal: null,
    lastActivityAt: now,
    createdAt: now
  };
  walletsById.set(id, created);
  return created;
}

function bindWalletAddress(walletId, lightningAddress) {
  const w = getWallet(walletId);
  const addr = formatLightningAddress(lightningAddress);
  if (!w.lightningAddress) {
    w.lightningAddress = addr;
    w.boundAt = new Date().toISOString();
    scheduleWalletStoreSave();
    scheduleLiabilitiesReportWrite();
    return w;
  }
  if (w.lightningAddress !== addr) {
    throw new Error('This wallet is bound to a different lightning address');
  }
  return w;
}

function noteWalletActivity(walletId) {
  const w = getWallet(walletId);
  w.lastActivityAt = new Date().toISOString();
  w.updatedAt = w.lastActivityAt;
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  return w.lastActivityAt;
}

function setWalletHold(walletId, holdSats) {
  const w = getWallet(walletId);
  const next = Math.max(0, Math.floor(Number(holdSats) || 0));
  w.holdSats = next;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  return next;
}

function getWalletBalance(walletId) {
  const w = getWallet(walletId);
  return Math.max(0, Number(w.balanceSats) || 0);
}

function setWalletBalance(walletId, balanceSats) {
  const w = getWallet(walletId);
  const next = Math.max(0, Math.floor(Number(balanceSats) || 0));
  w.balanceSats = next;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  return next;
}

function pickPayoutAmount(betAmount) {
  const opts = PAYOUT_TABLE[betAmount] || [0, betAmount];
  const weights = PAYOUT_WEIGHTS?.[betAmount] || null;
  const picked = pickWeighted(opts, weights);
  return { payoutAmount: picked.value, payoutOptions: opts, payoutWeights: picked.weights };
}

async function createLightningInvoice(amountSats, orderId, extraMetadata = {}) {
  if (!amountSats || Number.isNaN(Number(amountSats))) {
    throw new Error('Invalid amount');
  }
  const mode = (SPEED_INVOICE_AUTH_MODE || 'auto').toLowerCase();
  const tryPublishable = mode !== 'secret';
  const trySecret = mode !== 'publishable';

  const payload = {
    currency: 'SATS',
    amount: Number(amountSats),
    target_currency: 'SATS',
    ttl: 600,
    description: `BTC Slides - ${Number(amountSats)} SATS`,
    metadata: {
      Order_ID: orderId,
      Game_Type: 'BTC_Slides',
      Amount_SATS: String(amountSats),
      ...extraMetadata
    }
  };

  async function attemptCreate(header, label, extraHeaders = {}) {
    const resp = await axios.post(`${SPEED_API_BASE}/payments`, payload, {
      headers: {
        Authorization: `Basic ${header}`,
        'Content-Type': 'application/json',
        ...extraHeaders
      },
      timeout: 10000
    });

    const data = resp.data;
    const invoiceId = data.id;
    const hostedInvoiceUrl = data.hosted_invoice_url;

    let lightningInvoice =
      data.payment_method_options?.lightning?.payment_request ||
      data.lightning_invoice ||
      data.invoice ||
      data.payment_request ||
      data.bolt11 ||
      null;

    const isBolt11 = typeof lightningInvoice === 'string' && lightningInvoice.toLowerCase().startsWith('ln');
    if (!isBolt11) lightningInvoice = null;

    if (!lightningInvoice && invoiceId) {
      try {
        const details = await axios.get(`${SPEED_API_BASE}/payments/${invoiceId}`, {
          headers: {
            Authorization: `Basic ${header}`,
            'Content-Type': 'application/json',
            'speed-version': '2022-04-15',
            ...extraHeaders
          },
          timeout: 10000
        });

        const d = details.data;
        const maybe =
          d?.payment_method_options?.lightning?.payment_request ||
          d?.lightning_invoice ||
          d?.invoice ||
          d?.payment_request ||
          d?.bolt11 ||
          null;

        const ok = typeof maybe === 'string' && maybe.toLowerCase().startsWith('ln');
        if (ok) lightningInvoice = maybe;
      } catch {
      }
    }

    if (!invoiceId) throw new Error(`[${label}] No invoice ID returned from Speed API`);

    return {
      invoiceId,
      hostedInvoiceUrl,
      lightningInvoice,
      speedInterfaceUrl: hostedInvoiceUrl,
      amountSats: Number(amountSats)
    };
  }

  if (tryPublishable && PUB_AUTH_HEADER) {
    try {
      return await attemptCreate(PUB_AUTH_HEADER, 'publishable');
    } catch (error) {
      const status = error.response?.status;
      const msg = error.response?.data?.errors?.[0]?.message || error.message;
      const shouldFallback = trySecret && [401, 403, 422].includes(Number(status));
      if (!shouldFallback) {
        throw new Error(`Failed to create invoice (publishable): ${msg} (Status: ${status || 'n/a'})`);
      }
    }
  }

  if (trySecret) {
    if (!AUTH_HEADER) throw new Error('Missing SPEED_WALLET_SECRET_KEY');
    try {
      return await attemptCreate(AUTH_HEADER, 'secret', { 'speed-version': '2022-04-15' });
    } catch (error) {
      const status = error.response?.status;
      const msg = error.response?.data?.errors?.[0]?.message || error.message;
      throw new Error(`Failed to create invoice (secret): ${msg} (Status: ${status || 'n/a'})`);
    }
  }

  throw new Error('No valid invoice auth mode available. Set SPEED_INVOICE_AUTH_MODE to publishable|secret|auto.');
}

async function sendInstantPayment(withdrawRequest, amountSats, note = '') {
  if (!AUTH_HEADER) throw new Error('Missing SPEED_WALLET_SECRET_KEY');

  const payload = {
    amount: Number(amountSats),
    currency: 'SATS',
    target_currency: 'SATS',
    withdraw_method: 'lightning',
    withdraw_request: withdrawRequest,
    note
  };

  const resp = await axios.post(`${SPEED_WALLET_API_BASE}/send`, payload, {
    headers: {
      Authorization: `Basic ${AUTH_HEADER}`,
      'Content-Type': 'application/json',
      'speed-version': '2022-04-15'
    },
    timeout: 10000
  });

  return resp.data;
}

const app = express();
app.set('trust proxy', 1);

const corsOrigins = getCorsOrigins();
app.use(
  cors({
    origin: corsOrigins,
    credentials: true
  })
);

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

const invoiceToSocket = new Map();
const roundsByInvoice = new Map();
const walletToSocket = new Map();

const AUTO_REFUND_IDLE_MS = Math.max(60 * 1000, Number(process.env.AUTO_REFUND_IDLE_MS) || 30 * 60 * 1000);
const AUTO_REFUND_CHECK_MS = Math.max(10 * 1000, Number(process.env.AUTO_REFUND_CHECK_MS) || 60 * 1000);
const AUTO_REFUND_COOLDOWN_MS = Math.max(10 * 1000, Number(process.env.AUTO_REFUND_COOLDOWN_MS) || 5 * 60 * 1000);

function extractInvoiceIdFromEvent(event) {
  const candidates = [
    event?.data?.object?.id,
    event?.data?.id,
    event?.data?.object?.payment?.id,
    event?.data?.object?.invoice?.id,
    event?.data?.object?.payment_id,
    event?.data?.object?.invoice_id
  ];
  const found = candidates.find((v) => typeof v === 'string' && v.trim());
  return found || null;
}

function normalizeSpeedStatus(status) {
  return String(status || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_');
}

function isPaidLikeStatus(status) {
  const s = normalizeSpeedStatus(status);
  if (!s) return false;

  const tokens = s.split(/[._-]+/g).filter(Boolean);
  if (tokens.includes('unpaid') || tokens.includes('not_paid') || tokens.includes('not') && tokens.includes('paid')) {
    return false;
  }

  const paidTokens = new Set(['paid', 'confirmed', 'succeeded', 'success', 'complete', 'completed']);
  return tokens.some((t) => paidTokens.has(t));
}

async function fetchPaymentDetails(invoiceId) {
  const header = AUTH_HEADER || PUB_AUTH_HEADER;
  if (!header) throw new Error('Missing Speed auth header (set SPEED_WALLET_SECRET_KEY or SPEED_WALLET_PUBLISHABLE_KEY)');

  const headers = {
    Authorization: `Basic ${header}`,
    'Content-Type': 'application/json'
  };
  if (AUTH_HEADER) headers['speed-version'] = '2022-04-15';

  const details = await axios.get(`${SPEED_API_BASE}/payments/${invoiceId}`, {
    headers,
    timeout: 10000
  });

  return details.data;
}

async function verifyInvoicePaidWithSpeed(invoiceId) {
  const details = await fetchPaymentDetails(invoiceId);
  const status = details?.status || details?.payment_status || details?.state || null;
  const paidFlag = details?.paid === true || details?.is_paid === true || details?.paid_at != null;
  const paid = Boolean(paidFlag) || isPaidLikeStatus(status);
  return { paid, status, details };
}

function extractRoundFromPaymentDetails(invoiceId, details, socketId) {
  const md = details?.metadata || {};
  const walletIdRaw = String(md.Wallet_ID || md.wallet_id || md.walletId || '').trim();
  const addr = String(md.Lightning_Address || md.lightning_address || md.lightningAddress || '').trim().toLowerCase();
  const purposeRaw = String(md.Purpose || md.purpose || md.Type || md.type || '').trim().toLowerCase();
  const purpose = purposeRaw || 'spin';
  const amount = Number(md.Amount_SATS || md.amount_sats || details?.amount || details?.amount_sats);

  if (!walletIdRaw) return null;

  if (!addr || !addr.includes('@')) return null;
  if (!Number.isFinite(amount) || amount <= 0) return null;

  if (purpose === 'topup') {
    if (!TOPUP_OPTIONS.includes(amount)) return null;

    return {
      roundId: String(md.Order_ID || `recovered_${invoiceId}`),
      socketId: socketId || null,
      invoiceId,
      walletId: formatWalletId(walletIdRaw),
      lightningAddress: formatLightningAddress(addr),
      topupAmount: amount,
      purpose: 'topup',
      status: 'invoice_created',
      createdAt: new Date().toISOString(),
      recovered: true
    };
  }

  if (!BET_OPTIONS.includes(amount)) return null;

  return {
    roundId: String(md.Order_ID || `recovered_${invoiceId}`),
    socketId: socketId || null,
    invoiceId,
    walletId: formatWalletId(walletIdRaw),
    lightningAddress: formatLightningAddress(addr),
    betAmount: amount,
    purpose: 'spin',
    status: 'invoice_created',
    createdAt: new Date().toISOString(),
    recovered: true
  };
}

function scheduleRoundCleanup(invoiceId, delayMs = 30 * 60 * 1000) {
  setTimeout(() => {
    roundsByInvoice.delete(invoiceId);
    invoiceToSocket.delete(invoiceId);
  }, delayMs);
}

async function processPaidInvoice(invoiceId, opts = {}) {
  const round = roundsByInvoice.get(invoiceId);
  if (!round) {
    return { ok: false, reason: 'unknown_invoice' };
  }

  if (!opts?.paidVerified) {
    const { paid, status } = await verifyInvoicePaidWithSpeed(invoiceId);
    if (!paid) {
      return { ok: false, reason: 'not_paid', status: status || 'unknown' };
    }
  }

  const socketId = opts?.socketId || invoiceToSocket.get(invoiceId) || round.socketId;
  if (opts?.socketId) {
    invoiceToSocket.set(invoiceId, opts.socketId);
    round.socketId = opts.socketId;
  }

  const sock = socketId && io.sockets.sockets.get(socketId);

  if (round.status === 'invoice_created') {
    round.status = 'paid';
  }

  if (sock && !round.paymentVerifiedEmitted) {
    round.paymentVerifiedEmitted = true;
    sock.emit('paymentVerified');
  }

  if (round.purpose === 'topup') {
    if (processedInvoices.has(invoiceId)) {
      round.status = 'credited';
      if (sock) {
        const w = getWallet(round.walletId);
        sock.emit('walletBalance', {
          walletId: w.walletId,
          lightningAddress: w.lightningAddress,
          balanceSats: getWalletBalance(w.walletId)
        });
        sock.emit('topUpConfirmed', {
          invoiceId,
          walletId: w.walletId,
          amountSats: Number(round.topupAmount) || 0,
          balanceSats: getWalletBalance(w.walletId)
        });
      }
      scheduleRoundCleanup(invoiceId);
      return { ok: true, alreadyProcessed: true, credited: true, topupAmount: Number(round.topupAmount) || 0 };
    }

    if (round.status === 'credited') {
      return { ok: true, alreadyProcessed: true, credited: true, topupAmount: Number(round.topupAmount) || 0 };
    }

    const topupAmount = Number(round.topupAmount) || 0;
    if (!Number.isFinite(topupAmount) || topupAmount <= 0) {
      return { ok: false, reason: 'invalid_topup_amount' };
    }

    try {
      bindWalletAddress(round.walletId, round.lightningAddress);
    } catch (e) {
      return { ok: false, reason: 'wallet_address_mismatch', error: String(e?.message || e) };
    }

    const prev = getWalletBalance(round.walletId);
    const next = setWalletBalance(round.walletId, prev + topupAmount);

    processedInvoices.set(invoiceId, {
      purpose: 'topup',
      walletId: round.walletId,
      amountSats: topupAmount,
      processedAt: new Date().toISOString()
    });
    scheduleWalletStoreSave();

    round.status = 'credited';
    round.creditedAt = new Date().toISOString();
    round.balanceAfterCredit = next;

    if (sock) {
      sock.emit('walletBalance', {
        walletId: round.walletId,
        lightningAddress: round.lightningAddress,
        balanceSats: next
      });
      sock.emit('topUpConfirmed', {
        invoiceId,
        walletId: round.walletId,
        amountSats: topupAmount,
        balanceSats: next
      });
    }

    scheduleRoundCleanup(invoiceId);
    return { ok: true, credited: true, topupAmount, balanceSats: next };
  }

  if (!Number.isFinite(Number(round.payoutAmount))) {
    const { payoutAmount, payoutOptions, payoutWeights } = pickPayoutAmount(round.betAmount);
    round.payoutAmount = payoutAmount;
    round.payoutOptions = payoutOptions;
    round.payoutWeights = payoutWeights;
  }

  const spinOutcome = {
    invoiceId,
    betAmount: round.betAmount,
    payoutAmount: round.payoutAmount,
    payoutOptions: round.payoutOptions || PAYOUT_TABLE[round.betAmount] || [0, round.betAmount],
    payoutWeights: round.payoutWeights || null
  };

  if (sock && !round.spinEmitted) {
    round.spinEmitted = true;
    sock.emit('spinOutcome', spinOutcome);
  }

  if (round.status === 'payout_sent') {
    return { ok: true, alreadyProcessed: true, payoutAmount: round.payoutAmount, spinOutcome };
  }

  const payoutAmount = Number(round.payoutAmount) || 0;
  if (round.payoutInProgress) {
    return { ok: true, payoutInProgress: true, payoutAmount, spinOutcome };
  }

  round.payoutInProgress = true;

  if (payoutAmount > 0) {
    try {
      const payoutResp = await sendInstantPayment(
        round.lightningAddress,
        payoutAmount,
        `BTC Slides payout - Invoice ${invoiceId} - ${payoutAmount} SATS`
      );

      round.status = 'payout_sent';
      round.payoutResponse = payoutResp;

      if (sock) {
        sock.emit('payoutSent', {
          invoiceId,
          payoutAmount,
          recipient: round.lightningAddress,
          payoutResponse: payoutResp
        });
      }
    } catch (e) {
      round.status = 'paid';
      round.payoutError = String(e.message || e);

      if (sock) {
        sock.emit('payoutFailed', {
          invoiceId,
          payoutAmount,
          recipient: round.lightningAddress,
          error: round.payoutError
        });
      }
    }
  } else {
    round.status = 'payout_sent';
    if (sock) {
      sock.emit('payoutSent', {
        invoiceId,
        payoutAmount: 0,
        recipient: round.lightningAddress,
        payoutResponse: null
      });
    }
  }

  scheduleRoundCleanup(invoiceId);
  return { ok: true, payoutAmount, spinOutcome };
}

app.get('/verify/:invoiceId', async (req, res) => {
  const invoiceId = String(req.params.invoiceId || '').trim();
  const socketId = String(req.query.socketId || '').trim() || null;
  if (!invoiceId) return res.status(400).json({ error: 'Missing invoiceId' });

  try {
    let roundKnown = roundsByInvoice.has(invoiceId);
    const { paid, status, details } = await verifyInvoicePaidWithSpeed(invoiceId);
    if (!paid) {
      return res.json({ ok: true, invoiceId, paid: false, status: status || 'unknown', roundKnown });
    }

    if (!roundKnown) {
      const recovered = extractRoundFromPaymentDetails(invoiceId, details, socketId);
      if (recovered) {
        roundsByInvoice.set(invoiceId, recovered);
        if (socketId) invoiceToSocket.set(invoiceId, socketId);
        roundKnown = true;
      }
    }

    const processed = await processPaidInvoice(invoiceId, { socketId, paidVerified: true });
    return res.json({ ok: true, invoiceId, paid: true, status: status || 'paid', roundKnown, processed });
  } catch (e) {
    return res.status(500).json({ error: String(e.message || e) });
  }
});

app.post('/webhook', express.json(), async (req, res) => {
  const event = req.body;
  const eventType = event?.event_type;

  try {
    const invoiceId = extractInvoiceIdFromEvent(event);

    if (eventType === 'payment.failed') {
      const invoiceId = extractInvoiceIdFromEvent(event);
      if (invoiceId) {
        const socketId = invoiceToSocket.get(invoiceId);
        const sock = socketId && io.sockets.sockets.get(socketId);
        if (sock) {
          sock.emit('paymentFailed', {
            invoiceId
          });
        }
        invoiceToSocket.delete(invoiceId);
        scheduleRoundCleanup(invoiceId, 5 * 60 * 1000);
      }
    }

    if (invoiceId) {
      let round = roundsByInvoice.get(invoiceId);
      if (!round) {
        try {
          const { details } = await verifyInvoicePaidWithSpeed(invoiceId);
          const recovered = extractRoundFromPaymentDetails(invoiceId, details, null);
          if (recovered) {
            roundsByInvoice.set(invoiceId, recovered);
            round = recovered;
          }
        } catch {
        }
      }

      if (!round) return res.status(200).send('Webhook received (unknown invoice)');
      if (round.status === 'payout_sent' || round.status === 'credited') {
        return res.status(200).send('Webhook received (already processed)');
      }

      const { paid, status } = await verifyInvoicePaidWithSpeed(invoiceId);
      if (!paid) {
        return res.status(200).send(`Webhook received (not paid yet: ${status || 'unknown'})`);
      }

      await processPaidInvoice(invoiceId, { paidVerified: true });
    }

    res.status(200).send('Webhook received');
  } catch (error) {
    res.status(500).send(`Webhook processing failed: ${error.message}`);
  }
});

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: corsOrigins,
    credentials: true,
    methods: ['GET', 'POST']
  }
});

io.on('connection', (socket) => {
  socket.emit('serverInfo', {
    betOptions: BET_OPTIONS,
    topUpOptions: TOPUP_OPTIONS,
    payoutTable: PAYOUT_TABLE,
    payoutWeights: PAYOUT_WEIGHTS
  });

  socket.on('getWalletBalance', ({ walletId, walletSecret, lightningAddress }) => {
    try {
      ensureWalletAuth(walletId, walletSecret);
      const w = getWallet(walletId);
      if (lightningAddress) bindWalletAddress(w.walletId, lightningAddress);
      walletToSocket.set(w.walletId, socket.id);
      noteWalletActivity(w.walletId);
      socket.emit('walletBalance', {
        walletId: w.walletId,
        lightningAddress: w.lightningAddress,
        balanceSats: getWalletBalance(w.walletId)
      });
    } catch {
      socket.emit('walletBalance', { walletId: null, lightningAddress: null, balanceSats: 0 });
    }
  });

  socket.on('startTopUp', async ({ walletId, walletSecret, lightningAddress, amountSats }) => {
    try {
      ensureWalletAuth(walletId, walletSecret);
      const amount = Number(amountSats);
      if (!TOPUP_OPTIONS.includes(amount)) throw new Error('Invalid top up amount');
      const w = bindWalletAddress(walletId, lightningAddress);
      const formattedAddress = w.lightningAddress;

      walletToSocket.set(w.walletId, socket.id);
      noteWalletActivity(w.walletId);

      const topupId = `topup_${Date.now()}_${socket.id}`;
      const invoiceData = await createLightningInvoice(amount, `order_${topupId}`, {
        Wallet_ID: w.walletId,
        Lightning_Address: formattedAddress,
        Purpose: 'topup'
      });

      const round = {
        roundId: topupId,
        socketId: socket.id,
        invoiceId: invoiceData.invoiceId,
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        topupAmount: amount,
        purpose: 'topup',
        status: 'invoice_created',
        createdAt: new Date().toISOString()
      };

      roundsByInvoice.set(invoiceData.invoiceId, round);
      invoiceToSocket.set(invoiceData.invoiceId, socket.id);

      socket.emit('paymentRequest', {
        invoiceId: invoiceData.invoiceId,
        amountSats: amount,
        lightningInvoice: invoiceData.lightningInvoice,
        hostedInvoiceUrl: invoiceData.hostedInvoiceUrl,
        speedInterfaceUrl: invoiceData.speedInterfaceUrl,
        purpose: 'topup',
        walletId: w.walletId
      });

      setTimeout(() => {
        const r = roundsByInvoice.get(invoiceData.invoiceId);
        if (r && r.status === 'invoice_created') {
          roundsByInvoice.delete(invoiceData.invoiceId);
          invoiceToSocket.delete(invoiceData.invoiceId);
          socket.emit('paymentExpired', { invoiceId: invoiceData.invoiceId, purpose: 'topup' });
        }
      }, 10 * 60 * 1000);
    } catch (error) {
      socket.emit('errorMessage', { message: error.message });
    }
  });

  socket.on('startSpin', async ({ walletId, walletSecret, lightningAddress, betAmount }) => {
    try {
      ensureWalletAuth(walletId, walletSecret);
      const bet = Number(betAmount);
      if (!BET_OPTIONS.includes(bet)) throw new Error('Invalid bet amount');

      const w = bindWalletAddress(walletId, lightningAddress);
      const formattedAddress = w.lightningAddress;

      walletToSocket.set(w.walletId, socket.id);
      noteWalletActivity(w.walletId);

      const current = getWalletBalance(w.walletId);
      if (current < bet) {
        throw new Error(`Insufficient wallet balance. Add ${bet - current} SATS to play.`);
      }

      const next = setWalletBalance(w.walletId, current - bet);
      socket.emit('walletBalance', { walletId: w.walletId, lightningAddress: formattedAddress, balanceSats: next });

      const { payoutAmount, payoutOptions, payoutWeights } = pickPayoutAmount(bet);
      socket.emit('spinOutcome', {
        betAmount: bet,
        payoutAmount,
        payoutOptions,
        payoutWeights
      });

      if (payoutAmount > 0) {
        try {
          const payoutResp = await sendInstantPayment(
            formattedAddress,
            payoutAmount,
            `BTC Slides payout - ${payoutAmount} SATS`
          );

          socket.emit('payoutSent', {
            payoutAmount,
            recipient: formattedAddress,
            payoutResponse: payoutResp
          });
        } catch (e) {
          socket.emit('payoutFailed', {
            payoutAmount,
            recipient: formattedAddress,
            error: String(e.message || e)
          });
        }
      } else {
        socket.emit('payoutSent', {
          payoutAmount: 0,
          recipient: formattedAddress,
          payoutResponse: null
        });
      }
    } catch (error) {
      socket.emit('errorMessage', { message: error.message });
    }
  });

  socket.on('withdraw', async ({ walletId, walletSecret, lightningAddress }) => {
    try {
      ensureWalletAuth(walletId, walletSecret);
      const w = bindWalletAddress(walletId, lightningAddress);
      const formattedAddress = w.lightningAddress;

      walletToSocket.set(w.walletId, socket.id);
      noteWalletActivity(w.walletId);

      if (w.pendingWithdrawal) throw new Error('Withdrawal already in progress');

      const amount = getWalletBalance(w.walletId);
      if (!Number.isFinite(amount) || amount <= 0) throw new Error('Nothing to withdraw');

      setWalletBalance(w.walletId, 0);
      setWalletHold(w.walletId, amount);

      w.pendingWithdrawal = {
        withdrawalId: `wd_${Date.now()}_${socket.id}`,
        amountSats: amount,
        requestedAt: new Date().toISOString(),
        reason: 'manual'
      };
      w.updatedAt = new Date().toISOString();
      scheduleWalletStoreSave();
      scheduleLiabilitiesReportWrite();

      socket.emit('walletBalance', { walletId: w.walletId, lightningAddress: formattedAddress, balanceSats: 0 });
      socket.emit('withdrawalPending', { walletId: w.walletId, amountSats: amount, recipient: formattedAddress });

      try {
        const payoutResp = await sendInstantPayment(
          formattedAddress,
          amount,
          `BTC Slides withdrawal - ${amount} SATS`
        );

        setWalletHold(w.walletId, 0);
        w.pendingWithdrawal = null;
        w.lastWithdrawal = {
          amountSats: amount,
          recipient: formattedAddress,
          sentAt: new Date().toISOString(),
          reason: 'manual',
          payoutResponse: payoutResp
        };
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        socket.emit('withdrawalSent', { walletId: w.walletId, amountSats: amount, recipient: formattedAddress, payoutResponse: payoutResp, balanceSats: 0 });
      } catch (e) {
        setWalletHold(w.walletId, 0);
        setWalletBalance(w.walletId, amount);
        w.pendingWithdrawal = null;
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        socket.emit('walletBalance', { walletId: w.walletId, lightningAddress: formattedAddress, balanceSats: amount });
        socket.emit('withdrawalFailed', { walletId: w.walletId, amountSats: amount, recipient: formattedAddress, error: String(e?.message || e) });
      }
    } catch (error) {
      socket.emit('errorMessage', { message: error.message });
    }
  });
});

let autoRefundRunning = false;

async function runAutoRefundPass() {
  if (autoRefundRunning) return;
  autoRefundRunning = true;
  try {
    const nowMs = Date.now();
    const wallets = Array.from(walletsById.values());
    for (const w of wallets) {
      const balance = Math.max(0, Number(w?.balanceSats) || 0);
      if (balance <= 0) continue;
      const addr = String(w?.lightningAddress || '').trim().toLowerCase();
      if (!addr || !addr.includes('@')) continue;
      if (w?.pendingWithdrawal) continue;

      const lastActMs = Date.parse(String(w?.lastActivityAt || ''));
      if (!Number.isFinite(lastActMs)) continue;
      if (nowMs - lastActMs < AUTO_REFUND_IDLE_MS) continue;

      const lastTryMs = Date.parse(String(w?.lastAutoRefundAttemptAt || ''));
      if (Number.isFinite(lastTryMs) && nowMs - lastTryMs < AUTO_REFUND_COOLDOWN_MS) continue;

      w.lastAutoRefundAttemptAt = new Date().toISOString();
      w.updatedAt = w.lastAutoRefundAttemptAt;
      scheduleWalletStoreSave();
      scheduleLiabilitiesReportWrite();

      const walletId = String(w.walletId);
      const sockId = walletToSocket.get(walletId);
      const sock = sockId && io.sockets.sockets.get(sockId);

      try {
        setWalletBalance(walletId, 0);
        setWalletHold(walletId, balance);
        w.pendingWithdrawal = {
          withdrawalId: `auto_${Date.now()}_${walletId.slice(0, 6)}`,
          amountSats: balance,
          requestedAt: new Date().toISOString(),
          reason: 'auto_refund'
        };
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        const payoutResp = await sendInstantPayment(
          addr,
          balance,
          `BTC Slides auto-refund - ${balance} SATS`
        );

        setWalletHold(walletId, 0);
        w.pendingWithdrawal = null;
        w.lastWithdrawal = {
          amountSats: balance,
          recipient: addr,
          sentAt: new Date().toISOString(),
          reason: 'auto_refund',
          payoutResponse: payoutResp
        };
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        if (sock) {
          sock.emit('walletBalance', { walletId, lightningAddress: addr, balanceSats: 0 });
          sock.emit('autoRefundSent', { walletId, amountSats: balance, recipient: addr });
        }
      } catch (e) {
        setWalletHold(walletId, 0);
        setWalletBalance(walletId, balance);
        w.pendingWithdrawal = null;
        w.lastAutoRefundError = String(e?.message || e);
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        if (sock) {
          sock.emit('walletBalance', { walletId, lightningAddress: addr, balanceSats: balance });
          sock.emit('autoRefundFailed', { walletId, amountSats: balance, recipient: addr, error: String(e?.message || e) });
        }
      }
    }
  } finally {
    autoRefundRunning = false;
  }
}

setInterval(() => {
  runAutoRefundPass().catch(() => {});
}, AUTO_REFUND_CHECK_MS);

const port = Number(process.env.PORT || 3001);

if (!SPEED_WALLET_SECRET_KEY) {
  console.warn('SPEED_WALLET_SECRET_KEY is not set. Payouts will fail until configured.');
}

if (!SPEED_WALLET_WEBHOOK_SECRET) {
  console.warn('SPEED_WALLET_WEBHOOK_SECRET is not set.');
}

server.listen(port, () => {
  console.log(`BTC Slides backend listening on :${port}`);
});
