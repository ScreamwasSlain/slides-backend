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

const ADMIN_TOKEN = process.env.ADMIN_TOKEN ? String(process.env.ADMIN_TOKEN) : 'screamwasslain_btcslides';

const AUTH_HEADER = SPEED_WALLET_SECRET_KEY
  ? Buffer.from(`${SPEED_WALLET_SECRET_KEY}:`).toString('base64')
  : null;

const PUB_AUTH_HEADER = SPEED_WALLET_PUBLISHABLE_KEY
  ? Buffer.from(`${SPEED_WALLET_PUBLISHABLE_KEY}:`).toString('base64')
  : null;

const BET_OPTIONS = [20, 100, 300, 500, 1000, 5000, 10000];
const TOPUP_OPTIONS = [1000, 5000, 10000];
const NEW_USER_REWARD_SATS = 50;
const MAX_REWARD_BONUS_BANKROLL_SATS = 150;
const REWARD_BONUS_WAGER_LIMIT_SATS_BY_BET = {
  default: 300,
  20: 300,
  100: 500
};

const walletsById = new Map();
const processedInvoices = new Map();
const rewardedLightningAddresses = new Set();

function detectPersistentDataDir() {
  const candidates = [
    process.env.DATA_DIR,
    process.env.PERSISTENT_DATA_DIR,
    process.env.RENDER_DISK_PATH,
    process.env.RENDER_DISK_MOUNT_PATH,
    process.env.RENDER_DISK_ROOT
  ].map((v) => String(v || '').trim()).filter(Boolean);

  for (const c of candidates) {
    try {
      fs.mkdirSync(c, { recursive: true });
      return c;
    } catch {
    }
  }
  return null;
}

function ensureParentDir(filePath) {
  try {
    const dir = path.dirname(String(filePath || ''));
    if (dir) fs.mkdirSync(dir, { recursive: true });
  } catch {
  }
}

function resolveDataFilePath(explicitPath, fallbackFileName) {
  if (explicitPath) return String(explicitPath);
  const dataDir = detectPersistentDataDir();
  if (dataDir) return path.join(dataDir, fallbackFileName);
  return path.join(__dirname, fallbackFileName);
}

function deriveDefaultGithubRawUrl(branch, fileName) {
  const explicitRepo = String(process.env.BACKUP_GITHUB_REPOSITORY || '').trim();
  const repoFromRender = String(process.env.RENDER_GIT_REPO_URL || '').trim();
  let repo = explicitRepo;

  if (!repo && repoFromRender) {
    const m = repoFromRender.match(/github\.com[:/](.+?)(?:\.git)?$/i);
    if (m && m[1]) repo = m[1];
  }

  if (!repo || !branch || !fileName) return null;
  return `https://raw.githubusercontent.com/${repo}/${branch}/backups/${fileName}`;
}

const AUDIT_LOG_PATH = resolveDataFilePath(process.env.AUDIT_LOG_PATH, 'audit_log.jsonl');

const WALLET_STORE_PATH = resolveDataFilePath(process.env.WALLET_STORE_PATH, 'wallet_store.json');

const WALLET_STORE_BOOTSTRAP_URL = process.env.WALLET_STORE_BOOTSTRAP_URL
  ? String(process.env.WALLET_STORE_BOOTSTRAP_URL)
  : deriveDefaultGithubRawUrl('wallet-store-backups', 'wallet_store_latest.json');

const WALLET_STORE_BOOTSTRAP_AUTH = process.env.WALLET_STORE_BOOTSTRAP_AUTH
  ? String(process.env.WALLET_STORE_BOOTSTRAP_AUTH)
  : null;

const AUDIT_LOG_BOOTSTRAP_URL = process.env.AUDIT_LOG_BOOTSTRAP_URL
  ? String(process.env.AUDIT_LOG_BOOTSTRAP_URL)
  : deriveDefaultGithubRawUrl('audit-backups', 'audit_latest.jsonl');

const AUDIT_LOG_BOOTSTRAP_AUTH = process.env.AUDIT_LOG_BOOTSTRAP_AUTH
  ? String(process.env.AUDIT_LOG_BOOTSTRAP_AUTH)
  : null;

let walletStoreSaveTimer = null;

const LIABILITIES_REPORT_PATH = resolveDataFilePath(process.env.LIABILITIES_REPORT_PATH, 'wallet_liabilities.csv');

let liabilitiesReportTimer = null;

 function logLine(kind, data) {
   try {
     const payload = {
       kind,
       ...(data && typeof data === 'object' ? data : { value: data })
     };
     if (!payload.ts) payload.ts = new Date().toISOString();
     console.log(JSON.stringify(payload));
   } catch {
   }
 }

function serializeWallet(w) {
  return {
    walletId: w.walletId,
    balanceSats: Number(w.balanceSats) || 0,
    holdSats: Number(w.holdSats) || 0,
    rewardBonusBalanceSats: Number(w.rewardBonusBalanceSats) || 0,
    rewardBonusWageredSats: Number(w.rewardBonusWageredSats) || 0,
    lightningAddress: w.lightningAddress || null,
    createdAt: w.createdAt || null,
    updatedAt: w.updatedAt || null,
    lastActivityAt: w.lastActivityAt || null,
    boundAt: w.boundAt || null,
    secretSetAt: w.secretSetAt || null,
    rewardClaimedAt: w.rewardClaimedAt || null,
    rewardClaimedAddress: w.rewardClaimedAddress || null,
    rewardBonusDeactivatedAt: w.rewardBonusDeactivatedAt || null,
    pendingWithdrawal: sanitizePendingWithdrawal(w.pendingWithdrawal),
    lastWithdrawal: sanitizeWithdrawalRecord(w.lastWithdrawal),
    onboardingSpinsByBet: w.onboardingSpinsByBet && typeof w.onboardingSpinsByBet === 'object' ? w.onboardingSpinsByBet : null,
    rewardBonusSpinsByBet: w.rewardBonusSpinsByBet && typeof w.rewardBonusSpinsByBet === 'object' ? w.rewardBonusSpinsByBet : null,
    walletSecretHash: Buffer.isBuffer(w.walletSecretHash) ? w.walletSecretHash.toString('hex') : (w.walletSecretHash || null)
  };
}

function sanitizePendingWithdrawal(rec) {
  if (!rec || typeof rec !== 'object') return null;
  const reason = String(rec?.reason || '').trim();
  return {
    ...rec,
    reason: reason === 'manual_withdraw' ? reason : null
  };
}

function sanitizeWithdrawalRecord(rec) {
  if (!rec || typeof rec !== 'object') return null;
  return String(rec?.reason || '').trim() === 'manual_withdraw' ? rec : null;
}

function writeLiabilitiesReport() {
  try {
    ensureParentDir(LIABILITIES_REPORT_PATH);
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

function buildWalletStorePayload() {
  const wallets = Array.from(walletsById.values()).map(serializeWallet);
  const processed = Array.from(processedInvoices.entries()).map(([invoiceId, rec]) => ({
    invoiceId,
    purpose: rec?.purpose || null,
    walletId: rec?.walletId || null,
    amountSats: Number(rec?.amountSats) || 0,
    processedAt: rec?.processedAt || null
  }));
  const rewardedAddresses = Array.from(rewardedLightningAddresses.values()).sort();
  return { wallets, processedInvoices: processed, rewardedLightningAddresses: rewardedAddresses };
}

function isMissingOrTiny(filePath, minBytes = 10) {
  try {
    if (!filePath) return true;
    if (!fs.existsSync(filePath)) return true;
    const st = fs.statSync(filePath);
    return !st || st.size < minBytes;
  } catch {
    return true;
  }
}

async function fetchBootstrapData(url, authHeaderValue = null) {
  const headers = {};
  if (authHeaderValue) headers.Authorization = authHeaderValue;
  if (String(url).includes('api.github.com')) {
    headers.Accept = 'application/vnd.github.raw';
  }
  const resp = await axios.get(url, { timeout: 12000, headers });
  return resp?.data;
}

async function bootstrapWalletStoreIfMissing() {
  try {
    if (!WALLET_STORE_BOOTSTRAP_URL) return false;
    if (!WALLET_STORE_PATH) return false;
    if (!isMissingOrTiny(WALLET_STORE_PATH)) return false;

    let data = await fetchBootstrapData(WALLET_STORE_BOOTSTRAP_URL, WALLET_STORE_BOOTSTRAP_AUTH);
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch {
        return false;
      }
    }

    const store = (data && typeof data === 'object' && Array.isArray(data.wallets) && Array.isArray(data.processedInvoices))
      ? data
      : null;
    if (!store) return false;

    const payload = JSON.stringify({
      wallets: store.wallets,
      processedInvoices: store.processedInvoices
    }, null, 2);
    ensureParentDir(WALLET_STORE_PATH);
    const tmp = `${WALLET_STORE_PATH}.tmp`;
    fs.writeFileSync(tmp, payload, 'utf8');
    fs.renameSync(tmp, WALLET_STORE_PATH);

    logLine('wallet_store_bootstrap_ok', {
      walletCount: Array.isArray(store.wallets) ? store.wallets.length : 0,
      processedInvoiceCount: Array.isArray(store.processedInvoices) ? store.processedInvoices.length : 0
    });
    return true;
  } catch (e) {
    logLine('wallet_store_bootstrap_failed', { error: String(e?.message || e) });
    return false;
  }
}

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function csvToAuditJsonl(csvText) {
  const rawLines = String(csvText || '').split(/\r?\n/).filter(Boolean);
  if (rawLines.length < 2) return '';

  const header = parseCsvLine(rawLines[0]).map((s) => String(s || '').trim());
  const idx = Object.fromEntries(header.map((h, i) => [h, i]));
  const mustHave = ['ts', 'type'];
  for (const key of mustHave) {
    if (!Number.isInteger(idx[key])) return '';
  }

  const lines = [];
  for (let i = 1; i < rawLines.length; i += 1) {
    const cols = parseCsvLine(rawLines[i]);
    const get = (k) => {
      const p = idx[k];
      if (!Number.isInteger(p)) return null;
      const v = cols[p];
      if (v == null || v === '') return null;
      return v;
    };
    const num = (k) => {
      const v = get(k);
      if (v == null) return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    const e = {
      id: `boot_${Date.now()}_${i}`,
      ts: get('ts') || new Date().toISOString(),
      type: get('type') || 'unknown',
      walletId: get('walletId'),
      lightningAddress: get('lightningAddress'),
      invoiceId: get('invoiceId'),
      betAmount: num('betAmount'),
      payoutAmount: num('payoutAmount'),
      amountSats: num('amountSats'),
      balanceBeforeSats: num('balanceBeforeSats'),
      balanceAfterSats: num('balanceAfterSats'),
      recipient: get('recipient'),
      reason: get('reason'),
      error: get('error')
    };
    lines.push(JSON.stringify(e));
  }
  return lines.length ? `${lines.join('\n')}\n` : '';
}

async function bootstrapAuditLogIfMissing() {
  try {
    if (!AUDIT_LOG_BOOTSTRAP_URL) return false;
    if (!AUDIT_LOG_PATH) return false;
    if (!isMissingOrTiny(AUDIT_LOG_PATH, 20)) return false;

    const data = await fetchBootstrapData(AUDIT_LOG_BOOTSTRAP_URL, AUDIT_LOG_BOOTSTRAP_AUTH);
    let text = '';
    if (typeof data === 'string') {
      const s = data.trim();
      if (!s) return false;
      if (s[0] === '{' || s[0] === '[') {
        try {
          const parsed = JSON.parse(s);
          if (Array.isArray(parsed)) {
            text = parsed.map((x) => JSON.stringify(x)).join('\n');
            if (text) text += '\n';
          } else {
            text = `${JSON.stringify(parsed)}\n`;
          }
        } catch {
          if (s.includes(',') && s.toLowerCase().includes('ts') && s.toLowerCase().includes('type')) {
            text = csvToAuditJsonl(s);
          } else {
            text = s.endsWith('\n') ? s : `${s}\n`;
          }
        }
      } else if (s.includes(',') && s.toLowerCase().includes('ts') && s.toLowerCase().includes('type')) {
        text = csvToAuditJsonl(s);
      } else {
        text = s.endsWith('\n') ? s : `${s}\n`;
      }
    } else if (Array.isArray(data)) {
      text = data.map((x) => JSON.stringify(x)).join('\n');
      if (text) text += '\n';
    } else if (data && typeof data === 'object') {
      text = `${JSON.stringify(data)}\n`;
    }

    if (!text || text.trim().length < 2) return false;
    ensureParentDir(AUDIT_LOG_PATH);
    const tmp = `${AUDIT_LOG_PATH}.tmp`;
    fs.writeFileSync(tmp, text, 'utf8');
    fs.renameSync(tmp, AUDIT_LOG_PATH);

    logLine('audit_log_bootstrap_ok', {
      path: AUDIT_LOG_PATH,
      bytes: Buffer.byteLength(text, 'utf8')
    });
    return true;
  } catch (e) {
    logLine('audit_log_bootstrap_failed', { error: String(e?.message || e) });
    return false;
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
    const rewardedAddresses = Array.isArray(parsed?.rewardedLightningAddresses) ? parsed.rewardedLightningAddresses : [];
    rewardedLightningAddresses.clear();
    for (const item of arr) {
      const id = String(item?.walletId || '').trim();
      if (!id) continue;
      const rewardClaimedAddress = item?.rewardClaimedAddress ? String(item.rewardClaimedAddress) : null;
      walletsById.set(id, {
        walletId: id,
        balanceSats: Math.max(0, Math.floor(Number(item?.balanceSats) || 0)),
        holdSats: Math.max(0, Math.floor(Number(item?.holdSats) || 0)),
        rewardBonusBalanceSats: Math.max(0, Math.floor(Number(item?.rewardBonusBalanceSats) || 0)),
        rewardBonusWageredSats: Math.max(0, Math.floor(Number(item?.rewardBonusWageredSats) || 0)),
        lightningAddress: item?.lightningAddress ? String(item.lightningAddress) : null,
        createdAt: item?.createdAt || null,
        updatedAt: item?.updatedAt || null,
        lastActivityAt: item?.lastActivityAt || null,
        boundAt: item?.boundAt || null,
        secretSetAt: item?.secretSetAt || null,
        rewardClaimedAt: item?.rewardClaimedAt || null,
        rewardClaimedAddress: rewardClaimedAddress,
        rewardBonusDeactivatedAt: item?.rewardBonusDeactivatedAt || null,
        pendingWithdrawal: sanitizePendingWithdrawal(item?.pendingWithdrawal),
        lastWithdrawal: sanitizeWithdrawalRecord(item?.lastWithdrawal),
        onboardingSpinsByBet: item?.onboardingSpinsByBet && typeof item.onboardingSpinsByBet === 'object' ? item.onboardingSpinsByBet : null,
        rewardBonusSpinsByBet: item?.rewardBonusSpinsByBet && typeof item.rewardBonusSpinsByBet === 'object' ? item.rewardBonusSpinsByBet : null,
        walletSecretHash: item?.walletSecretHash || null
      });
      if (item?.rewardClaimedAt && rewardClaimedAddress) {
        rewardedLightningAddresses.add(formatLightningAddress(rewardClaimedAddress));
      }
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

    for (const addr of rewardedAddresses) {
      try {
        rewardedLightningAddresses.add(formatLightningAddress(addr));
      } catch {
      }
    }

    logLine('wallet_store_loaded', {
      walletCount: walletsById.size,
      processedInvoiceCount: processedInvoices.size,
      path: WALLET_STORE_PATH
    });

    scheduleLiabilitiesReportWrite();
  } catch (e) {
    console.warn(`Failed to load wallet store: ${String(e.message || e)}`);
  }
}

function saveWalletStore() {
  try {
    if (!WALLET_STORE_PATH) return;
    ensureParentDir(WALLET_STORE_PATH);
    const payload = JSON.stringify(buildWalletStorePayload(), null, 2);
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

function appendAuditEvent(event) {
  try {
    if (!AUDIT_LOG_PATH) return;
    const e = {
      id: `a_${Date.now()}_${Math.random().toString(16).slice(2)}`,
      ts: new Date().toISOString(),
      ...event
    };
    const line = `${JSON.stringify(e)}\n`;
    try {
      ensureParentDir(AUDIT_LOG_PATH);
      fs.appendFileSync(AUDIT_LOG_PATH, line, 'utf8');
    } catch (err) {
      console.warn(`Failed to append audit event: ${String(err?.message || err)}`);
    }
    logLine('audit', e);
  } catch (e) {
    console.warn(`Failed to append audit event: ${String(e.message || e)}`);
  }
}

function isAdminRequest(req) {
  if (!ADMIN_TOKEN) return false;
  const h = req?.headers || {};
  const token = String(h['x-admin-token'] || h['x_admin_token'] || req.query?.token || '').trim();
  return token && token === ADMIN_TOKEN;
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(500).json({ error: 'ADMIN_TOKEN not configured' });
  if (!isAdminRequest(req)) return res.status(401).json({ error: 'Unauthorized' });
  return next();
}

function toWholeSats(value) {
  return Math.max(0, Math.floor(Number(value) || 0));
}

function readAuditEventsRaw() {
  const out = [];
  try {
    if (!AUDIT_LOG_PATH) return out;
    if (!fs.existsSync(AUDIT_LOG_PATH)) return out;
    const raw = fs.readFileSync(AUDIT_LOG_PATH, 'utf8');
    const lines = raw.split(/\r?\n/).filter(Boolean);
    for (const line of lines) {
      try {
        const e = JSON.parse(line);
        if (e && typeof e === 'object') out.push(e);
      } catch {
      }
    }
  } catch (e) {
    console.warn(`Failed to read audit log: ${String(e.message || e)}`);
  }
  return out;
}

function filterAuditEvents(events, { walletId, type, from, to } = {}) {
  const fromMs = from ? Date.parse(String(from)) : NaN;
  const toMs = to ? Date.parse(String(to)) : NaN;
  const wFilter = walletId ? String(walletId).trim() : '';
  const tFilter = type ? String(type).trim() : '';
  const visibleTypes = new Set([
    'deposit',
    'spin_bet',
    'spin_payout',
    'withdraw_requested',
    'withdraw_sent',
    'withdraw_failed',
    'pending_withdrawal_reverted'
  ]);
  const filtered = [];

  for (const e of Array.isArray(events) ? events : []) {
    if (!e || typeof e !== 'object') continue;
    const eventType = String(e?.type || '').trim();
    if (!visibleTypes.has(eventType)) continue;
    if (wFilter && String(e.walletId || '') !== wFilter) continue;
    if (tFilter && eventType !== tFilter) continue;

    const tsMs = Date.parse(String(e.ts || ''));
    if (Number.isFinite(fromMs) && Number.isFinite(tsMs) && tsMs < fromMs) continue;
    if (Number.isFinite(toMs) && Number.isFinite(tsMs) && tsMs > toMs) continue;
    filtered.push(e);
  }

  filtered.sort((a, b) => {
    const am = Date.parse(String(a.ts || ''));
    const bm = Date.parse(String(b.ts || ''));
    if (Number.isFinite(am) && Number.isFinite(bm)) return bm - am;
    return 0;
  });
  return filtered;
}

function readAuditEvents({ walletId, type, from, to, limit, offset } = {}) {
  const filtered = filterAuditEvents(readAuditEventsRaw(), { walletId, type, from, to });
  const out = [];
  const off = Math.max(0, Math.floor(Number(offset) || 0));
  const lim = Math.max(1, Math.min(5000, Math.floor(Number(limit) || 200)));
  for (let i = off; i < filtered.length && out.length < lim; i += 1) out.push(filtered[i]);
  return out;
}

function summarizeTreasuryMetrics(events) {
  const stats = {
    eventCount: 0,
    depositsCreditedSats: 0,
    spinBetSats: 0,
    spinPayoutSats: 0,
    grossGamingRevenueSats: 0,
    withdrawalsRequestedCount: 0,
    withdrawalsSentCount: 0,
    withdrawalsFailedCount: 0,
    withdrawalsSentSats: 0,
    pendingWithdrawalRevertedCount: 0,
    payoutRequestedCount: 0,
    payoutSentCount: 0,
    payoutFailedCount: 0,
    payoutFailureRate: 0,
    netPlayerFundsFlowSats: 0
  };

  for (const e of Array.isArray(events) ? events : []) {
    const type = String(e?.type || '');
    stats.eventCount += 1;

    if (type === 'deposit') {
      stats.depositsCreditedSats += toWholeSats(e?.amountSats);
    } else if (type === 'spin_bet') {
      stats.spinBetSats += toWholeSats(e?.betAmount);
    } else if (type === 'spin_payout') {
      stats.spinPayoutSats += toWholeSats(e?.payoutAmount);
    } else if (type === 'withdraw_requested') {
      stats.withdrawalsRequestedCount += 1;
    } else if (type === 'withdraw_sent') {
      stats.withdrawalsSentCount += 1;
      stats.withdrawalsSentSats += toWholeSats(e?.amountSats);
    } else if (type === 'withdraw_failed') {
      stats.withdrawalsFailedCount += 1;
    } else if (type === 'pending_withdrawal_reverted') {
      stats.pendingWithdrawalRevertedCount += 1;
    }
  }

  stats.grossGamingRevenueSats = stats.spinBetSats - stats.spinPayoutSats;
  stats.payoutRequestedCount = stats.withdrawalsRequestedCount;
  stats.payoutSentCount = stats.withdrawalsSentCount;
  stats.payoutFailedCount = stats.withdrawalsFailedCount;
  stats.payoutFailureRate = stats.payoutRequestedCount > 0
    ? Number((stats.payoutFailedCount / stats.payoutRequestedCount).toFixed(4))
    : 0;
  stats.netPlayerFundsFlowSats = stats.depositsCreditedSats - stats.withdrawalsSentSats;

  return stats;
}

function buildTreasuryDashboard() {
  const allEvents = filterAuditEvents(readAuditEventsRaw());
  const statsByWallet = new Map();

  function ensureWalletStats(walletId) {
    const id = String(walletId || '').trim();
    if (!id) return null;
    if (!statsByWallet.has(id)) {
      statsByWallet.set(id, {
        walletId: id,
        depositCount: 0,
        depositsCreditedSats: 0,
        spinsMade: 0,
        totalBetSats: 0,
        totalPayoutSats: 0,
        gameplayNetSats: 0,
        withdrawalsRequestedCount: 0,
        withdrawalsSentCount: 0,
        withdrawalsFailedCount: 0,
        withdrawalsSentSats: 0,
        payoutOutSats: 0,
        lastPlayedAt: null,
        lastDepositAt: null,
        lastWithdrawAt: null
      });
    }
    return statsByWallet.get(id);
  }

  for (const e of allEvents) {
    const walletStats = ensureWalletStats(e?.walletId);
    if (!walletStats) continue;
    const type = String(e?.type || '');
    const ts = e?.ts || null;

    if (type === 'deposit') {
      walletStats.depositCount += 1;
      walletStats.depositsCreditedSats += toWholeSats(e?.amountSats);
      walletStats.lastDepositAt = ts || walletStats.lastDepositAt;
    } else if (type === 'spin_bet') {
      walletStats.spinsMade += 1;
      walletStats.totalBetSats += toWholeSats(e?.betAmount);
      walletStats.lastPlayedAt = ts || walletStats.lastPlayedAt;
    } else if (type === 'spin_payout') {
      walletStats.totalPayoutSats += toWholeSats(e?.payoutAmount);
      walletStats.lastPlayedAt = ts || walletStats.lastPlayedAt;
    } else if (type === 'withdraw_requested') {
      walletStats.withdrawalsRequestedCount += 1;
    } else if (type === 'withdraw_sent') {
      walletStats.withdrawalsSentCount += 1;
      walletStats.withdrawalsSentSats += toWholeSats(e?.amountSats);
      walletStats.lastWithdrawAt = ts || walletStats.lastWithdrawAt;
    } else if (type === 'withdraw_failed') {
      walletStats.withdrawalsFailedCount += 1;
    }
  }

  for (const stats of statsByWallet.values()) {
    stats.gameplayNetSats = stats.totalBetSats - stats.totalPayoutSats;
    stats.payoutOutSats = stats.withdrawalsSentSats;
  }

  const wallets = Array.from(walletsById.values()).map((w) => {
    const walletStats = ensureWalletStats(w?.walletId) || {};
    const balanceSats = toWholeSats(w?.balanceSats);
    const holdSats = toWholeSats(w?.holdSats);
    const totalSats = balanceSats + holdSats;
    return {
      walletId: String(w?.walletId || ''),
      lightningAddress: w?.lightningAddress ? String(w.lightningAddress) : null,
      balanceSats,
      holdSats,
      totalSats,
      hasPendingWithdrawal: Boolean(w?.pendingWithdrawal),
      pendingWithdrawal: sanitizePendingWithdrawal(w?.pendingWithdrawal),
      lastWithdrawal: sanitizeWithdrawalRecord(w?.lastWithdrawal),
      createdAt: w?.createdAt || null,
      updatedAt: w?.updatedAt || null,
      lastActivityAt: w?.lastActivityAt || null,
      stats: {
        depositCount: Number(walletStats.depositCount) || 0,
        depositsCreditedSats: Number(walletStats.depositsCreditedSats) || 0,
        spinsMade: Number(walletStats.spinsMade) || 0,
        totalBetSats: Number(walletStats.totalBetSats) || 0,
        totalPayoutSats: Number(walletStats.totalPayoutSats) || 0,
        gameplayNetSats: Number(walletStats.gameplayNetSats) || 0,
        withdrawalsRequestedCount: Number(walletStats.withdrawalsRequestedCount) || 0,
        withdrawalsSentCount: Number(walletStats.withdrawalsSentCount) || 0,
        withdrawalsFailedCount: Number(walletStats.withdrawalsFailedCount) || 0,
        withdrawalsSentSats: Number(walletStats.withdrawalsSentSats) || 0,
        payoutOutSats: Number(walletStats.payoutOutSats) || 0,
        lastPlayedAt: walletStats.lastPlayedAt || null,
        lastDepositAt: walletStats.lastDepositAt || null,
        lastWithdrawAt: walletStats.lastWithdrawAt || null
      }
    };
  });

  wallets.sort((a, b) => {
    if (b.totalSats !== a.totalSats) return b.totalSats - a.totalSats;
    return String(a.walletId).localeCompare(String(b.walletId));
  });

  const totals = {
    walletCount: wallets.length,
    playersWithBalanceCount: wallets.filter((w) => w.balanceSats > 0).length,
    playersWithFundsCount: wallets.filter((w) => w.totalSats > 0).length,
    pendingWithdrawalCount: wallets.filter((w) => w.hasPendingWithdrawal).length,
    totalBalanceSats: wallets.reduce((sum, w) => sum + w.balanceSats, 0),
    totalHoldSats: wallets.reduce((sum, w) => sum + w.holdSats, 0),
    totalLiabilitySats: wallets.reduce((sum, w) => sum + w.totalSats, 0),
    pendingWithdrawalSats: wallets
      .filter((w) => w.hasPendingWithdrawal)
      .reduce((sum, w) => sum + w.holdSats, 0)
  };

  const nowMs = Date.now();
  const lastHourIso = new Date(nowMs - (60 * 60 * 1000)).toISOString();
  const lastDayIso = new Date(nowMs - (24 * 60 * 60 * 1000)).toISOString();

  const recentEvents = allEvents.slice(0, 100);
  const lastHourEvents = filterAuditEvents(allEvents, { from: lastHourIso });
  const last24HoursEvents = filterAuditEvents(allEvents, { from: lastDayIso });

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    totals,
    metrics: {
      lastHour: summarizeTreasuryMetrics(lastHourEvents),
      last24Hours: summarizeTreasuryMetrics(last24HoursEvents),
      allTime: summarizeTreasuryMetrics(allEvents)
    },
    topPlayers: wallets.slice(0, 25),
    players: wallets,
    recentEvents
  };
}

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

// Bonus-mode scripted sequences are split by bankroll band:
// - starter: front-loads a few stronger early hits while staying cap-safe
// - hover: keeps a steadier mid-range with mixed small wins and misses
// - high: tapers off more clearly once the post-bet bonus bankroll is in its top range
const REWARD_BONUS_SCRIPTED_PAYOUTS_BY_BET = {
  20: {
    starter: [50, 80, 20, 50, 0, 50, 20, 20, 50, 80, 0, 0, 20, 80, 20, 50, 20, 20, 0, 50, 20, 80, 20, 20],
    hover: [20, 50, 20, 0, 20, 50, 20, 20, 0, 50, 20, 80, 20, 0, 20, 50, 20, 20, 0, 50, 20, 20, 0, 50],
    high: [0, 20, 0, 20, 0, 50, 0, 20, 0, 20, 0, 50, 20, 0, 20, 0, 0, 20, 0, 50, 0, 20, 0, 0]
  },
  100: {
    starter: [120, 120, 0, 50, 100, 50, 120, 50, 120, 0, 50, 50],
    hover: [50, 120, 50, 120, 50, 50, 120, 0, 50, 120, 50, 0],
    high: [50, 0, 50, 0, 50, 120, 0, 50, 0, 50, 0, 0]
  },
  300: [],
  500: [],
  1000: [],
  5000: [],
  10000: []
};

const ONBOARDING_PAYOUTS_BY_BET = {
  20: [50, 20, 50, 0, 0, 80],
  100: [120, 120, 50, 0, 50, 0]
};

const REWARD_BONUS_BAND_THRESHOLDS_BY_BET = {
  default: { hoverMin: 60, highMin: 100 },
  20: { hoverMin: 60, highMin: 100 },
  100: { hoverMin: 20, highMin: 40 }
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
    rewardBonusBalanceSats: 0,
    rewardBonusWageredSats: 0,
    lightningAddress: null,
    rewardClaimedAt: null,
    rewardClaimedAddress: null,
    rewardBonusDeactivatedAt: null,
    pendingWithdrawal: null,
    lastWithdrawal: null,
    rewardBonusSpinsByBet: null,
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

function getRewardBonusBalance(walletId) {
  const w = getWallet(walletId);
  return Math.max(0, Number(w.rewardBonusBalanceSats) || 0);
}

function setRewardBonusBalance(walletId, rewardBonusBalanceSats) {
  const w = getWallet(walletId);
  const next = Math.max(0, Math.floor(Number(rewardBonusBalanceSats) || 0));
  w.rewardBonusBalanceSats = next;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  return next;
}

function getRewardBonusWagered(walletId) {
  const w = getWallet(walletId);
  return Math.max(0, Number(w.rewardBonusWageredSats) || 0);
}

function setRewardBonusWagered(walletId, rewardBonusWageredSats) {
  const w = getWallet(walletId);
  const next = Math.max(0, Math.floor(Number(rewardBonusWageredSats) || 0));
  w.rewardBonusWageredSats = next;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  return next;
}

function getRewardBonusWagerLimitForBet(betAmount) {
  const bet = Number(betAmount);
  const limit = REWARD_BONUS_WAGER_LIMIT_SATS_BY_BET?.[bet] ?? REWARD_BONUS_WAGER_LIMIT_SATS_BY_BET.default;
  return Math.max(0, Math.floor(Number(limit) || 0));
}

function isRewardBonusOddsActive(wallet) {
  const w = wallet && typeof wallet === 'object' ? wallet : getWallet(wallet);
  return !w.rewardBonusDeactivatedAt && (Number(w.rewardBonusBalanceSats) || 0) > 0;
}

function buildRewardBonusState(walletId) {
  const w = getWallet(walletId);
  return {
    rewardClaimedAt: w.rewardClaimedAt || null,
    rewardClaimedAddress: w.rewardClaimedAddress || null,
    rewardBonusBalanceSats: getRewardBonusBalance(w.walletId),
    rewardBonusWageredSats: getRewardBonusWagered(w.walletId),
    rewardBonusOddsActive: isRewardBonusOddsActive(w),
    rewardBonusDeactivatedAt: w.rewardBonusDeactivatedAt || null
  };
}

function buildWalletBalancePayload(walletId) {
  const w = getWallet(walletId);
  return {
    walletId: w.walletId,
    lightningAddress: w.lightningAddress,
    balanceSats: getWalletBalance(w.walletId),
    ...buildRewardBonusState(w.walletId)
  };
}

function hasRewardClaimForLightningAddress(lightningAddress, excludeWalletId = null) {
  const addr = formatLightningAddress(lightningAddress);
  if (rewardedLightningAddresses.has(addr)) return true;
  const excludeId = excludeWalletId ? formatWalletId(excludeWalletId) : null;
  for (const wallet of walletsById.values()) {
    if (!wallet?.rewardClaimedAt) continue;
    if (excludeId && String(wallet.walletId || '') === excludeId) continue;
    if (String(wallet.rewardClaimedAddress || wallet.lightningAddress || '').toLowerCase() === addr) {
      return true;
    }
  }
  return false;
}

function disableRewardBonusForWallet(walletId, reason = 'topup') {
  const w = getWallet(walletId);
  if (!w.rewardClaimedAt) return false;
  if ((Number(w.rewardBonusBalanceSats) || 0) <= 0 && w.rewardBonusDeactivatedAt) return false;
  w.rewardBonusBalanceSats = 0;
  if (!w.rewardBonusDeactivatedAt) w.rewardBonusDeactivatedAt = new Date().toISOString();
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();
  appendAuditEvent({
    type: 'reward_bonus_deactivated',
    walletId: w.walletId,
    lightningAddress: w.lightningAddress || null,
    amountSats: 0,
    reason
  });
  return true;
}

function maybeGrantNewUserReward(walletId) {
  const w = getWallet(walletId);
  if (!w.lightningAddress) return { granted: false, reason: 'missing_lightning_address' };
  if (w.rewardClaimedAt) return { granted: false, reason: 'already_claimed_for_wallet' };

  const claimedElsewhere = hasRewardClaimForLightningAddress(w.lightningAddress, w.walletId);
  if (claimedElsewhere) {
    return { granted: false, reason: 'address_already_rewarded' };
  }

  const prevBalance = getWalletBalance(w.walletId);
  const nextBalance = setWalletBalance(w.walletId, prevBalance + NEW_USER_REWARD_SATS);
  const nextBonusBalance = setRewardBonusBalance(w.walletId, getRewardBonusBalance(w.walletId) + NEW_USER_REWARD_SATS);

  w.rewardClaimedAt = new Date().toISOString();
  w.rewardClaimedAddress = w.lightningAddress;
  rewardedLightningAddresses.add(formatLightningAddress(w.lightningAddress));
  w.rewardBonusDeactivatedAt = null;
  w.rewardBonusWageredSats = 0;
  w.rewardBonusSpinsByBet = {};
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();

  appendAuditEvent({
    type: 'new_user_reward',
    walletId: w.walletId,
    lightningAddress: w.lightningAddress,
    amountSats: NEW_USER_REWARD_SATS,
    balanceBeforeSats: prevBalance,
    balanceAfterSats: nextBalance,
    reason: 'first_login_reward'
  });

  return {
    granted: true,
    amountSats: NEW_USER_REWARD_SATS,
    balanceSats: nextBalance,
    rewardBonusBalanceSats: nextBonusBalance,
    walletId: w.walletId,
    lightningAddress: w.lightningAddress
  };
}

function pickPayoutAmount(betAmount) {
  const opts = PAYOUT_TABLE[betAmount] || [0, betAmount];
  const weights = PAYOUT_WEIGHTS?.[betAmount] || null;
  const picked = pickWeighted(opts, weights);
  return { payoutAmount: picked.value, payoutOptions: opts, payoutWeights: picked.weights };
}

function buildRewardBonusPayoutTable() {
  const out = {};
  for (const [betKey, seqRaw] of Object.entries(REWARD_BONUS_SCRIPTED_PAYOUTS_BY_BET || {})) {
    const parts = Array.isArray(seqRaw)
      ? [seqRaw]
      : (seqRaw && typeof seqRaw === 'object' ? Object.values(seqRaw) : []);
    const seq = parts
      .flatMap((part) => Array.isArray(part) ? part : [])
      .map((n) => Number(n))
      .filter((n) => Number.isFinite(n) && n >= 0);
    if (!seq.length) continue;
    out[betKey] = Array.from(new Set(seq)).sort((a, b) => a - b);
  }
  return out;
}

function getRewardBonusBand(betAmount, balanceAfterBetSats) {
  const bet = Number(betAmount);
  const balance = Math.max(0, Math.floor(Number(balanceAfterBetSats) || 0));
  const thresholds = REWARD_BONUS_BAND_THRESHOLDS_BY_BET?.[bet] || REWARD_BONUS_BAND_THRESHOLDS_BY_BET.default;
  const hoverMin = Math.max(0, Math.floor(Number(thresholds?.hoverMin) || 0));
  const highMin = Math.max(hoverMin, Math.floor(Number(thresholds?.highMin) || 0));
  if (balance >= highMin) return 'high';
  if (balance >= hoverMin) return 'hover';
  return 'starter';
}

function getRewardBonusSequenceForBet(betAmount, balanceAfterBetSats) {
  const config = REWARD_BONUS_SCRIPTED_PAYOUTS_BY_BET?.[Number(betAmount)];
  if (Array.isArray(config)) return { band: 'default', sequence: config };
  if (!config || typeof config !== 'object') return { band: 'default', sequence: [] };

  const band = getRewardBonusBand(betAmount, balanceAfterBetSats);
  const preferred = Array.isArray(config?.[band]) ? config[band] : [];
  if (preferred.length) return { band, sequence: preferred };

  const fallback = Object.values(config).find((part) => Array.isArray(part) && part.length > 0) || [];
  return { band, sequence: fallback };
}

function pickPayoutAmountForWallet(walletId, betAmount) {
  const bet = Number(betAmount);
  const base = pickPayoutAmount(bet);
  const seq = ONBOARDING_PAYOUTS_BY_BET?.[bet];
  if (!Array.isArray(seq) || seq.length === 0) return base;

  const w = getWallet(walletId);
  if (!w.onboardingSpinsByBet || typeof w.onboardingSpinsByBet !== 'object') {
    w.onboardingSpinsByBet = {};
  }

  const key = String(bet);
  const idx = Math.max(0, Math.floor(Number(w.onboardingSpinsByBet?.[key]) || 0));
  if (idx >= seq.length) return base;

  const scripted = Number(seq[idx]);
  w.onboardingSpinsByBet[key] = idx + 1;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();

  return {
    payoutAmount: Number.isFinite(scripted) ? scripted : base.payoutAmount,
    payoutOptions: base.payoutOptions,
    payoutWeights: base.payoutWeights
  };
}

function pickRewardBonusPayoutAmountForWallet(walletId, betAmount) {
  const bet = Number(betAmount);
  const base = pickPayoutAmountForWallet(walletId, bet);
  const normalPayoutOptions = Array.isArray(base?.payoutOptions) ? base.payoutOptions : (PAYOUT_TABLE?.[bet] || [0, bet]);
  const rewardBonusBalanceAfterBet = getRewardBonusBalance(walletId);
  const { band, sequence: seq } = getRewardBonusSequenceForBet(bet, rewardBonusBalanceAfterBet);
  if (!Array.isArray(seq) || seq.length === 0) return { ...base, rewardBonusOddsActive: false };

  const w = getWallet(walletId);
  if (!w.rewardBonusSpinsByBet || typeof w.rewardBonusSpinsByBet !== 'object') {
    w.rewardBonusSpinsByBet = {};
  }

  const key = `${String(bet)}:${band}`;
  const idx = Math.max(0, Math.floor(Number(w.rewardBonusSpinsByBet?.[key]) || 0));
  const scripted = Number(seq[idx % seq.length]);
  const maxAllowedPayout = Math.max(0, MAX_REWARD_BONUS_BANKROLL_SATS - rewardBonusBalanceAfterBet);
  const allowedPayouts = Array.from(new Set(
    normalPayoutOptions
      .map((n) => Number(n))
      .filter((n) => Number.isFinite(n) && n >= 0 && n <= maxAllowedPayout)
  )).sort((a, b) => a - b);
  const scriptedOrZero = Number.isFinite(scripted) ? scripted : 0;
  const cappedScripted = allowedPayouts.length
    ? allowedPayouts.reduce((best, value) => (value <= scriptedOrZero ? value : best), allowedPayouts[0])
    : 0;
  w.rewardBonusSpinsByBet[key] = idx + 1;
  w.updatedAt = new Date().toISOString();
  scheduleWalletStoreSave();
  scheduleLiabilitiesReportWrite();

  return {
    payoutAmount: cappedScripted,
    payoutOptions: base.payoutOptions,
    payoutWeights: null,
    rewardBonusOddsActive: true
  };
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

  const isLnAddr = String(withdrawRequest || '').includes('@');
  const payload = {
    amount: Math.floor(Number(amountSats)),
    currency: 'SATS',
    withdraw_method: 'lightning',
    withdraw_request: withdrawRequest,
    withdraw_type: isLnAddr ? 'lightning_address' : 'lightning_invoice',
    note: String(note || '').slice(0, 255)
  };

  try {
    const resp = await axios.post(`${SPEED_WALLET_API_BASE}/send`, payload, {
      headers: {
        Authorization: `Basic ${AUTH_HEADER}`,
        'Content-Type': 'application/json',
        'speed-version': '2022-04-15'
      },
      timeout: 15000
    });
    return resp.data;
  } catch (error) {
    const status = error.response?.status;
    const data = error.response?.data;
    
    // Speed API often returns error message in data.message or data.errors[0].message
    let errMsg = error.message;
    if (data) {
      if (typeof data === 'string') {
        errMsg = data;
      } else if (data.message) {
        errMsg = data.message;
      } else if (data.errors && Array.isArray(data.errors) && data.errors[0]?.message) {
        errMsg = data.errors[0].message;
      } else if (data.error?.message) {
        errMsg = data.error.message;
      }
    }

    console.error(`Speed Payout Error [${status || 'n/a'}]:`, {
      message: errMsg,
      payload,
      response: data
    });

    throw new Error(`${errMsg}${status ? ` (Status: ${status})` : ''}`);
  }
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

app.get('/', (req, res) => {
  res.json({ ok: true, service: 'btc-slides' });
});

app.get('/health', (req, res) => {
  try {
    const ua = String(req.get('user-agent') || '').toLowerCase();
    const src = String(req.query?.src || req.get('x-ping-source') || '').toLowerCase();
    const isCronLike = Boolean(
      src.includes('cron') ||
      src.includes('github') ||
      ua.includes('github-actions') ||
      ua.includes('curl/') ||
      ua.includes('uptimerobot')
    );

    if (isCronLike) {
      logLine('health_ping', {
        source: src || (ua.includes('uptimerobot') ? 'uptimerobot' : 'cron'),
        ua: ua || null,
        ip: String(req.ip || ''),
        uptimeSec: Math.floor(process.uptime())
      });
    } else {
      const nowMs = Date.now();
      if (nowMs - lastHealthBrowserLogMs > 60 * 1000) {
        lastHealthBrowserLogMs = nowMs;
        logLine('health_ping', {
          source: src || 'browser',
          ua: ua || null,
          ip: String(req.ip || ''),
          uptimeSec: Math.floor(process.uptime())
        });
      }
    }
  } catch {
  }
  res.json({ ok: true, uptimeSec: Math.floor(process.uptime()), now: new Date().toISOString() });
});

app.get('/webhook', (req, res) => {
  res.json({ ok: true });
});

app.get('/admin/wallet-store.json', requireAdmin, (req, res) => {
  res.json(buildWalletStorePayload());
});

app.get('/admin/treasury', requireAdmin, (req, res) => {
  res.json(buildTreasuryDashboard());
});

app.get('/admin/audit', requireAdmin, (req, res) => {
  const events = readAuditEvents({
    walletId: req.query?.walletId,
    type: req.query?.type,
    from: req.query?.from,
    to: req.query?.to,
    limit: req.query?.limit,
    offset: req.query?.offset
  });
  res.json({ ok: true, count: events.length, events });
});

app.get('/admin/audit.jsonl', requireAdmin, (req, res) => {
  try {
    if (!AUDIT_LOG_PATH || !fs.existsSync(AUDIT_LOG_PATH)) {
      res.setHeader('Content-Type', 'application/x-ndjson');
      return res.send('');
    }
    const raw = fs.readFileSync(AUDIT_LOG_PATH, 'utf8');
    res.setHeader('Content-Type', 'application/x-ndjson');
    return res.send(raw);
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
});

app.get('/admin/audit.csv', requireAdmin, (req, res) => {
  const events = readAuditEvents({
    walletId: req.query?.walletId,
    type: req.query?.type,
    from: req.query?.from,
    to: req.query?.to,
    limit: req.query?.limit,
    offset: req.query?.offset
  });

  const header = [
    'ts',
    'type',
    'walletId',
    'lightningAddress',
    'invoiceId',
    'betAmount',
    'payoutAmount',
    'amountSats',
    'balanceBeforeSats',
    'balanceAfterSats',
    'recipient',
    'reason',
    'error'
  ];

  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [header.join(',')];
  for (const e of events) {
    lines.push([
      esc(e?.ts),
      esc(e?.type),
      esc(e?.walletId),
      esc(e?.lightningAddress),
      esc(e?.invoiceId),
      esc(e?.betAmount),
      esc(e?.payoutAmount),
      esc(e?.amountSats),
      esc(e?.balanceBeforeSats),
      esc(e?.balanceAfterSats),
      esc(e?.recipient),
      esc(e?.reason),
      esc(e?.error)
    ].join(','));
  }

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="btc-slides-audit.csv"');
  res.send(`${lines.join('\n')}\n`);
});

const invoiceToSocket = new Map();
const roundsByInvoice = new Map();
const walletToSocket = new Map();

const lastSpinAtByWallet = new Map();
const SPIN_REQUEST_COOLDOWN_MS = Math.max(150, Number(process.env.SPIN_REQUEST_COOLDOWN_MS) || 500);

const PENDING_WITHDRAWAL_STALE_MS = Math.max(
  60 * 1000,
  Number(process.env.PENDING_WITHDRAWAL_STALE_MS) || 10 * 60 * 1000
);

let lastHealthBrowserLogMs = 0;

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
    disableRewardBonusForWallet(round.walletId, 'topup');
    const next = setWalletBalance(round.walletId, prev + topupAmount);

    appendAuditEvent({
      type: 'topup_credited',
      invoiceId,
      walletId: round.walletId,
      lightningAddress: round.lightningAddress,
      amountSats: topupAmount,
      balanceBeforeSats: prev,
      balanceAfterSats: next
    });

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
        ...buildWalletBalancePayload(round.walletId),
        lightningAddress: round.lightningAddress
      });
      sock.emit('topUpConfirmed', {
        invoiceId,
        walletId: round.walletId,
        amountSats: topupAmount,
        balanceSats: next,
        ...buildRewardBonusState(round.walletId)
      });
    }

    scheduleRoundCleanup(invoiceId);
    return { ok: true, credited: true, topupAmount, balanceSats: next };
  }

  if (!Number.isFinite(Number(round.payoutAmount))) {
    const { payoutAmount, payoutOptions, payoutWeights } = round.walletId
      ? pickPayoutAmountForWallet(round.walletId, round.betAmount)
      : pickPayoutAmount(round.betAmount);
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

  if (round.walletId) {
    const prev = getWalletBalance(round.walletId);
    const credited = payoutAmount > 0 ? setWalletBalance(round.walletId, prev + payoutAmount) : prev;
    round.status = 'payout_sent';
    round.payoutResponse = { creditedToWallet: true, balanceSats: credited };

    if (sock) {
      const w = getWallet(round.walletId);
      sock.emit('walletBalance', {
        walletId: w.walletId,
        lightningAddress: w.lightningAddress,
        balanceSats: credited
      });

      sock.emit('payoutSent', {
        invoiceId,
        payoutAmount,
        recipient: 'wallet',
        creditedToWallet: true,
        balanceSats: credited,
        payoutResponse: null
      });
    }
  } else {
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

async function handleWebhookEvent(event) {
  const eventType = event?.event_type;
  const invoiceId = extractInvoiceIdFromEvent(event);

  if (eventType === 'payment.failed') {
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
    return;
  }

  if (!invoiceId) return;

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

  if (!round) return;
  if (round.status === 'payout_sent' || round.status === 'credited') return;

  const { paid } = await verifyInvoicePaidWithSpeed(invoiceId);
  if (!paid) return;

  await processPaidInvoice(invoiceId, { paidVerified: true });
}

app.post('/webhook', express.json(), (req, res) => {
  const event = req.body;
  const eventType = event?.event_type || null;
  const invoiceId = extractInvoiceIdFromEvent(event);
  logLine('webhook_received', { eventType, invoiceId });
  res.status(200).send('ok');

  setImmediate(() => {
    logLine('webhook_process_start', { eventType, invoiceId });
    handleWebhookEvent(event)
      .then(() => {
        logLine('webhook_process_done', { eventType, invoiceId });
      })
      .catch((e) => {
        logLine('webhook_process_error', { eventType, invoiceId, error: String(e?.message || e) });
        console.warn(`Webhook processing error: ${String(e?.message || e)}`);
      });
  });
});

const server = http.createServer(app);

server.keepAliveTimeout = 65 * 1000;
server.headersTimeout = 70 * 1000;

const io = new Server(server, {
  cors: {
    origin: corsOrigins,
    credentials: true,
    methods: ['GET', 'POST']
  },
  pingInterval: 25000,
  pingTimeout: 20000
});

io.on('connection', (socket) => {
  socket.emit('serverInfo', {
    betOptions: BET_OPTIONS,
    topUpOptions: TOPUP_OPTIONS,
    payoutTable: PAYOUT_TABLE,
    payoutWeights: PAYOUT_WEIGHTS,
    newUserRewardSats: NEW_USER_REWARD_SATS
  });

  socket.on('getWalletBalance', ({ walletId, walletSecret, lightningAddress }) => {
    try {
      ensureWalletAuth(walletId, walletSecret);
      const w = getWallet(walletId);
      if (lightningAddress) bindWalletAddress(w.walletId, lightningAddress);
      walletToSocket.set(w.walletId, socket.id);
      const rewardGrant = maybeGrantNewUserReward(w.walletId);
      logLine('wallet_balance', {
        walletId: w.walletId,
        lightningAddress: w.lightningAddress,
        balanceSats: getWalletBalance(w.walletId),
        socketId: socket.id
      });
      socket.emit('walletBalance', buildWalletBalancePayload(w.walletId));
      if (rewardGrant?.granted) {
        socket.emit('newUserRewardGranted', {
          walletId: w.walletId,
          lightningAddress: w.lightningAddress,
          rewardSats: rewardGrant.amountSats,
          balanceSats: rewardGrant.balanceSats,
          rewardBonusBalanceSats: rewardGrant.rewardBonusBalanceSats
        });
      }
    } catch {
      socket.emit('errorMessage', { message: 'Invalid wallet credentials' });
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

      logLine('topup_invoice_created', {
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        amountSats: amount,
        invoiceId: invoiceData?.invoiceId || null,
        socketId: socket.id
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

      const nowMs = Date.now();
      const lastMs = Number(lastSpinAtByWallet.get(w.walletId)) || 0;
      if (nowMs - lastMs < SPIN_REQUEST_COOLDOWN_MS) {
        throw new Error('Spin already in progress. Please wait.');
      }
      lastSpinAtByWallet.set(w.walletId, nowMs);

      walletToSocket.set(w.walletId, socket.id);
      noteWalletActivity(w.walletId);

      const current = getWalletBalance(w.walletId);
      if (current < bet) {
        throw new Error(`Insufficient wallet balance. Add ${bet - current} SATS to play.`);
      }

      const next = setWalletBalance(w.walletId, current - bet);
      const rewardBonusActiveBeforeSpin = isRewardBonusOddsActive(w);
      const rewardBonusBalanceBeforeSpin = getRewardBonusBalance(w.walletId);
      let rewardBonusActiveForPayout = rewardBonusActiveBeforeSpin;
      if (rewardBonusActiveBeforeSpin) {
        setRewardBonusBalance(w.walletId, Math.max(0, rewardBonusBalanceBeforeSpin - bet));
        const nextRewardBonusWagered = setRewardBonusWagered(w.walletId, getRewardBonusWagered(w.walletId) + bet);
        const rewardBonusWagerLimit = getRewardBonusWagerLimitForBet(bet);
        if (rewardBonusWagerLimit > 0 && nextRewardBonusWagered >= rewardBonusWagerLimit) {
          disableRewardBonusForWallet(w.walletId, 'wager_limit_reached');
          rewardBonusActiveForPayout = false;
        }
      }
      socket.emit('walletBalance', buildWalletBalancePayload(w.walletId));

      appendAuditEvent({
        type: 'spin_bet',
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        betAmount: bet,
        balanceBeforeSats: current,
        balanceAfterSats: next
      });

      const { payoutAmount, payoutOptions, payoutWeights, rewardBonusOddsActive } = rewardBonusActiveForPayout
        ? pickRewardBonusPayoutAmountForWallet(w.walletId, bet)
        : pickPayoutAmountForWallet(w.walletId, bet);
      socket.emit('spinOutcome', {
        betAmount: bet,
        payoutAmount,
        payoutOptions,
        payoutWeights,
        rewardBonusOddsActive: Boolean(rewardBonusActiveForPayout && rewardBonusOddsActive !== false)
      });

      logLine('spin_outcome', {
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        betAmount: bet,
        payoutAmount,
        balanceAfterBetSats: next,
        socketId: socket.id
      });

      const creditedBalance = payoutAmount > 0
        ? setWalletBalance(w.walletId, next + payoutAmount)
        : next;
      if (rewardBonusActiveForPayout && payoutAmount > 0) {
        setRewardBonusBalance(w.walletId, getRewardBonusBalance(w.walletId) + payoutAmount);
      }

      socket.emit('walletBalance', buildWalletBalancePayload(w.walletId));
      socket.emit('payoutSent', {
        payoutAmount,
        recipient: 'wallet',
        creditedToWallet: true,
        balanceSats: creditedBalance,
        payoutResponse: null,
        ...buildRewardBonusState(w.walletId)
      });

      appendAuditEvent({
        type: 'spin_payout',
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        betAmount: bet,
        payoutAmount,
        balanceBeforeSats: next,
        balanceAfterSats: creditedBalance,
        recipient: 'wallet'
      });
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

      socket.emit('walletBalance', buildWalletBalancePayload(w.walletId));
      socket.emit('withdrawalPending', { walletId: w.walletId, amountSats: amount, recipient: formattedAddress });

      logLine('withdraw_pending', {
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        amountSats: amount,
        balanceAfterSats: 0,
        socketId: socket.id
      });

      appendAuditEvent({
        type: 'withdraw_requested',
        walletId: w.walletId,
        lightningAddress: formattedAddress,
        amountSats: amount,
        balanceBeforeSats: amount,
        balanceAfterSats: 0,
        recipient: formattedAddress,
        reason: 'manual_withdraw'
      });

      try {
        logLine('withdraw_send_start', {
          walletId: w.walletId,
          lightningAddress: formattedAddress,
          amountSats: amount,
          socketId: socket.id
        });
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

        socket.emit('withdrawalSent', {
          walletId: w.walletId,
          amountSats: amount,
          recipient: formattedAddress,
          payoutResponse: payoutResp,
          balanceSats: 0,
          ...buildRewardBonusState(w.walletId)
        });

        logLine('withdraw_sent', {
          walletId: w.walletId,
          lightningAddress: formattedAddress,
          amountSats: amount,
          balanceAfterSats: 0,
          socketId: socket.id
        });

        appendAuditEvent({
          type: 'withdraw_sent',
          walletId: w.walletId,
          lightningAddress: formattedAddress,
          amountSats: amount,
          balanceBeforeSats: amount,
          balanceAfterSats: 0,
          recipient: formattedAddress,
          reason: 'manual_withdraw'
        });
      } catch (e) {
        setWalletHold(w.walletId, 0);
        setWalletBalance(w.walletId, amount);
        w.pendingWithdrawal = null;
        w.updatedAt = new Date().toISOString();
        scheduleWalletStoreSave();
        scheduleLiabilitiesReportWrite();

        socket.emit('walletBalance', buildWalletBalancePayload(w.walletId));
        socket.emit('withdrawalFailed', {
          walletId: w.walletId,
          amountSats: amount,
          recipient: formattedAddress,
          error: String(e?.message || e),
          ...buildRewardBonusState(w.walletId)
        });

        logLine('withdraw_failed', {
          walletId: w.walletId,
          lightningAddress: formattedAddress,
          amountSats: amount,
          balanceAfterSats: amount,
          socketId: socket.id,
          error: String(e?.message || e)
        });

        appendAuditEvent({
          type: 'withdraw_failed',
          walletId: w.walletId,
          lightningAddress: formattedAddress,
          amountSats: amount,
          balanceBeforeSats: 0,
          balanceAfterSats: amount,
          recipient: formattedAddress,
          reason: 'manual_withdraw',
          error: String(e?.message || e)
        });
      }
    } catch (error) {
      socket.emit('errorMessage', { message: error.message });
    }
  });
});

let pendingWithdrawalsSweepRunning = false;

async function runPendingWithdrawalSweep() {
  if (pendingWithdrawalsSweepRunning) return;
  pendingWithdrawalsSweepRunning = true;
  try {
    let revertedCount = 0;
    const nowMs = Date.now();
    const wallets = Array.from(walletsById.values());

    const playersWithBalance = wallets.filter(w => (Number(w?.balanceSats) || 0) > 0);
    const totalSatsInAccounts = playersWithBalance.reduce((sum, w) => sum + (Number(w?.balanceSats) || 0), 0);
    
    logLine('balance_stats', {
      playerCountWithBalance: playersWithBalance.length,
      totalSatsInAccounts,
      totalWallets: wallets.length
    });

    for (const w of wallets) {
      const walletId = String(w?.walletId || '');
      if (!walletId) continue;

      const hold = Math.max(0, Number(w?.holdSats) || 0);
      const pending = w?.pendingWithdrawal;
      if (pending && hold > 0) {
        const reqMs = Date.parse(String(pending?.requestedAt || ''));
        if (Number.isFinite(reqMs) && nowMs - reqMs > PENDING_WITHDRAWAL_STALE_MS) {
          const prevBal = Math.max(0, Number(w?.balanceSats) || 0);
          const nextBal = setWalletBalance(walletId, prevBal + hold);
          setWalletHold(walletId, 0);
          w.pendingWithdrawal = null;
          w.updatedAt = new Date().toISOString();
          scheduleWalletStoreSave();
          scheduleLiabilitiesReportWrite();

          const sockId = walletToSocket.get(walletId);
          const sock = sockId && io.sockets.sockets.get(sockId);
          if (sock) {
            sock.emit('walletBalance', { walletId, lightningAddress: w?.lightningAddress || null, balanceSats: nextBal });
          }

          appendAuditEvent({
            type: 'pending_withdrawal_reverted',
            walletId,
            lightningAddress: w?.lightningAddress || null,
            amountSats: hold,
            balanceBeforeSats: prevBal,
            balanceAfterSats: nextBal,
            reason: String(pending?.reason || 'unknown')
          });

          revertedCount += 1;
        }
      }
    }

    if (revertedCount) {
      logLine('pending_withdrawal_sweep', { revertedCount });
    }
  } finally {
    pendingWithdrawalsSweepRunning = false;
  }
}

const port = Number(process.env.PORT || 3001);

if (!SPEED_WALLET_SECRET_KEY) {
  console.warn('SPEED_WALLET_SECRET_KEY is not set. Withdrawals will fail until configured.');
}

if (!SPEED_WALLET_WEBHOOK_SECRET) {
  console.warn('SPEED_WALLET_WEBHOOK_SECRET is not set.');
}

let shuttingDown = false;
function beginShutdown(signal) {
  try {
    if (shuttingDown) return;
    shuttingDown = true;
    logLine('shutdown', { signal });
    try {
      if (walletStoreSaveTimer) {
        clearTimeout(walletStoreSaveTimer);
        walletStoreSaveTimer = null;
      }
      saveWalletStore();
      writeLiabilitiesReport();
    } catch {
    }
    try {
      server.close(() => process.exit(0));
      setTimeout(() => process.exit(0), 3000).unref();
    } catch {
      process.exit(0);
    }
  } catch {
    process.exit(0);
  }
}

process.on('SIGTERM', () => beginShutdown('SIGTERM'));
process.on('SIGINT', () => beginShutdown('SIGINT'));

async function main() {
  await bootstrapWalletStoreIfMissing();
  await bootstrapAuditLogIfMissing();
  loadWalletStore();
  runPendingWithdrawalSweep().catch(() => {});
  setInterval(() => {
    runPendingWithdrawalSweep().catch(() => {});
  }, 60 * 1000);
  server.listen(port, () => {
    console.log(`BTC Slides backend listening on :${port}`);
  });
}

main().catch((e) => {
  console.error(String(e?.message || e));
  loadWalletStore();
  runPendingWithdrawalSweep().catch(() => {});
  setInterval(() => {
    runPendingWithdrawalSweep().catch(() => {});
  }, 60 * 1000);
  server.listen(port, () => {
    console.log(`BTC Slides backend listening on :${port}`);
  });
});
