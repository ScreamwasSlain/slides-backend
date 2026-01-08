const dotenv = require('dotenv');

dotenv.config();

const http = require('http');
const crypto = require('crypto');

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

const PAYOUT_TABLE = {
  20: [0, 20, 50, 80, 100],
  100: [0, 50, 120, 200, 300],
  300: [0, 100, 350, 500, 700],
  500: [0, 200, 400, 800, 1200],
  1000: [0, 300, 1000, 1500, 3000],
  5000: [0, 1000, 3000, 5000, 11000],
  10000: [0, 2000, 5000, 12000, 30000]
};

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

function pickPayoutAmount(betAmount) {
  const opts = PAYOUT_TABLE[betAmount] || [0, betAmount];
  const idx = crypto.randomInt(0, opts.length);
  return { payoutAmount: opts[idx], payoutOptions: opts };
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
  const addr = String(md.Lightning_Address || md.lightning_address || md.lightningAddress || '').trim().toLowerCase();
  const bet = Number(md.Amount_SATS || md.amount_sats || details?.amount || details?.amount_sats);

  if (!addr || !addr.includes('@')) return null;
  if (!Number.isFinite(bet) || bet <= 0) return null;
  if (!BET_OPTIONS.includes(bet)) return null;

  return {
    roundId: String(md.Order_ID || `recovered_${invoiceId}`),
    socketId: socketId || null,
    invoiceId,
    lightningAddress: formatLightningAddress(addr),
    betAmount: bet,
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

  if (!Number.isFinite(Number(round.payoutAmount))) {
    const { payoutAmount, payoutOptions } = pickPayoutAmount(round.betAmount);
    round.payoutAmount = payoutAmount;
    round.payoutOptions = payoutOptions;
  }

  const spinOutcome = {
    invoiceId,
    betAmount: round.betAmount,
    payoutAmount: round.payoutAmount,
    payoutOptions: round.payoutOptions || PAYOUT_TABLE[round.betAmount] || [0, round.betAmount]
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
      const round = roundsByInvoice.get(invoiceId);
      if (!round) return res.status(200).send('Webhook received (unknown invoice)');
      if (round.status === 'payout_sent') {
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
    betOptions: BET_OPTIONS
  });

  socket.on('startSpin', async ({ lightningAddress, betAmount }) => {
    try {
      const bet = Number(betAmount);
      if (!BET_OPTIONS.includes(bet)) throw new Error('Invalid bet amount');
      const formattedAddress = formatLightningAddress(lightningAddress);

      const roundId = `round_${Date.now()}_${socket.id}`;
      const invoiceData = await createLightningInvoice(bet, `order_${roundId}`, {
        Lightning_Address: formattedAddress
      });

      const round = {
        roundId,
        socketId: socket.id,
        invoiceId: invoiceData.invoiceId,
        lightningAddress: formattedAddress,
        betAmount: bet,
        status: 'invoice_created',
        createdAt: new Date().toISOString()
      };

      roundsByInvoice.set(invoiceData.invoiceId, round);
      invoiceToSocket.set(invoiceData.invoiceId, socket.id);

      socket.emit('paymentRequest', {
        invoiceId: invoiceData.invoiceId,
        amountSats: bet,
        lightningInvoice: invoiceData.lightningInvoice,
        hostedInvoiceUrl: invoiceData.hostedInvoiceUrl,
        speedInterfaceUrl: invoiceData.speedInterfaceUrl
      });

      setTimeout(() => {
        const r = roundsByInvoice.get(invoiceData.invoiceId);
        if (r && r.status === 'invoice_created') {
          roundsByInvoice.delete(invoiceData.invoiceId);
          invoiceToSocket.delete(invoiceData.invoiceId);
          socket.emit('paymentExpired', { invoiceId: invoiceData.invoiceId });
        }
      }, 10 * 60 * 1000);
    } catch (error) {
      socket.emit('errorMessage', { message: error.message });
    }
  });
});

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
