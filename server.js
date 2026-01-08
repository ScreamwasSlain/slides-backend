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
  100: [0, 50, 100, 200, 500],
  300: [0, 100, 200, 300, 600, 1000],
  500: [0, 200, 500, 800, 1200, 2000],
  1000: [0, 500, 1000, 1500, 2500, 5000],
  5000: [0, 2000, 5000, 8000, 12000, 20000],
  10000: [0, 5000, 10000, 15000, 25000, 50000]
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

async function createLightningInvoice(amountSats, orderId) {
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
      Amount_SATS: String(amountSats)
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

    if (!lightningInvoice && hostedInvoiceUrl) lightningInvoice = hostedInvoiceUrl;

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

function scheduleRoundCleanup(invoiceId, delayMs = 30 * 60 * 1000) {
  setTimeout(() => {
    roundsByInvoice.delete(invoiceId);
    invoiceToSocket.delete(invoiceId);
  }, delayMs);
}

app.post('/webhook', express.json(), async (req, res) => {
  const event = req.body;
  const eventType = event?.event_type;

  try {
    if (eventType === 'invoice.paid' || eventType === 'payment.paid' || eventType === 'payment.confirmed') {
      const invoiceId = event?.data?.object?.id || event?.data?.id;
      if (!invoiceId) return res.status(400).send('No invoiceId in webhook payload');

      const round = roundsByInvoice.get(invoiceId);
      if (!round) return res.status(200).send('Webhook received (unknown invoice)');
      if (round.status === 'paid' || round.status === 'payout_sent') {
        return res.status(200).send('Webhook received (already processed)');
      }

      round.status = 'paid';

      const socketId = invoiceToSocket.get(invoiceId) || round.socketId;
      const sock = socketId && io.sockets.sockets.get(socketId);
      if (sock) sock.emit('paymentVerified');

      const { payoutAmount, payoutOptions } = pickPayoutAmount(round.betAmount);
      round.payoutAmount = payoutAmount;

      if (sock) {
        sock.emit('spinOutcome', {
          betAmount: round.betAmount,
          payoutAmount,
          payoutOptions
        });
      }

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
            payoutAmount: 0,
            recipient: round.lightningAddress,
            payoutResponse: null
          });
        }
      }

      invoiceToSocket.delete(invoiceId);
      scheduleRoundCleanup(invoiceId);
    }

    if (eventType === 'payment.failed') {
      const invoiceId = event?.data?.object?.id || event?.data?.id;
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
      const invoiceData = await createLightningInvoice(bet, `order_${roundId}`);

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
