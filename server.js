// ============================================================
// Maricopa · Inventory + Orders Management API
// server.js
// ============================================================
// Run with:  node server.js
// Requires:  npm install express cors helmet express-rate-limit
// Optional:  npm install compression
// ============================================================

'use strict';

// ============================================================
// 1. DEPENDENCIES
// ============================================================
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

// Optional compression — gracefully skipped if not installed
let compression = null;
try { compression = require('compression'); } catch { /* optional */ }

// ============================================================
// 2. CONFIGURATION + VALIDATION
// ============================================================
function loadConfig(env = process.env) {
  const cfg = {
    PORT: Number(env.PORT) || 3000,
    HOST: env.HOST || '0.0.0.0',
    NODE_ENV: env.NODE_ENV || 'development',
    DATA_DIR: path.join(__dirname, 'data'),
    DB_FILE: path.join(__dirname, 'data', 'inventory.json'),
    ORDERS_DB_FILE: path.join(__dirname, 'data', 'orders.json'),
    PUBLIC_DIR: path.join(__dirname, 'public'),
    BODY_LIMIT: env.BODY_LIMIT || '1mb',
    SHUTDOWN_TIMEOUT_MS: Number(env.SHUTDOWN_TIMEOUT_MS) || 5000,
    RATE_LIMIT_WINDOW_MS: Number(env.RATE_LIMIT_WINDOW_MS) || 60 * 1000,
    RATE_LIMIT_MAX_PROD: Number(env.RATE_LIMIT_MAX_PROD) || 120,
    RATE_LIMIT_MAX_DEV: Number(env.RATE_LIMIT_MAX_DEV) || 1000,
    VALID_CATEGORIES: Object.freeze([
      'Ready-mix', 'Precast', 'Cement',
      'Chemicals', 'Aggregates', 'Steel', 'Other'
    ]),
    VALID_ORDER_STATUSES: Object.freeze([
      'pending', 'processing', 'shipped', 'delivered', 'cancelled'
    ]),
    CORS_ORIGINS: env.CORS_ORIGINS
      ? env.CORS_ORIGINS.split(',').map(s => s.trim()).filter(Boolean)
      : ['*']
  };

  cfg.IS_PROD = cfg.NODE_ENV === 'production';

  // Fail-fast validation
  if (!Number.isFinite(cfg.PORT) || cfg.PORT <= 0 || cfg.PORT > 65535) {
    throw new Error(`Invalid PORT: ${env.PORT}`);
  }
  if (!Number.isFinite(cfg.SHUTDOWN_TIMEOUT_MS) || cfg.SHUTDOWN_TIMEOUT_MS < 1000) {
    throw new Error('SHUTDOWN_TIMEOUT_MS must be >= 1000');
  }

  return Object.freeze(cfg);
}

const CONFIG = loadConfig();

// ============================================================
// 3. STRUCTURED LOGGER + METRICS
// ============================================================
function createLogger(isProd) {
  const write = (level, msg, meta = {}) => {
    const entry = { ts: new Date().toISOString(), level, msg, ...meta };
    const line = isProd
      ? JSON.stringify(entry)
      : `[${entry.ts}] ${level.toUpperCase()} ${msg}` +
        (Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '');
    (level === 'error' ? console.error : console.log)(line);
  };
  return {
    info:  (msg, meta) => write('info', msg, meta),
    warn:  (msg, meta) => write('warn', msg, meta),
    error: (msg, meta) => write('error', msg, meta),
    debug: (msg, meta) => { if (!isProd) write('debug', msg, meta); }
  };
}

function createMetrics() {
  const counters = new Map();
  const histograms = new Map();

  const bump = (key, by = 1) => {
    counters.set(key, (counters.get(key) || 0) + by);
  };

  const record = (key, value) => {
    if (!histograms.has(key)) histograms.set(key, { count: 0, sum: 0, min: Infinity, max: -Infinity });
    const h = histograms.get(key);
    h.count += 1;
    h.sum += value;
    if (value < h.min) h.min = value;
    if (value > h.max) h.max = value;
  };

  return {
    increment: bump,
    observe: record,
    snapshot: () => ({
      counters: Object.fromEntries(counters),
      histograms: Object.fromEntries(
        [...histograms.entries()].map(([k, h]) => [
          k,
          { count: h.count, avg: h.count ? h.sum / h.count : 0, min: h.min === Infinity ? 0 : h.min, max: h.max === -Infinity ? 0 : h.max }
        ])
      )
    })
  };
}

// ============================================================
// 4. REPOSITORIES — pluggable persistence (inventory + orders)
// ============================================================
function createJsonFileRepository(filePath, config, logger, label = 'data') {
  return {
    async ensure() {
      await fs.mkdir(config.DATA_DIR, { recursive: true });
      try {
        await fs.access(filePath);
      } catch {
        await fs.writeFile(filePath, '[]', 'utf8');
      }
    },

    async readAll() {
      await this.ensure();
      const raw = await fs.readFile(filePath, 'utf8');
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (err) {
        logger.warn(`Corrupt ${label} file — resetting`, { error: err.message, file: filePath });
        await fs.writeFile(filePath, '[]', 'utf8');
        return [];
      }
    },

    async writeAll(items) {
      await this.ensure();
      const tmp = filePath + '.tmp';
      await fs.writeFile(tmp, JSON.stringify(items, null, 2), 'utf8');
      await fs.rename(tmp, filePath); // atomic swap
    },

    async findById(id) {
      const items = await this.readAll();
      return items.find(i => i.id === id) || null;
    },

    async create(item) {
      const items = await this.readAll();
      items.push(item);
      await this.writeAll(items);
      return item;
    },

    async update(id, patch) {
      const items = await this.readAll();
      const idx = items.findIndex(i => i.id === id);
      if (idx === -1) return null;
      items[idx] = patch;
      await this.writeAll(items);
      return items[idx];
    },

    async remove(id) {
      const items = await this.readAll();
      const idx = items.findIndex(i => i.id === id);
      if (idx === -1) return null;
      const [removed] = items.splice(idx, 1);
      await this.writeAll(items);
      return removed;
    },

    async clear() {
      await this.writeAll([]);
    },

    async count() {
      const items = await this.readAll();
      return items.length;
    }
  };
}

function createInventoryRepository(config, logger) {
  const repo = createJsonFileRepository(config.DB_FILE, config, logger, 'inventory');
  repo.findBySku = async function(sku, excludeId = null) {
    const items = await this.readAll();
    const needle = String(sku).toLowerCase();
    return items.find(i =>
      String(i.sku).toLowerCase() === needle && i.id !== excludeId
    ) || null;
  };
  return repo;
}

function createOrderRepository(config, logger) {
  const repo = createJsonFileRepository(config.ORDERS_DB_FILE, config, logger, 'orders');
  // Optional: enforce unique reference, if you want it later
  repo.findByReference = async function(reference, excludeId = null) {
    if (!reference) return null;
    const items = await this.readAll();
    const needle = String(reference).trim().toLowerCase();
    return items.find(i =>
      String(i.reference || '').trim().toLowerCase() === needle && i.id !== excludeId
    ) || null;
  };
  return repo;
}

// ============================================================
// 5. VALIDATION + NORMALISATION
// ============================================================
function createValidators(config) {
  const REQUIRED_FIELDS = Object.freeze(['name', 'sku', 'category', 'quantity', 'price']);

  function validateItem(body, { partial = false } = {}) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return ['request body must be a JSON object'];
    }

    const errors = [];

    if (!partial) {
      for (const field of REQUIRED_FIELDS) {
        const v = body[field];
        if (v === undefined || v === null || v === '') {
          errors.push(`${field} is required`);
        }
      }
    }

    if (body.name !== undefined && (typeof body.name !== 'string' || !body.name.trim())) {
      errors.push('name must be a non-empty string');
    }
    if (body.sku !== undefined && (typeof body.sku !== 'string' || !body.sku.trim())) {
      errors.push('sku must be a non-empty string');
    }
    if (body.category !== undefined && !config.VALID_CATEGORIES.includes(body.category)) {
      errors.push(`category must be one of: ${config.VALID_CATEGORIES.join(', ')}`);
    }
    if (body.quantity !== undefined && (isNaN(Number(body.quantity)) || Number(body.quantity) < 0)) {
      errors.push('quantity must be a non-negative number');
    }
    if (body.price !== undefined && (isNaN(Number(body.price)) || Number(body.price) < 0)) {
      errors.push('price must be a non-negative number');
    }
    if (body.threshold !== undefined && body.threshold !== '' &&
        (isNaN(Number(body.threshold)) || Number(body.threshold) < 0)) {
      errors.push('threshold must be a non-negative number');
    }

    return errors;
  }

  function normaliseItem(body) {
    return {
      name: String(body.name).trim(),
      sku: String(body.sku).trim(),
      category: body.category,
      quantity: Number(body.quantity),
      unit: body.unit ? String(body.unit).trim() : 'units',
      price: Number(body.price),
      threshold: body.threshold === undefined || body.threshold === ''
        ? 10
        : Number(body.threshold),
      supplier: body.supplier ? String(body.supplier).trim() : '',
      notes: body.notes ? String(body.notes).trim() : ''
    };
  }

  // ---- Orders ----
  const REQUIRED_ORDER_FIELDS = Object.freeze([
    'customer', 'status', 'date', 'items', 'total'
  ]);

  function isValidISODate(str) {
    if (typeof str !== 'string') return false;
    // Accept YYYY-MM-DD or full ISO
    if (!/^\d{4}-\d{2}-\d{2}/.test(str)) return false;
    const d = new Date(str);
    return !isNaN(d.getTime());
  }

  function validateOrder(body, { partial = false } = {}) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return ['request body must be a JSON object'];
    }

    const errors = [];

    if (!partial) {
      for (const field of REQUIRED_ORDER_FIELDS) {
        const v = body[field];
        if (v === undefined || v === null || v === '') {
          errors.push(`${field} is required`);
        }
      }
    }

    if (body.customer !== undefined &&
        (typeof body.customer !== 'string' || !body.customer.trim())) {
      errors.push('customer must be a non-empty string');
    }
    if (body.status !== undefined && !config.VALID_ORDER_STATUSES.includes(body.status)) {
      errors.push(`status must be one of: ${config.VALID_ORDER_STATUSES.join(', ')}`);
    }
    if (body.date !== undefined && !isValidISODate(body.date)) {
      errors.push('date must be a valid ISO date (YYYY-MM-DD)');
    }
    if (body.items !== undefined &&
        (typeof body.items !== 'string' || !body.items.trim())) {
      errors.push('items must be a non-empty string');
    }
    if (body.total !== undefined &&
        (isNaN(Number(body.total)) || Number(body.total) < 0)) {
      errors.push('total must be a non-negative number');
    }

    return errors;
  }

  function normaliseOrder(body) {
    return {
      customer: String(body.customer).trim(),
      status: body.status,
      date: String(body.date).slice(0, 10),   // store YYYY-MM-DD
      items: String(body.items).trim(),
      total: Number(body.total),
      reference: body.reference ? String(body.reference).trim() : '',
      notes: body.notes ? String(body.notes).trim() : ''
    };
  }

  return { validateItem, normaliseItem, validateOrder, normaliseOrder };
}

// ============================================================
// 6. ERROR CLASS + ASYNC WRAPPER
// ============================================================
class HttpError extends Error {
  constructor(status, message, details = null) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.details = details;
  }
}

const asyncHandler = fn => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

// ============================================================
// 7. MIDDLEWARE FACTORIES (unchanged from your version)
// ============================================================
function createMiddlewares(config, logger, metrics) {
  function securityHeaders() {
    return helmet({
      contentSecurityPolicy: {
        useDefaults: false,
        directives: {
          defaultSrc: ["'self'"],
          scriptSrc: ["'self'", "'unsafe-inline'"],
          styleSrc: ["'self'", "'unsafe-inline'", 'https://cdnjs.cloudflare.com', 'https://cdn.jsdelivr.net'],
          imgSrc: ["'self'", 'data:', 'https:'],
          fontSrc: ["'self'", 'https://cdnjs.cloudflare.com'],
          connectSrc: ["'self'", ...config.CORS_ORIGINS.filter(o => o !== '*')],
          objectSrc: ["'none'"],
          baseUri: ["'self'"],
          frameAncestors: ["'self'"]
        }
      },
      crossOriginEmbedderPolicy: false
    });
  }

  function corsMiddleware() {
    const isWildcard = config.CORS_ORIGINS.includes('*');
    return cors({
      origin: isWildcard ? true : config.CORS_ORIGINS,
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Accept', 'X-Request-Id'],
      exposedHeaders: ['X-Request-Id'],
      maxAge: 86400
    });
  }

  function bodyParsers() {
    return [
      express.json({ limit: config.BODY_LIMIT }),
      express.urlencoded({ extended: true, limit: config.BODY_LIMIT })
    ];
  }

  function requestId() {
    return (req, res, next) => {
      req.id = req.headers['x-request-id'] || crypto.randomBytes(8).toString('hex');
      res.setHeader('X-Request-Id', req.id);
      next();
    };
  }

  function requestLogger() {
    return (req, res, next) => {
      if (req.path === '/api/health') return next();
      const start = process.hrtime.bigint();
      res.on('finish', () => {
        const ms = Number(process.hrtime.bigint() - start) / 1e6;
        logger.info('request', {
          id: req.id,
          method: req.method,
          path: req.originalUrl,
          status: res.statusCode,
          durationMs: Number(ms.toFixed(1)),
          ip: req.ip
        });
        metrics.increment(`http.${req.method}.${res.statusCode}`);
        metrics.observe('http.duration_ms', ms);
      });
      next();
    };
  }

  function rateLimiter() {
    return rateLimit({
      windowMs: config.RATE_LIMIT_WINDOW_MS,
      max: config.IS_PROD ? config.RATE_LIMIT_MAX_PROD : config.RATE_LIMIT_MAX_DEV,
      standardHeaders: true,
      legacyHeaders: false,
      handler: (req, res) => {
        metrics.increment('http.429');
        res.status(429).json({ error: 'Too many requests. Please slow down.' });
      }
    });
  }

  function apiNotFound() {
    return (req, res, next) => {
      if (!req.path.startsWith('/api/')) return next();
      res.status(404).json({ error: `Unknown API route: ${req.method} ${req.path}` });
    };
  }

  function errorHandler() {
    // eslint-disable-next-line no-unused-vars
    return (err, req, res, next) => {
      if (err.type === 'entity.parse.failed') {
        metrics.increment('http.400');
        return res.status(400).json({ error: 'Malformed JSON in request body' });
      }
      if (err.type === 'entity.too.large') {
        metrics.increment('http.413');
        return res.status(413).json({ error: 'Request body too large' });
      }
      if (err instanceof HttpError) {
        metrics.increment(`http.${err.status}`);
        const body = { error: err.message };
        if (err.details) body.details = err.details;
        return res.status(err.status).json(body);
      }
      metrics.increment('http.500');
      logger.error('Unhandled error', {
        id: req.id,
        message: err.message,
        stack: err.stack
      });
      res.status(500).json({ error: 'Internal server error' });
    };
  }

  return {
    securityHeaders,
    corsMiddleware,
    bodyParsers,
    requestId,
    requestLogger,
    rateLimiter,
    apiNotFound,
    errorHandler
  };
}

// ============================================================
// 8. ROUTES
// ============================================================
function createInventoryRoutes({ repository, validators, logger }) {
  const { validateItem, normaliseItem } = validators;
  const router = express.Router();

  const generateId = () => 'id_' + crypto.randomBytes(8).toString('hex');

  router.get('/', asyncHandler(async (req, res) => {
    const items = await repository.readAll();
    res.json(items);
  }));

  router.get('/:id', asyncHandler(async (req, res) => {
    const item = await repository.findById(req.params.id);
    if (!item) throw new HttpError(404, 'Item not found');
    res.json(item);
  }));

  router.post('/', asyncHandler(async (req, res) => {
    const errors = validateItem(req.body);
    if (errors.length) throw new HttpError(400, 'Validation failed', errors);

    const sku = String(req.body.sku).trim();
    if (await repository.findBySku(sku)) {
      throw new HttpError(409, `SKU "${sku}" already exists`);
    }

    const now = Date.now();
    const item = {
      id: generateId(),
      ...normaliseItem(req.body),
      createdAt: now,
      updatedAt: now
    };

    await repository.create(item);
    logger.info('item created', { id: item.id, sku: item.sku, requestId: req.id });
    res.status(201).location(`/api/inventory/${item.id}`).json(item);
  }));

  router.put('/:id', asyncHandler(async (req, res) => {
    const existing = await repository.findById(req.params.id);
    if (!existing) throw new HttpError(404, 'Item not found');

    const errors = validateItem(req.body, { partial: true });
    if (errors.length) throw new HttpError(400, 'Validation failed', errors);

    if (req.body.sku !== undefined) {
      const sku = String(req.body.sku).trim();
      const dup = await repository.findBySku(sku, req.params.id);
      if (dup) throw new HttpError(409, `SKU "${sku}" already exists`);
    }

    const merged = { ...existing, ...req.body };
    const updated = {
      ...normaliseItem(merged),
      id: existing.id,
      createdAt: existing.createdAt,
      updatedAt: Date.now()
    };

    await repository.update(req.params.id, updated);
    logger.info('item updated', { id: updated.id, sku: updated.sku, requestId: req.id });
    res.json(updated);
  }));

  router.delete('/:id', asyncHandler(async (req, res) => {
    const removed = await repository.remove(req.params.id);
    if (!removed) throw new HttpError(404, 'Item not found');
    logger.info('item deleted', { id: removed.id, sku: removed.sku, requestId: req.id });
    res.json({ success: true, deleted: removed.id });
  }));

  router.delete('/', asyncHandler(async (req, res) => {
    await repository.clear();
    logger.warn('inventory cleared', { requestId: req.id });
    res.json({ success: true, message: 'All inventory cleared' });
  }));

  return router;
}

function createOrderRoutes({ repository, validators, logger }) {
  const { validateOrder, normaliseOrder } = validators;
  const router = express.Router();

  const generateId = () => 'ord_' + crypto.randomBytes(8).toString('hex');

  // ---- List all ----
  router.get('/', asyncHandler(async (req, res) => {
    const orders = await repository.readAll();
    res.json(orders);
  }));

  // ---- Get one ----
  router.get('/:id', asyncHandler(async (req, res) => {
    const order = await repository.findById(req.params.id);
    if (!order) throw new HttpError(404, 'Order not found');
    res.json(order);
  }));

  // ---- Create ----
  router.post('/', asyncHandler(async (req, res) => {
    const errors = validateOrder(req.body);
    if (errors.length) throw new HttpError(400, 'Validation failed', errors);

    // Optional: enforce unique reference
    if (req.body.reference) {
      const ref = String(req.body.reference).trim();
      if (ref && await repository.findByReference(ref)) {
        throw new HttpError(409, `Reference "${ref}" already exists`);
      }
    }

    const now = Date.now();
    const order = {
      id: generateId(),
      ...normaliseOrder(req.body),
      createdAt: now,
      updatedAt: now
    };

    await repository.create(order);
    logger.info('order created', { id: order.id, customer: order.customer, requestId: req.id });
    res.status(201).location(`/api/orders/${order.id}`).json(order);
  }));

  // ---- Update ----
  router.put('/:id', asyncHandler(async (req, res) => {
    const existing = await repository.findById(req.params.id);
    if (!existing) throw new HttpError(404, 'Order not found');

    const errors = validateOrder(req.body, { partial: true });
    if (errors.length) throw new HttpError(400, 'Validation failed', errors);

    if (req.body.reference !== undefined) {
      const ref = String(req.body.reference || '').trim();
      if (ref) {
        const dup = await repository.findByReference(ref, req.params.id);
        if (dup) throw new HttpError(409, `Reference "${ref}" already exists`);
      }
    }

    const merged = { ...existing, ...req.body };
    const updated = {
      ...normaliseOrder(merged),
      id: existing.id,
      createdAt: existing.createdAt,
      updatedAt: Date.now()
    };

    await repository.update(req.params.id, updated);
    logger.info('order updated', { id: updated.id, customer: updated.customer, requestId: req.id });
    res.json(updated);
  }));

  // ---- Delete one ----
  router.delete('/:id', asyncHandler(async (req, res) => {
    const removed = await repository.remove(req.params.id);
    if (!removed) throw new HttpError(404, 'Order not found');
    logger.info('order deleted', { id: removed.id, customer: removed.customer, requestId: req.id });
    res.json({ success: true, deleted: removed.id });
  }));

  // ---- Clear all ----
  router.delete('/', asyncHandler(async (req, res) => {
    await repository.clear();
    logger.warn('orders cleared', { requestId: req.id });
    res.json({ success: true, message: 'All orders cleared' });
  }));

  return router;
}

function createHealthRoutes({ inventoryRepo, orderRepo, metrics }) {
  const router = express.Router();

  router.get('/', asyncHandler(async (req, res) => {
    const base = {
      service: 'Maricopa Inventory + Orders API',
      timestamp: new Date().toISOString()
    };

    try {
      const [items, orders] = await Promise.all([
        inventoryRepo.count(),
        orderRepo.count()
      ]);
      res.json({
        ...base,
        status: 'ok',
        items,
        orders,
        uptime: process.uptime(),
        metrics: metrics.snapshot()
      });
    } catch (err) {
      res.status(503).json({
        ...base,
        status: 'degraded',
        error: 'data store unavailable'
      });
    }
  }));

  return router;
}

// ============================================================
// 9. APP FACTORY
// ============================================================
function createApp(options = {}) {
  const config = options.config || CONFIG;
  const logger = options.logger || createLogger(config.IS_PROD);
  const metrics = options.metrics || createMetrics();
  const inventoryRepo = options.inventoryRepository || createInventoryRepository(config, logger);
  const orderRepo = options.orderRepository || createOrderRepository(config, logger);
  const validators = options.validators || createValidators(config);
  const middlewares = createMiddlewares(config, logger, metrics);

  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1);

  app.use(middlewares.requestId());
  app.use(middlewares.securityHeaders());
  app.use(middlewares.corsMiddleware());
  app.use(middlewares.rateLimiter());
  if (compression) app.use(compression());
  app.use(...middlewares.bodyParsers());
  app.use(middlewares.requestLogger());

  // Routes
  app.use('/api/health', createHealthRoutes({
    inventoryRepo, orderRepo, metrics
  }));
  app.use('/api/inventory', createInventoryRoutes({
    repository: inventoryRepo, validators, logger
  }));
  app.use('/api/orders', createOrderRoutes({
    repository: orderRepo, validators, logger
  }));

  // Static frontend
  app.use(express.static(config.PUBLIC_DIR, { maxAge: config.IS_PROD ? '1h' : 0 }));

  // SPA fallback (non-API)
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api/')) return next();
    const indexPath = path.join(config.PUBLIC_DIR, 'index.html');
    fs.access(indexPath)
      .then(() => res.sendFile(indexPath))
      .catch(() => next());
  });

  // Terminal middleware
  app.use(middlewares.apiNotFound());
  app.use(middlewares.errorHandler());

  app.locals.config = config;
  app.locals.logger = logger;
  app.locals.metrics = metrics;
  app.locals.inventoryRepository = inventoryRepo;
  app.locals.orderRepository = orderRepo;

  return app;
}

// ============================================================
// 10. STARTUP / SHUTDOWN
// ============================================================
async function start(options = {}) {
  const config = options.config || CONFIG;
  const logger = options.logger || createLogger(config.IS_PROD);
  const inventoryRepo = options.inventoryRepository || createInventoryRepository(config, logger);
  const orderRepo = options.orderRepository || createOrderRepository(config, logger);

  try {
    await Promise.all([inventoryRepo.ensure(), orderRepo.ensure()]);
  } catch (err) {
    logger.error('Failed to initialise data stores', { error: err.message });
    process.exit(1);
  }

  const app = createApp({
    config, logger, inventoryRepository: inventoryRepo, orderRepository: orderRepo
  });

  const server = await new Promise((resolve) => {
    const s = app.listen(config.PORT, config.HOST, () => resolve(s));
  });

  logger.info('Server started', {
    env: config.NODE_ENV,
    url: `http://${config.HOST}:${config.PORT}`,
    inventoryFile: config.DB_FILE,
    ordersFile: config.ORDERS_DB_FILE
  });

  if (!config.IS_PROD) {
    console.log('------------------------------------------------------------');
    console.log('  Maricopa API is ready');
    console.log('  Dashboard:  http://localhost:' + config.PORT + '/dashboard.html');
    console.log('  Inventory:  http://localhost:' + config.PORT + '/inventory.html');
    console.log('  Orders:     http://localhost:' + config.PORT + '/orders.html');
    console.log('  Analytics:  http://localhost:' + config.PORT + '/analytics.html');
    console.log('  Health:     http://localhost:' + config.PORT + '/api/health');
    console.log('------------------------------------------------------------');
  }

  let shuttingDown = false;
  const shutdown = (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info(`Received ${signal} — shutting down gracefully`);

    const forceExit = setTimeout(() => {
      logger.warn('Forced shutdown after timeout');
      process.exit(1);
    }, config.SHUTDOWN_TIMEOUT_MS);
    forceExit.unref();

    if (typeof server.closeIdleConnections === 'function') {
      server.closeIdleConnections();
    }

    server.close((err) => {
      clearTimeout(forceExit);
      if (err) {
        logger.error('Error closing server', { error: err.message });
        return process.exit(1);
      }
      logger.info('Server closed cleanly');
      process.exit(0);
    });
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason: String(reason) });
  });
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception', { message: err.message, stack: err.stack });
    shutdown('uncaughtException');
  });

  return { server, app, inventoryRepo, orderRepo, config, logger };
}

// ============================================================
// 11. ENTRYPOINT
// ============================================================
if (require.main === module) {
  start().catch((err) => {
    console.error('Fatal startup error:', err);
    process.exit(1);
  });
}

module.exports = {
  loadConfig,
  createLogger,
  createMetrics,
  createInventoryRepository,
  createOrderRepository,
  createValidators,
  createMiddlewares,
  createApp,
  start,
  HttpError,
  CONFIG
};
