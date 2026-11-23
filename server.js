// ============================================================
// Maricopa · Inventory + Orders Management API (MongoDB)
// server.js
// ============================================================
// Run with: node server.js
// Requires: npm install express cors helmet express-rate-limit mongodb
// Optional: npm install compression
// ============================================================

'use strict';

// ============================================================
// 1. DEPENDENCIES
// ============================================================
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

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

    // ---- MongoDB ----
    // Set MONGODB_URI in Render environment variables.
    // Never hardcode database credentials in this file.
    MONGODB_URI: env.MONGODB_URI || '',
    MONGODB_DB: env.MONGODB_DB || 'maricopa',
    INVENTORY_COLLECTION: env.INVENTORY_COLLECTION || 'inventory',
    ORDERS_COLLECTION: env.ORDERS_COLLECTION || 'orders',
    MONGO_CONNECT_TIMEOUT_MS: Number(env.MONGO_CONNECT_TIMEOUT_MS) || 10000,

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

  if (!cfg.MONGODB_URI) {
    throw new Error('MONGODB_URI is required. Set it in your environment variables.');
  }

  if (!/^mongodb(\+srv)?:\/\//.test(cfg.MONGODB_URI)) {
    throw new Error('MONGODB_URI must start with mongodb:// or mongodb+srv://');
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
    info: (msg, meta) => write('info', msg, meta),
    warn: (msg, meta) => write('warn', msg, meta),
    error: (msg, meta) => write('error', msg, meta),
    debug: (msg, meta) => {
      if (!isProd) write('debug', msg, meta);
    }
  };
}

function createMetrics() {
  const counters = new Map();
  const histograms = new Map();

  const bump = (key, by = 1) => {
    counters.set(key, (counters.get(key) || 0) + by);
  };

  const record = (key, value) => {
    if (!histograms.has(key)) {
      histograms.set(key, {
        count: 0,
        sum: 0,
        min: Infinity,
        max: -Infinity
      });
    }

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
          {
            count: h.count,
            avg: h.count ? h.sum / h.count : 0,
            min: h.min === Infinity ? 0 : h.min,
            max: h.max === -Infinity ? 0 : h.max
          }
        ])
      )
    })
  };
}

// ============================================================
// 4. MONGODB CLIENT — shared, lazy-connect, auto-reconnect
// ============================================================
function createMongoClient(config, logger) {
  const client = new MongoClient(config.MONGODB_URI, {
    serverSelectionTimeoutMS: config.MONGO_CONNECT_TIMEOUT_MS,
    maxPoolSize: 20,
    retryWrites: true
  });

  let connectPromise = null;
  let closed = false;

  async function getClient() {
    if (closed) throw new Error('Mongo client is closed');

    if (!connectPromise) {
      connectPromise = client.connect()
        .then(() => {
          logger.info('MongoDB connected', {
            db: config.MONGODB_DB,

            host: (() => {
              try {
                return new URL(
                  config.MONGODB_URI
                    .replace('mongodb+srv://', 'https://')
                    .replace('mongodb://', 'http://')
                ).host;
              } catch {
                return 'unknown';
              }
            })()
          });

          return client;
        })
        .catch(err => {
          connectPromise = null;

          logger.error('MongoDB connection failed', {
            error: err.message
          });

          throw err;
        });
    }

    return connectPromise;
  }

  return {
    async db() {
      const c = await getClient();
      return c.db(config.MONGODB_DB);
    },

    async close() {
      closed = true;
      try {
        await client.close();
      } catch (_) {
        /* ignore */
      }
    },

    raw: client
  };
}

// ============================================================
// 5. MONGODB REPOSITORIES
// ============================================================
function escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function createMongoRepository(collectionName, mongo, config, logger, label) {
  return {
    async ensure() {
      try {
        const db = await mongo.db();
        const coll = db.collection(collectionName);

        // Indexes — safe to create every boot (idempotent)
        if (label === 'inventory') {
          await coll.createIndex({ sku: 1 }, { unique: true });
          await coll.createIndex({ name: 1 });
          await coll.createIndex({ category: 1 });
        }

        if (label === 'orders') {
          // MongoDB does not allow sparse and partialFilterExpression together.
          // This unique partial index applies only to non-empty string references.
          await coll.createIndex(
            { reference: 1 },
            {
              unique: true,
              name: 'reference_1',
              partialFilterExpression: {
                reference: { $type: 'string', $gt: '' }
              }
            }
          );

          await coll.createIndex({ status: 1 });
          await coll.createIndex({ date: -1 });
        }

        await coll.createIndex({ id: 1 }, { unique: true });
        await coll.createIndex({ createdAt: -1 });

        logger.info(`MongoDB ${label} collection ready`, {
          collection: collectionName
        });

      } catch (err) {
        logger.error(`MongoDB ${label} ensure() failed`, {
          error: err.message
        });

        throw err;
      }
    },

    async readAll() {
      const db = await mongo.db();

      return db.collection(collectionName)
        .find({}, { projection: { _id: 0 } })
        .sort({ createdAt: -1 })
        .toArray();
    },

    async findById(id) {
      const db = await mongo.db();

      return db.collection(collectionName).findOne(
        { id },
        { projection: { _id: 0 } }
      );
    },

    async create(item) {
      const db = await mongo.db();

      // Copy so we never mutate the caller's object
      const doc = { ...item };

      await db.collection(collectionName).insertOne(doc);

      return item;
    },

    async update(id, patch) {
      const db = await mongo.db();

      const result = await db.collection(collectionName).findOneAndUpdate(
        { id },
        { $set: patch },
        {
          returnDocument: 'after',
          projection: { _id: 0 }
        }
      );

      // mongodb driver v5+ returns { value }, v6+ returns the doc directly
      return (result && result.value)
        ? result.value
        : (result || null);
    },

    async remove(id) {
      const db = await mongo.db();

      const result = await db.collection(collectionName).findOneAndDelete(
        { id },
        { projection: { _id: 0 } }
      );

      return (result && result.value)
        ? result.value
        : (result || null);
    },

    async clear() {
      const db = await mongo.db();

      await db.collection(collectionName).deleteMany({});
    },

    async count() {
      const db = await mongo.db();

      return db.collection(collectionName).countDocuments();
    }
  };
}

// ============================================================
// 6. IN-MEMORY REPOSITORY
// ============================================================
function createMemoryRepository(label, logger) {
  const items = new Map();

  return {
    async ensure() {
      logger.info(`Memory ${label} repository ready`);
    },

    async readAll() {
      return [...items.values()].sort(
        (a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0)
      );
    },

    async findById(id) {
      return items.get(id) || null;
    },

    async create(item) {
      if (items.has(item.id)) {
        throw new Error(`${label} item already exists`);
      }

      items.set(item.id, { ...item });
      return item;
    },

    async update(id, patch) {
      const existing = items.get(id);

      if (!existing) return null;

      const updated = { ...existing, ...patch };
      items.set(id, updated);

      return updated;
    },

    async remove(id) {
      const existing = items.get(id);

      if (!existing) return null;

      items.delete(id);
      return existing;
    },

    async clear() {
      items.clear();
    },

    async count() {
      return items.size;
    }
  };
}

// ============================================================
// 7. VALIDATION HELPERS
// ============================================================
function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function cleanString(value, maxLength = 200) {
  if (typeof value !== 'string') return '';

  return value.trim().slice(0, maxLength);
}

function toNumber(value, fallback = 0) {
  const number = Number(value);

  return Number.isFinite(number) ? number : fallback;
}

function makeId() {
  return crypto.randomUUID();
}

function makeReference() {
  const date = new Date();

  const year = date.getFullYear().toString().slice(-2);
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');

  const random = crypto.randomBytes(3).toString('hex').toUpperCase();

  return `MAR-${year}${month}${day}-${random}`;
}

// ============================================================
// 8. APPLICATION FACTORY
// ============================================================
function createApp(config = CONFIG) {
  const logger = createLogger(config.IS_PROD);
  const metrics = createMetrics();

  const mongo = createMongoClient(config, logger);

  const inventoryRepo = createMongoRepository(
    config.INVENTORY_COLLECTION,
    mongo,
    config,
    logger,
    'inventory'
  );

  const ordersRepo = createMongoRepository(
    config.ORDERS_COLLECTION,
    mongo,
    config,
    logger,
    'orders'
  );

  const app = express();

  // ==========================================================
  // MIDDLEWARE
  // ==========================================================
  app.disable('x-powered-by');

  app.set('trust proxy', 1);

  app.use(helmet({
    crossOriginResourcePolicy: { policy: 'cross-origin' }
  }));

  app.use(cors({
    origin: config.CORS_ORIGINS.includes('*')
      ? '*'
      : config.CORS_ORIGINS,

    credentials: !config.CORS_ORIGINS.includes('*')
  }));

  if (compression) {
    app.use(compression());
  }

  app.use(express.json({
    limit: config.BODY_LIMIT
  }));

  app.use(express.urlencoded({
    extended: true,
    limit: config.BODY_LIMIT
  }));

  const limiter = rateLimit({
    windowMs: config.RATE_LIMIT_WINDOW_MS,

    max: config.IS_PROD
      ? config.RATE_LIMIT_MAX_PROD
      : config.RATE_LIMIT_MAX_DEV,

    standardHeaders: true,
    legacyHeaders: false,

    message: {
      success: false,
      message: 'Too many requests. Please try again later.'
    }
  });

  app.use('/api', limiter);

  // ==========================================================
  // REQUEST METRICS
  // ==========================================================
  app.use((req, res, next) => {
    const started = Date.now();

    res.on('finish', () => {
      const duration = Date.now() - started;

      metrics.increment(`${req.method} ${req.route?.path || req.path}`);
      metrics.observe('request_duration_ms', duration);

      logger.debug('Request completed', {
        method: req.method,
        path: req.path,
        status: res.statusCode,
        duration
      });
    });

    next();
  });

  // ==========================================================
  // HEALTH CHECK
  // ==========================================================
  app.get('/health', async (req, res) => {
    try {
      const db = await mongo.db();
      await db.command({ ping: 1 });

      res.json({
        success: true,
        status: 'healthy',
        database: 'connected',
        timestamp: new Date().toISOString()
      });

    } catch (err) {
      logger.error('Health check failed', {
        error: err.message
      });

      res.status(503).json({
        success: false,
        status: 'unhealthy',
        database: 'disconnected'
      });
    }
  });

  // ==========================================================
  // API ROOT
  // ==========================================================
  app.get('/api', (req, res) => {
    res.json({
      success: true,
      message: 'Maricopa Inventory and Orders API',
      version: '1.0.0',
      endpoints: {
        health: '/health',
        inventory: '/api/inventory',
        orders: '/api/orders',
        metrics: '/api/metrics'
      }
    });
  });

  // ==========================================================
  // METRICS
  // ==========================================================
  app.get('/api/metrics', (req, res) => {
    res.json({
      success: true,
      metrics: metrics.snapshot()
    });
  });

  // ==========================================================
  // INVENTORY ROUTES
  // ==========================================================
  app.get('/api/inventory', async (req, res, next) => {
    try {
      let items = await inventoryRepo.readAll();

      const search = cleanString(req.query.search, 100).toLowerCase();
      const category = cleanString(req.query.category, 100);

      if (search) {
        items = items.filter(item =>
          String(item.name || '').toLowerCase().includes(search) ||
          String(item.sku || '').toLowerCase().includes(search) ||
          String(item.description || '').toLowerCase().includes(search)
        );
      }

      if (category) {
        items = items.filter(item => item.category === category);
      }

      res.json({
        success: true,
        count: items.length,
        data: items
      });

    } catch (err) {
      next(err);
    }
  });

  app.get('/api/inventory/:id', async (req, res, next) => {
    try {
      const item = await inventoryRepo.findById(req.params.id);

      if (!item) {
        return res.status(404).json({
          success: false,
          message: 'Inventory item not found'
        });
      }

      res.json({
        success: true,
        data: item
      });

    } catch (err) {
      next(err);
    }
  });

  app.post('/api/inventory', async (req, res, next) => {
    try {
      const body = req.body;

      if (!isObject(body)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid request body'
        });
      }

      const name = cleanString(body.name, 150);
      const sku = cleanString(body.sku, 80);
      const category = cleanString(body.category, 80);

      if (!name || !sku || !category) {
        return res.status(400).json({
          success: false,
          message: 'Name, SKU and category are required'
        });
      }

      if (!config.VALID_CATEGORIES.includes(category)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid inventory category'
        });
      }

      const item = {
        id: makeId(),
        name,
        sku,
        category,
        description: cleanString(body.description, 1000),
        quantity: Math.max(0, toNumber(body.quantity)),
        unit: cleanString(body.unit, 40),
        price: Math.max(0, toNumber(body.price)),
        supplier: cleanString(body.supplier, 150),
        location: cleanString(body.location, 150),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      await inventoryRepo.create(item);

      res.status(201).json({
        success: true,
        message: 'Inventory item created successfully',
        data: item
      });

    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          success: false,
          message: 'An inventory item with this SKU or ID already exists'
        });
      }

      next(err);
    }
  });

  app.put('/api/inventory/:id', async (req, res, next) => {
    try {
      const body = req.body;

      if (!isObject(body)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid request body'
        });
      }

      const patch = {};

      const allowedFields = [
        'name',
        'sku',
        'category',
        'description',
        'quantity',
        'unit',
        'price',
        'supplier',
        'location'
      ];

      for (const field of allowedFields) {
        if (body[field] !== undefined) {
          patch[field] = body[field];
        }
      }

      if (patch.name !== undefined) {
        patch.name = cleanString(patch.name, 150);
      }

      if (patch.sku !== undefined) {
        patch.sku = cleanString(patch.sku, 80);
      }

      if (patch.category !== undefined) {
        patch.category = cleanString(patch.category, 80);

        if (!config.VALID_CATEGORIES.includes(patch.category)) {
          return res.status(400).json({
            success: false,
            message: 'Invalid inventory category'
          });
        }
      }

      if (patch.description !== undefined) {
        patch.description = cleanString(patch.description, 1000);
      }

      if (patch.quantity !== undefined) {
        patch.quantity = Math.max(0, toNumber(patch.quantity));
      }

      if (patch.price !== undefined) {
        patch.price = Math.max(0, toNumber(patch.price));
      }

      if (patch.unit !== undefined) {
        patch.unit = cleanString(patch.unit, 40);
      }

      if (patch.supplier !== undefined) {
        patch.supplier = cleanString(patch.supplier, 150);
      }

      if (patch.location !== undefined) {
        patch.location = cleanString(patch.location, 150);
      }

      patch.updatedAt = new Date().toISOString();

      const updated = await inventoryRepo.update(req.params.id, patch);

      if (!updated) {
        return res.status(404).json({
          success: false,
          message: 'Inventory item not found'
        });
      }

      res.json({
        success: true,
        message: 'Inventory item updated successfully',
        data: updated
      });

    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          success: false,
          message: 'An inventory item with this SKU or ID already exists'
        });
      }

      next(err);
    }
  });

  app.delete('/api/inventory/:id', async (req, res, next) => {
    try {
      const deleted = await inventoryRepo.remove(req.params.id);

      if (!deleted) {
        return res.status(404).json({
          success: false,
          message: 'Inventory item not found'
        });
      }

      res.json({
        success: true,
        message: 'Inventory item deleted successfully',
        data: deleted
      });

    } catch (err) {
      next(err);
    }
  });

  // ==========================================================
  // ORDERS ROUTES
  // ==========================================================
  app.get('/api/orders', async (req, res, next) => {
    try {
      let orders = await ordersRepo.readAll();

      const status = cleanString(req.query.status, 50);
      const search = cleanString(req.query.search, 100).toLowerCase();

      if (status) {
        orders = orders.filter(order => order.status === status);
      }

      if (search) {
        orders = orders.filter(order =>
          String(order.reference || '').toLowerCase().includes(search) ||
          String(order.customerName || '').toLowerCase().includes(search) ||
          String(order.customerEmail || '').toLowerCase().includes(search)
        );
      }

      res.json({
        success: true,
        count: orders.length,
        data: orders
      });

    } catch (err) {
      next(err);
    }
  });

  app.get('/api/orders/:id', async (req, res, next) => {
    try {
      const order = await ordersRepo.findById(req.params.id);

      if (!order) {
        return res.status(404).json({
          success: false,
          message: 'Order not found'
        });
      }

      res.json({
        success: true,
        data: order
      });

    } catch (err) {
      next(err);
    }
  });

  app.post('/api/orders', async (req, res, next) => {
    try {
      const body = req.body;

      if (!isObject(body)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid request body'
        });
      }

      const customerName = cleanString(body.customerName, 150);
      const customerEmail = cleanString(body.customerEmail, 150);

      if (!customerName || !customerEmail) {
        return res.status(400).json({
          success: false,
          message: 'Customer name and email are required'
        });
      }

      const products = Array.isArray(body.products)
        ? body.products
        : [];

      if (!products.length) {
        return res.status(400).json({
          success: false,
          message: 'At least one product is required'
        });
      }

      const cleanProducts = products.map(product => ({
        id: cleanString(product.id, 100),
        name: cleanString(product.name, 150),
        quantity: Math.max(1, toNumber(product.quantity, 1)),
        price: Math.max(0, toNumber(product.price))
      }));

      const total = cleanProducts.reduce(
        (sum, product) => sum + product.quantity * product.price,
        0
      );

      const order = {
        id: makeId(),
        reference: makeReference(),
        customerName,
        customerEmail,
        customerPhone: cleanString(body.customerPhone, 50),
        deliveryAddress: cleanString(body.deliveryAddress, 500),
        products: cleanProducts,
        total,
        status: 'pending',
        notes: cleanString(body.notes, 1000),
        date: new Date().toISOString(),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      await ordersRepo.create(order);

      res.status(201).json({
        success: true,
        message: 'Order created successfully',
        data: order
      });

    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          success: false,
          message: 'An order with this reference or ID already exists'
        });
      }

      next(err);
    }
  });

  app.patch('/api/orders/:id/status', async (req, res, next) => {
    try {
      const status = cleanString(req.body.status, 50);

      if (!config.VALID_ORDER_STATUSES.includes(status)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid order status',
          allowedStatuses: config.VALID_ORDER_STATUSES
        });
      }

      const updated = await ordersRepo.update(req.params.id, {
        status,
        updatedAt: new Date().toISOString()
      });

      if (!updated) {
        return res.status(404).json({
          success: false,
          message: 'Order not found'
        });
      }

      res.json({
        success: true,
        message: 'Order status updated successfully',
        data: updated
      });

    } catch (err) {
      next(err);
    }
  });

  app.put('/api/orders/:id', async (req, res, next) => {
    try {
      const body = req.body;

      if (!isObject(body)) {
        return res.status(400).json({
          success: false,
          message: 'Invalid request body'
        });
      }

      const patch = {};

      if (body.status !== undefined) {
        const status = cleanString(body.status, 50);

        if (!config.VALID_ORDER_STATUSES.includes(status)) {
          return res.status(400).json({
            success: false,
            message: 'Invalid order status'
          });
        }

        patch.status = status;
      }

      if (body.notes !== undefined) {
        patch.notes = cleanString(body.notes, 1000);
      }

      if (body.deliveryAddress !== undefined) {
        patch.deliveryAddress = cleanString(body.deliveryAddress, 500);
      }

      patch.updatedAt = new Date().toISOString();

      const updated = await ordersRepo.update(req.params.id, patch);

      if (!updated) {
        return res.status(404).json({
          success: false,
          message: 'Order not found'
        });
      }

      res.json({
        success: true,
        message: 'Order updated successfully',
        data: updated
      });

    } catch (err) {
      next(err);
    }
  });

  app.delete('/api/orders/:id', async (req, res, next) => {
    try {
      const deleted = await ordersRepo.remove(req.params.id);

      if (!deleted) {
        return res.status(404).json({
          success: false,
          message: 'Order not found'
        });
      }

      res.json({
        success: true,
        message: 'Order deleted successfully',
        data: deleted
      });

    } catch (err) {
      next(err);
    }
  });

  // ==========================================================
  // STATIC FILES
  // ==========================================================
  app.use(express.static(config.PUBLIC_DIR));

  app.get('/', (req, res) => {
    res.sendFile(path.join(config.PUBLIC_DIR, 'index.html'), err => {
      if (err) {
        res.status(200).send(
          'Maricopa Inventory and Orders API is running.'
        );
      }
    });
  });

  // ==========================================================
  // 404 HANDLER
  // ==========================================================
  app.use((req, res) => {
    res.status(404).json({
      success: false,
      message: 'Route not found',
      path: req.originalUrl
    });
  });

  // ==========================================================
  // GLOBAL ERROR HANDLER
  // ==========================================================
  app.use((err, req, res, next) => {
    logger.error('Unhandled application error', {
      error: err.message,
      path: req.originalUrl,
      method: req.method
    });

    if (res.headersSent) {
      return next(err);
    }

    const status = err.status || err.statusCode || 500;

    res.status(status).json({
      success: false,
      message: config.IS_PROD
        ? 'An unexpected server error occurred'
        : err.message
    });
  });

  // ==========================================================
  // DATABASE INITIALISATION
  // ==========================================================
  async function initialiseDatabase() {
    await inventoryRepo.ensure();
    await ordersRepo.ensure();

    logger.info('Database initialisation completed');
  }

  return {
    app,
    mongo,
    logger,
    metrics,
    initialiseDatabase
  };
}

// ============================================================
// 9. SERVER STARTUP + GRACEFUL SHUTDOWN
// ============================================================
async function startServer() {
  const {
    app,
    mongo,
    logger,
    initialiseDatabase
  } = createApp(CONFIG);

  try {
    await initialiseDatabase();

    const server = app.listen(CONFIG.PORT, CONFIG.HOST, () => {
      logger.info('Maricopa API server started', {
        host: CONFIG.HOST,
        port: CONFIG.PORT,
        environment: CONFIG.NODE_ENV
      });
    });

    let shuttingDown = false;

    async function shutdown(signal) {
      if (shuttingDown) return;

      shuttingDown = true;

      logger.info(`Received ${signal}. Shutting down gracefully...`);

      const timeout = setTimeout(() => {
        logger.error('Shutdown timeout reached. Forcing exit.');
        process.exit(1);
      }, CONFIG.SHUTDOWN_TIMEOUT_MS);

      timeout.unref();

      server.close(async err => {
        if (err) {
          logger.error('HTTP server shutdown error', {
            error: err.message
          });
        }

        try {
          await mongo.close();
          logger.info('MongoDB connection closed');
        } catch (closeErr) {
          logger.error('MongoDB shutdown error', {
            error: closeErr.message
          });
        }

        clearTimeout(timeout);
        process.exit(err ? 1 : 0);
      });
    }

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    process.on('unhandledRejection', reason => {
      logger.error('Unhandled promise rejection', {
        error: reason instanceof Error
          ? reason.message
          : String(reason)
      });
    });

    process.on('uncaughtException', err => {
      logger.error('Uncaught exception', {
        error: err.message,
        stack: err.stack
      });

      shutdown('uncaughtException');
    });

    return server;

  } catch (err) {
    logger.error('Server startup failed', {
      error: err.message,
      stack: err.stack
    });

    await mongo.close();

    process.exitCode = 1;
  }
}

// ============================================================
// 10. START APPLICATION
// ============================================================
if (require.main === module) {
  startServer();
}

module.exports = {
  createApp,
  loadConfig,
  createMongoRepository,
  createMemoryRepository,
  escapeRegex,
  makeId,
  makeReference
};
