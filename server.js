// ============================================================
// Maricopa · Inventory + Orders Management API (MongoDB)
// server.js — v1.1.0
// ============================================================
// NEW in 1.1.0 (all existing routes keep working unchanged):
//   POST /inventory/bulk-movement        apply a whole receipt at once, all-or-nothing
//   GET  /inventory/movements            movement history (newest first)
//   POST /inventory/movements/:id/undo   reverse a movement
//   Rate limit now also covers /inventory and /orders
//   Metrics no longer create one key per item id
// ============================================================
// Run with: node server.js
// Requires: npm install express cors helmet express-rate-limit mongodb
// Optional: npm install compression
// Env:
//   MONGODB_URI (required)   MONGODB_DB   PORT   NODE_ENV
//   CORS_ORIGINS (comma-separated, or "*")
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

    MONGODB_URI: env.MONGODB_URI || '',
    MONGODB_DB: env.MONGODB_DB || 'maricopa',
    INVENTORY_COLLECTION: env.INVENTORY_COLLECTION || 'inventory',
    ORDERS_COLLECTION: env.ORDERS_COLLECTION || 'orders',
    MOVEMENTS_COLLECTION: env.MOVEMENTS_COLLECTION || 'movements',
    MONGO_CONNECT_TIMEOUT_MS: Number(env.MONGO_CONNECT_TIMEOUT_MS) || 10000,

    PUBLIC_DIR: path.join(__dirname, 'public'),
    BODY_LIMIT: env.BODY_LIMIT || '1mb',
    SHUTDOWN_TIMEOUT_MS: Number(env.SHUTDOWN_TIMEOUT_MS) || 5000,
    RATE_LIMIT_WINDOW_MS: Number(env.RATE_LIMIT_WINDOW_MS) || 60 * 1000,
    RATE_LIMIT_MAX_PROD: Number(env.RATE_LIMIT_MAX_PROD) || 240,
    RATE_LIMIT_MAX_DEV: Number(env.RATE_LIMIT_MAX_DEV) || 1000,
    MAX_BULK_LINES: Number(env.MAX_BULK_LINES) || 200,

    VALID_CATEGORIES: Object.freeze([
      'Ready-mix', 'Precast', 'Cement',
      'Chemicals', 'Aggregates', 'Steel', 'Other'
    ]),

    VALID_ORDER_STATUSES: Object.freeze([
      'pending', 'processing', 'shipped', 'delivered', 'cancelled'
    ]),

    VALID_MOVEMENT_TYPES: Object.freeze(['in', 'out']),

    CORS_ORIGINS: env.CORS_ORIGINS
      ? env.CORS_ORIGINS.split(',').map(s => s.trim()).filter(Boolean)
      : ['*']
  };

  cfg.IS_PROD = cfg.NODE_ENV === 'production';

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
    info:  (msg, meta) => write('info',  msg, meta),
    warn:  (msg, meta) => write('warn',  msg, meta),
    error: (msg, meta) => write('error', msg, meta),
    debug: (msg, meta) => { if (!isProd) write('debug', msg, meta); }
  };
}

function createMetrics() {
  const counters = new Map();
  const histograms = new Map();

  const bump = (key, by = 1) => counters.set(key, (counters.get(key) || 0) + by);

  const record = (key, value) => {
    if (!histograms.has(key)) {
      histograms.set(key, { count: 0, sum: 0, min: Infinity, max: -Infinity });
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
// 4. MONGODB CLIENT
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
              } catch { return 'unknown'; }
            })()
          });
          return client;
        })
        .catch(err => {
          connectPromise = null;
          logger.error('MongoDB connection failed', { error: err.message });
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
      try { await client.close(); } catch (_) { /* ignore */ }
    },
    raw: client
  };
}

// ============================================================
// 5. MONGODB REPOSITORY
// ============================================================
function escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Works with both driver styles (v5 returns { value }, v6 returns the document)
function unwrap(result) {
  if (result && Object.prototype.hasOwnProperty.call(result, 'value')) return result.value || null;
  return result || null;
}

function createMongoRepository(collectionName, mongo, config, logger, label) {
  return {
    async ensure() {
      try {
        const db = await mongo.db();
        const coll = db.collection(collectionName);

        if (label === 'inventory') {
          await coll.createIndex({ sku: 1 }, { unique: true });
          await coll.createIndex({ name: 1 });
          await coll.createIndex({ category: 1 });
        }

        if (label === 'orders') {
          // Unique reference only when non-empty string (frontend sends "" when blank)
          await coll.createIndex(
            { reference: 1 },
            {
              unique: true,
              name: 'reference_1',
              partialFilterExpression: { reference: { $type: 'string', $gt: '' } }
            }
          );
          await coll.createIndex({ status: 1 });
          await coll.createIndex({ date: -1 });
          await coll.createIndex({ customer: 1 });
        }

        await coll.createIndex({ id: 1 }, { unique: true });
        await coll.createIndex({ createdAt: -1 });

        logger.info(`MongoDB ${label} collection ready`, { collection: collectionName });
      } catch (err) {
        logger.error(`MongoDB ${label} ensure() failed`, { error: err.message });
        throw err;
      }
    },

    async readAll(limit = 0) {
      const db = await mongo.db();
      let cursor = db.collection(collectionName)
        .find({}, { projection: { _id: 0 } })
        .sort({ createdAt: -1 });
      if (limit > 0) cursor = cursor.limit(limit);
      return cursor.toArray();
    },

    async findById(id) {
      const db = await mongo.db();
      return db.collection(collectionName).findOne({ id }, { projection: { _id: 0 } });
    },

    async create(item) {
      const db = await mongo.db();
      const doc = { ...item };
      await db.collection(collectionName).insertOne(doc);
      return item;
    },

    async update(id, patch) {
      const db = await mongo.db();
      const result = await db.collection(collectionName).findOneAndUpdate(
        { id },
        { $set: patch },
        { returnDocument: 'after', projection: { _id: 0 } }
      );
      return unwrap(result);
    },

    // Atomic stock change: $inc quantity by delta, optionally $set other fields.
    // If minQty > 0 the change only happens when quantity >= minQty (prevents negative stock).
    // Returns the updated document, or null if the item is missing / stock too low.
    async adjust(id, delta, set = {}, minQty = 0) {
      const db = await mongo.db();
      const filter = { id };
      if (minQty > 0) filter.quantity = { $gte: minQty };
      const result = await db.collection(collectionName).findOneAndUpdate(
        filter,
        { $inc: { quantity: delta }, $set: set },
        { returnDocument: 'after', projection: { _id: 0 } }
      );
      return unwrap(result);
    },

    // Atomically update a document only if it is not already marked undone
    async claim(id, patch) {
      const db = await mongo.db();
      const result = await db.collection(collectionName).findOneAndUpdate(
        { id, undone: { $ne: true } },
        { $set: patch },
        { returnDocument: 'after', projection: { _id: 0 } }
      );
      return unwrap(result);
    },

    async remove(id) {
      const db = await mongo.db();
      const result = await db.collection(collectionName).findOneAndDelete(
        { id },
        { projection: { _id: 0 } }
      );
      return unwrap(result);
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
// 6. IN-MEMORY REPOSITORY (fallback / tests)
// ============================================================
function createMemoryRepository(label, logger) {
  const items = new Map();

  return {
    async ensure() { logger.info(`Memory ${label} repository ready`); },
    async readAll(limit = 0) {
      const all = [...items.values()].sort(
        (a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0)
      );
      return limit > 0 ? all.slice(0, limit) : all;
    },
    async findById(id) { return items.get(id) || null; },
    async create(item) {
      if (items.has(item.id)) throw new Error(`${label} item already exists`);
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
    async adjust(id, delta, set = {}, minQty = 0) {
      const existing = items.get(id);
      if (!existing) return null;
      const current = Number(existing.quantity) || 0;
      if (minQty > 0 && current < minQty) return null;
      const updated = { ...existing, ...set, quantity: current + delta };
      items.set(id, updated);
      return updated;
    },
    async claim(id, patch) {
      const existing = items.get(id);
      if (!existing || existing.undone) return null;
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
    async clear() { items.clear(); },
    async count() { return items.size; }
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

function round6(n) {
  return Math.round(n * 1e6) / 1e6;
}

function isValidDateString(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
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
// 8. ROUTERS — mounted at both /api/* and /
// ============================================================
function buildInventoryRouter({ inventoryRepo, movementsRepo, config, logger }) {
  const router = express.Router();

  // ---------- stock-movement helpers (all-or-nothing) ----------
  // ops: [{ id, delta, price?, priceBefore? }]
  async function rollback(done) {
    for (const d of [...done].reverse()) {
      const set = { updatedAt: new Date().toISOString() };
      if (d.op.price !== undefined) set.price = d.op.priceBefore;
      try {
        await inventoryRepo.adjust(d.op.id, -d.op.delta, set, 0);
      } catch (err) {
        logger.error('Rollback step failed', { id: d.op.id, error: err.message });
      }
    }
  }

  async function applyOps(ops) {
    const done = [];
    try {
      for (const op of ops) {
        const set = { updatedAt: new Date().toISOString() };
        if (op.price !== undefined) set.price = op.price;
        const minQty = op.delta < 0 ? -op.delta : 0;
        const after = await inventoryRepo.adjust(op.id, op.delta, set, minQty);
        if (!after) {
          await rollback(done);
          return { ok: false, failedId: op.id };
        }
        done.push({ op, after });
      }
      return { ok: true, done };
    } catch (err) {
      await rollback(done);
      throw err;
    }
  }

  // ---------- movement history & undo (must be before '/:id') ----------
  router.get('/movements', async (req, res, next) => {
    try {
      const limit = Math.min(200, Math.max(1, toNumber(req.query.limit, 50)));
      res.json(await movementsRepo.readAll(limit));
    } catch (err) { next(err); }
  });

  router.post('/movements/:id/undo', async (req, res, next) => {
    try {
      const movement = await movementsRepo.findById(req.params.id);
      if (!movement) return res.status(404).json({ error: 'Movement not found' });
      if (movement.undone) return res.status(409).json({ error: 'This movement was already undone' });

      // Build reverse operations
      const ops = [];
      const problems = [];
      for (const line of movement.lines || []) {
        const item = await inventoryRepo.findById(line.itemId);
        if (!item) { problems.push(`${line.name} no longer exists`); continue; }
        const delta = movement.type === 'in' ? -line.quantity : line.quantity;
        const restorePrice = line.priceUpdated && Number(item.price) === Number(line.unitPrice);
        ops.push({
          id: item.id,
          delta,
          price: restorePrice ? line.priceBefore : undefined,
          priceBefore: item.price
        });
        if (delta < 0 && (Number(item.quantity) || 0) < -delta) {
          problems.push(`${item.name}: only ${Number(item.quantity) || 0} left, cannot remove ${-delta}`);
        }
      }
      if (problems.length) {
        return res.status(409).json({ error: 'Cannot undo this movement', details: problems });
      }

      // Claim first so a double-click cannot undo twice
      const claimed = await movementsRepo.claim(movement.id, {
        undone: true,
        undoneAt: new Date().toISOString()
      });
      if (!claimed) return res.status(409).json({ error: 'This movement was already undone' });

      const result = await applyOps(ops);
      if (!result.ok) {
        await movementsRepo.update(movement.id, { undone: false, undoneAt: null });
        return res.status(409).json({
          error: 'Stock changed while undoing. Nothing was changed — please retry.'
        });
      }

      res.json({
        success: true,
        movement: claimed,
        items: result.done.map(d => d.after)
      });
    } catch (err) { next(err); }
  });

  // ---------- bulk movement (receipt scanner) ----------
  // Body: { type: 'in'|'out', reference?, note?, source?,
  //         lines: [{ id, quantity, price?, updatePrice? }] }
  router.post('/bulk-movement', async (req, res, next) => {
    try {
      const body = req.body;
      if (!isObject(body)) {
        return res.status(400).json({ error: 'Invalid request body', details: ['Body must be a JSON object'] });
      }

      const errors = [];
      const type = cleanString(body.type, 10);
      if (!config.VALID_MOVEMENT_TYPES.includes(type)) errors.push("type must be 'in' or 'out'");
      if (!Array.isArray(body.lines) || !body.lines.length) {
        errors.push('lines must be a non-empty array');
      } else if (body.lines.length > config.MAX_BULK_LINES) {
        errors.push(`lines cannot exceed ${config.MAX_BULK_LINES}`);
      }
      if (errors.length) return res.status(400).json({ error: 'Validation failed', details: errors });

      // Merge duplicate items so one item is only changed once
      const merged = new Map();
      body.lines.forEach((line, i) => {
        if (!isObject(line)) { errors.push(`lines[${i}] must be an object`); return; }
        const id = cleanString(line.id, 100);
        const quantity = toNumber(line.quantity, NaN);
        if (!id) errors.push(`lines[${i}].id is required`);
        if (!Number.isFinite(quantity) || quantity <= 0) errors.push(`lines[${i}].quantity must be a number > 0`);
        let price;
        if (line.price !== undefined && line.price !== null && line.price !== '') {
          price = toNumber(line.price, NaN);
          if (!Number.isFinite(price) || price < 0) errors.push(`lines[${i}].price must be a number >= 0`);
        }
        if (errors.length) return;
        const entry = merged.get(id) || { id, quantity: 0, updatePrice: false };
        entry.quantity = round6(entry.quantity + quantity);
        if (price !== undefined) entry.unitPrice = price;
        if (line.updatePrice === true) entry.updatePrice = true;
        merged.set(id, entry);
      });
      if (errors.length) return res.status(400).json({ error: 'Validation failed', details: errors });

      // Load items and validate before touching anything
      const missing = [];
      const insufficient = [];
      const ops = [];
      const lines = [];
      for (const entry of merged.values()) {
        const item = await inventoryRepo.findById(entry.id);
        if (!item) { missing.push(`item ${entry.id} not found`); continue; }
        const current = Number(item.quantity) || 0;
        if (type === 'out' && entry.quantity > current) {
          insufficient.push(`${item.name}: need ${entry.quantity}, have ${current}`);
        }
        const setPrice = type === 'in' && entry.updatePrice &&
          entry.unitPrice !== undefined && Number(entry.unitPrice) !== Number(item.price);
        ops.push({
          id: item.id,
          delta: type === 'in' ? entry.quantity : -entry.quantity,
          price: setPrice ? entry.unitPrice : undefined,
          priceBefore: item.price
        });
        lines.push({
          itemId: item.id,
          sku: item.sku,
          name: item.name,
          unit: item.unit || 'units',
          quantity: entry.quantity,
          unitPrice: entry.unitPrice !== undefined ? entry.unitPrice : null,
          priceBefore: item.price,
          priceUpdated: !!setPrice
        });
      }
      if (missing.length) return res.status(404).json({ error: 'Some items were not found', details: missing });
      if (insufficient.length) return res.status(409).json({ error: 'Insufficient stock', details: insufficient });

      const result = await applyOps(ops);
      if (!result.ok) {
        return res.status(409).json({
          error: 'Stock changed while saving. Nothing was updated — please retry.'
        });
      }

      lines.forEach((line, i) => {
        line.after = round6(Number(result.done[i].after.quantity) || 0);
        line.before = round6(line.after - ops[i].delta);
      });

      const movement = {
        id: makeId(),
        type,
        reference: cleanString(body.reference, 100),
        note: cleanString(body.note, 1000),
        source: cleanString(body.source, 50) || 'api',
        lines,
        undone: false,
        createdAt: new Date().toISOString()
      };

      try {
        await movementsRepo.create(movement);
      } catch (err) {
        await rollback(result.done);
        throw err;
      }

      res.status(201).json({
        success: true,
        movement,
        items: result.done.map(d => d.after)
      });
    } catch (err) { next(err); }
  });

  // ---------- standard CRUD ----------

  // GET /inventory  — frontend expects a bare array
  router.get('/', async (req, res, next) => {
    try {
      let items = await inventoryRepo.readAll();

      const search = cleanString(req.query.search, 100).toLowerCase();
      const category = cleanString(req.query.category, 100);

      if (search) {
        items = items.filter(item =>
          String(item.name || '').toLowerCase().includes(search) ||
          String(item.sku || '').toLowerCase().includes(search) ||
          String(item.description || '').toLowerCase().includes(search) ||
          String(item.supplier || '').toLowerCase().includes(search)
        );
      }
      if (category) {
        items = items.filter(item => item.category === category);
      }

      res.json(items);
    } catch (err) { next(err); }
  });

  router.get('/:id', async (req, res, next) => {
    try {
      const item = await inventoryRepo.findById(req.params.id);
      if (!item) return res.status(404).json({ error: 'Inventory item not found' });
      res.json(item);
    } catch (err) { next(err); }
  });

  // POST /inventory
  router.post('/', async (req, res, next) => {
    try {
      const body = req.body;
      if (!isObject(body)) {
        return res.status(400).json({ error: 'Invalid request body', details: ['Body must be a JSON object'] });
      }

      const name = cleanString(body.name, 150);
      const sku = cleanString(body.sku, 80);
      const category = cleanString(body.category, 80);

      const errors = [];
      if (!name) errors.push('name is required');
      if (!sku) errors.push('sku is required');
      if (!category) errors.push('category is required');
      if (category && !config.VALID_CATEGORIES.includes(category)) {
        errors.push(`category must be one of: ${config.VALID_CATEGORIES.join(', ')}`);
      }
      if (errors.length) {
        return res.status(400).json({ error: 'Validation failed', details: errors });
      }

      const item = {
        id: makeId(),
        name,
        sku,
        category,
        description: cleanString(body.description, 1000),
        quantity: Math.max(0, toNumber(body.quantity)),
        unit: cleanString(body.unit, 40) || 'units',
        price: Math.max(0, toNumber(body.price)),
        threshold: Math.max(0, toNumber(body.threshold, 10)),
        supplier: cleanString(body.supplier, 150),
        location: cleanString(body.location, 150),
        notes: cleanString(body.notes, 1000),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      await inventoryRepo.create(item);
      res.status(201).json(item);
    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          error: `SKU "${cleanString(req.body.sku, 80)}" already exists`,
          details: ['sku must be unique']
        });
      }
      next(err);
    }
  });

  // PUT /inventory/:id
  router.put('/:id', async (req, res, next) => {
    try {
      const body = req.body;
      if (!isObject(body)) {
        return res.status(400).json({ error: 'Invalid request body', details: ['Body must be a JSON object'] });
      }

      const patch = {};
      const allowedFields = [
        'name', 'sku', 'category', 'description',
        'quantity', 'unit', 'price', 'threshold',
        'supplier', 'location', 'notes'
      ];
      for (const field of allowedFields) {
        if (body[field] !== undefined) patch[field] = body[field];
      }

      if (patch.name !== undefined) patch.name = cleanString(patch.name, 150);
      if (patch.sku !== undefined) patch.sku = cleanString(patch.sku, 80);
      if (patch.category !== undefined) {
        patch.category = cleanString(patch.category, 80);
        if (!config.VALID_CATEGORIES.includes(patch.category)) {
          return res.status(400).json({
            error: 'Invalid category',
            details: [`category must be one of: ${config.VALID_CATEGORIES.join(', ')}`]
          });
        }
      }
      if (patch.description !== undefined) patch.description = cleanString(patch.description, 1000);
      if (patch.quantity !== undefined) patch.quantity = Math.max(0, toNumber(patch.quantity));
      if (patch.price !== undefined) patch.price = Math.max(0, toNumber(patch.price));
      if (patch.threshold !== undefined) patch.threshold = Math.max(0, toNumber(patch.threshold, 10));
      if (patch.unit !== undefined) patch.unit = cleanString(patch.unit, 40) || 'units';
      if (patch.supplier !== undefined) patch.supplier = cleanString(patch.supplier, 150);
      if (patch.location !== undefined) patch.location = cleanString(patch.location, 150);
      if (patch.notes !== undefined) patch.notes = cleanString(patch.notes, 1000);

      patch.updatedAt = new Date().toISOString();

      const updated = await inventoryRepo.update(req.params.id, patch);
      if (!updated) return res.status(404).json({ error: 'Inventory item not found' });
      res.json(updated);
    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          error: 'SKU already exists',
          details: ['sku must be unique']
        });
      }
      next(err);
    }
  });

  // DELETE /inventory  — clear all. MUST be registered before '/:id'.
  router.delete('/', async (req, res, next) => {
    try {
      await inventoryRepo.clear();
      res.status(204).end();
    } catch (err) { next(err); }
  });

  // DELETE /inventory/:id
  router.delete('/:id', async (req, res, next) => {
    try {
      const deleted = await inventoryRepo.remove(req.params.id);
      if (!deleted) return res.status(404).json({ error: 'Inventory item not found' });
      res.json(deleted);
    } catch (err) { next(err); }
  });

  return router;
}

function buildOrdersRouter({ ordersRepo, config, logger }) {
  const router = express.Router();

  // GET /orders — bare array
  router.get('/', async (req, res, next) => {
    try {
      let orders = await ordersRepo.readAll();
      const status = cleanString(req.query.status, 50);
      const search = cleanString(req.query.search, 100).toLowerCase();

      if (status) orders = orders.filter(o => o.status === status);
      if (search) {
        orders = orders.filter(o =>
          String(o.id || '').toLowerCase().includes(search) ||
          String(o.customer || '').toLowerCase().includes(search) ||
          String(o.items || '').toLowerCase().includes(search) ||
          String(o.reference || '').toLowerCase().includes(search)
        );
      }
      res.json(orders);
    } catch (err) { next(err); }
  });

  router.get('/:id', async (req, res, next) => {
    try {
      const order = await ordersRepo.findById(req.params.id);
      if (!order) return res.status(404).json({ error: 'Order not found' });
      res.json(order);
    } catch (err) { next(err); }
  });

  // POST /orders — accept the frontend's exact shape
  router.post('/', async (req, res, next) => {
    try {
      const body = req.body;
      if (!isObject(body)) {
        return res.status(400).json({ error: 'Invalid request body', details: ['Body must be a JSON object'] });
      }

      const errors = [];

      const customer = cleanString(body.customer, 150);
      if (!customer) errors.push('customer is required');

      const status = cleanString(body.status, 50) || 'pending';
      if (!config.VALID_ORDER_STATUSES.includes(status)) {
        errors.push(`status must be one of: ${config.VALID_ORDER_STATUSES.join(', ')}`);
      }

      const date = cleanString(body.date, 10);
      if (!date) errors.push('date is required');
      else if (!isValidDateString(date)) errors.push('date must be YYYY-MM-DD');

      const items = cleanString(body.items, 1000);
      if (!items) errors.push('items is required');

      if (body.total === undefined || body.total === null || body.total === '') {
        errors.push('total is required');
      }
      const total = toNumber(body.total, NaN);
      if (!Number.isFinite(total) || total < 0) errors.push('total must be a number >= 0');

      // reference is optional; auto-generate if blank
      let reference = cleanString(body.reference, 100);
      if (!reference) reference = makeReference();

      if (errors.length) {
        return res.status(400).json({ error: 'Validation failed', details: errors });
      }

      const order = {
        id: makeId(),
        customer,
        status,
        date,
        items,
        total,
        reference,
        notes: cleanString(body.notes, 1000),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      await ordersRepo.create(order);
      res.status(201).json(order);
    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          error: 'An order with this reference already exists',
          details: ['reference must be unique']
        });
      }
      next(err);
    }
  });

  // PUT /orders/:id — allow all frontend-editable fields
  router.put('/:id', async (req, res, next) => {
    try {
      const body = req.body;
      if (!isObject(body)) {
        return res.status(400).json({ error: 'Invalid request body', details: ['Body must be a JSON object'] });
      }

      const patch = {};

      if (body.customer !== undefined) {
        const customer = cleanString(body.customer, 150);
        if (!customer) {
          return res.status(400).json({ error: 'Validation failed', details: ['customer cannot be empty'] });
        }
        patch.customer = customer;
      }

      if (body.status !== undefined) {
        const status = cleanString(body.status, 50);
        if (!config.VALID_ORDER_STATUSES.includes(status)) {
          return res.status(400).json({
            error: 'Invalid order status',
            details: [`status must be one of: ${config.VALID_ORDER_STATUSES.join(', ')}`]
          });
        }
        patch.status = status;
      }

      if (body.date !== undefined) {
        const date = cleanString(body.date, 10);
        if (!isValidDateString(date)) {
          return res.status(400).json({ error: 'Invalid date', details: ['date must be YYYY-MM-DD'] });
        }
        patch.date = date;
      }

      if (body.items !== undefined) {
        const items = cleanString(body.items, 1000);
        if (!items) {
          return res.status(400).json({ error: 'Validation failed', details: ['items cannot be empty'] });
        }
        patch.items = items;
      }

      if (body.total !== undefined) {
        const total = toNumber(body.total, NaN);
        if (!Number.isFinite(total) || total < 0) {
          return res.status(400).json({ error: 'Invalid total', details: ['total must be a number >= 0'] });
        }
        patch.total = total;
      }

      if (body.reference !== undefined) {
        patch.reference = cleanString(body.reference, 100);
      }

      if (body.notes !== undefined) {
        patch.notes = cleanString(body.notes, 1000);
      }

      patch.updatedAt = new Date().toISOString();

      const updated = await ordersRepo.update(req.params.id, patch);
      if (!updated) return res.status(404).json({ error: 'Order not found' });
      res.json(updated);
    } catch (err) {
      if (err.code === 11000) {
        return res.status(409).json({
          error: 'An order with this reference already exists',
          details: ['reference must be unique']
        });
      }
      next(err);
    }
  });

  // DELETE /orders — clear all. MUST be registered before '/:id'.
  router.delete('/', async (req, res, next) => {
    try {
      await ordersRepo.clear();
      res.status(204).end();
    } catch (err) { next(err); }
  });

  router.delete('/:id', async (req, res, next) => {
    try {
      const deleted = await ordersRepo.remove(req.params.id);
      if (!deleted) return res.status(404).json({ error: 'Order not found' });
      res.json(deleted);
    } catch (err) { next(err); }
  });

  router.patch('/:id/status', async (req, res, next) => {
    try {
      const status = cleanString(req.body && req.body.status, 50);
      if (!config.VALID_ORDER_STATUSES.includes(status)) {
        return res.status(400).json({
          error: 'Invalid order status',
          details: [`status must be one of: ${config.VALID_ORDER_STATUSES.join(', ')}`]
        });
      }
      const updated = await ordersRepo.update(req.params.id, {
        status,
        updatedAt: new Date().toISOString()
      });
      if (!updated) return res.status(404).json({ error: 'Order not found' });
      res.json(updated);
    } catch (err) { next(err); }
  });

  return router;
}

// ============================================================
// 9. APPLICATION FACTORY
// ============================================================
function createApp(config = CONFIG) {
  const logger = createLogger(config.IS_PROD);
  const metrics = createMetrics();
  const mongo = createMongoClient(config, logger);

  const inventoryRepo = createMongoRepository(
    config.INVENTORY_COLLECTION, mongo, config, logger, 'inventory'
  );
  const ordersRepo = createMongoRepository(
    config.ORDERS_COLLECTION, mongo, config, logger, 'orders'
  );
  const movementsRepo = createMongoRepository(
    config.MOVEMENTS_COLLECTION, mongo, config, logger, 'movements'
  );

  const app = express();

  app.disable('x-powered-by');
  app.set('trust proxy', 1);

  app.use(helmet({
    crossOriginResourcePolicy: { policy: 'cross-origin' }
  }));

  app.use(cors({
    origin: config.CORS_ORIGINS.includes('*') ? '*' : config.CORS_ORIGINS,
    credentials: !config.CORS_ORIGINS.includes('*')
  }));

  if (compression) app.use(compression());

  app.use(express.json({ limit: config.BODY_LIMIT }));
  app.use(express.urlencoded({ extended: true, limit: config.BODY_LIMIT }));

  // Request metrics (route pattern only, so ids never create new keys)
  app.use((req, res, next) => {
    const started = Date.now();
    res.on('finish', () => {
      const duration = Date.now() - started;
      const pattern = req.route ? (req.baseUrl || '') + req.route.path : 'unmatched';
      metrics.increment(`${req.method} ${pattern}`);
      metrics.observe('request_duration_ms', duration);
      logger.debug('Request completed', {
        method: req.method, path: req.path,
        status: res.statusCode, duration
      });
    });
    next();
  });

  // Rate limit — covers /api and the bare paths the frontends actually call
  const limiter = rateLimit({
    windowMs: config.RATE_LIMIT_WINDOW_MS,
    max: config.IS_PROD ? config.RATE_LIMIT_MAX_PROD : config.RATE_LIMIT_MAX_DEV,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests. Please try again later.' }
  });
  app.use(['/api', '/inventory', '/orders'], limiter);

  // Health check
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
      logger.error('Health check failed', { error: err.message });
      res.status(503).json({
        success: false,
        status: 'unhealthy',
        database: 'disconnected'
      });
    }
  });

  // API root
  app.get('/api', (req, res) => {
    res.json({
      success: true,
      message: 'Maricopa Inventory and Orders API',
      version: '1.1.0',
      endpoints: {
        health: '/health',
        inventory: '/inventory',
        orders: '/orders',
        bulkMovement: 'POST /inventory/bulk-movement',
        movements: 'GET /inventory/movements',
        undoMovement: 'POST /inventory/movements/:id/undo',
        metrics: '/api/metrics'
      }
    });
  });

  app.get('/api/metrics', (req, res) => {
    res.json({ success: true, metrics: metrics.snapshot() });
  });

  // Build routers once, mount twice
  const inventoryRouter = buildInventoryRouter({ inventoryRepo, movementsRepo, config, logger });
  const ordersRouter = buildOrdersRouter({ ordersRepo, config, logger });

  app.use('/api/inventory', inventoryRouter);
  app.use('/api/orders', ordersRouter);

  // Bare paths — what the frontends actually call
  app.use('/inventory', inventoryRouter);
  app.use('/orders', ordersRouter);

  // Static files
  app.use(express.static(config.PUBLIC_DIR));
  app.get('/', (req, res) => {
    res.sendFile(path.join(config.PUBLIC_DIR, 'index.html'), err => {
      if (err) res.status(200).send('Maricopa Inventory and Orders API is running.');
    });
  });

  // 404
  app.use((req, res) => {
    res.status(404).json({
      error: 'Route not found',
      details: [`${req.method} ${req.originalUrl} does not exist`]
    });
  });

  // Global error handler
  app.use((err, req, res, next) => {
    logger.error('Unhandled application error', {
      error: err.message,
      path: req.originalUrl,
      method: req.method
    });
    if (res.headersSent) return next(err);
    const status = err.status || err.statusCode || 500;
    res.status(status).json({
      error: config.IS_PROD && status >= 500 ? 'An unexpected server error occurred' : err.message,
      details: config.IS_PROD && status >= 500 ? [] : [err.message]
    });
  });

  async function initialiseDatabase() {
    await inventoryRepo.ensure();
    await ordersRepo.ensure();
    await movementsRepo.ensure();
    logger.info('Database initialisation completed');
  }

  return { app, mongo, logger, metrics, initialiseDatabase };
}

// ============================================================
// 10. SERVER STARTUP + GRACEFUL SHUTDOWN
// ============================================================
async function startServer() {
  const { app, mongo, logger, initialiseDatabase } = createApp(CONFIG);

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
        if (err) logger.error('HTTP server shutdown error', { error: err.message });
        try {
          await mongo.close();
          logger.info('MongoDB connection closed');
        } catch (closeErr) {
          logger.error('MongoDB shutdown error', { error: closeErr.message });
        }
        clearTimeout(timeout);
        process.exit(err ? 1 : 0);
      });
    }

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    process.on('unhandledRejection', reason => {
      logger.error('Unhandled promise rejection', {
        error: reason instanceof Error ? reason.message : String(reason)
      });
    });

    process.on('uncaughtException', err => {
      logger.error('Uncaught exception', { error: err.message, stack: err.stack });
      shutdown('uncaughtException');
    });

    return server;
  } catch (err) {
    logger.error('Server startup failed', { error: err.message, stack: err.stack });
    await mongo.close();
    process.exitCode = 1;
  }
}

// ============================================================
// 11. START
// ============================================================
if (require.main === module) {
  startServer();
}

module.exports = {
  createApp,
  loadConfig,
  createMongoRepository,
  createMemoryRepository,
  buildInventoryRouter,
  buildOrdersRouter,
  escapeRegex,
  makeId,
  makeReference
};
