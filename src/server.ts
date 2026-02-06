import http from "node:http";
import { promises as fs } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

type JsonValue = null | boolean | number | string | JsonValue[] | { [k: string]: JsonValue };

type CollectionFile = {
  items: Record<string, JsonValue>;
  updatedAt: number;
};

const PORT = Number(process.env.PORT ?? 8787);
const HOST = process.env.HOST ?? "0.0.0.0";
const BASE_PATH = "/v1/data";
const DATA_DIR = process.env.DATA_DIR ?? path.join(process.cwd(), "data");

const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES ?? 1_000_000); // 1MB
const ENABLE_CORS = (process.env.CORS ?? "1") === "1";

const locks = new Map<string, Promise<void>>();

function json(res: http.ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(payload).toString(),
    ...headers,
  });
  res.end(payload);
}

function noContent(res: http.ServerResponse, status = 204, headers: Record<string, string> = {}) {
  res.writeHead(status, { ...headers });
  res.end();
}

function badRequest(res: http.ServerResponse, message: string) {
  json(res, 400, { error: "bad_request", message });
}

function notFound(res: http.ServerResponse) {
  json(res, 404, { error: "not_found" });
}

function methodNotAllowed(res: http.ServerResponse, allow: string[]) {
  json(res, 405, { error: "method_not_allowed", allow }, { Allow: allow.join(", ") });
}

function conflict(res: http.ServerResponse, message: string) {
  json(res, 409, { error: "conflict", message });
}

function internalError(res: http.ServerResponse, message: string) {
  json(res, 500, { error: "internal_error", message });
}

function addCors(res: http.ServerResponse) {
  if (!ENABLE_CORS) return;
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, If-Match");
  res.setHeader("Access-Control-Expose-Headers", "ETag, Location");
}

function sanitizeCollectionName(name: string): string | null {
  if (!name || name.length > 128) return null;
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return null;
  return name;
}

async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

function collectionFilePath(collection: string) {
  return path.join(DATA_DIR, `${collection}.json`);
}

async function readCollection(collection: string): Promise<CollectionFile> {
  const p = collectionFilePath(collection);
  try {
    const text = await fs.readFile(p, "utf8");
    const parsed = JSON.parse(text) as CollectionFile;
    if (!parsed || typeof parsed !== "object" || typeof (parsed as any).updatedAt !== "number" || typeof (parsed as any).items !== "object") {
      return { items: {}, updatedAt: Date.now() };
    }
    return parsed;
  } catch (e: any) {
    if (e?.code === "ENOENT") return { items: {}, updatedAt: Date.now() };
    throw e;
  }
}

async function writeCollectionAtomic(collection: string, data: CollectionFile) {
  const p = collectionFilePath(collection);
  const tmp = `${p}.${process.pid}.${Date.now()}.tmp`;
  const text = JSON.stringify(data);
  await fs.writeFile(tmp, text, "utf8");
  await fs.rename(tmp, p);
}

function withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const prev = locks.get(key) ?? Promise.resolve();
  let release!: () => void;
  const next = new Promise<void>((r) => (release = r));
  locks.set(key, prev.then(() => next));

  return prev
    .then(fn)
    .finally(() => {
      release();
      // Note: leaving entry is fine; minimal cleanup omitted for simplicity
    });
}

async function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error("payload_too_large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function parseJsonBody(text: string): JsonValue {
  if (!text) return null;
  return JSON.parse(text) as JsonValue;
}

function shallowMerge(a: JsonValue, b: JsonValue): JsonValue {
  if (a && typeof a === "object" && !Array.isArray(a) && b && typeof b === "object" && !Array.isArray(b)) {
    return { ...(a as any), ...(b as any) };
  }
  return b;
}

function etagOf(updatedAt: number) {
  return `"u${updatedAt}"`;
}

function pickQueryInt(u: URL, key: string, def: number, min: number, max: number) {
  const v = u.searchParams.get(key);
  if (!v) return def;
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

const server = http.createServer(async (req, res) => {
  try {
    addCors(res);

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    const host = req.headers.host ?? "localhost";
    const url = new URL(req.url ?? "/", `http://${host}`);

    if (!url.pathname.startsWith(BASE_PATH)) {
      notFound(res);
      return;
    }

    const rest = url.pathname.slice(BASE_PATH.length).replace(/^\/+/, "");
    if (!rest) {
      json(res, 200, {
        name: "rest-kv",
        basePath: BASE_PATH,
        endpoints: {
          list: "GET /v1/data/:collection",
          create: "POST /v1/data/:collection",
          get: "GET /v1/data/:collection/:id",
          put: "PUT /v1/data/:collection/:id",
          patch: "PATCH /v1/data/:collection/:id",
          delete: "DELETE /v1/data/:collection/:id",
        },
      });
      return;
    }

    const parts = rest.split("/").filter(Boolean).map(decodeURIComponent);
    if (parts.length < 1) return badRequest(res, "collection is required");
    if (parts.length > 2) return badRequest(res, "path depth > 2 is not supported");

    const collectionRaw = parts[0];
    const collection = sanitizeCollectionName(collectionRaw);
    if (!collection) return badRequest(res, "invalid collection name (allowed: A-Z a-z 0-9 . _ -)");
    const id = parts.length === 2 ? parts[1] : undefined;

    await ensureDataDir();

    if (!id) {
      if (req.method === "GET") {
        const limit = pickQueryInt(url, "limit", 100, 1, 1000);
        const offset = pickQueryInt(url, "offset", 0, 0, 1_000_000);
        const q = url.searchParams.get("q")?.toLowerCase() ?? "";

        const data = await readCollection(collection);
        const all = Object.entries(data.items).map(([id, v]) => {
          if (v && typeof v === "object" && !Array.isArray(v)) return { id, ...(v as any) };
          return { id, value: v };
        });

        const filtered = q ? all.filter((x) => JSON.stringify(x).toLowerCase().includes(q)) : all;
        const page = filtered.slice(offset, offset + limit);

        json(res, 200, { data: page, total: filtered.length, offset, limit }, { ETag: etagOf(data.updatedAt) });
        return;
      }

      if (req.method === "POST") {
        const raw = await readBody(req).catch((e) => {
          if (String(e?.message) === "payload_too_large") {
            json(res, 413, { error: "payload_too_large", maxBytes: MAX_BODY_BYTES });
            return null;
          }
          throw e;
        });
        if (raw === null) return;

        let body: JsonValue;
        try {
          body = parseJsonBody(raw);
        } catch {
          return badRequest(res, "invalid JSON");
        }

        const newId = crypto.randomUUID();
        const location = `${BASE_PATH}/${encodeURIComponent(collection)}/${encodeURIComponent(newId)}`;

        await withLock(collection, async () => {
          const data = await readCollection(collection);
          data.items[newId] = body;
          data.updatedAt = Date.now();
          await writeCollectionAtomic(collection, data);

          const response = body && typeof body === "object" && !Array.isArray(body) ? { id: newId, ...(body as any) } : { id: newId, value: body };
          json(res, 201, response, { Location: location, ETag: etagOf(data.updatedAt) });
        });
        return;
      }

      return methodNotAllowed(res, ["GET", "POST", "OPTIONS"]);
    }

    const itemId = id;

    if (req.method === "GET") {
      const data = await readCollection(collection);
      const found = data.items[itemId];
      if (found === undefined) return notFound(res);

      const payload = found && typeof found === "object" && !Array.isArray(found) ? { id: itemId, ...(found as any) } : { id: itemId, value: found };
      json(res, 200, payload, { ETag: etagOf(data.updatedAt) });
      return;
    }

    if (req.method === "DELETE") {
      await withLock(collection, async () => {
        const data = await readCollection(collection);
        if (data.items[itemId] === undefined) return notFound(res);
        delete data.items[itemId];
        data.updatedAt = Date.now();
        await writeCollectionAtomic(collection, data);
        noContent(res, 204, { ETag: etagOf(data.updatedAt) });
      });
      return;
    }

    if (req.method === "PUT" || req.method === "PATCH") {
      const raw = await readBody(req).catch((e) => {
        if (String(e?.message) === "payload_too_large") {
          json(res, 413, { error: "payload_too_large", maxBytes: MAX_BODY_BYTES });
          return null;
        }
        throw e;
      });
      if (raw === null) return;

      let body: JsonValue;
      try {
        body = parseJsonBody(raw);
      } catch {
        return badRequest(res, "invalid JSON");
      }

      await withLock(collection, async () => {
        const data = await readCollection(collection);
        const ifMatch = req.headers["if-match"];
        if (ifMatch && ifMatch !== etagOf(data.updatedAt)) return conflict(res, "ETag mismatch (concurrent modification detected)");

        const exists = data.items[itemId] !== undefined;
        const nextValue = req.method === "PATCH" && exists ? shallowMerge(data.items[itemId], body) : body;

        data.items[itemId] = nextValue;
        data.updatedAt = Date.now();
        await writeCollectionAtomic(collection, data);

        const payload = nextValue && typeof nextValue === "object" && !Array.isArray(nextValue) ? { id: itemId, ...(nextValue as any) } : { id: itemId, value: nextValue };
        json(res, exists ? 200 : 201, payload, { ETag: etagOf(data.updatedAt) });
      });
      return;
    }

    return methodNotAllowed(res, ["GET", "PUT", "PATCH", "DELETE", "OPTIONS"]);
  } catch (e: any) {
    console.error(e);
    internalError(res, e?.message ?? "unknown error");
  }
});

server.listen(PORT, HOST, () => {
  console.log(`rest-kv listening on http://${HOST}:${PORT}${BASE_PATH}`);
  console.log(`data dir: ${DATA_DIR}`);
});
