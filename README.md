# rest-kv (Minimal REST-ish DB)

Node.js + TypeScript, **no Express**, runtime deps **zero**. A tiny CRUD server that treats `/v1/data/<collection>` as a resource collection backed by JSON files.

## Endpoints

- `GET    /v1/data/:collection` list
- `POST   /v1/data/:collection` create (UUID id)
- `GET    /v1/data/:collection/:id` get
- `PUT    /v1/data/:collection/:id` replace (upsert)
- `PATCH  /v1/data/:collection/:id` partial update
- `DELETE /v1/data/:collection/:id` delete

## Run

```bash
npm i
npm run build
npm start
```

Dev (two terminals):

```bash
# A
npm run watch

# B
npm run dev
```

## Env

- `PORT` (default 8787)
- `HOST` (default 0.0.0.0)
- `DATA_DIR` (default ./data)
- `CORS` (default 1)
- `MAX_BODY_BYTES` (default 1000000)

## Quick test

```bash
curl -X POST http://localhost:8787/v1/data/machines   -H "Content-Type: application/json"   -d '{"name":"router1","ip":"10.0.0.1"}'

curl http://localhost:8787/v1/data/machines
```
