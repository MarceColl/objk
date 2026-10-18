# objK

A very simple Kafka-like message log built on top of S3.
Right now lacking most QoL features you'd want.

## Run locally

```sh
OBJK_PATH=$PWD/data cargo run
```

Listens on `:3000` and stores data under `./data`.

## Run with Docker

```sh
docker run -p 3000:3000 -v objk:/data ghcr.io/marcecoll/objk:main
```

Against an S3-compatible store (S3, RustFS, MinIO, R2); the bucket must already exist:

```sh
docker run -p 3000:3000 \
  -e OBJK_STORE=s3://my-bucket -e OBJK_PATH=objk \
  -e AWS_ENDPOINT=http://rustfs:9000 -e AWS_ALLOW_HTTP=true \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_REGION=us-east-1 \
  ghcr.io/marcecoll/objk:main
```

Only one objk may run per store + path: slateDB fences the older writer.

## Config

| Env          | Default    | Meaning                                                      |
|--------------|------------|--------------------------------------------------------------|
| `OBJK_STORE` | `file:///` | Object store root URL, no path (`s3://bucket`, `memory:///`) |
| `OBJK_PATH`  | `data`     | DB location inside the store                                 |
| `AWS_*`      |            | Store options (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, ...)        |

## API

```
POST /t/{topic}/{p}              body: message(s)  -> first offset
GET  /t/{topic}/{p}                                -> head (next offset to be written)
GET  /t/{topic}/{p}/{from}                         -> up to 1000 messages from `from`
PUT  /g/{group}/{topic}/{p}/{off}                  -> commit next offset to read (monotonic)
GET  /g/{group}/{topic}/{p}                        -> committed offset (0 if none)
GET  /metrics                                      -> Prometheus
```

Topics and partitions exist once written to. A batch is written atomically at contiguous offsets.

Formats are picked by header:

| Header                    | Value                      | Body                                      |
|---------------------------|----------------------------|-------------------------------------------|
| produce `Content-Type`    | `application/json`         | `["msg", ...]` (UTF-8 strings)            |
|                           | `application/x-objk-batch` | `(u32 len \| bytes)*`, big-endian         |
|                           | anything else              | the whole body is one message             |
| fetch `Accept`            | `application/json`         | `[{"offset": 0, "value": "msg"}, ...]`    |
|                           | anything else              | `(u64 off \| u32 len \| bytes)*`, big-endian |

JSON fetch fails with 400 on non-UTF-8 messages; use the binary format for those.

```sh
curl -XPOST --data-binary hello localhost:3000/t/orders/0                                      # 0
curl -XPOST -H 'content-type: application/json' -d '["a","b"]' localhost:3000/t/orders/0      # 1
curl localhost:3000/t/orders/0                                                                 # 3
curl -H 'accept: application/json' localhost:3000/t/orders/0/0
curl -XPUT localhost:3000/g/billing/orders/0/3
```

Logs go to stdout; filter with `RUST_LOG` (default `info`). SIGTERM/SIGINT drain in-flight requests and close the DB.
