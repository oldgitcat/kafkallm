# S3 / RustFS

RustFS 1.0 provides S3-compatible storage for uploaded inputs and job
artifacts. The service is defined in the top-level `compose.yaml`.

- S3 API: <http://localhost:19000>
- Console: <http://localhost:19001>
- Data volume: `rustfs-data`
- Internal endpoint used by the worker: `http://rustfs:9000`

Set `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and the bucket settings
in `.env`. Without a configured S3 endpoint, jobs still run, but uploaded inputs
and collected output artifacts are unavailable.
