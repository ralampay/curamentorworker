# Curamentor Worker

`curamentorworker` is a CLI daemon that polls an AWS SQS FIFO queue, downloads payloads from S3, vectorizes them with a local `llama-cpp-python` model, and persists the results to PostgreSQL.

## Quick start

1. Create and activate your Python 3.11+ environment.
2. Install the package (and dependencies) with `pip install -e .`.
3. Populate the required environment variables (see below).
4. Run the worker with `python -m curamentorworker` or the `curamentor-worker` console script.

## Python dependencies

The worker ships with its required packages in `pyproject.toml`, so `pip install -e .` installs the same versions as these direct requirements:

```bash
pip install boto3>=1.30.0 psycopg[binary]>=3.1 llama-cpp-python>=0.1.79
```

- `boto3` powers the FIFO SQS consumer and S3 download logic.
- `psycopg[binary]` provides the PostgreSQL client with prebuilt binaries for easier installs.
- `llama-cpp-python` wraps the local GGML/Llama model used for vector embeddings; keep `LLAMA_MODEL_PATH` pointed at a matching `.ggml*` file.

If you prefer not to install in editable mode, pin the same dependency versions elsewhere (for example, in a `requirements.txt`) and install them into your environment before running the worker.

The worker logs verbosely to `stdout` and to `log/development.log` or `log/production.log` depending on the value of `APP_ENV`. Logs roll as long as the target directory exists (created automatically).

## Environment variables

| Variable | Description |
|---|---|
| `APP_ENV` | `development` or `production`; determines the log file and default log level. |
| `AWS_REGION` | AWS region where your FIFO SQS queue and S3 bucket live. |
| `AWS_SQS_QUEUE_URL` | Full URL of the AWS FIFO queue to consume. |
| `AWS_SQS_QUEUE_NAME` | Name of the queue; used to build a LocalStack URL in development (falls back to the name segment of `AWS_SQS_QUEUE_URL`). |
| `LOCALSTACK_URL` | Base endpoint for LocalStack; when running with `APP_ENV=development` this is used in place of the live `AWS_SQS_QUEUE_URL`. |
| `S3_BUCKET_NAME` | Default S3 bucket name that payloads in the queue reference. |
| `S3_PREFIX` | Optional key prefix prepended when downloading assets. |
| `DB_HOST` | PostgreSQL host (required). |
| `DB_PORT` | PostgreSQL port (default `5432`). |
| `DB_NAME` | Database name (default `curamentor`). |
| `DB_USER` | PostgreSQL username (required). |
| `DB_PASSWORD` | PostgreSQL password (required). |
| `LLAMA_MODEL_PATH` | Filesystem path to the `llama-cpp-python` GGML model file. |
| `OPENAI_API_KEY` | API key for OpenAI if using hosted embeddings; required when `--local` is not passed. |
| `OPENAI_API_BASE` | Optional base URL when pointing at OpenAI-compatible hosts. |
| `OPENAI_API_VERSION` | Optional API version header override for OpenAI. |
| `OPENAI_EMBEDDING_MODEL` | OpenAI model used for remote embeddings (default `text-embedding-ada-002`). |
| `POLL_INTERVAL_SECONDS` | Seconds to wait with no messages before polling again (default `5`). |
| `MAX_MESSAGES` | How many messages to fetch per batch (default `1`). |
| `SQS_VISIBILITY_TIMEOUT` | Visibility timeout used when claiming messages (default `30`). |

When `APP_ENV=development` and `LOCALSTACK_URL` is provided, the worker builds the queue address as `<LOCALSTACK_URL>/000000000000/<queue-name>` where the queue name comes from `AWS_SQS_QUEUE_NAME` (or, if that variable is unset, the final path segment of `AWS_SQS_QUEUE_URL`). In this mode the boto3 SQS client also uses `<LOCALSTACK_URL>` as its endpoint so polling happens locally instead of hitting live AWS.

- Remote OpenAI embeddings automatically chunk large documents into 4k-character slices, average the resulting embeddings, and persist the combined vector so documents that otherwise exceed the model's context limit still succeed.

## Logging

- `APP_ENV=development` writes to `log/development.log` and enables `DEBUG` output.
- `APP_ENV=production` writes to `log/production.log` and uses `INFO` level.
- Console logging mirrors file output for easy observation during local execution or container logs.

## Future work

- Add schema migration or table creation for `vectorized_documents`.
- Replace the simple embedding call with a more robust text chunking and batching strategy.
