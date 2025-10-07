# ex8-producer

Comprehensive documentation for the ex8-producer project.

This project implements a small data-producer that reads CSV files from an S3-compatible storage, serializes rows using an Avro schema and produces them to a Kafka topic in chunks.

## Table of contents

- Project overview
- Installation & dependencies
- Configuration (environment variables)
- Quick start / example
- Modules, classes and functions (detailed)
- Avro schema (fields and types)
- Contract & edge cases
- Troubleshooting
- Development notes and next steps

## Project overview

The producer continuously polls an S3 bucket (or S3-compatible endpoint) for CSV files under a configured prefix. For each CSV it finds, it:

- downloads and parses the CSV (expects `;` as delimiter and `iso-8859-1` encoding),
- sanitizes CSV header names to match schema fields,
- groups rows into chunks and encodes each row using an Avro writer,
- sends chunked messages to a configured Kafka topic via `kafka-python`.

It is implemented in the `ex8_producer` package under `src/`.

## Installation & dependencies

This project uses Poetry for dependency management. Required runtime dependencies (from `pyproject.toml`) include:

- boto3 (S3 client)
- python-dotenv (load .env file)
- avro (Avro schema + IO)
- kafka-python (Kafka producer)

To install the project in editable/development mode (if you use poetry):

```bash
poetry install
poetry shell
```

Alternatively, create a virtualenv and pip-install the dependencies listed above.

## Configuration (environment variables)

The project expects environment variables (a `.env` file is loaded by default from the repository root):

- AWS_DEFAULT_REGION: AWS region name (required)
- AWS_ACCESS_KEY_ID: AWS access key id (required)
- AWS_SECRET_ACCESS_KEY: AWS secret access key (required)
- AWS_ENDPOINT_URL: optional S3 endpoint URL (for local S3-compatible stores like MinIO)
- S3_BUCKET_NAME: name of the S3 bucket to poll (required)
- S3_PATH_PREFIX: optional prefix within the bucket to list CSVs from (default empty)
- KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap server addresses (default `localhost:9092`)
- KAFKA_TOPIC: Kafka topic to publish to (default `my-topic`)

Project constants (default values) are defined in `src/ex8_producer/settings.py`:

- CHUNK_SIZE (default 10): number of rows to accumulate before sending to Kafka
- PROCESS_INTERVAL (default 60 seconds): wait time between bucket checks
- RETRY_DELAY (default 20 seconds): short delay after sending each chunk
- DOTENV_PATH: path used to load the `.env` file (`/home/bait/dev/ex8-producer/.env` by default)
- SCHEMA_PATH: relative path to the Avro schema file (`src/ex8_producer/schemas/reclamacoes.avsc`)

Make sure your `.env` sets at least the AWS and S3/Kafka variables required.

## Quick start / example

Run the producer directly with Python (it uses `if __name__ == '__main__'`):

```bash
# from project root
python -m ex8_producer.app
```

Or run the module file directly:

```bash
python src/ex8_producer/app.py
```

The producer will read the environment, create an S3 client, a Kafka producer, and begin polling.

## Modules, classes and functions (detailed)

All source is under `src/ex8_producer/`.

### ex8_producer.app

Primary responsibilities: orchestrate S3 listing, CSV parsing, Avro encoding and Kafka production.

- Class: `Producer`

	- Purpose: Read CSV files from S3, convert rows to Avro and send to Kafka in chunks.

	- Constructor: `Producer(bucket_name: str, kafka_bootstrap_servers: str, kafka_topic: str, s3_path_prefix: str)`
		- Inputs:
			- `bucket_name`: the S3 bucket to use
			- `kafka_bootstrap_servers`: Kafka bootstrap servers string
			- `kafka_topic`: topic to publish messages to
			- `s3_path_prefix`: prefix under which CSV files reside
		- Side effects: creates an S3 client, a Kafka producer and parses the Avro schema
		- Raises: `ValueError` if AWS credentials/region are missing when creating the S3 client; other exceptions may propagate when initializing Kafka or parsing schema

	- Methods:

		- `_get_aws_clients() -> boto3.client`
			- Initializes and returns a Boto3 S3 client using environment variables from `settings`.
			- Raises `ValueError` if required AWS variables are missing.

		- `_get_kafka_producer() -> KafkaProducer`
			- Creates and returns a `kafka.KafkaProducer` instance.
			- Uses `value_serializer=lambda v: v` to send raw bytes (Avro binary payloads).
			- Logs and re-raises on failure.

		- `_get_parsed_schema() -> Dict[str, Any]`
			- Reads `SCHEMA_PATH` and parses the Avro schema via `avro.schema.parse`.
			- Logs and raises if the schema file is missing or cannot be parsed.

		- `list_csv_files() -> List[str]`
			- Uses S3 `list_objects_v2` through a paginator to list keys under `s3_path_prefix` that end with `.csv`.
			- Returns list of S3 object keys (strings). Returns empty list if none found or on `ClientError`.

		- `_process_csv_rows(object_key: str) -> Iterator[Dict[str, Any]]`
			- Downloads the object from S3 using `get_object` and reads its body decoded with `iso-8859-1`.
			- Parses it using `csv.DictReader` with `;` delimiter.
			- For each row it:
				- sanitizes each CSV header using `sinitize_text(field).lower()` (see utils),
				- if the sanitized field exists in the in-memory `SCHEMA` mapping (from settings), it keeps the value, otherwise ignores the column,
				- yields a dict mapping sanitized_field -> value or None when empty.
			- Handles `NoSuchKey` (file not found) and logs unexpected exceptions.

		- `_send_chunk_to_kafka(chunk: List[Dict[str, Any]])`
			- For each record in the chunk, uses `avro.io.DatumWriter` with the parsed schema to write binary Avro to a `BytesIO` buffer.
			- Sends each Avro binary payload as-is to Kafka using `self.kafka_producer.send(self.kafka_topic, avro_binary_data)`.
			- Flushes the producer after the chunk is sent.
			- Logs errors on failure.

		- `process_file(object_key: str)`
			- Iterates rows from `_process_csv_rows`, accumulates them into a list of size `CHUNK_SIZE` and calls `_send_chunk_to_kafka` for each chunk.
			- Sends any remaining rows after iteration completes.

		- `run()`
			- Main loop: polls S3 for CSVs, processes each file, then sleeps `PROCESS_INTERVAL` seconds before repeating.
			- If no CSVs are found, it waits and continues.

	- Function-level `main()` provided at module level to create a `Producer` from environment settings and call `run()`.

	- Important behavior notes:
		- CSVs are expected to use `;` as a delimiter and `iso-8859-1` encoding.
		- Header names are sanitized and lowercased before matching against the `SCHEMA` mapping.
		- Avro encoding happens per-row; rows are sent individually inside chunk loops, but chunking controls flush frequency.

### ex8_producer.settings

Small module that:

- loads environment variables using `python-dotenv` (DOTENV_PATH defaulted)
- defines constants used by the producer: `CHUNK_SIZE`, `PROCESS_INTERVAL`, `RETRY_DELAY`, etc.
- exposes `SCHEMA_PATH` (path to Avro schema) and an in-memory `SCHEMA` mapping (field name -> expected type).

Notes:

- `SCHEMA_PATH` points to `src/ex8_producer/schemas/reclamacoes.avsc`.
- The `SCHEMA` in `settings.py` is a convenience mapping used by `Producer` to know which fields to keep while reading CSVs. The Avro schema file itself should be considered authoritative for produced messages.

### ex8_producer.utils.functions

- Function: `sinitize_text(text: str) -> str`
	- Purpose: Normalizes and sanitizes CSV header strings into safe field names.
	- Behavior:
		- Unicode normalization (NFKD).
		- Removes non-word characters (keeps letters, numbers and underscore).
		- Replaces whitespace runs with a single underscore.
		- Collapses multiple underscores and strips leading/trailing underscores.
		- If the result starts with a digit, prefixes with `col_`.
	- Examples:
		- `"Quantidade de Reclamacoes"` -> `quantidade_de_reclamacoes`
		- `"123 coluna"` -> `col_123_coluna`

	- Note: There is a small naming inconsistency in the code: the function is named `sinitize_text` (with an `i`) and not `sanitize_text`. Be aware when importing or referencing it.

## Avro schema (`src/ex8_producer/schemas/reclamacoes.avsc`)

The Avro record is named `reclamacoes` in the `ex7_producer` namespace. The schema defines the following fields (as stored in the `.avsc` file):

| Field name | Type (in schema) | Default |
|---|---:|---|
| ano | string | - |
| trimestre | string | - |
| categoria | string | - |
| tipo | string | - |
| cnpj_if | [null, string] | null |
| instituicao_financeira | string | - |
| indice | string | - |
| quantidade_de_reclamacoes_reguladas_procedentes | string | - |
| quantidade_de_reclamacoes_reguladas_outras | [null, string] | null |
| quantidade_de_reclamacoes_nao_reguladas | [null, string] | null |
| quantidade_total_de_reclamacoes | string | - |
| quantidade_total_de_clientes_ccs_e_scr | string | - |
| quantidade_de_clientes_ccs | [null, string] | null |
| quantidade_de_clientes_scr | [null, string] | null |

Important: The Avro `.avsc` file stores many numeric-looking fields as `string`. The `settings.SCHEMA` mapping uses some types like `int` for a few fields. This mismatch can cause issues when producing Avro `DatumWriter` payloads if Python dictionaries contain strings while the Avro schema expects ints (or vice-versa). Consider adding explicit validation or type conversion in `_process_csv_rows` before encoding.

## Contract (tiny)

- Inputs: CSV files stored in S3 bucket/prefix. CSV rows must contain headers that can be sanitized and mapped to schema fields.
- Outputs: Binary Avro-encoded messages sent to Kafka topic.
- Data shape: Each message is an Avro-serialized record following `reclamacoes` schema. The code currently writes each row using the parsed Avro schema; ensure type compatibility.
- Error modes: missing AWS credentials -> startup `ValueError`; missing schema file -> `FileNotFoundError` on parse; S3 or Kafka errors logged and may be retried by the loop; Avro serialization errors will be logged.
- Success criteria: Messages are flushed to Kafka successfully without exceptions; chunking controls throughput.

