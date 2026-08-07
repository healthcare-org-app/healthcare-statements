# statements-service

statements-service — domain: billing

- **Port:** 8708
- **Language:** Python 3.11 + Flask
- **Database:** `billing` (Postgres, table `statements`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/statements/`          |
| POST      | `/api/statements/`          |
| GET       | `/api/statements/<id>`      |
| PUT/PATCH | `/api/statements/<id>`      |
| DELETE    | `/api/statements/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** invoice.issued, invoice.paid

## HTTP peer dependencies

- `invoicing-service`
- `patients-service`
- `notifications-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
