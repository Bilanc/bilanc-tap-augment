# bilanc-tap-augment

A [Singer](https://www.singer.io/) tap for extracting analytics data from Augment.

## Streams

This tap currently exposes:

- `user_activity_daily`
- `credit_usage_by_user`

## Configuration

Create a `config.json` file:

```json
{
  "api_key": "your_augment_api_key",
  "start_date": "2026-01-01T00:00:00Z"
}
```

- `api_key` (required): Augment API key.
- `start_date` (required): Start date for incremental sync.

## Usage

```bash
# Install locally
pip install -e .

# Discovery mode
tap-augment --config config.json --discover > catalog.json

# Sync mode
tap-augment --config config.json --catalog catalog.json
```
