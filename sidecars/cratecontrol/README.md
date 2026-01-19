# CrateDB Control Sidecar

Lightweight HTTP proxy for executing SQL statements against CrateDB.

## Features

- **Minimal footprint**: ~20-25 MB memory
- **Simple**: Single Python file, 2 dependencies

## Endpoints

- `GET /health` - Health check
- `POST /exec` - Execute SQL statement

## Environment Variables

- `BOOTSTRAP_TOKEN` (required) - Authentication token
- `CRATE_HTTP_URL` - CrateDB HTTP endpoint (default: `http://localhost:4200/_sql`)
- `CRATE_USERNAME` - CrateDB username (optional)
- `CRATE_PASSWORD` - CrateDB password (optional)
- `VERIFY_SSL` - Verify SSL certificates (default: `false`)
- `HTTP_TIMEOUT` - Request timeout in seconds (default: `30`)
- `LOG_LEVEL` - Logging level (default: `INFO`)

## Usage

```bash
curl -X POST http://localhost:5050/exec \
  -H "Token: your-bootstrap-token" \
  -H "Content-Type: application/json" \
  -d '{"stmt": "SELECT current_user;"}'
```

## Build & Run

```bash
docker build -t crate/crate-control:latest .
docker run -p 5050:5050 \
  -e BOOTSTRAP_TOKEN=test123 \
  -e CRATE_HTTP_URL=http://localhost:4200/_sql \
  crate/crate-control:latest
```

## License

Licensed to Crate.IO GmbH ("Crate") under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information regarding
copyright ownership. Crate licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

However, if you have executed another commercial license agreement with Crate these terms
will supersede the license and you may use the software solely pursuant to the terms of
the relevant commercial agreement.
