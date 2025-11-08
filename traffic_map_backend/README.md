# Traffic Map Backend API

RESTful API backend for Traffic Map application built with Node.js, Express, and PostgreSQL.

## Quick Start

### Prerequisites

- Node.js 16+
- PostgreSQL 13+ with PostGIS extension
- npm or yarn

### Installation

```bash
# Install dependencies
npm install

# Copy environment template
cp .env.example .env

# Edit .env with your database credentials
nano .env
```

### Environment Configuration

Update `.env` file with your database credentials:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=traffic_db
DB_USER=your_username
DB_PASSWORD=your_password
```

See `.env.example` for all available configuration options.

### Running the Server

```bash
npm start

Server will start at `http://localhost:3000`
