# TradeManthan

A comprehensive trading platform with automated strategy execution, market scanning, and broker integrations.

## Features

- **Algorithmic Trading**: Custom strategy creation and execution
- **Market Scanner**: Real-time market scanning and analysis
- **Broker Integration**: Support for Delta Exchange, Dhan, and Upstox
- **Dashboard**: Real-time monitoring and portfolio management
- **Strategy Runner**: Automated strategy execution engine
- **API Integration**: RESTful API for trading operations

## Project Structure

```
TradeManthan/
├── backend/           # FastAPI backend server
│   ├── routers/      # API route handlers
│   ├── models/       # Database models
│   ├── services/     # Business logic services
│   ├── strategy_runner/  # Strategy execution engine
│   └── utils/        # Utility functions
├── frontend/         # Web frontend
│   └── public/       # Static files and HTML/JS/CSS
├── algos/           # Algorithm implementations
│   ├── api/         # Trading API integrations
│   ├── indicators/  # Technical indicators
│   └── strategy/    # Trading strategies
└── main.py          # Main entry point

```

## Setup

### Backend

1. Navigate to the backend directory:
```bash
cd backend
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy and configure environment variables:
```bash
cp env.example .env
# Edit .env with your credentials
```

5. Run the backend:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend

The frontend is served as static files. You can access it by opening the HTML files in the `frontend/public` directory or by serving them through the backend.

## API Endpoints

- `/auth` - Authentication endpoints
- `/dashboard` - Dashboard data
- `/strategy` - Strategy management
- `/algo` - Algorithm execution
- `/scan` - Market scanning
- `/broker` - Broker integrations
- `/products` - Trading products

## Technologies

- **Backend**: FastAPI, SQLAlchemy, Python 3.13
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Database**: SQLite (development), PostgreSQL (production ready)
- **APIs**: Delta Exchange, Dhan, Upstox, Yahoo Finance

## License

Private - All rights reserved

