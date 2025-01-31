/FastAPI_Project/
├── my_repo/
│ ├── app/ # Main app folder
│ │ ├── main.py # Entry point
│ │ ├── database.py # Database connection & models
│ │ ├── models.py # SQLAlchemy models
│ │ ├── crud.py # Functions for interacting with the database
│ │ ├── routes/ # Folder for API endpoints
│ │ │ ├── crypto.py # Crypto data endpoint
│ │ │ ├── logs.py # Logs API
│ ├── database/ # Stores SQLite database (outside repo)
│ │ ├── requests.db
│ ├── keys.py # API keys (excluded from Git)
│ ├── .gitignore # Prevents sensitive files from being committed
