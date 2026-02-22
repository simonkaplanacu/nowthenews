import os
from dotenv import load_dotenv

load_dotenv()

GUARDIAN_API_KEY = os.environ["GUARDIAN_API_KEY"]
GUARDIAN_BASE_URL = "https://content.guardianapis.com"

CLICKHOUSE_DSN = os.getenv("CLICKHOUSE_DSN", "clickhouse://localhost:9000/news")

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_NUM_CTX = 8192
