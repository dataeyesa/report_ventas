# app.py
import os
import re
import time
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ======================================
# Config (variables de entorno)
# ======================================
# Para Render con disco persistente:
#   DATABASE_URL = sqlite:////var/data/report.db
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./local.db")
API_KEY = os.getenv("API_KEY")  # opcional: exige header X-API-Key si está seteada
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "5000"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "500"))
MAX_TIMEOUT_MS = int(os.getenv("MAX_TIMEOUT_MS", "15000"))

# Bloqueos básicos (solo SELECT; sin DDL/DML/comentarios ni múltiples sentencias)
FORBIDDEN_PATTERNS = [
    r";",  # prohibir múltiples sentencias
    r"\b(insert|update|delete|merge|call|grant|revoke|truncate|alter|drop|create)\b",
    r"\bcopy\b",   # COPY (pg)
    r"--",         # comentario inline
    r"/\*",        # comentario multi
]
SELECT_PREFIX = re.compile(r"^\s*select\b", re.IGNORECASE)

# ======================================
# App & DB
# ======================================
app = Flask(__name__)
CORS(app)


def _create_engine(url: str) -> Engine:
    """
    Crea engine según motor. Para SQLite:
    - check_same_thread=False (multi-thread con gunicorn --threads)
    - pool_pre_ping para evitar conexiones muertas
    """
    if url.startswith("sqlite"):
        # Asegura que la carpeta exista si apuntas a /var/data
        if url.startswith("sqlite:////var/data/"):
            os.makedirs("/var/data", exist_ok=True)
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
            future=True,
        )
    # Otros motores (si migras a Postgres)
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=180,
        future=True,
    )


engine: Engine = _create_engine(DATABASE_URL)

# ======================================
# Helpers
# ======================================
def _check_api_key() -> bool:
    """Valida API key si está configurada."""
    if API_KEY:
        return request.headers.get("X-API-Key") == API_KEY
    return True


def _validate_sql(sql: str) -> Tuple[bool, str]:
    """Solo acepta SELECT al inicio y rechaza patrones peligrosos."""
    if not SELECT_PREFIX.search(sql or ""):
        return False, "Solo se permiten consultas que inicien con SELECT."
    lower_sql = sql.lower()
    for pat in FORBIDDEN_PATTERNS:
        if re.search(pat, lower_sql):
            return False, f"Patrón prohibido detectado: {pat}"
    return True, ""


def _apply_limit_offset(sql: str, limit: int, offset: int) -> str:
    """Si el SQL no trae LIMIT/OFFSET, los agrega al final."""
    if re.search(r"\blimit\b\s+\d+", sql, re.IGNORECASE):
        return sql
    return f"{sql.strip()} LIMIT :__limit OFFSET :__offset"


def _set_statement_timeout(conn, timeout_ms: int):
    """Configura PRAGMAs y timeouts según motor."""
    if DATABASE_URL.startswith("sqlite"):
        # PRAGMAs recomendados para SQLite en producción ligera
        conn.exec_driver_sql("PRAGMA foreign_keys = ON;")
        conn.exec_driver_sql("PRAGMA journal_mode = WAL;")
        conn.exec_driver_sql("PRAGMA synchronous = NORMAL;")
        conn.exec_driver_sql(f"PRAGMA busy_timeout = {timeout_ms};")
    elif DATABASE_URL.startswith("postgresql"):
        conn.exec_driver_sql(f"SET LOCAL statement_timeout = {timeout_ms}")
        conn.exec_driver_sql("SET LOCAL default_transaction_read_only = on")


# ======================================
# Endpoints
# ======================================
@app.get("/")
def root():
    return jsonify({"status": "up", "hint": "try /health or POST /run_query"}), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/run_query")
def run_query():
    # Autenticación opcional
    if not _check_api_key():
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    sql = payload.get("sql", "")
    params: Dict[str, Any] = payload.get("params", {}) or {}

    # Validaciones
    ok, msg = _validate_sql(sql)
    if not ok:
        return jsonify({"error": msg}), 400

    try:
        req_limit = int(payload.get("limit", DEFAULT_LIMIT))
        req_offset = int(payload.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset deben ser enteros"}), 400

    if req_limit < 1:
        req_limit = DEFAULT_LIMIT
    limit_effective = min(req_limit, MAX_LIMIT)

    try:
        timeout_ms_req = int(payload.get("timeout_ms", MAX_TIMEOUT_MS))
    except ValueError:
        timeout_ms_req = MAX_TIMEOUT_MS
    timeout_ms = min(timeout_ms_req, MAX_TIMEOUT_MS)

    # Paginación si falta
    sql_final = _apply_limit_offset(sql, limit_effective, req_offset)

    # Params reservados si agregamos LIMIT/OFFSET
    bound_params = dict(params)
    if ":__limit" in sql_final or " :__limit" in sql_final:
        bound_params["__limit"] = limit_effective
        bound_params["__offset"] = req_offset

    start = time.time()
    try:
        with engine.begin() as conn:
            _set_statement_timeout(conn, timeout_ms)
            res = conn.execute(text(sql_final), bound_params)
            rows = res.fetchall()
            fields = list(res.keys())
            elapsed_ms = round((time.time() - start) * 1000, 3)

        return jsonify({
            "fields": fields,
            "rows": [list(r) for r in rows],
            "rowcount": len(rows),
            "limit_applied": limit_effective,
            "offset": req_offset,
            "elapsed_ms": elapsed_ms
        }), 200

    except SQLAlchemyError as e:
        return jsonify({"error": "DB_ERROR", "detail": str(e.__cause__ or e)}), 500
    except Exception as e:
        return jsonify({"error": "SERVER_ERROR", "detail": str(e)}), 500


# ======================================
# Entrypoint local
# ======================================
if __name__ == "__main__":
    # Para pruebas locales: DATABASE_URL=sqlite:///./local.db
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
