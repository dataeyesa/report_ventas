from flask import Flask, jsonify
import os, sqlite3

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "ventas.db")  # AJUSTA

def db_stats():
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    info = {"path": DB_PATH, "exists": exists, "size_bytes": size}

    if not exists or size < 1024:  # <1KB huele a pointer o archivo creado vacío
        info["warning"] = "DB no encontrada o es un pointer/archivo vacío"
        return info

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        info["tables"] = tables

        if "ventas" in tables:
            cur.execute("SELECT COUNT(*) FROM ventas")
            info["ventas_count"] = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        info["error"] = str(e)

    return info

@app.get("/health_db")
def health_db():
    return jsonify(db_stats())
