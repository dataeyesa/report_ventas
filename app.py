from flask import Flask, jsonify
import os, sqlite3

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /opt/render/project/src
# Permite override por env: DB_PATH=/opt/render/project/src/data/ventas.db
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "ventas.db"))

def db_stats():
    exists = os.path.exists(DB_PATH)
    size = os.path.getsize(DB_PATH) if exists else 0
    info = {"path": DB_PATH, "exists": exists, "size_bytes": size}

    if not exists or size < 1024:
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

# --- Diagnóstico rápido: lista todos los .db con tamaño ---
@app.get("/debug_repo")
def debug_repo():
    hits = []
    for dirpath, _, filenames in os.walk(BASE_DIR):
        for f in filenames:
            if f.endswith(".db"):
                fp = os.path.join(dirpath, f)
                try:
                    size = os.path.getsize(fp)
                except Exception:
                    size = None
                hits.append({"path": fp, "size_bytes": size})
    return jsonify({"root": BASE_DIR, "DB_PATH_env": DB_PATH, "db_files": hits})

# Opcional: endpoint mínimo para probar lectura directa de 'ventas'
@app.get("/ventas_count")
def ventas_count():
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ventas")
        total = cur.fetchone()[0]
        conn.close()
        return jsonify({"ok": True, "ventas_count": total, "path": DB_PATH})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "path": DB_PATH}), 500

if __name__ == "__main__":
    # Para correr local si quieres: python app.py
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
