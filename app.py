from flask import Flask, request, jsonify
import os, sqlite3
import pandas as pd

app = Flask(__name__)

# ===== Config =====
# Carpeta efímera (sin pago). Se borra en cada redeploy/restart.
DATA_DIR  = os.environ.get("DATA_DIR", "/tmp/data")
XLSX_NAME = os.environ.get("XLSX_NAME", "bd.ventas.xlsx")
XLSX_PATH = os.path.join(DATA_DIR, XLSX_NAME)
DB_PATH   = os.path.join(DATA_DIR, "ventas.db")

# ===== Bootstrap: descargar Excel y convertir a SQLite =====
def ensure_excel_present():
    """
    Descarga el Excel desde la variable de entorno EXCEL_URL si no existe localmente.
    """
    excel_url = os.environ.get("EXCEL_URL")
    if not excel_url:
        raise RuntimeError("Falta la variable de entorno EXCEL_URL con la URL pública del Excel.")
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(XLSX_PATH):
        # Descarga sin usar requests (no hay internet outbound en algunos entornos),
        # pero en Render sí hay curl desde el Start Command. Aquí por si acaso:
        try:
            # Intento 1: usar pandas directa (solo funciona si la URL es de descarga directa)
            df_head = pd.read_excel(excel_url, engine="openpyxl", nrows=0)
            # Si funcionó, descargamos completo
            df_full = pd.read_excel(excel_url, engine="openpyxl")
            df_full.to_excel(XLSX_PATH, index=False)
        except Exception as e:
            # Si falla por permisos/red, instruir al usuario a verificar Start Command
            raise RuntimeError(f"No se encontró {XLSX_PATH} y no se pudo descargar desde EXCEL_URL dentro de la app. "
                               f"Deja la descarga en el Start Command. Detalle: {e}")

def ensure_db():
    """
    Crea /tmp/data/ventas.db a partir del Excel si no existe.
    Como /tmp es efímero, esto se ejecutará en cada arranque si no persistes archivos.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    # Si no está el Excel, tratar de asegurar que exista (o advertir)
    if not os.path.exists(XLSX_PATH):
        # Normalmente el Start Command ya descargó el Excel.
        # Si no, intenta la ruta alterna (ver función arriba).
        ensure_excel_present()

    if not os.path.exists(DB_PATH):
        print("⚙️ Convirtiendo Excel a SQLite (primer arranque de este contenedor)...")
        # SUGERENCIA: usa usecols=[...] si quieres bajar memoria/tiempo
        df = pd.read_excel(XLSX_PATH, engine="openpyxl")
        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql("ventas", conn, if_exists="replace", index=False)
            # Índices (ajusta a tus columnas reales si existen)
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fecha    ON ventas(fecha)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_codigo   ON ventas(codigo)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_vendedor ON ventas(vendedor)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_nit      ON ventas(nit)")
            except Exception as e:
                print("Aviso al crear índices:", e)
        print(f"✅ Base creada en {DB_PATH}")

# Ejecuta bootstrap
ensure_db()

# ===== Helpers =====
def get_conn_ro():
    # mode=ro evita que SQLite cree una DB vacía si la ruta es inválida
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)

# ===== Endpoints de diagnóstico =====
@app.get("/health_db")
def health_db():
    info = {
        "data_dir": DATA_DIR,
        "xlsx_path": XLSX_PATH,
        "db_path": DB_PATH,
        "excel_exists": os.path.exists(XLSX_PATH),
        "db_exists": os.path.exists(DB_PATH),
    }
    if info["db_exists"]:
        info["db_size_bytes"] = os.path.getsize(DB_PATH)
    try:
        if info["db_exists"] and (info.get("db_size_bytes", 0) > 0):
            with get_conn_ro() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [r[0] for r in cur.fetchall()]
                info["tables"] = tables
                if "ventas" in tables:
                    cur.execute("SELECT COUNT(*) FROM ventas")
                    info["ventas_count"] = cur.fetchone()[0]
    except Exception as e:
        info["error"] = str(e)
    return jsonify(info), 200

@app.get("/debug_repo")
def debug_repo():
    hits = []
    # Limitamos recorrido para ser rápidos
    for base in ("/tmp/data", "/opt/render/project/src"):
        if os.path.isdir(base):
            for dp, _, fns in os.walk(base):
                for f in fns:
                    if f.endswith((".db", ".xlsx")):
                        fp = os.path.join(dp, f)
                        try:
                            size = os.path.getsize(fp)
                        except Exception:
                            size = None
                        hits.append({"path": fp, "size_bytes": size})
    return jsonify({"files": hits}), 200

@app.get("/ventas_count")
def ventas_count():
    try:
        with get_conn_ro() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM ventas")
            total = cur.fetchone()[0]
        return jsonify({"ok": True, "ventas_count": total, "db_path": DB_PATH}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Endpoint de consultas: SOLO SELECT seguro =====
FORBIDDEN_PATTERNS = (
    "pragma", "attach", "detach", "vacuum", "reindex",
    "drop", "alter", "insert", "update", "delete", "replace",
    "create", "truncate", "grant", "revoke", "begin", "commit", "rollback",
)

@app.post("/run_query")
def run_query():
    """
    Body JSON:
    {
      "sql": "SELECT campo1, campo2 FROM ventas WHERE vendedor = :v LIMIT 100",
      "params": {"v": "123"},
      "limit": 500,
      "offset": 0
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        sql_raw = (payload.get("sql") or "").strip()
        params  = payload.get("params") or {}
        limit   = int(payload.get("limit") or 500)
        offset  = int(payload.get("offset") or 0)

        if not sql_raw.lower().startswith("select"):
            return jsonify({"error": "Solo se permiten consultas que inicien con SELECT."}), 400

        lowered = sql_raw.lower()
        if ";" in sql_raw:
            return jsonify({"error": "Patrón prohibido detectado: ;"}), 400
        if any(x in lowered for x in FORBIDDEN_PATTERNS):
            return jsonify({"error": "Consulta contiene operaciones no permitidas."}), 400

        # Forzar LIMIT/OFFSET si no vienen para evitar respuestas gigantes
        sql = sql_raw
        if " limit " not in lowered:
            sql += f" LIMIT {limit}"
        if " offset " not in lowered and offset > 0:
            sql += f" OFFSET {offset}"

        with get_conn_ro() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            fields = rows[0].keys() if rows else []
            data = [dict(r) for r in rows]

        return jsonify({
            "fields": list(fields),
            "limit_applied": limit,
            "offset": offset,
            "rowcount": len(data),
            "rows": data
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ===== Main (solo útil para correr localmente) =====
if __name__ == "__main__":
    # Local: exporta EXCEL_URL o coloca el archivo en /tmp/data antes de correr
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
