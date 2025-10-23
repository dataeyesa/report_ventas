from flask import Flask, request, jsonify
import os, sqlite3
import pandas as pd

app = Flask(__name__)

# --- Rutas de trabajo en disco persistente de Render ---
DATA_DIR   = "/var/data"
XLSX_PATH  = os.path.join(DATA_DIR, "bd.ventas.xlsx")
DB_PATH    = os.path.join(DATA_DIR, "ventas.db")

# --- Conversión inicial: Excel -> SQLite (una sola vez) ---
def ensure_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DB_PATH):
        if not os.path.exists(XLSX_PATH):
            # Si llegas aquí, tu Start Command no descargó el Excel.
            # Revisa EXCEL_URL y el Start Command en Render.
            raise FileNotFoundError(f"No se encontró el Excel en {XLSX_PATH}")
        print("⚙️ Convirtiendo Excel a SQLite por primera vez...")
        # TIP: Si necesitas reducir memoria, usa usecols=[...] para cargar solo columnas necesarias.
        df = pd.read_excel(XLSX_PATH, engine="openpyxl")
        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql("ventas", conn, if_exists="replace", index=False)
            try:
                # Índices recomendados (ajusta a tus campos reales)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fecha    ON ventas(fecha)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_codigo   ON ventas(codigo)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_vendedor ON ventas(vendedor)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_nit      ON ventas(nit)")
            except Exception as e:
                print("Aviso al crear índices:", e)
        print("✅ Base creada en", DB_PATH)

# Asegura la DB antes de exponer endpoints
ensure_db()

# Helper: abrir conexión RO y ejecutar SELECT seguro
def get_conn_ro():
    # mode=ro evita crear DB vacías si la ruta está mal
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)

# --- Endpoints de diagnóstico ---
@app.get("/health_db")
def health_db():
    info = {"path": DB_PATH, "xlsx": XLSX_PATH}
    info["exists"] = os.path.exists(DB_PATH)
    info["size_bytes"] = os.path.getsize(DB_PATH) if info["exists"] else 0
    if not info["exists"] or info["size_bytes"] < 1024:
        info["warning"] = "DB no encontrada o demasiado pequeña"
        return jsonify(info), 200
    try:
        with get_conn_ro() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite
