import os
import shutil
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

# Ruta de la DB (Render la leerá de /var/data)
DB_PATH = os.getenv("DB_PATH", "/var/data/ventas.db")
REPO_DB = os.path.join("data", "ventas.db")

app = Flask(__name__)
CORS(app)

# Garantiza que la DB exista
def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if not os.path.exists(DB_PATH):
        if os.path.exists(REPO_DB):
            shutil.copyfile(REPO_DB, DB_PATH)
        else:
            raise FileNotFoundError(f"No existe {DB_PATH} ni {REPO_DB}")

def get_conn():
    ensure_db()
    uri = f"file:{DB_PATH}?mode=ro"
    return sqlite3.connect(uri, uri=True, check_same_thread=False)

def rows_to_dicts(cur, rows):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]

# ===========================
# ENDPOINTS
# ===========================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/health/db")
def health_db():
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1;")
        return {"status": "ok", "db_path": DB_PATH}
    except Exception as e:
        return jsonify({"status": "error", "db_path": DB_PATH, "detail": str(e)}), 503

# ===========================
# REPORT DINÁMICO SIN FILTROS
# ===========================

ALLOWED_FIELDS = [
    "bodega", "codigo", "vendedor", "nit", "sucursal",
    "sector", "subsector", "referencia", "descripcion",
    "dpto", "desc_dpto", "tipo"
]

ALLOWED_METRICS = ["venta", "cant"]

@app.post("/api/v1/report")
def report():
    try:
        payload = request.get_json(force=True, silent=False) or {}
        fields = payload.get("fields", [])
        metrics = payload.get("metrics", ["venta"])
        sort = payload.get("sort", [])
        limit = int(payload.get("limit", 500))
        offset = int(payload.get("offset", 0))

        # Validaciones
        for f in fields:
            if f not in ALLOWED_FIELDS:
                return jsonify({"status": "error", "error": f"Campo no permitido: {f}"}), 400
        for m in metrics:
            if m not in ALLOWED_METRICS:
                return jsonify({"status": "error", "error": f"Métrica no permitida: {m}"}), 400

        # SELECT
        select_parts = fields.copy()
        for m in metrics:
            select_parts.append(f"SUM({m}) AS {m}")
        select_sql = ", ".join(select_parts)

        # GROUP BY
        group_sql = f" GROUP BY {', '.join(fields)}" if fields else ""

        # ORDER BY
        order_sql = ""
        if sort:
            order_parts = []
            for item in sort:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    key, direction = item
                    if key not in (ALLOWED_FIELDS + ALLOWED_METRICS):
                        return jsonify({"status": "error", "error": f"Orden no permitido por: {key}"}), 400
                    order_parts.append(f"{key} {direction.upper()}")
                else:
                    return jsonify({"status":"error","error":"Cada elemento de 'sort' debe ser [campo,'asc'|'desc']"}),400
            order_sql = " ORDER BY " + ", ".join(order_parts)

        # SQL Final
        sql = f"""
            SELECT {select_sql}
            FROM ventas
            {group_sql}
            {order_sql}
            LIMIT ? OFFSET ?;
        """

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, (limit, offset))
            rows = cur.fetchall()
            items = rows_to_dicts(cur, rows)

        return jsonify({
            "status": "ok",
            "fields": fields,
            "metrics": metrics,
            "count": len(items),
            "rows": items,
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        return jsonify({"status":"error","detail":str(e)}), 500

# ===========================
# ENDPOINTS EXISTENTES
# ===========================

@app.get("/ventas")
def ventas_list():
    cliente = request.args.get("cliente")
    referencia = request.args.get("referencia")
    fecha_desde = request.args.get("fecha_desde")
    fecha_hasta = request.args.get("fecha_hasta")
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit/offset inválidos"}), 400

    where = []
    params = []

    if cliente:
        where.append("LOWER(cliente) LIKE ?")
        params.append(f"%{cliente.lower()}%")
    if referencia:
        where.append("LOWER(referencia) LIKE ?")
        params.append(f"%{referencia.lower()}%")
    if fecha_desde:
        where.append("fecha >= ?")
        params.append(fecha_desde)
    if fecha_hasta:
        where.append("fecha <= ?")
        params.append(fecha_hasta)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM ventas {where_sql} LIMIT ? OFFSET ?;"
    params += [limit, offset]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        items = rows_to_dicts(cur, rows)

        cur2 = conn.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM ventas {where_sql}", params[:-2])
        total = cur2.fetchone()[0]

    return jsonify({
        "items": items,
        "limit": limit,
        "offset": offset,
        "total": total
    })

@app.get("/ventas/<int:rowid>")
def ventas_by_id(rowid: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT rowid AS id, * FROM ventas WHERE rowid = ?", (rowid,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "no encontrado"}), 404
        return jsonify(rows_to_dicts(cur, [row])[0])

# ===========================
# RUN LOCAL
# ===========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
