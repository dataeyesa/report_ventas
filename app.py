import os
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

DB_PATH = os.getenv("DB_PATH", os.path.join("data", "ventas.db"))

app = Flask(__name__)
CORS(app)

def get_conn():
    # Abrir en modo solo lectura (más seguro para Render)
    uri = f"file:{DB_PATH}?mode=ro"
    return sqlite3.connect(uri, uri=True)

def rows_to_dicts(cur, rows):
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ventas")
def ventas_list():
    """
    Filtros opcionales:
      - cliente (substring)
      - referencia (substring)
      - fecha_desde (YYYY-MM-DD)
      - fecha_hasta (YYYY-MM-DD)
      - limit (por defecto 100)
      - offset (por defecto 0)
    """
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
    sql = f"""
        SELECT *
        FROM ventas
        {where_sql}
        LIMIT ? OFFSET ?;
    """
    params += [limit, offset]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        # total para paginación
        cur2 = conn.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM ventas {where_sql}", params[:-2])
        total = cur2.fetchone()[0]

    return jsonify({
        "items": rows_to_dicts(cur, rows),
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
