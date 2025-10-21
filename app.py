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

# -------------------------------
# Endpoint dinámico SIN FILTROS
# -------------------------------
# Tabla: ventas
# Dimensiones permitidas (para GROUP BY):
ALLOWED_FIELDS = [
    "bodega", "codigo", "vendedor", "nit", "sucursal",
    "sector", "subsector", "referencia", "descripcion",
    "dpto", "desc_dpto", "tipo"
]

# Métricas (SUM):
ALLOWED_METRICS = {
    "venta": "SUM(venta) AS venta",
    "cant":  "SUM(cant)  AS cant"
}

# Claves válidas para ORDER BY (campos o alias de métricas)
ALLOWED_SORT_KEYS = set(ALLOWED_FIELDS + list(ALLOWED_METRICS.keys()))

@app.post("/api/v1/report")
def report():
    """
    Body esperado (sin filtros por ahora):
    {
      "fields":  ["bodega", "vendedor"],
      "metrics": ["venta", "cant"],
      "sort":    [["venta", "desc"], ["bodega", "asc"]],
      "limit":   200,
      "offset":  0
    }
    """
    try:
        payload = request.get_json(force=True, silent=False) or {}
        fields = payload.get("fields", [])
        metrics = payload.get("metrics", ["venta"])  # por defecto sumar venta
        sort = payload.get("sort", [])
        limit = int(payload.get("limit", 500))
        offset = int(payload.get("offset", 0))

        # Validaciones
        if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
            return jsonify({"status": "error", "error": "'fields' debe ser lista de strings"}), 400
        if not isinstance(metrics, list) or not all(isinstance(x, str) for x in metrics):
            return jsonify({"status": "error", "error": "'metrics' debe ser lista de strings"}), 400

        # Validar campos y métricas contra allowlist
        for f in fields:
            if f not in ALLOWED_FIELDS:
                return jsonify({"status": "error", "error": f"Campo no permitido: {f}"}), 400
        for m in metrics:
            if m not in ALLOWED_METRICS:
                return jsonify({"status": "error", "error": f"Métrica no permitida: {m}"}), 400

        # SELECT: fields + agregaciones
        select_parts = []
        if fields:
            select_parts.extend(fields)
        # agregar métricas como expresiones SUM(...)
        for m in metrics:
            select_parts.append(ALLOWED_METRICS[m])

        select_sql = ", ".join(select_parts) if select_parts else ", ".join(ALLOWED_METRICS[m] for m in metrics)

        # FROM
        base_sql = f"SELECT {select_sql} FROM ventas"

        # GROUP BY (solo si hay fields)
        group_by_sql = f" GROUP BY {', '.join(fields)}" if fields else ""

        # ORDER BY (opcional)
        order_sql = ""
        if sort:
            order_elems = []
            for item in sort:
                if (not isinstance(item, (list, tuple))) or len(item) != 2:
                    return jsonify({"status": "error", "error": "Cada elemento de 'sort' debe ser [clave, 'asc'|'desc']"}), 400
                key, direction = item[0], str(item[1]).lower()
                if key not in ALLOWED_SORT_KEYS:
                    return jsonify({"status": "error", "error": f"Orden no permitido por: {key}"}), 400
                if direction not in ("asc", "desc"):
                    return jsonify({"status": "error", "error": "Dirección de orden inválida (use 'asc' o 'desc')"}), 400
                # Para métricas, ordenamos por su alias (venta/cant)
                order_elems.append(f"{key} {direction.upper()}")
            if order_elems:
                order_sql = " ORDER BY " + ", ".join(order_elems)

        # LIMIT/OFFSET (parametrizados)
        limit = max(0, min(limit, 5000))  # hard cap
        offset = max(0, offset)

        final_sql = f"{base_sql}{group_by_sql}{order_sql} LIMIT ? OFFSET ?;"

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(final_sql, (limit, offset))
            rows = cur.fetchall()
            items = rows_to_dicts(cur, rows)

        return jsonify({
            "status": "ok",
            "count": len(items),
            "fields": fields,
            "metrics": metrics,
            "rows": items,
            "limit": limit,
            "offset": offset
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# -------------------------------
# Tus endpoints existentes
# -------------------------------
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
