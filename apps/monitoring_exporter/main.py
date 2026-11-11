import os
import traceback
from fastapi import FastAPI
from fastapi.responses import Response
from deltalake import DeltaTable
from pandas.api.types import is_datetime64_any_dtype

app = FastAPI(title="AIOps Monitoring Exporter")

STORAGE_PATH = "/app/storage/silver/metrics"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/metrics/silver")
def get_silver_metrics():
    """Lit les dernieres metriques depuis Delta Silver (toujours une liste JSON)"""
    try:
        if not os.path.exists(os.path.join(STORAGE_PATH, "_delta_log")):
            return Response("[]", media_type="application/json")

        df = DeltaTable(STORAGE_PATH).to_pandas()
        if df.empty:
            return Response("[]", media_type="application/json")

        # trier par la premiere colonne datetime, si elle existe
        dt_cols = [c for c in df.columns if is_datetime64_any_dtype(df[c])]
        if dt_cols:
            df = df.sort_values(dt_cols[0])

        df = df.tail(100)

        # to_json transforme NaN en null et les timestamps en ISO 8601
        body = df.to_json(orient="records", date_format="iso", default_handler=str)
        return Response(body, media_type="application/json")
    except Exception:
        print(traceback.format_exc(), flush=True)
        return Response("[]", media_type="application/json")