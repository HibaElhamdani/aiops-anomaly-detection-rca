"""
COUCHE API (FastAPI) - Pure Streaming Kafka
-------------------------------------------
Contient :
  - L'application FastAPI (MaBanque API)
  - Le middleware de logging (Envoie les logs directement vers le topic Kafka 'aiops-logs')
  - Les endpoints bancaires et le Chaos Engine
"""
import os
import time
import sqlite3
from typing import Optional
from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from database import get_db, init_db
from services import login_user, get_balance, make_transfer, get_history, set_chaos
from kafka_producer import send_kafka_message

# Initialisation de l'application
app = FastAPI(
    title="MaBanque API - AIOps Victim",
    description="API bancaire avec Chaos Engineering et export Kafka en temps reel",
    version="1.0.0"
)
init_db()

# Nom du topic Kafka configure via variable d'environnement
TOPIC_LOGS = os.getenv("KAFKA_TOPIC_LOGS", "aiops-logs")


# ============================================================
# MODELES PYDANTIC
# ============================================================
class LoginRequest(BaseModel):
    username: str
    password: str

class VirementRequest(BaseModel):
    destinataire: str
    montant: float
    motif: str

class ChaosCommand(BaseModel):
    action: str                    # "enable" ou "disable"
    target: Optional[str] = None   # ex: "/virement"
    type: Optional[str] = None     # ex: "error_500"
    delay: Optional[int] = 0       # millisecondes (pour "slow")


# ============================================================
# MIDDLEWARE : Envoi direct des logs HTTP vers Kafka
# ============================================================
@app.middleware("http")
async def log_middleware(request: Request, call_next):
    start = time.time()
    error_msg = None

    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        error_msg = str(e)
        response = JSONResponse(status_code=500, content={"detail": error_msg})

    latency_ms = int((time.time() - start) * 1000)

    # On exclut le bruit (/health, /docs, etc.)
    if request.url.path not in ["/health", "/docs", "/openapi.json", "/redoc"]:
        log = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "latency_ms": latency_ms,
            "error_message": error_msg
        }

        # Envoi direct vers Kafka (Topic: aiops-logs)
        send_kafka_message(TOPIC_LOGS, log)

    return response


# ============================================================
# ENDPOINTS BANCAIRES
# ============================================================

@app.post("/login", tags=["Banque"], summary="Connexion client")
async def endpoint_login(req: LoginRequest, db: sqlite3.Connection = Depends(get_db)):
    return await login_user(db, req.username, req.password)


@app.get("/solde", tags=["Banque"], summary="Consulter le solde")
async def endpoint_solde(db: sqlite3.Connection = Depends(get_db)):
    return await get_balance(db)


@app.post("/virement", tags=["Banque"], summary="Effectuer un virement")
async def endpoint_virement(req: VirementRequest, db: sqlite3.Connection = Depends(get_db)):
    return await make_transfer(db, req.destinataire, req.montant, req.motif)


@app.get("/historique", tags=["Banque"], summary="Historique des operations")
async def endpoint_historique(db: sqlite3.Connection = Depends(get_db)):
    return await get_history(db)


@app.get("/health", tags=["Monitoring"], summary="Pouls de l'API")
async def endpoint_health():
    return {"status": "healthy"}


# ============================================================
# ENDPOINT CHAOS
# ============================================================

@app.post("/chaos", tags=["Chaos Engineering"], summary="Activer / Desactiver un bug")
async def endpoint_chaos(cmd: ChaosCommand):
    return set_chaos(cmd.action, cmd.target, cmd.type, cmd.delay)