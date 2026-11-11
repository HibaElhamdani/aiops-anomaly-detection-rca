"""
COUCHE MÉTIER
-------------
Contient :
  - La logique bancaire (login, solde, virement, historique)
  - Les requêtes SQL
  - L'injection des bugs Chaos Engineering
"""
import asyncio
import sqlite3
from fastapi import HTTPException

# ============================================================
# ÉTAT GLOBAL DU CHAOS (contrôlé depuis main.py via /chaos)
# ============================================================
CHAOS_STATE = {
    "active": False,
    "target": None,   # ex: "/virement"
    "type": None,     # ex: "error_500"
    "delay": 0        # en millisecondes (pour type "slow")
}

# Liste qui grossit pour simuler une fuite mémoire
MEMORY_LEAK_LIST = []

# Dictionnaire : type de bug → message d'erreur dans le log
BUG_MESSAGES = {
    "error_500": "DatabaseConnectionError",
    "slow": "ResponseTimeout",
    "timeout": "GatewayTimeout",
    "memory_leak": "OutOfMemoryError"
}


# ============================================================
# MOTEUR CHAOS : appelé AU DÉBUT de chaque fonction métier
# ============================================================
async def apply_chaos(endpoint: str):
    """
    Si le chaos est actif sur cet endpoint, injecte le bug correspondant.
    - error_500  → lève une Exception (devient status 500 dans le log)
    - slow       → attend X millisecondes puis continue normalement
    - timeout    → attend 30s puis lève une Exception
    - memory_leak→ alloue 10 Mo de RAM à chaque appel
    """
    if not CHAOS_STATE["active"] or CHAOS_STATE["target"] != endpoint:
        return  # Pas de chaos → on continue normalement

    bug = CHAOS_STATE["type"]

    if bug == "error_500":
        raise Exception(BUG_MESSAGES["error_500"])

    elif bug == "slow":
        await asyncio.sleep(CHAOS_STATE["delay"] / 1000.0)

    elif bug == "timeout":
        await asyncio.sleep(30)
        raise Exception(BUG_MESSAGES["timeout"])

    elif bug == "memory_leak":
        MEMORY_LEAK_LIST.append("X" * 10_000_000)  # ~10 Mo non libérés


# ============================================================
# FONCTIONS MÉTIER BANCAIRES
# ============================================================

async def login_user(db: sqlite3.Connection, username: str, password: str):
    """
    Vérifie username/password dans la table clients.
    Retourne un token si OK, sinon erreur 401.
    """
    await apply_chaos("/login")

    cursor = db.execute(
        "SELECT id FROM clients WHERE username=? AND password=?",
        (username, password)
    )
    user = cursor.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants incorrects")

    return {"token": f"token_user_{user['id']}", "client_id": user["id"]}


async def get_balance(db: sqlite3.Connection, client_id: int = 1):
    """
    Lit le solde du compte dans la table comptes.
    """
    await apply_chaos("/solde")

    cursor = db.execute(
        "SELECT numero_compte, solde, devise FROM comptes WHERE client_id=?",
        (client_id,)
    )
    compte = cursor.fetchone()
    if not compte:
        raise HTTPException(status_code=404, detail="Compte introuvable")

    return dict(compte)


async def make_transfer(db: sqlite3.Connection, destinataire: str,
                        montant: float, motif: str, client_id: int = 1):
    """
    Effectue un virement :
      1. Vérifie le solde
      2. Débite le compte
      3. Enregistre la transaction dans l'historique
    """
    await apply_chaos("/virement")

    cursor = db.execute("SELECT solde FROM comptes WHERE client_id=?", (client_id,))
    compte = cursor.fetchone()

    if not compte or compte["solde"] < montant:
        raise HTTPException(status_code=400, detail="Solde insuffisant")

    try:
        nouveau_solde = compte["solde"] - montant
        db.execute(
            "UPDATE comptes SET solde=? WHERE client_id=?",
            (nouveau_solde, client_id)
        )
        db.execute(
            "INSERT INTO transactions (compte_id, type, montant, destinataire, motif) "
            "VALUES (?, 'VIREMENT', ?, ?, ?)",
            (client_id, -montant, destinataire, motif)
        )
        db.commit()
        return {"statut": "effectué", "nouveau_solde": nouveau_solde}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur transaction : {e}")


async def get_history(db: sqlite3.Connection, client_id: int = 1, limit: int = 20):
    """
    Retourne les N dernières transactions du client.
    """
    await apply_chaos("/historique")

    cursor = db.execute(
        "SELECT * FROM transactions WHERE compte_id=? ORDER BY date DESC LIMIT ?",
        (client_id, limit)
    )
    return {"transactions": [dict(row) for row in cursor.fetchall()]}


def set_chaos(action: str, target: str = None, bug_type: str = None, delay: int = 0):
    """
    Active ou désactive le mode chaos (appelé par l'endpoint /chaos).
    """
    if action == "disable":
        CHAOS_STATE["active"] = False
        MEMORY_LEAK_LIST.clear()
        return {"message": "Mode CHAOS desactive. API reparee."}

    if action == "enable":
        CHAOS_STATE["active"] = True
        CHAOS_STATE["target"] = target
        CHAOS_STATE["type"] = bug_type
        CHAOS_STATE["delay"] = delay
        return {
            "message": f"Mode CHAOS active : '{bug_type}' sur '{target}'",
            "error_message_in_logs": BUG_MESSAGES.get(bug_type, "UnknownError")
        }

    raise HTTPException(status_code=400, detail="action doit etre 'enable' ou 'disable'")