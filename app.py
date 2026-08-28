# -*- coding: utf-8 -*-
"""
IR PREDICTOR 360 WEB LIVE V7 PROFESSIONAL CLOUD - LABORATÓRIO DE ESTRATÉGIAS
Pydroid 3 / Android

Instale:
    pip install flask requests

Execute e abra:
    http://127.0.0.1:5000

MÓDULOS:
1. Roleta Nanda: Malibu / Noronha / Bali / Maldivas / Grécia
2. Arqueiro: Vizinhos de Race / padrões de terminal / camuflados / gatilho sequencial
3. Tabela de Puxadores: mapeamentos publicados + validação local
4. Números Fixos: famílias por terminal
5. X 2.0: grupos publicados + aprendizado local de transições
6. Motor Histórico: transições reais observadas nesta mesa
7. Meta-Learner: pesos adaptativos por desempenho fora da amostra

IMPORTANTE:
O programa não garante previsão de roleta. Cada estratégia é tratada como
hipótese e precisa provar desempenho no histórico futuro. O meta-learner
reduz peso de estratégias que não entregam sinal acima do acaso.
"""

import json
import math
import os
import sqlite3
import threading
import time
import logging
import atexit
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template_string, request

# ============================================================
# CONFIG
# ============================================================

API_URL = (
    "https://api-cs.casino.org/svc-evolution-game-events/api/"
    "immersiveroulette?page=0&size=29&sort=data.settledAt,desc&duration=6"
)

DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_FILE = os.getenv("DB_FILE", os.path.join(DATA_DIR, "ir_predictor360.sqlite3"))

# Arquivos antigos são usados apenas para migração automática, se existirem.
HISTORY_FILE = os.getenv("LEGACY_HISTORY_FILE", "ir_v3_history.json")
LEARNING_FILE = os.getenv("LEGACY_LEARNING_FILE", "ir_v3_learning.json")

MAX_HISTORY = int(os.getenv("MAX_HISTORY", "50000"))
UPSTREAM_MIN_INTERVAL = float(os.getenv("UPSTREAM_MIN_INTERVAL", "6.0"))
COLLECT_INTERVAL = max(6.0, float(os.getenv("COLLECT_INTERVAL", "8.0")))

RUN_COLLECTOR = os.getenv("RUN_COLLECTOR", "1").lower() not in {"0", "false", "no"}
PANEL_USER = os.getenv("PANEL_USER", "admin")
PANEL_PASSWORD = os.getenv("PANEL_PASSWORD", "").strip()

BACKUP_DIR = os.getenv("BACKUP_DIR", os.path.join(DATA_DIR, "backups"))
BACKUP_INTERVAL_SECONDS = max(
    900,
    int(os.getenv("BACKUP_INTERVAL_SECONDS", "21600"))
)  # padrão: 6h
BACKUP_KEEP = max(3, int(os.getenv("BACKUP_KEEP", "14")))

Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("ir360")

WHEEL = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23,
    10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26
]

REDS = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

# Tabela de puxadores disponível no material analisado.
# Alguns números não aparecem como "gatilho principal" na visualização pública,
# então eles ficam sem mapa fixo e o motor histórico assume.
PULL_TABLE = {
    1: [3, 36],
    2: [22, 5],
    3: [0, 15, 35, 33],
    5: [22, 25],
    7: [27, 19],
    8: [11, 30, 36],
    9: [19, 27],
    11: [8, 30, 36],
    12: [21, 16],
    13: [0, 33],
    14: [34],
    15: [0, 3, 35, 33],
    16: [21, 12],
    17: [20],
    19: [9, 27],
    20: [17],
    21: [16, 12],
    22: [2, 25, 5],
    25: [5, 22],
    26: [33],
    27: [19, 7],
    30: [8, 11, 36],
    36: [8, 11, 30],
}

# Grupos da Tabela X 2.0 conforme material disponível.
# O zero aparece como referência especial no começo.
X_GROUPS = [
    [5, 32, 6, 22],
    [15, 24, 27, 18],
    [19, 16, 13, 29],
    [4, 33, 36, 7],
    [21, 1, 11, 28],
    [2, 20, 30, 12],
    [25, 14, 8, 35],
    [17, 31, 23, 3],
    [34, 9, 10, 26],
]

STRATEGY_NAMES = [
    "Nanda",
    "Arqueiro",
    "Tabela Puxadores",
    "Números Fixos",
    "X 2.0",
    "Histórico Mesa",
]

app = Flask(__name__)


class StateDB:
    """SQLite simples e durável para estado, histórico e aprendizado."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self._init()

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=20, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=20000")
        return conn

    def _init(self):
        with self.lock, self.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS spins (
                    round_id TEXT PRIMARY KEY,
                    number INTEGER NOT NULL,
                    settled_at TEXT,
                    started_at TEXT,
                    inserted_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_spins_inserted
                ON spins(inserted_at)
            """)
            conn.commit()

    def get_json(self, key, default=None):
        with self.lock, self.connect() as conn:
            row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except Exception:
            return default

    def set_json(self, key, value):
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        with self.lock, self.connect() as conn:
            conn.execute("""
                INSERT INTO state(key, value, updated_at)
                VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
            """, (key, payload, now_text()))
            conn.commit()

    def upsert_spin(self, rec):
        with self.lock, self.connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO spins
                (round_id, number, settled_at, started_at, inserted_at)
                VALUES(?,?,?,?,?)
            """, (
                str(rec["id"]), int(rec["number"]),
                rec.get("settledAt"), rec.get("startedAt"), now_text()
            ))
            conn.commit()

    def load_spins(self, limit=50000):
        with self.lock, self.connect() as conn:
            rows = conn.execute("""
                SELECT round_id, number, settled_at, started_at
                FROM spins
                ORDER BY rowid DESC
                LIMIT ?
            """, (int(limit),)).fetchall()
        rows.reverse()
        return [
            {
                "id": r[0],
                "number": int(r[1]),
                "settledAt": r[2],
                "startedAt": r[3],
            }
            for r in rows
        ]

    def stats(self):
        try:
            with self.lock, self.connect() as conn:
                spins = conn.execute("SELECT COUNT(*) FROM spins").fetchone()[0]
                states = conn.execute("SELECT COUNT(*) FROM state").fetchone()[0]
            size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
            return {"spins": spins, "states": states, "bytes": size}
        except Exception:
            return {"spins": 0, "states": 0, "bytes": 0}

    def integrity_check(self):
        """Executa PRAGMA integrity_check e retorna um diagnóstico curto."""
        try:
            with self.lock, self.connect() as conn:
                row = conn.execute("PRAGMA integrity_check").fetchone()
            ok = bool(row and str(row[0]).lower() == "ok")
            return {"ok": ok, "message": row[0] if row else "sem resposta"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def backup(self, backup_dir=BACKUP_DIR):
        """
        Faz backup consistente usando a API nativa de backup do SQLite.
        O arquivo é criado no mesmo Persistent Disk.
        """
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(backup_dir, f"ir360_{stamp}.sqlite3")

        with self.lock:
            src_conn = self.connect()
            dst_conn = sqlite3.connect(dest)
            try:
                src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
                src_conn.close()

        self.prune_backups(backup_dir)
        return dest

    def prune_backups(self, backup_dir=BACKUP_DIR):
        files = sorted(
            Path(backup_dir).glob("ir360_*.sqlite3"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        for old in files[BACKUP_KEEP:]:
            try:
                old.unlink()
            except Exception:
                pass

    def latest_backup(self, backup_dir=BACKUP_DIR):
        files = sorted(
            Path(backup_dir).glob("ir360_*.sqlite3"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        return str(files[0]) if files else None

    def backup_stats(self, backup_dir=BACKUP_DIR):
        files = sorted(
            Path(backup_dir).glob("ir360_*.sqlite3"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        return {
            "count": len(files),
            "latest": files[0].name if files else None,
            "keep": BACKUP_KEEP,
        }


# ============================================================
# BÁSICO
# ============================================================

def now_text():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


db = StateDB(DB_FILE)
_db_integrity = db.integrity_check()
if not _db_integrity["ok"]:
    logger.error("Falha de integridade no SQLite: %s", _db_integrity["message"])
else:
    logger.info("SQLite íntegro: %s", DB_FILE)

def color(n):
    if n == 0:
        return "green"
    return "red" if n in REDS else "black"

def terminal(n):
    return int(n) % 10

def terminal_numbers(t):
    return [n for n in range(37) if terminal(n) == t]

def wheel_index(n):
    return WHEEL.index(n)

def wheel_distance(a, b):
    ia, ib = wheel_index(a), wheel_index(b)
    d = abs(ia - ib)
    return min(d, 37 - d)

def wheel_neighbors(n, radius=1, include_center=True):
    i = wheel_index(n)
    out = []
    for d in range(-radius, radius + 1):
        if d == 0 and not include_center:
            continue
        out.append(WHEEL[(i + d) % 37])
    return out

def unique(seq):
    out = []
    seen = set()
    for x in seq:
        x = int(x)
        if 0 <= x <= 36 and x not in seen:
            out.append(x)
            seen.add(x)
    return out

def safe_div(a, b, default=0.0):
    return a / b if b else default

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def rank_candidates_by_history(candidates, nums, trigger=None):
    """Ordena candidatos usando transição local + frequência recente."""
    candidates = unique(candidates)
    if not candidates:
        return []

    recent = Counter(nums[-200:])
    trans = Counter()
    trans_total = 0

    if trigger is not None:
        for a, b in zip(nums[:-1], nums[1:]):
            if a == trigger:
                trans[b] += 1
                trans_total += 1

    scored = []
    for n in candidates:
        tr = safe_div(trans[n] + 1, trans_total + 37)
        fq = safe_div(recent[n] + 1, len(nums[-200:]) + 37)
        physical = 1 / (1 + wheel_distance(trigger, n)) if trigger is not None else 0
        s = 0.62 * tr + 0.28 * fq + 0.10 * physical
        scored.append((n, s))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [n for n, _ in scored]


# ============================================================
# ESTRATÉGIAS
# ============================================================

def no_signal(name, reason):
    return {
        "name": name,
        "active": False,
        "primary": None,
        "candidates": [],
        "coverage": [],
        "reason": reason,
        "trigger": "",
        "confidence": 0.0,
    }

def make_signal(name, candidates, nums, reason, trigger_text="", expand_radius=0):
    candidates = unique(candidates)
    if not candidates:
        return no_signal(name, reason)

    trigger_num = nums[-1] if nums else None
    ordered = rank_candidates_by_history(candidates, nums, trigger_num)

    coverage = []
    for n in ordered:
        if expand_radius > 0:
            coverage.extend(wheel_neighbors(n, expand_radius))
        else:
            coverage.append(n)
    coverage = unique(coverage)

    # Confiança é força do gatilho / amostra, NÃO probabilidade de acerto.
    conf = min(100.0, 48 + 4 * len(nums[-100:]) / 10)
    conf -= min(20, max(0, len(coverage) - 5) * 1.2)

    return {
        "name": name,
        "active": True,
        "primary": ordered[0] if ordered else None,
        "candidates": ordered[:10],
        "coverage": coverage[:24],
        "reason": reason,
        "trigger": trigger_text,
        "confidence": round(clamp(conf, 10, 95), 1),
    }

def strategy_nanda(nums):
    """
    Implementação operacional dos gatilhos textuais:
    - Malibu: repetição de terminal; busca terminal anterior à repetição.
    - Noronha: padrão A-B-A de terminais; joga B-1/B/B+1.
    - Maldivas/Grécia: terminais com diferença 2; busca terminal intermediário.
    - Bali: usa padrão de terminal intermediário em sequência de três.
    """
    if len(nums) < 3:
        return no_signal("Nanda", "Aguardando pelo menos 3 giros.")

    a, b, c = nums[-3], nums[-2], nums[-1]
    ta, tb, tc = terminal(a), terminal(b), terminal(c)

    # Malibu
    if tb == tc:
        cand = terminal_numbers(ta)
        return make_signal(
            "Nanda", cand, nums,
            "Malibu: repetição de terminal detectada; retorna ao terminal anterior.",
            f"T{ta} → T{tb} → T{tc}", expand_radius=0
        )

    # Noronha: mesma ponta com um número entre
    if ta == tc and ta != tb:
        ts = [(tb - 1) % 10, tb, (tb + 1) % 10]
        cand = []
        for t in ts:
            cand.extend(terminal_numbers(t))
        return make_signal(
            "Nanda", cand, nums,
            "Noronha: terminais repetidos com uma casa de separação.",
            f"T{ta} • T{tb} • T{tc}", expand_radius=0
        )

    # Maldivas / Grécia: diferença de 2 entre os dois últimos terminais
    diff = (tc - tb) % 10
    if diff in (2, 8):
        if diff == 2:
            mid = (tb + 1) % 10
        else:
            mid = (tb - 1) % 10

        cand = terminal_numbers(mid)
        label = "Maldivas" if mid <= 6 else "Grécia"
        # filtro dos últimos 5: se terminal-alvo acabou de aparecer, gatilho perde força
        recent_t = [terminal(x) for x in nums[-5:]]
        weakened = mid in recent_t[:-1]
        reason = f"{label}: dois terminais pularam uma casa; alvo é o terminal intermediário."
        if weakened:
            reason += " ⚠️ Alvo apareceu recentemente, gatilho enfraquecido."
        sig = make_signal("Nanda", cand, nums, reason, f"T{tb} → T{tc} ⇒ T{mid}", 1)
        if weakened:
            sig["confidence"] = max(10, sig["confidence"] - 20)
        return sig

    # Bali aproximado: usa a relação do terminal central com os lados
    if abs((ta - tb) % 10) in (1, 9) or abs((tc - tb) % 10) in (1, 9):
        mid = (tb - 1) % 10
        return make_signal(
            "Nanda", terminal_numbers(mid), nums,
            "Bali: sequência compatível com terminal anterior/posterior ao número central.",
            f"T{ta} • T{tb} • T{tc} ⇒ T{mid}", 0
        )

    return no_signal("Nanda", "Nenhum gatilho Malibu/Noronha/Bali/Maldivas/Grécia ativo.")

def strategy_arqueiro(nums):
    """
    Versão testável:
    - VR: dois últimos números vizinhos físicos.
    - Cercado pelo mesmo terminal: compara última ocorrência anterior do número atual
      e seus dois vizinhos temporais.
    - Gatilho perfeito: sequência numérica com passo 1..3.
    """
    if len(nums) < 4:
        return no_signal("Arqueiro", "Aguardando histórico.")

    last, prev = nums[-1], nums[-2]

    # Vizinhos de Race
    if wheel_distance(prev, last) == 1:
        # terminal anterior ao terminal predominante dos dois
        base_t = terminal(last)
        target_t = (base_t - 1) % 10
        return make_signal(
            "Arqueiro", terminal_numbers(target_t), nums,
            "V.R.: os dois últimos resultados são vizinhos físicos na Race.",
            f"{prev} ↔ {last} | alvo T{target_t}", 1
        )

    # Gatilho perfeito: números crescentes/decrescentes com passo até 3
    delta = last - prev
    if delta != 0 and abs(delta) <= 3:
        next_n = last + delta
        if 0 <= next_n <= 36:
            return make_signal(
                "Arqueiro", [next_n], nums,
                "Gatilho perfeito: continuação de sequência numérica curta.",
                f"{prev} → {last} → {next_n}", 2
            )

    # Cercado pelo mesmo terminal na ocorrência anterior do atual
    indexes = [i for i, n in enumerate(nums[:-1]) if n == last]
    if indexes:
        idx = indexes[-1]
        around = []
        for j in (idx-2, idx-1, idx+1, idx+2):
            if 0 <= j < len(nums):
                around.append(nums[j])

        if len(around) >= 3:
            tc = Counter(terminal(x) for x in around)
            t, count = tc.most_common(1)[0]
            if count >= 2:
                return make_signal(
                    "Arqueiro", terminal_numbers(t), nums,
                    "Cercado pelo mesmo terminal: contexto da ocorrência anterior concentra um terminal.",
                    f"Última ocorrência anterior de {last} ⇒ T{t}", 1
                )

    return no_signal("Arqueiro", "Nenhum gatilho V.R./cercado/sequencial ativo.")

def strategy_pull_table(nums):
    if not nums:
        return no_signal("Tabela Puxadores", "Sem histórico.")

    last = nums[-1]
    targets = PULL_TABLE.get(last)

    if not targets:
        return no_signal(
            "Tabela Puxadores",
            f"O material público analisado não traz mapa fixo para o {last}; aguardando outro gatilho."
        )

    return make_signal(
        "Tabela Puxadores", targets, nums,
        "Mapa fixo do e-book, reordenado pelo comportamento observado nesta mesa.",
        f"{last} ⇒ {' / '.join(map(str, targets))}", 0
    )

def strategy_fixed(nums):
    if not nums:
        return no_signal("Números Fixos", "Sem histórico.")

    last = nums[-1]
    t = terminal(last)
    cand = terminal_numbers(t)

    # Estratégia original trabalha muito com famílias de terminais.
    return make_signal(
        "Números Fixos", cand, nums,
        "Família fixa do mesmo terminal do último resultado; ordem adaptada pela mesa.",
        f"{last} ⇒ Terminal {t}", 0
    )

def x_group_of(n):
    for i, group in enumerate(X_GROUPS):
        if n in group:
            return i
    return None

def strategy_x20(nums):
    if len(nums) < 2:
        return no_signal("X 2.0", "Aguardando histórico.")

    last = nums[-1]
    gi = x_group_of(last)

    if gi is None:
        # zero como referência especial
        if last == 0:
            cand = X_GROUPS[0]
            return make_signal(
                "X 2.0", cand, nums,
                "Zero tratado como referência para o primeiro conjunto da tabela X.",
                "0 ⇒ grupo inicial", 1
            )
        return no_signal("X 2.0", f"{last} não localizado nos grupos X.")

    # Aprendizado de qual grupo costuma seguir o grupo atual nesta mesa
    gt = Counter()
    total = 0
    for a, b in zip(nums[:-1], nums[1:]):
        ga, gb = x_group_of(a), x_group_of(b)
        if ga == gi and gb is not None:
            gt[gb] += 1
            total += 1

    if total >= 5:
        best_group = gt.most_common(1)[0][0]
        reason = "X 2.0 adaptativo: grupo seguinte escolhido pela transição mais comum nesta mesa."
        trig = f"G{gi+1} ⇒ G{best_group+1}"
    else:
        # fallback 1H: grupo seguinte circular
        best_group = (gi + 1) % len(X_GROUPS)
        reason = "X 2.0: pouca amostra local; usa deslocamento 1H como hipótese inicial."
        trig = f"G{gi+1} ⇒ 1H ⇒ G{best_group+1}"

    return make_signal(
        "X 2.0", X_GROUPS[best_group], nums,
        reason, trig, 1
    )

def strategy_history(nums):
    if len(nums) < 20:
        return no_signal("Histórico Mesa", "Poucos giros para transições locais.")

    last = nums[-1]
    c = Counter()
    total = 0

    for a, b in zip(nums[:-1], nums[1:]):
        if a == last:
            c[b] += 1
            total += 1

    if total < 3:
        # fallback para hot recentes
        hot = [n for n, _ in Counter(nums[-100:]).most_common(5)]
        return make_signal(
            "Histórico Mesa", hot, nums,
            "Poucas repetições do último número; fallback para frequência recente.",
            f"Amostra após {last}: {total}", 0
        )

    targets = [n for n, _ in c.most_common(6)]
    return make_signal(
        "Histórico Mesa", targets, nums,
        "Transições observadas diretamente nesta Immersive Roulette.",
        f"{last} apareceu como antecedente {total}×", 0
    )

def all_strategies(nums):
    return [
        strategy_nanda(nums),
        strategy_arqueiro(nums),
        strategy_pull_table(nums),
        strategy_fixed(nums),
        strategy_x20(nums),
        strategy_history(nums),
    ]


# ============================================================
# APRENDIZADO
# ============================================================

class Learning:
    def __init__(self):
        self.data = {
            "evaluated_rounds": 0,
            "strategies": {},
            "journal": [],
            "last_evaluated_id": None,
            "meta": {
                "resolved": 0,
                "top1_hits": 0,
                "top3_hits": 0,
                "top5_hits": 0,
                "green_signals": 0,
                "green_top5_hits": 0,
                "recent": []
            },
        }
        for name in STRATEGY_NAMES:
            self.data["strategies"][name] = self.empty_stats()
        self.load()

    @staticmethod
    def empty_stats():
        return {
            "signals": 0,
            "primary_hits": 0,
            "coverage_hits": 0,
            "coverage_sum": 0,
            "recent": [],
        }

    def load(self):
        try:
            loaded = db.get_json("learning", None)

            # Migração automática do JSON antigo.
            if loaded is None and os.path.exists(LEARNING_FILE):
                try:
                    with open(LEARNING_FILE, "r", encoding="utf-8") as f:
                        loaded = json.load(f)
                except Exception:
                    loaded = None

            if isinstance(loaded, dict):
                self.data.update(loaded)

            for name in STRATEGY_NAMES:
                self.data.setdefault("strategies", {}).setdefault(name, self.empty_stats())

            self.data.setdefault("meta", {
                "resolved": 0,
                "top1_hits": 0,
                "top3_hits": 0,
                "top5_hits": 0,
                "green_signals": 0,
                "green_top5_hits": 0,
                "recent": []
            })

            db.set_json("learning", self.data)
        except Exception:
            pass

    def save(self):
        try:
            db.set_json("learning", self.data)
        except Exception:
            pass

    def record_prediction(self, round_id, prediction_list, meta_snapshot=None):
        # Não duplica previsão para o mesmo estado.
        if any(j.get("round_id") == round_id for j in self.data["journal"][-200:]):
            return

        packed = []
        for p in prediction_list:
            packed.append({
                "name": p["name"],
                "active": p["active"],
                "primary": p["primary"],
                "coverage": p["coverage"],
                "candidates": p["candidates"],
            })

        self.data["journal"].append({
            "round_id": round_id,
            "created_at": now_text(),
            "predictions": packed,
            "meta": meta_snapshot or {},
            "resolved": False,
            "actual": None,
        })
        self.data["journal"] = self.data["journal"][-2500:]
        self.save()

    def resolve_with_next(self, prior_round_id, actual):
        target = None
        for j in reversed(self.data["journal"]):
            if j.get("round_id") == prior_round_id and not j.get("resolved"):
                target = j
                break
        if not target:
            return

        target["resolved"] = True
        target["actual"] = actual
        self.data["evaluated_rounds"] += 1

        for p in target["predictions"]:
            if not p.get("active"):
                continue

            name = p["name"]
            s = self.data["strategies"].setdefault(name, self.empty_stats())
            coverage = unique(p.get("coverage", []))

            s["signals"] += 1
            s["coverage_sum"] += len(coverage)

            primary_hit = p.get("primary") == actual
            coverage_hit = actual in coverage

            if primary_hit:
                s["primary_hits"] += 1
            if coverage_hit:
                s["coverage_hits"] += 1

            s["recent"].append({
                "p": 1 if primary_hit else 0,
                "c": 1 if coverage_hit else 0,
                "k": len(coverage),
            })
            s["recent"] = s["recent"][-150:]

        # Resolve também o consenso/meta-learner
        meta = target.get("meta") or {}
        picks = [int(x) for x in meta.get("top5", []) if str(x).lstrip("-").isdigit()]
        quality = float(meta.get("quality", 0) or 0)
        level = str(meta.get("level", ""))
        auto_threshold = float(meta.get("auto_threshold", 72) or 72)

        if picks:
            M = self.data.setdefault("meta", {
                "resolved": 0,
                "top1_hits": 0,
                "top3_hits": 0,
                "top5_hits": 0,
                "green_signals": 0,
                "green_top5_hits": 0,
                "recent": []
            })
            top1_hit = actual == picks[0]
            top3_hit = actual in picks[:3]
            top5_hit = actual in picks[:5]
            was_green = level == "GREEN"

            M["resolved"] += 1
            M["top1_hits"] += int(top1_hit)
            M["top3_hits"] += int(top3_hit)
            M["top5_hits"] += int(top5_hit)

            if was_green:
                M["green_signals"] += 1
                M["green_top5_hits"] += int(top5_hit)

            M["recent"].append({
                "quality": round(quality, 2),
                "threshold": round(auto_threshold, 2),
                "level": level,
                "top1": int(top1_hit),
                "top3": int(top3_hit),
                "top5": int(top5_hit),
                "actual": int(actual),
                "k": min(5, len(picks)),
            })
            M["recent"] = M["recent"][-800:]

        self.save()

    def metric(self, name):
        s = self.data["strategies"].get(name, self.empty_stats())
        signals = s["signals"]

        if signals == 0:
            return {
                "weight": 1.0,
                "signals": 0,
                "primary_rate": 0,
                "coverage_rate": 0,
                "avg_coverage": 0,
                "edge_index": 0,
            }

        primary_rate = s["primary_hits"] / signals
        coverage_rate = s["coverage_hits"] / signals
        avg_cov = s["coverage_sum"] / signals

        # baseline proporcional à cobertura.
        random_cov = clamp(avg_cov / 37.0, 1/37, 1.0)
        primary_baseline = 1 / 37

        # shrinkage forte para evitar glorificar amostras pequenas.
        reliability = signals / (signals + 80.0)
        primary_lift = primary_rate / primary_baseline
        coverage_lift = coverage_rate / random_cov if random_cov else 1

        recent = s.get("recent", [])
        if recent:
            r_primary = sum(x["p"] for x in recent) / len(recent)
            r_cov = sum(x["c"] for x in recent) / len(recent)
            r_k = sum(x["k"] for x in recent) / len(recent)
            r_base = clamp(r_k / 37.0, 1/37, 1)
            recent_lift = 0.35 * (r_primary / primary_baseline) + 0.65 * (r_cov / r_base)
        else:
            recent_lift = 1.0

        lift = 0.25 * primary_lift + 0.50 * coverage_lift + 0.25 * recent_lift
        shrunk = 1.0 + (lift - 1.0) * reliability

        # Peso limitado: nenhum e-book domina só por sorte de curto prazo.
        weight = clamp(shrunk, 0.35, 2.40)

        return {
            "weight": round(weight, 3),
            "signals": signals,
            "primary_rate": round(100 * primary_rate, 2),
            "coverage_rate": round(100 * coverage_rate, 2),
            "avg_coverage": round(avg_cov, 2),
            "edge_index": round((shrunk - 1) * 100, 1),
        }


# ============================================================
# ENGINE
# ============================================================

class Engine:
    def __init__(self):
        self.lock = threading.RLock()
        self.history = []
        self.last_fetch = 0.0
        self.fetch_error = ""
        self.learning = Learning()
        self.load_history()

    def load_history(self):
        try:
            self.history = db.load_spins(MAX_HISTORY)

            # Migração do histórico JSON antigo para SQLite.
            if not self.history and os.path.exists(HISTORY_FILE):
                try:
                    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                        legacy = json.load(f)
                    if isinstance(legacy, list):
                        for rec in legacy[-MAX_HISTORY:]:
                            if isinstance(rec, dict) and "id" in rec and "number" in rec:
                                db.upsert_spin(rec)
                        self.history = db.load_spins(MAX_HISTORY)
                except Exception:
                    pass
        except Exception:
            self.history = []

    def save_history(self):
        try:
            for rec in self.history[-MAX_HISTORY:]:
                db.upsert_spin(rec)
        except Exception:
            pass

    def nums(self, upto=None):
        src = self.history if upto is None else self.history[:upto]
        return [int(x["number"]) for x in src]

    def fetch(self, force=False):
        with self.lock:
            if not force and time.time() - self.last_fetch < UPSTREAM_MIN_INTERVAL:
                return 0

            self.last_fetch = time.time()

            try:
                r = requests.get(
                    API_URL,
                    timeout=15,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Android) IRPredictor360-V3/1.0",
                        "Accept": "application/json,text/plain,*/*",
                    }
                )
                r.raise_for_status()
                payload = r.json()
                if not isinstance(payload, list):
                    raise ValueError("Formato inesperado da API.")

                parsed = []
                for item in payload:
                    try:
                        rid = str(item["id"])
                        data = item["data"]
                        outcome = data["result"]["outcome"]
                        n = int(outcome["number"])
                        if not 0 <= n <= 36:
                            continue
                        parsed.append({
                            "id": rid,
                            "number": n,
                            "settledAt": data.get("settledAt"),
                            "startedAt": data.get("startedAt"),
                        })
                    except Exception:
                        continue

                existing = {x["id"] for x in self.history}
                new_records = [rec for rec in reversed(parsed) if rec["id"] not in existing]

                for rec in new_records:
                    # Resolve a previsão que foi registrada sobre a rodada anterior.
                    if self.history:
                        prior_id = self.history[-1]["id"]
                        self.learning.resolve_with_next(prior_id, rec["number"])

                    self.history.append(rec)
                    db.upsert_spin(rec)

                    # Cria previsão imediatamente após incorporar o novo resultado.
                    nums = self.nums()
                    preds = all_strategies(nums)
                    enriched = []
                    for p in preds:
                        q = dict(p)
                        q["learning"] = self.learning.metric(p["name"])
                        enriched.append(q)
                    ens = self.ensemble(enriched, nums)
                    ent = self.entry_signal(enriched, ens, nums)
                    meta_snapshot = {
                        "top5": [x["number"] for x in ens[:5]],
                        "quality": ent.get("quality", 0),
                        "level": ent.get("level", "RED"),
                        "auto_threshold": ent.get("auto_threshold", 72),
                        "primary": ent.get("primary"),
                        "zone_numbers": ent.get("zone_numbers", []),
                    }
                    self.learning.record_prediction(rec["id"], preds, meta_snapshot)

                if new_records:
                    self.history = self.history[-MAX_HISTORY:]
                    self.save_history()

                # Se acabou de começar e nenhum registro tem previsão, cria para o último.
                if self.history:
                    last_id = self.history[-1]["id"]
                    nums = self.nums()
                    preds = all_strategies(nums)
                    enriched = []
                    for p in preds:
                        q = dict(p)
                        q["learning"] = self.learning.metric(p["name"])
                        enriched.append(q)
                    ens = self.ensemble(enriched, nums)
                    ent = self.entry_signal(enriched, ens, nums)
                    meta_snapshot = {
                        "top5": [x["number"] for x in ens[:5]],
                        "quality": ent.get("quality", 0),
                        "level": ent.get("level", "RED"),
                        "auto_threshold": ent.get("auto_threshold", 72),
                        "primary": ent.get("primary"),
                        "zone_numbers": ent.get("zone_numbers", []),
                    }
                    self.learning.record_prediction(last_id, preds, meta_snapshot)

                self.fetch_error = ""
                return len(new_records)

            except Exception as e:
                self.fetch_error = str(e)
                return 0

    def ensemble(self, preds, nums):
        scores = {n: 0.0 for n in range(37)}
        contributors = defaultdict(list)

        for p in preds:
            if not p["active"] or not p["candidates"]:
                continue

            metric = self.learning.metric(p["name"])
            w = metric["weight"]
            ordered = p["candidates"]

            for rank, n in enumerate(ordered):
                rank_weight = 1.0 / (1.0 + 0.36 * rank)
                scores[n] += w * rank_weight
                contributors[n].append(p["name"])

            # cobertura tem influência muito pequena, só como consenso geográfico
            for n in p["coverage"]:
                scores[n] += 0.08 * w

        # pequeno prior local, sem dominar
        recent = Counter(nums[-100:])
        for n in range(37):
            scores[n] += 0.04 * recent[n]

        ranked = sorted(range(37), key=lambda n: scores[n], reverse=True)
        mx = max(scores.values()) if scores else 1
        mn = min(scores.values()) if scores else 0

        out = []
        for n in ranked[:10]:
            norm = 50 if mx == mn else 100 * (scores[n] - mn) / (mx - mn)
            out.append({
                "number": n,
                "score": round(norm, 1),
                "color": color(n),
                "support": contributors[n],
                "support_count": len(set(contributors[n])),
            })
        return out

    def auto_green_threshold(self):
        """
        Aprende um limiar de qualidade para o verde usando apenas previsões
        já resolvidas. Usa suavização Bayesiana para não reagir demais a
        sequências curtas de sorte.
        """
        M = self.learning.data.get("meta", {})
        recent = list(M.get("recent", []))[-600:]

        default_threshold = 72.0
        baseline_top5 = 5 / 37

        if len(recent) < 40:
            return {
                "threshold": default_threshold,
                "sample": len(recent),
                "hit_rate": 0.0,
                "baseline": round(100 * baseline_top5, 2),
                "lift": 1.0,
                "mode": "PADRÃO",
            }

        best = None
        # thresholds conservadores
        for th in range(58, 86, 2):
            rows = [x for x in recent if float(x.get("quality", 0)) >= th]
            n = len(rows)
            if n < 25:
                continue

            hits = sum(int(x.get("top5", 0)) for x in rows)

            # Beta prior centrado no acaso: força equivalente a 37 observações.
            alpha0 = 5.0
            beta0 = 32.0
            posterior = (hits + alpha0) / (n + alpha0 + beta0)
            lift = posterior / baseline_top5

            # penaliza thresholds que disparam raramente
            coverage_factor = min(1.0, n / 70.0)
            utility = (lift - 1.0) * coverage_factor

            candidate = {
                "threshold": float(th),
                "sample": n,
                "hit_rate": round(100 * hits / n, 2),
                "posterior_rate": round(100 * posterior, 2),
                "baseline": round(100 * baseline_top5, 2),
                "lift": round(lift, 3),
                "utility": utility,
            }

            if best is None or candidate["utility"] > best["utility"]:
                best = candidate

        if not best:
            return {
                "threshold": default_threshold,
                "sample": len(recent),
                "hit_rate": 0.0,
                "baseline": round(100 * baseline_top5, 2),
                "lift": 1.0,
                "mode": "PADRÃO",
            }

        # Se o melhor corte não mostrou lift minimamente estável,
        # fica mais conservador.
        learned = best["threshold"]
        if best["lift"] < 1.03:
            learned = max(76.0, learned)

        learned = clamp(learned, 62.0, 84.0)
        best["threshold"] = round(learned, 1)
        best["mode"] = "APRENDIDO"
        return best

    def regime_analysis(self, nums):
        """
        Diagnóstico descritivo do comportamento recente.
        NÃO presume que o regime continuará no próximo giro.
        """
        sample = nums[-60:]
        if len(sample) < 20:
            return {
                "label": "AMOSTRA CURTA",
                "entropy": 0,
                "repeat_rate": 0,
                "mean_wheel_distance": 0,
                "cluster_rate": 0,
            }

        c = Counter(sample)
        total = len(sample)

        entropy = 0.0
        for count in c.values():
            p = count / total
            entropy -= p * math.log(p, 2)
        max_entropy = math.log(37, 2)
        entropy_norm = entropy / max_entropy if max_entropy else 0

        pairs = list(zip(sample[:-1], sample[1:]))
        repeat_rate = safe_div(sum(1 for a, b in pairs if a == b), len(pairs))
        distances = [wheel_distance(a, b) for a, b in pairs]
        mean_dist = safe_div(sum(distances), len(distances))
        cluster_rate = safe_div(sum(1 for d in distances if d <= 2), len(distances))

        if cluster_rate >= 0.24:
            label = "CLUSTER FÍSICO RECENTE"
        elif entropy_norm <= 0.72:
            label = "CONCENTRADO"
        elif entropy_norm >= 0.90:
            label = "MUITO DISPERSO"
        else:
            label = "NEUTRO"

        return {
            "label": label,
            "entropy": round(entropy_norm * 100, 1),
            "repeat_rate": round(repeat_rate * 100, 1),
            "mean_wheel_distance": round(mean_dist, 2),
            "cluster_rate": round(cluster_rate * 100, 1),
        }

    def meta_performance(self):
        M = self.learning.data.get("meta", {})
        n = int(M.get("resolved", 0))
        if n <= 0:
            return {
                "resolved": 0,
                "top1_pct": 0,
                "top3_pct": 0,
                "top5_pct": 0,
                "green_signals": 0,
                "green_top5_pct": 0,
            }

        gs = int(M.get("green_signals", 0))
        return {
            "resolved": n,
            "top1_pct": round(100 * M.get("top1_hits", 0) / n, 2),
            "top3_pct": round(100 * M.get("top3_hits", 0) / n, 2),
            "top5_pct": round(100 * M.get("top5_hits", 0) / n, 2),
            "green_signals": gs,
            "green_top5_pct": round(100 * M.get("green_top5_hits", 0) / gs, 2) if gs else 0,
            "baseline_top1": round(100 / 37, 2),
            "baseline_top3": round(300 / 37, 2),
            "baseline_top5": round(500 / 37, 2),
        }

    def entry_signal(self, preds, ensemble, nums):
        """
        Semáforo de ENTRADA.
        Verde não significa garantia. É apenas o melhor alinhamento interno
        entre os módulos disponíveis.
        """
        active = [p for p in preds if p.get("active")]
        active_count = len(active)

        if not ensemble:
            return {
                "level": "RED",
                "label": "NÃO APOSTAR",
                "reason": "Sem ranking disponível.",
                "bet_numbers": [],
                "zone_numbers": [],
                "zone_score": 0,
                "active_strategies": active_count,
                "support": 0,
                "spread": 0,
                "quality": 0,
            }

        top1 = ensemble[0]
        top5 = ensemble[:5]
        top5_nums = [x["number"] for x in top5]

        # Separação do ranking
        fifth_score = top5[-1]["score"] if len(top5) >= 5 else 0
        spread = top1["score"] - fifth_score

        # Qualidade média dos métodos ativos
        weights = []
        positive_edges = 0
        mature_methods = 0
        for p in active:
            L = p.get("learning", {})
            weights.append(float(L.get("weight", 1.0)))
            if float(L.get("edge_index", 0)) > 0:
                positive_edges += 1
            if int(L.get("signals", 0)) >= 25:
                mature_methods += 1

        avg_weight = sum(weights) / len(weights) if weights else 1.0
        support = int(top1.get("support_count", 0))
        learning_rounds = int(self.learning.data.get("evaluated_rounds", 0))

        # --------------------------------------------------------
        # Melhor arco físico de 5 casas da roda
        # --------------------------------------------------------
        score_map = {x["number"]: float(x["score"]) for x in ensemble}
        # Números fora do TOP10 têm score 0, evitando inventar força.
        best_zone = []
        best_zone_score = -1.0

        for start in range(37):
            arc = [WHEEL[(start + k) % 37] for k in range(5)]
            arc_score = sum(score_map.get(n, 0.0) for n in arc)
            # pequeno bônus quando contém candidatos TOP5
            arc_score += 8.0 * sum(1 for n in arc if n in top5_nums)

            if arc_score > best_zone_score:
                best_zone_score = arc_score
                best_zone = arc

        # --------------------------------------------------------
        # Índice de qualidade 0-100
        # --------------------------------------------------------
        q = 0.0
        q += min(25, active_count * 5.0)               # até 25
        q += min(22, support * 7.0)                    # até 22
        q += min(18, max(0, spread) * 0.75)            # até 18
        q += min(15, max(0, avg_weight - 0.8) * 18)    # até 15
        q += min(10, positive_edges * 2.5)              # até 10
        q += min(10, learning_rounds / 12.0)            # até 10
        quality = round(clamp(q, 0, 100), 1)

        # --------------------------------------------------------
        # Regras do semáforo com limiar AUTO-CALIBRADO
        # --------------------------------------------------------
        calibration = self.auto_green_threshold()
        green_threshold = float(calibration.get("threshold", 72.0))

        # Verde só aparece depois de alguma validação real.
        green = (
            len(nums) >= 150
            and learning_rounds >= 60
            and active_count >= 3
            and support >= 2
            and spread >= 18
            and avg_weight >= 0.95
            and quality >= green_threshold
        )

        yellow = (
            len(nums) >= 80
            and active_count >= 2
            and support >= 1
            and spread >= 8
            and quality >= 43
        )

        if green:
            level = "GREEN"
            label = "APOSTAR"
            reason = (
                f"Consenso forte: {active_count} métodos ativos, "
                f"{support} apoiando o nº {top1['number']}, "
                f"separação TOP1↔TOP5 de {spread:.1f}."
            )
            # Apenas no verde liberamos os números como entrada.
            bet_numbers = top5_nums
        elif yellow:
            level = "YELLOW"
            label = "OBSERVAR / AGUARDAR"
            reason = (
                f"Há sinal parcial, mas ainda não passou o filtro de entrada. "
                f"{active_count} métodos ativos, suporte {support}, separação {spread:.1f}."
            )
            bet_numbers = []
        else:
            level = "RED"
            label = "NÃO APOSTAR"
            reason = (
                "Consenso insuficiente ou histórico/aprendizado ainda fraco. "
                "O sistema bloqueou a entrada."
            )
            bet_numbers = []

        return {
            "level": level,
            "label": label,
            "reason": reason,
            "bet_numbers": bet_numbers,
            "watch_numbers": top5_nums,
            "zone_numbers": best_zone,
            "zone_score": round(best_zone_score, 1),
            "active_strategies": active_count,
            "support": support,
            "spread": round(spread, 1),
            "avg_weight": round(avg_weight, 3),
            "positive_edges": positive_edges,
            "mature_methods": mature_methods,
            "learning_rounds": learning_rounds,
            "quality": quality,
            "primary": top1["number"],
            "auto_threshold": round(green_threshold, 1),
            "calibration": calibration,
        }

    def snapshot(self):
        self.fetch()
        with self.lock:
            if not self.history:
                return {"ok": False, "error": self.fetch_error or "Sem histórico."}

            nums = self.nums()
            preds = all_strategies(nums)

            enriched = []
            for p in preds:
                q = dict(p)
                q["learning"] = self.learning.metric(p["name"])
                enriched.append(q)

            ensemble = self.ensemble(enriched, nums)
            entry = self.entry_signal(enriched, ensemble, nums)
            regime = self.regime_analysis(nums)
            meta_perf = self.meta_performance()
            last = nums[-1]

            # estatísticas 100
            hot = Counter(nums[-100:]).most_common(8)

            # gaps
            rev = list(reversed(nums))
            gaps = []
            for n in range(37):
                try:
                    g = rev.index(n)
                except ValueError:
                    g = len(nums)
                gaps.append((n, g))
            gaps.sort(key=lambda x: x[1], reverse=True)

            return {
                "ok": True,
                "server_time": now_text(),
                "history_size": len(nums),
                "fetch_error": self.fetch_error,
                "cloud": {
                    "collector": RUN_COLLECTOR,
                    "collect_interval": COLLECT_INTERVAL,
                    "db_file": os.path.basename(DB_FILE),
                    "db": db.stats(),
                    "backup": db.backup_stats(),
                    "integrity": db.integrity_check(),
                    "protected": bool(PANEL_PASSWORD),
                },
                "last": {
                    "id": self.history[-1]["id"],
                    "number": last,
                    "color": color(last),
                    "terminal": terminal(last),
                    "settledAt": self.history[-1].get("settledAt"),
                },
                "wheel": [
                    {"number": n, "color": color(n)}
                    for n in WHEEL
                ],
                "neighbors": {
                    "r1": wheel_neighbors(last, 1),
                    "r2": wheel_neighbors(last, 2),
                    "r3": wheel_neighbors(last, 3),
                },
                "strategies": enriched,
                "ensemble": ensemble,
                "entry": entry,
                "regime": regime,
                "meta_performance": meta_perf,
                "last20": list(reversed(nums[-20:])),
                "hot100": [{"number": n, "count": c} for n, c in hot],
                "gaps": [{"number": n, "gap": g} for n, g in gaps[:8]],
                "learning_rounds": self.learning.data["evaluated_rounds"],
            }

    def backtest_strategy(self, strategy_fn, nums, limit=400):
        if len(nums) < 100:
            return None

        start = max(60, len(nums) - limit)
        signals = primary_hits = coverage_hits = coverage_sum = 0

        for i in range(start, len(nums)):
            train = nums[:i]
            actual = nums[i]
            p = strategy_fn(train)
            if not p["active"]:
                continue

            signals += 1
            coverage = unique(p["coverage"])
            coverage_sum += len(coverage)

            if p["primary"] == actual:
                primary_hits += 1
            if actual in coverage:
                coverage_hits += 1

        if signals == 0:
            return {
                "signals": 0,
                "primary_pct": 0,
                "coverage_pct": 0,
                "baseline_coverage_pct": 0,
                "edge_pp": 0,
            }

        avg_cov = coverage_sum / signals
        cov_pct = 100 * coverage_hits / signals
        baseline = 100 * avg_cov / 37

        return {
            "signals": signals,
            "primary_pct": round(100 * primary_hits / signals, 2),
            "coverage_pct": round(cov_pct, 2),
            "avg_coverage": round(avg_cov, 2),
            "baseline_coverage_pct": round(baseline, 2),
            "edge_pp": round(cov_pct - baseline, 2),
        }

    def backtest_all(self, limit=400):
        nums = self.nums()
        fns = [
            ("Nanda", strategy_nanda),
            ("Arqueiro", strategy_arqueiro),
            ("Tabela Puxadores", strategy_pull_table),
            ("Números Fixos", strategy_fixed),
            ("X 2.0", strategy_x20),
            ("Histórico Mesa", strategy_history),
        ]

        rows = []
        for name, fn in fns:
            r = self.backtest_strategy(fn, nums, limit)
            if r is None:
                return {"ok": False, "message": "É preciso ter pelo menos 100 giros."}
            r["name"] = name
            rows.append(r)

        return {"ok": True, "tested_window": min(limit, max(0, len(nums)-60)), "rows": rows}


engine = Engine()


_collector_started = False
_collector_guard = threading.Lock()

def collector_loop():
    logger.info("Coletor iniciado; intervalo=%.1fs", COLLECT_INTERVAL)
    while True:
        try:
            added = engine.fetch(force=True)
            if added:
                logger.info("Coletor: +%s rodada(s); memória=%s", added, len(engine.history))
        except Exception as exc:
            logger.exception("Erro no coletor: %s", exc)
        time.sleep(COLLECT_INTERVAL)


_backup_started = False
_backup_guard = threading.Lock()

def backup_loop():
    logger.info(
        "Backup automático iniciado: intervalo=%ss, retenção=%s",
        BACKUP_INTERVAL_SECONDS,
        BACKUP_KEEP,
    )
    # Faz um backup inicial depois de um pequeno atraso para não competir com startup.
    time.sleep(20)
    while True:
        try:
            path = db.backup()
            logger.info("Backup concluído: %s", path)
        except Exception:
            logger.exception("Falha no backup automático")
        time.sleep(BACKUP_INTERVAL_SECONDS)

def start_backup_loop():
    global _backup_started
    with _backup_guard:
        if _backup_started:
            return
        _backup_started = True
        t = threading.Thread(
            target=backup_loop,
            name="ir360-backup",
            daemon=True
        )
        t.start()

def backup_on_exit():
    try:
        if os.path.exists(DB_FILE) and os.path.getsize(DB_FILE) > 0:
            path = db.backup()
            logger.info("Backup de encerramento: %s", path)
    except Exception:
        logger.exception("Falha ao gerar backup de encerramento")

atexit.register(backup_on_exit)

def start_collector():
    global _collector_started
    if not RUN_COLLECTOR:
        logger.info("Coletor desativado por RUN_COLLECTOR=0")
        return

    with _collector_guard:
        if _collector_started:
            return
        _collector_started = True
        t = threading.Thread(
            target=collector_loop,
            name="ir360-cloud-collector",
            daemon=True
        )
        t.start()

start_collector()
start_backup_loop()


# ============================================================
# WEB UI
# ============================================================

HTML = r"""
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>IR Predictor 360 V7 Professional Cloud</title>
<style>
:root{
 --bg:#07100d;--card:#101a16;--card2:#16231d;--line:#293a32;
 --text:#f4f7f5;--muted:#9eaea6;--red:#b82938;--black:#171a1b;
 --green:#148c54;--cyan:#4ac6d3;--gold:#d5ad52;
}
*{box-sizing:border-box}
body{margin:0;background:#07100d;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
.wrap{max-width:1180px;margin:auto;padding:12px}
header{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
h1{font-size:19px;margin:0} h2{font-size:13px;margin:0 0 10px;color:#dbe5e0;letter-spacing:.3px}
.small{font-size:12px;color:var(--muted)}
.actions{display:flex;gap:8px;flex-wrap:wrap}
button{min-height:44px;border:1px solid #405248;background:#17251f;color:white;border-radius:11px;padding:8px 13px;font-weight:750}
.grid{display:grid;gap:12px;grid-template-columns:1fr}
@media(min-width:860px){.grid.two{grid-template-columns:.92fr 1.08fr}.strategies{grid-template-columns:repeat(2,1fr)}}
.card{background:var(--card);border:1px solid var(--line);border-radius:15px;padding:13px}
.hero{display:flex;align-items:center;justify-content:center;gap:15px;flex-wrap:wrap}
.ball{width:88px;height:88px;border-radius:50%;display:grid;place-items:center;font-size:38px;font-weight:900;border:4px solid #ddd}
.red{background:var(--red)} .black{background:var(--black)} .green{background:var(--green)}
.badge{display:inline-block;padding:5px 8px;border:1px solid #3a4d44;background:var(--card2);border-radius:999px;margin:2px;font-size:11px}
#wheel{width:min(94vw,480px);height:auto;display:block;margin:auto}
.wedge{stroke:#c6b786;stroke-width:.65}.wtext{fill:white;font-size:9px;font-weight:900;text-anchor:middle;dominant-baseline:middle}
.lastMark{fill:none;stroke:white;stroke-width:3.5}
.topMark{fill:none;stroke:var(--cyan);stroke-width:2.4;stroke-dasharray:3 2}
.zoneMark{fill:none;stroke:var(--gold);stroke-width:5;opacity:.72}
.betMark{fill:none;stroke:#69efaa;stroke-width:4}
.trafficWrap{display:grid;grid-template-columns:auto 1fr;gap:12px;align-items:center}
.traffic{width:70px;background:#07100d;border:1px solid #33463c;border-radius:18px;padding:8px;display:grid;gap:7px}
.light{width:52px;height:52px;border-radius:50%;background:#1a2420;border:2px solid #34483e;display:grid;place-items:center;font-size:11px;font-weight:900;color:#6f7f77}
.light.on.redL{background:#d93848;color:white;box-shadow:0 0 16px rgba(217,56,72,.55)}
.light.on.yellowL{background:#e0ae35;color:#151515;box-shadow:0 0 16px rgba(224,174,53,.55)}
.light.on.greenL{background:#27ba70;color:white;box-shadow:0 0 16px rgba(39,186,112,.55)}
.entryLabel{font-size:26px;font-weight:950;margin-bottom:4px}
.entryNumbers{font-size:13px;line-height:1.6}
.entryBox{background:#0c1612;border:1px solid #2d4037;border-radius:12px;padding:10px;margin-top:10px}
.strategies{display:grid;gap:10px}
.strategy{background:#13201a;border:1px solid #2a3b33;border-radius:13px;padding:11px;min-width:0}
.strategy.off{opacity:.66}
.shead{display:flex;justify-content:space-between;gap:8px;align-items:center}
.sname{font-weight:850}.weight{font-size:11px;color:#cfc098}
.pick{font-size:31px;font-weight:900;margin:8px 0 3px}
.nums{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}
.num{width:34px;height:34px;border-radius:50%;display:grid;place-items:center;font-weight:850;font-size:13px;border:1px solid #5e6e66}
.reason{font-size:12px;color:#b7c4bd;line-height:1.4;margin-top:7px}
.learn{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-top:9px}
.learn div{background:#0c1612;padding:6px;border-radius:8px;text-align:center}.learn b{display:block;font-size:13px}.learn span{font-size:9px;color:var(--muted)}
.ensembleRow{display:grid;grid-template-columns:34px 42px 1fr 52px;gap:7px;align-items:center;padding:7px 0;border-bottom:1px solid #223129}
.rankBall{width:38px;height:38px;border-radius:50%;display:grid;place-items:center;font-weight:900}
.bar{height:7px;background:#23342c;border-radius:999px;overflow:hidden}.bar i{display:block;height:100%;background:linear-gradient(90deg,#6f887d,var(--cyan))}
.chips{display:flex;flex-wrap:wrap;gap:6px}.chip{min-width:36px;height:36px;padding:0 8px;border-radius:999px;display:grid;place-items:center;font-weight:800}
.note{font-size:12px;line-height:1.45;color:#c2ccc7;border-left:3px solid #a88e4c;padding:8px;background:#131c18;border-radius:8px}
.metrics4{display:grid;grid-template-columns:repeat(2,1fr);gap:7px}
@media(min-width:600px){.metrics4{grid-template-columns:repeat(4,1fr)}}
.metric{background:#0c1612;border:1px solid #25372e;border-radius:10px;padding:8px;text-align:center}
.metric b{display:block;font-size:17px}.metric span{font-size:9px;color:var(--muted)}
table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:7px 4px;border-bottom:1px solid #26362e;text-align:right}th:first-child,td:first-child{text-align:left}
.good{color:#6be0a5}.bad{color:#ef7e87}.mid{color:#e7c06a}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div>
    <h1>🎯 IR Predictor 360 • Strategy Lab V5 • Auto-Learning</h1>
    <div id="status" class="small">Inicializando...</div>
  </div>
  <div class="actions">
    <button id="refresh">↻ Atualizar</button>
    <button id="backtest">🧪 Comparar estratégias</button>
  </div>
</header>

<div class="grid two">
  <div>
    <section class="card">
      <h2>ÚLTIMO RESULTADO</h2>
      <div class="hero">
        <div id="ball" class="ball black">--</div>
        <div>
          <div id="lastMeta">Aguardando...</div>
          <div id="neighborBadges"></div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top:12px">
      <h2>🚦 SEMÁFORO DE ENTRADA</h2>
      <div class="trafficWrap">
        <div class="traffic" aria-label="Semáforo de entrada">
          <div id="redLight" class="light redL">STOP</div>
          <div id="yellowLight" class="light yellowL">WAIT</div>
          <div id="greenLight" class="light greenL">GO</div>
        </div>
        <div>
          <div id="entryLabel" class="entryLabel">ANALISANDO</div>
          <div id="entryReason" class="small">Aguardando dados...</div>
          <div id="entryBox" class="entryBox"></div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top:12px">
      <h2>RODA EUROPEIA • ONDE O MODELO INDICA</h2>
      <svg id="wheel" viewBox="0 0 500 500" aria-label="Roda europeia"></svg>
      <div class="small" style="text-align:center">
        Branco = último • verde = números liberados no 🟢 • azul = TOP 5 observado • dourado = zona física mais forte
      </div>
    </section>
  </div>

  <div>
    <section class="card">
      <h2>🧠 META-LEARNER • CONSENSO FINAL</h2>
      <div id="ensemble"></div>
      <div class="note" style="margin-top:10px">
        O peso de cada método sobe ou cai conforme previsões anteriores são resolvidas. O ajuste considera também quantos números cada estratégia cobre para evitar “ganhar” apenas por apostar em metade da roda.
      </div>
    </section>

    <section class="card" style="margin-top:12px">
      <h2>☁️ CLOUD 24H</h2>
      <div id="cloudPanel" class="reason">Carregando estado do servidor...</div>
    </section>

    <section class="card" style="margin-top:12px">
      <h2>🧬 AUTO-CALIBRAÇÃO & REGIME</h2>
      <div id="calibrationPanel"></div>
    </section>

    <section class="card" style="margin-top:12px">
      <h2>📚 PAINEL DAS ESTRATÉGIAS</h2>
      <div id="strategies" class="strategies"></div>
    </section>
  </div>
</div>

<div class="grid two" style="margin-top:12px">
  <section class="card">
    <h2>ÚLTIMOS 20</h2><div id="last20" class="chips"></div>
    <h2 style="margin-top:14px">🔥 QUENTES 100</h2><div id="hot" class="chips"></div>
    <h2 style="margin-top:14px">⏳ GAPS</h2><div id="gaps" class="chips"></div>
  </section>

  <section class="card">
    <h2>🧪 BACKTEST COMPARATIVO</h2>
    <div id="backtestResult" class="small">Use o botão “Comparar estratégias”.</div>
    <div class="note" style="margin-top:10px">
      Nenhuma estratégia de histórico altera a vantagem matemática da casa numa roleta equilibrada. Este painel serve para testar se algum padrão continua acima do baseline em dados futuros.
    </div>
  </section>
</div>
</div>

<script>
const REDS=new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);
const SVGNS='http://www.w3.org/2000/svg';
let DATA=null;

function cls(n){return n===0?'green':(REDS.has(Number(n))?'red':'black')}
function esc(x){return String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function chip(n){return `<span class="chip ${cls(n)}">${n}</span>`}
function polar(cx,cy,r,a){let q=(a-90)*Math.PI/180;return{x:cx+r*Math.cos(q),y:cy+r*Math.sin(q)}}
function pathW(cx,cy,r1,r2,a0,a1){
 let p1=polar(cx,cy,r2,a0),p2=polar(cx,cy,r2,a1),p3=polar(cx,cy,r1,a1),p4=polar(cx,cy,r1,a0);
 return `M${p1.x},${p1.y} A${r2},${r2} 0 0 1 ${p2.x},${p2.y} L${p3.x},${p3.y} A${r1},${r1} 0 0 0 ${p4.x},${p4.y} Z`
}
function svgEl(tag,attrs={}){
 let e=document.createElementNS(SVGNS,tag);for(const[k,v]of Object.entries(attrs))e.setAttribute(k,v);return e
}
function drawWheel(d){
 const svg=document.getElementById('wheel');svg.innerHTML='';
 const top=new Set((d.ensemble||[]).slice(0,5).map(x=>x.number));
 const zone=new Set((d.entry?.zone_numbers||[]).map(Number));
 const bets=new Set((d.entry?.bet_numbers||[]).map(Number));
 const last=d.last.number, step=360/37;
 svg.appendChild(svgEl('circle',{cx:250,cy:250,r:244,fill:'#08110e',stroke:'#907f53','stroke-width':4}));
 d.wheel.forEach((x,i)=>{
   const a0=i*step,a1=(i+1)*step,mid=(a0+a1)/2;
   const fill=x.color==='red'?'#b82938':x.color==='green'?'#148c54':'#171a1b';
   svg.appendChild(svgEl('path',{d:pathW(250,250,144,232,a0,a1),fill,class:'wedge'}));
   const p=polar(250,250,188,mid),t=svgEl('text',{x:p.x,y:p.y,class:'wtext'});t.textContent=x.number;svg.appendChild(t);
   if(zone.has(x.number))svg.appendChild(svgEl('path',{d:pathW(250,250,130,244,a0+.8,a1-.8),class:'zoneMark'}));
   if(top.has(x.number))svg.appendChild(svgEl('path',{d:pathW(250,250,136,239,a0+.5,a1-.5),class:'topMark'}));
   if(bets.has(x.number))svg.appendChild(svgEl('path',{d:pathW(250,250,128,246,a0+.35,a1-.35),class:'betMark'}));
   if(x.number===last)svg.appendChild(svgEl('path',{d:pathW(250,250,124,248,a0+.25,a1-.25),class:'lastMark'}));
 });
 svg.appendChild(svgEl('circle',{cx:250,cy:250,r:126,fill:'#0b1712',stroke:'#8c805b','stroke-width':3}));
 let t=svgEl('text',{x:250,y:244,fill:'#d5ad52','font-size':31,'font-weight':900,'text-anchor':'middle'});t.textContent=last;svg.appendChild(t);
 let s=svgEl('text',{x:250,y:268,fill:'#9eaea6','font-size':10,'text-anchor':'middle'});s.textContent='ÚLTIMO RESULTADO';svg.appendChild(s);
 let a=svgEl('text',{x:250,y:292,fill:(d.entry?.level==='GREEN'?'#69efaa':d.entry?.level==='YELLOW'?'#e0ae35':'#ef7e87'),'font-size':12,'font-weight':900,'text-anchor':'middle'});
 a.textContent=d.entry?.label||'ANALISANDO';svg.appendChild(a);
}
function render(d){
 DATA=d;
 document.getElementById('status').textContent=`Online • ${d.history_size} giros • ${d.learning_rounds} previsões resolvidas • ${d.server_time}`+(d.fetch_error?` • API: ${d.fetch_error}`:'');
 const b=document.getElementById('ball');b.className=`ball ${cls(d.last.number)}`;b.textContent=d.last.number;
 document.getElementById('lastMeta').innerHTML=`<b>Terminal ${d.last.terminal}</b><br><span class="small">${esc(d.last.settledAt||'')}</span>`;
 document.getElementById('neighborBadges').innerHTML=`<span class="badge">±1 ${d.neighbors.r1.join(' • ')}</span><span class="badge">±2 ${d.neighbors.r2.join(' • ')}</span>`;

 const E=d.entry||{};
 const red=document.getElementById('redLight'), yellow=document.getElementById('yellowLight'), green=document.getElementById('greenLight');
 red.className='light redL'+(E.level==='RED'?' on':'');
 yellow.className='light yellowL'+(E.level==='YELLOW'?' on':'');
 green.className='light greenL'+(E.level==='GREEN'?' on':'');
 document.getElementById('entryLabel').textContent=E.label||'ANALISANDO';
 document.getElementById('entryReason').textContent=E.reason||'';
 const exact=(E.bet_numbers||[]);
 const watch=(E.watch_numbers||[]);
 const zone=(E.zone_numbers||[]);
 document.getElementById('entryBox').innerHTML=
   `<b>Qualidade do sinal: ${Number(E.quality||0).toFixed(1)}/100</b><br>`+
   `<span class="small">${E.active_strategies||0} estratégias ativas • suporte TOP1 ${E.support||0} • spread ${Number(E.spread||0).toFixed(1)} • verde exige qualidade ≥ ${Number(E.auto_threshold||72).toFixed(0)}</span><br>`+
   (E.level==='GREEN'
      ? `<div style="margin-top:7px"><b>🎯 APOSTAR NOS NÚMEROS:</b> ${exact.map(chip).join('')}</div>`
      : `<div style="margin-top:7px"><b>👀 TOP 5 EM OBSERVAÇÃO:</b> ${watch.map(chip).join('')}</div>`) +
   `<div style="margin-top:7px"><b>🎡 ZONA FÍSICA MAIS FORTE:</b> ${zone.map(chip).join('')}</div>`+
   `<div class="small" style="margin-top:6px">A entrada só é liberada no verde. Amarelo e vermelho bloqueiam aposta.</div>`;

 drawWheel(d);

 const CL=d.cloud||{};
 document.getElementById('cloudPanel').innerHTML=
   `<b>Coletor:</b> ${CL.collector?'ATIVO 24H':'DESATIVADO'} • `+
   `<b>intervalo:</b> ${Number(CL.collect_interval||0).toFixed(0)}s • `+
   `<b>SQLite:</b> ${(CL.db?.spins||0)} giros persistidos • `+
   `<b>integridade:</b> ${CL.integrity?.ok?'OK':'ERRO'} • `+
   `<b>backups:</b> ${(CL.backup?.count||0)} • `+
   `<b>painel protegido:</b> ${CL.protected?'SIM':'NÃO'}<br>`+
   `<span class="small">O coletor e os backups rodam no servidor; fechar o navegador não apaga nem interrompe o histórico enquanto o serviço estiver ativo.</span>`;

 const C=E.calibration||{}, R=d.regime||{}, M=d.meta_performance||{};
 document.getElementById('calibrationPanel').innerHTML=`
   <div class="metrics4">
     <div class="metric"><b>${Number(E.auto_threshold||72).toFixed(0)}</b><span>LIMIAR VERDE</span></div>
     <div class="metric"><b>${Number(E.quality||0).toFixed(1)}</b><span>QUALIDADE ATUAL</span></div>
     <div class="metric"><b>${Number(M.top5_pct||0).toFixed(1)}%</b><span>META TOP5</span></div>
     <div class="metric"><b>${M.resolved||0}</b><span>PREVISÕES RESOLVIDAS</span></div>
   </div>
   <div class="reason" style="margin-top:9px">
     <b>Calibração:</b> ${esc(C.mode||'PADRÃO')} • amostra ${C.sample||0} • lift estimado ${Number(C.lift||1).toFixed(2)}×<br>
     <b>Regime recente:</b> ${esc(R.label||'-')} • entropia ${Number(R.entropy||0).toFixed(1)}% • cluster físico ${Number(R.cluster_rate||0).toFixed(1)}% • distância média ${Number(R.mean_wheel_distance||0).toFixed(2)}<br>
     <b>Meta histórico:</b> TOP1 ${Number(M.top1_pct||0).toFixed(2)}% (base ${Number(M.baseline_top1||2.70).toFixed(2)}%) • TOP5 ${Number(M.top5_pct||0).toFixed(2)}% (base ${Number(M.baseline_top5||13.51).toFixed(2)}%) • verdes resolvidos ${M.green_signals||0}
   </div>`;

 document.getElementById('ensemble').innerHTML=(d.ensemble||[]).map((x,i)=>`
   <div class="ensembleRow">
    <b>${i+1}º</b><div class="rankBall ${cls(x.number)}">${x.number}</div>
    <div><div class="small">${x.support_count} métodos: ${esc((x.support||[]).join(', ')||'prior local')}</div><div class="bar"><i style="width:${Math.max(2,x.score)}%"></i></div></div>
    <b>${x.score.toFixed(1)}</b>
   </div>`).join('');

 document.getElementById('strategies').innerHTML=(d.strategies||[]).map(p=>{
   const L=p.learning||{};
   return `<article class="strategy ${p.active?'':'off'}">
      <div class="shead"><span class="sname">${esc(p.name)}</span><span class="weight">peso IA ${Number(L.weight||1).toFixed(2)}×</span></div>
      ${p.active?`<div class="pick">${p.primary}</div><div class="small">palpite principal</div>`:`<div class="pick" style="font-size:18px">SEM GATILHO</div>`}
      <div class="nums">${(p.candidates||[]).slice(0,8).map(chip).join('')}</div>
      <div class="reason"><b>${esc(p.trigger||'')}</b><br>${esc(p.reason)}</div>
      <div class="learn">
       <div><b>${L.signals||0}</b><span>SINAIS</span></div>
       <div><b>${Number(L.primary_rate||0).toFixed(1)}%</b><span>TOP1</span></div>
       <div><b>${Number(L.edge_index||0).toFixed(1)}</b><span>ÍNDICE EDGE</span></div>
      </div>
   </article>`
 }).join('');

 document.getElementById('last20').innerHTML=d.last20.map(chip).join('');
 document.getElementById('hot').innerHTML=d.hot100.map(x=>chip(x.number)+`<span class="small">${x.count}×</span>`).join(' ');
 document.getElementById('gaps').innerHTML=d.gaps.map(x=>chip(x.number)+`<span class="small">g${x.gap}</span>`).join(' ');
}
async function load(force=false){
 try{
   const r=await fetch(force?'/api/status?force=1':'/api/status',{cache:'no-store'}),d=await r.json();
   if(d.ok)render(d);else document.getElementById('status').textContent=d.error||'Erro';
 }catch(e){document.getElementById('status').textContent='Falha local: '+e.message}
}
async function backtest(){
 const btn=document.getElementById('backtest'),box=document.getElementById('backtestResult');
 btn.disabled=true;box.textContent='Executando...';
 try{
   const r=await fetch('/api/backtest?limit=400',{cache:'no-store'}),d=await r.json();
   if(!d.ok){box.textContent=d.message||'Erro';return}
   box.innerHTML=`<table><thead><tr><th>Método</th><th>Sinais</th><th>TOP1</th><th>Cobertura</th><th>Baseline</th><th>Δ</th></tr></thead><tbody>`+
   d.rows.map(x=>`<tr><td>${esc(x.name)}</td><td>${x.signals}</td><td>${x.primary_pct}%</td><td>${x.coverage_pct}%</td><td>${x.baseline_coverage_pct}%</td><td class="${x.edge_pp>0?'good':x.edge_pp<0?'bad':'mid'}">${x.edge_pp>0?'+':''}${x.edge_pp}pp</td></tr>`).join('')+
   `</tbody></table><div class="small" style="margin-top:7px">Janela avaliada: até ${d.tested_window} giros. Δ compara cobertura real com cobertura aleatória equivalente.</div>`;
 }catch(e){box.textContent='Erro: '+e.message}finally{btn.disabled=false}
}
document.getElementById('refresh').addEventListener('click',()=>load(true));
document.getElementById('backtest').addEventListener('click',backtest);
load(true);setInterval(()=>load(false),5000);
</script>
</body>
</html>
"""


def _authorized():
    if not PANEL_PASSWORD:
        return True
    auth = request.authorization
    return bool(
        auth
        and auth.username == PANEL_USER
        and auth.password == PANEL_PASSWORD
    )

@app.before_request
def protect_panel():
    if request.path == "/health":
        return None
    if _authorized():
        return None
    return (
        "Autenticação necessária.",
        401,
        {"WWW-Authenticate": 'Basic realm="IR Predictor 360"'}
    )

@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "IR Predictor 360 V6 Cloud",
        "collector": RUN_COLLECTOR,
        "history": len(engine.history),
        "fetch_error": engine.fetch_error,
        "db": db.stats(),
        "backup": db.backup_stats(),
        "integrity": db.integrity_check(),
    })


@app.route("/api/storage")
def storage_status():
    return jsonify({
        "ok": True,
        "database": db.stats(),
        "integrity": db.integrity_check(),
        "backups": db.backup_stats(),
        "data_dir": DATA_DIR,
    })

@app.route("/api/backup", methods=["POST"])
def manual_backup():
    try:
        path = db.backup()
        return jsonify({
            "ok": True,
            "backup": os.path.basename(path),
            "stats": db.backup_stats(),
        })
    except Exception as exc:
        logger.exception("Falha no backup manual")
        return jsonify({"ok": False, "error": str(exc)}), 500

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/status")
def status():
    if request.args.get("force") == "1":
        engine.fetch(force=True)
    return jsonify(engine.snapshot())

@app.route("/api/backtest")
def backtest():
    try:
        limit = int(request.args.get("limit", "400"))
    except Exception:
        limit = 400
    limit = max(100, min(limit, 1000))
    return jsonify(engine.backtest_all(limit))

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("=" * 68)
    print("IR PREDICTOR 360 • V7 PROFESSIONAL CLOUD • SQLITE + AUTO-LEARNING")
    print(f"Servidor: 0.0.0.0:{port}")
    print(f"Banco: {DB_FILE}")
    print(f"Coletor: {'ATIVO' if RUN_COLLECTOR else 'DESATIVADO'}")
    print("=" * 68)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
