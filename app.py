#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import base64
import csv
import io
import json
import logging
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, Response, jsonify, request

# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "3.3-mega-roulette-round-intelligence"

API_URL = os.getenv(
    "API_URL",
    "https://api-cs.casino.org/svc-evolution-game-events/api/"
    "megaroulette?page=0&size=27&sort=data.settledAt,desc&duration=6&isLightningNumberMatched=false",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DB_FILE = Path(os.getenv("DB_FILE", str(DATA_DIR / "roulette_clean_ai.sqlite3")))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", str(DATA_DIR / "backups")))

COLLECT_INTERVAL = max(5, int(os.getenv("COLLECT_INTERVAL", "8")))
RUN_COLLECTOR = os.getenv("RUN_COLLECTOR", "1") == "1"
HISTORY_LIMIT = max(500, int(os.getenv("HISTORY_LIMIT", "5000")))

AI_MIN_SAMPLES = max(30, int(os.getenv("AI_MIN_SAMPLES", "120")))
AI_LR = float(os.getenv("AI_LR", "0.035"))
AI_L2 = float(os.getenv("AI_L2", "0.0008"))
AI_MAX_BLEND = float(os.getenv("AI_MAX_BLEND", "0.38"))

GREEN_MIN_RESOLVED = max(30, int(os.getenv("GREEN_MIN_RESOLVED", "120")))
GREEN_SCORE = float(os.getenv("GREEN_SCORE", "88"))
YELLOW_SCORE = float(os.getenv("YELLOW_SCORE", "68"))
MIN_WINDOW_AGREEMENT = float(os.getenv("MIN_WINDOW_AGREEMENT", "0.48"))
MAX_GREEN_LOSS_STREAK = max(0, int(os.getenv("MAX_GREEN_LOSS_STREAK", "2")))

MIN_CENTER_1 = float(os.getenv("MIN_CENTER_1", "76"))
MIN_CENTER_2 = float(os.getenv("MIN_CENTER_2", "70"))
MIN_CENTER_3 = float(os.getenv("MIN_CENTER_3", "64"))

MIN_CHIP = max(0.01, float(os.getenv("MIN_CHIP", "2.50")))
BANKROLL = max(0.0, float(os.getenv("BANKROLL", "500")))
RISK_PCT = max(0.0, float(os.getenv("RISK_PCT", "5.0")))

# Rede resiliente
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "5"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "12"))
HTTP_RETRIES = max(0, int(os.getenv("HTTP_RETRIES", "2")))
BACKOFF_MAX = max(30, int(os.getenv("BACKOFF_MAX", "120")))
BACKFILL_PAGES = max(1, min(8, int(os.getenv("BACKFILL_PAGES", "4"))))

# Precisão V2
GREEN_PLUS_SCORE = float(os.getenv("GREEN_PLUS_SCORE", "93"))
GREEN_ELITE_SCORE = float(os.getenv("GREEN_ELITE_SCORE", "96"))
MIN_MODEL_CONSENSUS = float(os.getenv("MIN_MODEL_CONSENSUS", "0.60"))
REGIME_WARN_TV = float(os.getenv("REGIME_WARN_TV", "0.22"))
REGIME_BLOCK_TV = float(os.getenv("REGIME_BLOCK_TV", "0.38"))
CALIBRATION_MIN_N = max(20, int(os.getenv("CALIBRATION_MIN_N", "60")))
MEGA_STATS_WINDOW = max(30, int(os.getenv("MEGA_STATS_WINDOW", "300")))
STALE_DATA_SECONDS = max(30, int(os.getenv("STALE_DATA_SECONDS", "120")))
AI_SKILL_MIN_SAMPLES = max(30, int(os.getenv("AI_SKILL_MIN_SAMPLES", "80")))
CLOUD_MODE = os.getenv("CLOUD_MODE", "0") == "1"
MAX_BACKFILL_PAGES = max(4, min(200, int(os.getenv("MAX_BACKFILL_PAGES", "80"))))
DEEP_SYNC_INTERVAL = max(300, int(os.getenv("DEEP_SYNC_INTERVAL", "1800")))
BACKUP_INTERVAL = max(900, int(os.getenv("BACKUP_INTERVAL", "21600")))
BACKUP_KEEP = max(3, int(os.getenv("BACKUP_KEEP", "14")))
COLLECTOR_RESTART_DELAY = max(3, int(os.getenv("COLLECTOR_RESTART_DELAY", "5")))

# Motor de Puxadores V3.2
PULLER_LONG_WINDOW = max(300, int(os.getenv("PULLER_LONG_WINDOW", "3000")))
PULLER_RECENT_WINDOW = max(100, int(os.getenv("PULLER_RECENT_WINDOW", "600")))
PULLER_PAIR_WINDOW = max(300, int(os.getenv("PULLER_PAIR_WINDOW", "2500")))
PULLER_PRIOR_ALPHA = max(0.20, float(os.getenv("PULLER_PRIOR_ALPHA", "1.20")))
PULLER_MIN_SOURCE_SUPPORT = max(3, int(os.getenv("PULLER_MIN_SOURCE_SUPPORT", "8")))
PULLER_STRONG_SOURCE_SUPPORT = max(PULLER_MIN_SOURCE_SUPPORT, int(os.getenv("PULLER_STRONG_SOURCE_SUPPORT", "18")))
PULLER_SKILL_MIN_SAMPLES = max(30, int(os.getenv("PULLER_SKILL_MIN_SAMPLES", "100")))
PULLER_MAX_BLEND = max(0.0, min(0.45, float(os.getenv("PULLER_MAX_BLEND", "0.24"))))
PULLER_RECENCY_DECAY = max(0.95, min(0.9999, float(os.getenv("PULLER_RECENCY_DECAY", "0.992"))))
PULLER_MIN_DISPLAY_STRENGTH = float(os.getenv("PULLER_MIN_DISPLAY_STRENGTH", "20"))
PULLER_BOOTSTRAP_POINTS = max(100, min(1500, int(os.getenv("PULLER_BOOTSTRAP_POINTS", "700"))))

# Inteligência específica por rodada V3.3
ROUND_LOCAL_MAX_EVENTS = max(12, min(120, int(os.getenv("ROUND_LOCAL_MAX_EVENTS", "60"))))
ROUND_LOCAL_MIN_EVENTS = max(6, int(os.getenv("ROUND_LOCAL_MIN_EVENTS", "12")))
ROUND_STRONG_EVIDENCE = float(os.getenv("ROUND_STRONG_EVIDENCE", "82"))
ROUND_GOOD_EVIDENCE = float(os.getenv("ROUND_GOOD_EVIDENCE", "70"))
ROUND_MAX_SPECIAL_BLEND = max(0.20, min(0.60, float(os.getenv("ROUND_MAX_SPECIAL_BLEND", "0.48"))))
ROUND_AI_ALIGNMENT_MIN = float(os.getenv("ROUND_AI_ALIGNMENT_MIN", "0.18"))

PANEL_USER = os.getenv("PANEL_USER", "").strip()
PANEL_PASSWORD = os.getenv("PANEL_PASSWORD", "").strip()

DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("clean-ai")

# ============================================================
# ROLETA EUROPEIA
# ============================================================

WHEEL = [
    0,32,15,19,4,21,2,25,17,34,6,27,13,36,11,30,8,23,10,5,24,16,
    33,1,20,14,31,9,22,18,29,7,28,12,35,3,26
]
WHEEL_INDEX = {n:i for i,n in enumerate(WHEEL)}
RED = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

VOISINS = {22,18,29,7,28,12,35,3,26,0,32,15,19,4,21,2,25}
TIERS = {27,13,36,11,30,8,23,10,5,24,16,33}
ORPHELINS = {1,20,14,31,9,17,34,6}

UNIFORM_P = 1/37
UNIFORM_LOG_LOSS = math.log(37.0)
UNIFORM_BRIER = 36.0/37.0

FEATURES = [
    "bias","transicao","freq20","freq50","freq100","freq300","freq1000",
    "cluster_roda","terminal","atraso","perto_ultimo","mesmo_terminal",
    "mesma_cor","mesma_duzia","setor"
]

# ============================================================
# HELPERS
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def iso_age_seconds(value):
    if not value:
        return None
    try:
        dt=datetime.fromisoformat(str(value).replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt=dt.replace(tzinfo=timezone.utc)
        return max(0.0,(datetime.now(timezone.utc)-dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None

def clamp(x,a,b):
    return max(a,min(b,x))

def normalize(v):
    v = [max(0.0,float(x)) for x in v]
    s = sum(v)
    return [x/s for x in v] if s > 0 else [UNIFORM_P]*37

def softmax(logits):
    m = max(logits)
    e = [math.exp(clamp(x-m,-40,40)) for x in logits]
    return normalize(e)

def topk(probs,k):
    return sorted(range(37), key=lambda n: probs[n], reverse=True)[:k]

def mass(probs, nums):
    return sum(probs[n] for n in set(nums))

def neighbors(n,r=2):
    i = WHEEL_INDEX[int(n)]
    return [WHEEL[(i+d)%37] for d in range(-r,r+1)]

def wheel_distance(a,b):
    ia, ib = WHEEL_INDEX[int(a)], WHEEL_INDEX[int(b)]
    d = abs(ia-ib)
    return min(d,37-d)

def color(n):
    n=int(n)
    if n==0: return "green"
    return "red" if n in RED else "black"

def dozen(n):
    n=int(n)
    return 0 if n==0 else 1+(n-1)//12

def terminal(n):
    return int(n)%10

def sector(n):
    n=int(n)
    if n in VOISINS: return "voisins"
    if n in TIERS: return "tiers"
    return "orphelins"

def brier(probs, actual):
    return sum((probs[i]-(1.0 if i==actual else 0.0))**2 for i in range(37))

def logloss(probs, actual):
    return -math.log(clamp(probs[actual],1e-12,1.0))

def pnl(coverage, actual, chip):
    cov = list(dict.fromkeys(coverage))
    total = len(cov)*chip
    return round((36*chip-total) if actual in cov else -total,2)


def url_for_page(page):
    parts = urlsplit(API_URL)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q["page"] = str(int(page))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))

def tv_distance(p, q):
    return 0.5 * sum(abs(float(a)-float(b)) for a,b in zip(p,q))

def regime_analysis(nums):
    """
    Mede mudança entre os últimos 30 giros e o bloco anterior.
    TV=0 -> distribuições parecidas; quanto maior, mais houve mudança.
    """
    if len(nums) < 60:
        return {"tv":0.0, "level":"WARMUP", "stable":False}
    recent = freq_probs(nums[-30:], 30)
    prior_slice = nums[-330:-30] if len(nums) >= 330 else nums[:-30]
    prior = freq_probs(prior_slice, max(20, len(prior_slice)))
    tv = tv_distance(recent, prior)
    if tv >= REGIME_BLOCK_TV:
        level = "MUDANÇA_FORTE"
    elif tv >= REGIME_WARN_TV:
        level = "MUDANDO"
    else:
        level = "ESTÁVEL"
    return {"tv":round(tv,4), "level":level, "stable":tv < REGIME_WARN_TV}

def independent_models(nums, ai_probs):
    """Sete visões independentes usadas para consenso."""
    pull_probs,_=puller_model(nums)
    short = normalize([
        0.45*freq_probs(nums,20)[i] + 0.55*freq_probs(nums,50)[i]
        for i in range(37)
    ])
    long = normalize([
        0.35*freq_probs(nums,100)[i] + 0.35*freq_probs(nums,300)[i] + 0.30*freq_probs(nums,1000)[i]
        for i in range(37)
    ])
    context = normalize([
        0.65*terminal_probs(nums)[i] + 0.35*sector_probs(nums)[i]
        for i in range(37)
    ])
    return {
        "transicao_simples": trans_probs(nums),
        "puxadores_bayes": pull_probs,
        "frequencia_curta": short,
        "frequencia_longa": long,
        "roda_fisica": cluster_probs(nums),
        "contexto": context,
        "ia_online": ai_probs,
    }

def consensus_for_center(center, models):
    zone = set(neighbors(center,2))
    support = 0
    details = {}
    baseline = len(zone)/37
    for name,p in models.items():
        in_top = center in topk(p,8)
        zone_lift = mass(p, zone) / baseline if baseline else 1.0
        ok = in_top or zone_lift >= 1.05
        support += int(ok)
        details[name] = {
            "support":ok,
            "zone_lift":round(zone_lift,3),
            "center_top8":in_top,
        }
    ratio = support/max(1,len(models))
    return ratio, details

# ============================================================
# SQLITE
# ============================================================

class DB:
    def __init__(self,path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.init()

    def conn(self):
        c = sqlite3.connect(self.path,timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA busy_timeout=30000")
        return c

    def init(self):
        with self.lock, self.conn() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS spins(
                round_id TEXT PRIMARY KEY,
                number INTEGER NOT NULL,
                settled_at TEXT,
                inserted_at TEXT NOT NULL,
                lucky_json TEXT,
                table_name TEXT,
                game_type TEXT
            );

            CREATE TABLE IF NOT EXISTS predictions(
                source_round_id TEXT PRIMARY KEY,
                source_number INTEGER NOT NULL,
                predicted_at TEXT NOT NULL,
                signal TEXT NOT NULL,
                quality REAL NOT NULL,
                threshold REAL NOT NULL,
                reason TEXT,
                centers_json TEXT NOT NULL,
                zones_json TEXT NOT NULL,
                coverage_json TEXT NOT NULL,
                center_details_json TEXT NOT NULL,
                probs_json TEXT NOT NULL,
                features_json TEXT NOT NULL,
                weights_json TEXT NOT NULL,
                ai_metrics_json TEXT NOT NULL,
                next_round_id TEXT,
                actual INTEGER,
                resolved_at TEXT,
                hit INTEGER,
                brier REAL,
                log_loss REAL,
                paper_pnl REAL,
                explanation_json TEXT,
                round_analysis_json TEXT
            );

            CREATE TABLE IF NOT EXISTS state(
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)
            cols={r[1] for r in c.execute("PRAGMA table_info(spins)").fetchall()}
            for name,ctype in {
                "lucky_json":"TEXT",
                "table_name":"TEXT",
                "game_type":"TEXT",
            }.items():
                if name not in cols:
                    c.execute(f"ALTER TABLE spins ADD COLUMN {name} {ctype}")
            pcols={r[1] for r in c.execute("PRAGMA table_info(predictions)").fetchall()}
            if "round_analysis_json" not in pcols:
                c.execute("ALTER TABLE predictions ADD COLUMN round_analysis_json TEXT")
            c.commit()

    def integrity(self):
        try:
            with self.lock, self.conn() as c:
                return c.execute("PRAGMA integrity_check").fetchone()[0]
        except Exception as e:
            return f"error: {e}"

    def get_state(self,key,default=None):
        with self.lock, self.conn() as c:
            r = c.execute("SELECT value_json FROM state WHERE key=?",(key,)).fetchone()
        if not r: return default
        try: return json.loads(r[0])
        except Exception: return default

    def set_state(self,key,value):
        with self.lock, self.conn() as c:
            c.execute("""
            INSERT INTO state(key,value_json,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at
            """,(key,json.dumps(value,separators=(",",":")),now_iso()))
            c.commit()

    def insert_spin(self,rec):
        with self.lock, self.conn() as c:
            cur = c.execute("""
            INSERT OR IGNORE INTO spins(
                round_id,number,settled_at,inserted_at,lucky_json,table_name,game_type
            ) VALUES(?,?,?,?,?,?,?)
            """,(
                str(rec["id"]),int(rec["number"]),rec.get("settledAt"),now_iso(),
                json.dumps(rec.get("luckyNumbers",[]),separators=(",",":")),
                rec.get("tableName"),rec.get("gameType")
            ))
            c.commit()
            return cur.rowcount>0

    def has_spin(self,rid):
        with self.lock, self.conn() as c:
            return bool(c.execute("SELECT 1 FROM spins WHERE round_id=?",(str(rid),)).fetchone())

    def spins(self,limit=5000):
        with self.lock, self.conn() as c:
            rows = c.execute("""
            SELECT round_id,number,settled_at,lucky_json,table_name,game_type
            FROM spins ORDER BY rowid DESC LIMIT ?
            """,(limit,)).fetchall()
        out=[]
        for r in reversed(rows):
            try: lucky=json.loads(r["lucky_json"] or "[]")
            except Exception: lucky=[]
            out.append({
                "id":r["round_id"],"number":r["number"],"settledAt":r["settled_at"],
                "luckyNumbers":lucky,"tableName":r["table_name"],"gameType":r["game_type"]
            })
        return out

    def count_spins(self):
        with self.lock, self.conn() as c:
            return c.execute("SELECT COUNT(*) FROM spins").fetchone()[0]

    def prediction_exists(self,rid):
        with self.lock, self.conn() as c:
            return bool(c.execute("SELECT 1 FROM predictions WHERE source_round_id=?",(str(rid),)).fetchone())

    def insert_prediction(self,row):
        with self.lock, self.conn() as c:
            cur = c.execute("""
            INSERT OR IGNORE INTO predictions(
                source_round_id,source_number,predicted_at,signal,quality,threshold,reason,
                centers_json,zones_json,coverage_json,center_details_json,
                probs_json,features_json,weights_json,ai_metrics_json,round_analysis_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,(
                str(row["source_round_id"]),int(row["source_number"]),row["predicted_at"],
                row["signal"],float(row["quality"]),float(row["threshold"]),row["reason"],
                json.dumps(row["centers"]),json.dumps(row["zones"]),json.dumps(row["coverage"]),
                json.dumps(row["center_details"]),json.dumps(row["probs"]),
                json.dumps(row["features"]),json.dumps(row["weights"]),
                json.dumps(row["ai_metrics"]),json.dumps(row.get("round_analysis",{}))
            ))
            c.commit()
            return cur.rowcount>0

    def unresolved(self,rid):
        with self.lock, self.conn() as c:
            r = c.execute("""
            SELECT * FROM predictions WHERE source_round_id=? AND resolved_at IS NULL
            """,(str(rid),)).fetchone()
        return dict(r) if r else None

    def resolve(self,source_rid,next_rid,actual,explanation):
        with self.lock, self.conn() as c:
            r = c.execute("""
            SELECT coverage_json,probs_json FROM predictions
            WHERE source_round_id=? AND resolved_at IS NULL
            """,(str(source_rid),)).fetchone()
            if not r: return False
            coverage = json.loads(r["coverage_json"])
            probs = json.loads(r["probs_json"])
            hit = int(actual in coverage)
            c.execute("""
            UPDATE predictions SET next_round_id=?,actual=?,resolved_at=?,hit=?,
            brier=?,log_loss=?,paper_pnl=?,explanation_json=?
            WHERE source_round_id=?
            """,(
                str(next_rid),int(actual),now_iso(),hit,brier(probs,actual),
                logloss(probs,actual),pnl(coverage,actual,MIN_CHIP),
                json.dumps(explanation),str(source_rid)
            ))
            c.commit()
            return True

    def history(self,limit=100):
        with self.lock, self.conn() as c:
            rows = c.execute("SELECT * FROM predictions ORDER BY rowid DESC LIMIT ?",(limit,)).fetchall()
        out=[]
        for rr in rows:
            d=dict(rr)
            for key,new,default in [
                ("centers_json","centers",[]),("zones_json","zones",{}),
                ("coverage_json","coverage",[]),("center_details_json","center_details",[]),
                ("ai_metrics_json","ai_metrics",{}),("explanation_json","explanation",[]),
                ("round_analysis_json","round_analysis",{})
            ]:
                raw=d.pop(key,None)
                try: d[new]=json.loads(raw) if raw else default
                except Exception: d[new]=default
            d.pop("probs_json",None); d.pop("features_json",None); d.pop("weights_json",None)
            d["hit"] = None if d["hit"] is None else bool(d["hit"])
            out.append(d)
        return out

    def metrics(self,limit=1000):
        with self.lock,self.conn() as c:
            rows=c.execute("""
            SELECT signal,hit,paper_pnl,coverage_json,brier,log_loss
            FROM predictions WHERE resolved_at IS NOT NULL
            ORDER BY rowid DESC LIMIT ?
            """,(limit,)).fetchall()
        if not rows:
            return {"resolved":0,"coverage_hit_pct":0,"baseline_pct":0,"green_count":0,
                    "green_hit_pct":0,"green_baseline_pct":0,"green_pnl":0,
                    "green_loss_streak":0,"avg_brier":None,"avg_log_loss":None}
        hits=sum(int(r["hit"] or 0) for r in rows)
        baselines=[len(set(json.loads(r["coverage_json"])))/37 for r in rows]
        greens=[r for r in rows if str(r["signal"]).startswith("GREEN")]
        gh=sum(int(r["hit"] or 0) for r in greens)
        gb=[len(set(json.loads(r["coverage_json"])))/37 for r in greens]
        streak=0
        for r in greens:
            if r["hit"]: break
            streak+=1
        return {
            "resolved":len(rows),
            "coverage_hit_pct":100*hits/len(rows),
            "baseline_pct":100*sum(baselines)/len(baselines),
            "green_count":len(greens),
            "green_hit_pct":100*gh/len(greens) if greens else 0,
            "green_baseline_pct":100*sum(gb)/len(gb) if gb else 0,
            "green_pnl":round(sum(float(r["paper_pnl"] or 0) for r in greens),2),
            "green_loss_streak":streak,
            "avg_brier":sum(float(r["brier"]) for r in rows)/len(rows),
            "avg_log_loss":sum(float(r["log_loss"]) for r in rows)/len(rows),
        }

    def mega_stats(self,limit=MEGA_STATS_WINDOW):
        with self.lock,self.conn() as c:
            rows=c.execute("""
            SELECT number,lucky_json,settled_at FROM spins
            ORDER BY rowid DESC LIMIT ?
            """,(int(limit),)).fetchall()

        lucky_counts=[0]*37
        mult_sum=[0.0]*37
        mult_n=[0]*37
        total_lucky=0
        max_multiplier=0
        multipliers=[]
        last_lucky=[]
        for idx,r in enumerate(rows):
            try: lucky=json.loads(r["lucky_json"] or "[]")
            except Exception: lucky=[]
            if idx==0: last_lucky=lucky
            total_lucky+=len(lucky)
            for item in lucky:
                try:
                    n=int(item.get("number"))
                    m=float(item.get("roundedMultiplier",0) or 0)
                    if 0<=n<=36:
                        lucky_counts[n]+=1
                        mult_sum[n]+=m
                        mult_n[n]+=1
                    if m>0:
                        multipliers.append(m)
                        max_multiplier=max(max_multiplier,m)
                except Exception:
                    pass

        nrows=len(rows)
        top_numbers=sorted(range(37),key=lambda n:lucky_counts[n],reverse=True)[:8]
        return {
            "rounds":nrows,
            "last_lucky":last_lucky,
            "avg_lucky_per_round":(total_lucky/nrows if nrows else 0.0),
            "avg_multiplier":(sum(multipliers)/len(multipliers) if multipliers else 0.0),
            "max_multiplier":max_multiplier,
            "top_lucky_numbers":[
                {
                    "number":n,
                    "count":lucky_counts[n],
                    "pct_rounds":100*lucky_counts[n]/nrows if nrows else 0.0,
                    "avg_multiplier":mult_sum[n]/mult_n[n] if mult_n[n] else 0.0,
                } for n in top_numbers if lucky_counts[n]>0
            ],
            "note":"Bônus Mega são exibidos separadamente e não entram no modelo preditivo principal."
        }

    def calibration(self,limit=600):
        with self.lock,self.conn() as c:
            rows=c.execute("""
            SELECT probs_json,coverage_json,hit
            FROM predictions
            WHERE resolved_at IS NOT NULL
            ORDER BY rowid DESC LIMIT ?
            """,(limit,)).fetchall()

        if not rows:
            return {"n":0,"expected_pct":0.0,"observed_pct":0.0,"gap_pp":0.0,"ece":0.0}

        bins = [[] for _ in range(5)]
        expected = []
        observed = []
        for r in rows:
            probs=json.loads(r["probs_json"])
            coverage=json.loads(r["coverage_json"])
            p=mass(probs,coverage)
            y=float(r["hit"] or 0)
            expected.append(p); observed.append(y)
            idx=min(4,int(p*5))
            bins[idx].append((p,y))

        ece=0.0
        n=len(rows)
        for b in bins:
            if not b: continue
            ep=sum(x[0] for x in b)/len(b)
            op=sum(x[1] for x in b)/len(b)
            ece += (len(b)/n)*abs(ep-op)

        exp=sum(expected)/n
        obs=sum(observed)/n
        return {
            "n":n,
            "expected_pct":100*exp,
            "observed_pct":100*obs,
            "gap_pp":100*(obs-exp),
            "ece":100*ece,
        }

    def backup(self):
        stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        target=BACKUP_DIR/f"mega_ai_{stamp}.sqlite3"
        with self.lock:
            src=self.conn(); dst=sqlite3.connect(target)
            try:
                src.backup(dst)
            finally:
                dst.close()
                src.close()

        backups=sorted(BACKUP_DIR.glob("mega_ai_*.sqlite3"), reverse=True)
        for old in backups[BACKUP_KEEP:]:
            try:
                old.unlink()
            except Exception:
                pass
        return str(target)

    def collector_state(self):
        return self.get_state("collector_state", {}) or {}

    def save_collector_state(self, value):
        self.set_state("collector_state", value)

# ============================================================
# FEATURES / MODELOS
# ============================================================

def freq_probs(nums,window):
    counts=[1.0]*37
    for n in nums[-window:]:
        counts[int(n)]+=1
    return normalize(counts)

def trans_probs(nums,window=600):
    if not nums: return [UNIFORM_P]*37
    last=int(nums[-1])
    counts=[1.0]*37
    seq=nums[-(window+1):]
    for a,b in zip(seq[:-1],seq[1:]):
        if int(a)==last: counts[int(b)]+=1
    return normalize(counts)


def global_empirical_probs(nums, window=3000):
    data=nums[-window:] if nums else []
    counts=[5.0]*37
    for n in data: counts[int(n)]+=1.0
    return normalize(counts)


def puller_model(nums, source=None):
    """Associações source -> target com Bayes, recência e contexto de 2 passos."""
    if not nums:
        return [UNIFORM_P]*37,{"source":None,"source_support":0,"pair_support":0,"reliability":0.0,"top":[]}

    source=int(nums[-1] if source is None else source)
    baseline=global_empirical_probs(nums,min(PULLER_LONG_WINDOW,max(37,len(nums))))
    alpha=PULLER_PRIOR_ALPHA

    seq=nums[-(PULLER_LONG_WINDOW+1):]
    long_counts=[0.0]*37; source_support=0
    for a,b in zip(seq[:-1],seq[1:]):
        if int(a)==source:
            long_counts[int(b)]+=1.0; source_support+=1
    long_den=source_support+37.0*alpha
    long_p=[(long_counts[t]+alpha)/long_den if long_den>0 else UNIFORM_P for t in range(37)]

    recent=nums[-(PULLER_RECENT_WINDOW+1):]
    recent_counts=[0.0]*37; recent_support=0.0
    for age,(a,b) in enumerate(reversed(list(zip(recent[:-1],recent[1:])))):
        if int(a)!=source: continue
        w=PULLER_RECENCY_DECAY**age
        recent_counts[int(b)]+=w; recent_support+=w
    recent_den=recent_support+37.0*alpha
    recent_p=[(recent_counts[t]+alpha)/recent_den if recent_den>0 else UNIFORM_P for t in range(37)]

    previous=int(nums[-2]) if len(nums)>=2 else None
    pair_counts=[0.0]*37; pair_support=0
    if previous is not None:
        pseq=nums[-(PULLER_PAIR_WINDOW+2):]
        for i in range(len(pseq)-2):
            if int(pseq[i])==previous and int(pseq[i+1])==source:
                pair_counts[int(pseq[i+2])]+=1.0; pair_support+=1
    pair_den=pair_support+37.0*alpha
    pair_p=[(pair_counts[t]+alpha)/pair_den if pair_den>0 else UNIFORM_P for t in range(37)]

    long_conf=1.0-math.exp(-source_support/12.0)
    recent_conf=1.0-math.exp(-recent_support/7.0)
    pair_conf=1.0-math.exp(-pair_support/5.0)
    reliability=clamp(0.62*long_conf+0.25*recent_conf+0.13*pair_conf,0,1)

    raw=[]; details=[]
    for t in range(37):
        base=max(baseline[t],1e-9)
        ll=long_p[t]/base; rl=recent_p[t]/base; pl=pair_p[t]/base
        evidence=(0.56*long_conf*math.log(clamp(ll,0.35,3.5))+
                  0.29*recent_conf*math.log(clamp(rl,0.35,4.0))+
                  0.15*pair_conf*math.log(clamp(pl,0.35,4.5)))
        raw.append(base*math.exp(clamp(evidence,-0.70,0.70)))
        expected=max(source_support*base,1e-6)
        z=(long_counts[t]-expected)/math.sqrt(max(expected*(1-base),0.25))
        strength=100*clamp(0.34*clamp((ll-1)/1.2,0,1)+0.24*clamp((rl-1)/1.4,0,1)+
                           0.14*clamp((pl-1)/1.6,0,1)+0.18*long_conf+0.10*clamp((z-.5)/2.5,0,1),0,1)
        details.append({"target":t,"count":int(long_counts[t]),"source_support":source_support,
                        "recent_weighted_count":round(recent_counts[t],3),"recent_support":round(recent_support,3),
                        "pair_count":int(pair_counts[t]),"pair_support":pair_support,
                        "posterior_pct":round(100*long_p[t],3),"long_lift":round(ll,3),
                        "recent_lift":round(rl,3),"pair_lift":round(pl,3),"z":round(z,3),
                        "strength":round(strength,1),"zone":neighbors(t,2)})
    probs=normalize(raw)
    for d in details: d["model_probability_pct"]=round(100*probs[d["target"]],3)
    details.sort(key=lambda d:(d["strength"],d["model_probability_pct"],d["count"]),reverse=True)
    shown=[d for d in details[:10] if d["strength"]>=PULLER_MIN_DISPLAY_STRENGTH] or details[:5]
    return probs,{"source":source,"previous":previous,"source_support":source_support,
                  "recent_support":round(recent_support,3),"pair_support":pair_support,
                  "reliability":round(reliability,4),"enough_support":source_support>=PULLER_MIN_SOURCE_SUPPORT,
                  "strong_support":source_support>=PULLER_STRONG_SOURCE_SUPPORT,"top":shown}


def strongest_puller_relations(nums, limit=12):
    if len(nums)<80: return []
    rows=[]
    for source in range(37):
        seq=list(nums)
        if not seq or seq[-1]!=source: seq=seq+[source]
        _,rep=puller_model(seq,source=source)
        if rep["source_support"]<PULLER_MIN_SOURCE_SUPPORT: continue
        for item in rep["top"][:3]:
            rows.append({"source":source,"target":item["target"],"strength":item["strength"],
                         "lift":item["long_lift"],"count":item["count"],"support":rep["source_support"]})
    rows.sort(key=lambda x:(x["strength"],x["count"],x["lift"]),reverse=True)
    return rows[:limit]


def wilson_lower(hits,n,z=1.2816):
    if n<=0: return 0.0
    p=hits/n
    den=1+(z*z/n)
    center=p+(z*z/(2*n))
    margin=z*math.sqrt((p*(1-p)+z*z/(4*n))/n)
    return max(0.0,(center-margin)/den)


def source_local_validation(nums,source=None,max_events=None):
    """Replay walk-forward apenas quando o MESMO número-fonte apareceu."""
    if not nums:
        return {"source":None,"n":0,"top1_pct":0.0,"top3_pct":0.0,"top5_pct":0.0,
                "top5_lower_pct":0.0,"avg_log_loss":None,"skill":0.0,
                "enough_data":False,"recent_top5_pct":0.0}

    source=int(nums[-1] if source is None else source)
    max_events=max_events or ROUND_LOCAL_MAX_EVENTS
    idxs=[i for i in range(45,len(nums)-1) if int(nums[i])==source][-max_events:]

    t1=t3=t5=0
    losses=[]
    recent=[]

    for i in idxs:
        before=nums[:i+1]
        actual=int(nums[i+1])
        p,rep=puller_model(before,source=source)
        if rep.get("source_support",0)<3:
            continue
        rank=topk(p,5)
        h1=int(actual in rank[:1]); h3=int(actual in rank[:3]); h5=int(actual in rank[:5])
        t1+=h1; t3+=h3; t5+=h5
        recent.append(h5)
        losses.append(logloss(p,actual))

    n=len(losses)
    if not n:
        return {"source":source,"n":0,"top1_pct":0.0,"top3_pct":0.0,"top5_pct":0.0,
                "top5_lower_pct":0.0,"avg_log_loss":None,"skill":0.0,
                "enough_data":False,"recent_top5_pct":0.0}

    top5=t5/n
    ll=sum(losses)/n
    ll_skill=clamp((UNIFORM_LOG_LOSS-ll)/0.22,0,1)
    top5_skill=clamp((top5-(5/37))/0.12,0,1)
    shrink=n/(n+18.0)
    skill=shrink*(0.58*ll_skill+0.42*top5_skill)
    rr=recent[-20:]

    return {
        "source":source,"n":n,
        "top1_pct":100*t1/n,"top3_pct":100*t3/n,"top5_pct":100*top5,
        "top5_lower_pct":100*wilson_lower(t5,n),
        "top5_baseline_pct":100*(5/37),
        "recent_top5_pct":100*(sum(rr)/len(rr)) if rr else 0.0,
        "avg_log_loss":ll,"uniform_log_loss":UNIFORM_LOG_LOSS,
        "skill":clamp(skill,0,1),
        "enough_data":n>=ROUND_LOCAL_MIN_EVENTS,
    }


def pair_context_report(nums):
    if len(nums)<2:
        return {"previous":None,"source":None,"support":0,"top":[]}
    previous=int(nums[-2]); source=int(nums[-1])
    counts=[0]*37; support=0
    seq=nums[-(PULLER_PAIR_WINDOW+2):]
    for i in range(len(seq)-2):
        if int(seq[i])==previous and int(seq[i+1])==source:
            counts[int(seq[i+2])]+=1; support+=1
    den=support+37.0
    p=[(c+1.0)/den for c in counts]
    rank=topk(p,6)
    return {"previous":previous,"source":source,"support":support,
            "top":[{"number":n,"count":counts[n],"pct":100*p[n]} for n in rank]}


def cluster_context_report(nums):
    if not nums:
        return {"source":None,"zone":[],"recent_hits":0,"recent_n":0,"recent_pct":0.0,"fair_pct":0.0}
    source=int(nums[-1]); zone=set(neighbors(source,2)); recent=nums[-30:]
    hits=sum(int(n) in zone for n in recent)
    return {"source":source,"zone":neighbors(source,2),"recent_hits":hits,
            "recent_n":len(recent),"recent_pct":100*hits/len(recent) if recent else 0.0,
            "fair_pct":100*len(zone)/37}


def cluster_probs(nums,window=220):
    counts=[0.35]*37
    for age,n in enumerate(reversed(nums[-window:])):
        w=0.995**age
        for x in neighbors(n,2): counts[x]+=w
    return normalize(counts)

def terminal_probs(nums,window=300):
    t=[1.0]*10
    for n in nums[-window:]: t[terminal(n)]+=1
    total=sum(t)
    p=[0.0]*37
    for n in range(37):
        group=[x for x in range(37) if terminal(x)==terminal(n)]
        p[n]=(t[terminal(n)]/total)/len(group)
    return normalize(p)

def sector_probs(nums,window=250):
    groups={"voisins":VOISINS,"tiers":TIERS,"orphelins":ORPHELINS}
    cnt={k:1.0 for k in groups}
    for n in nums[-window:]: cnt[sector(n)]+=1
    total=sum(cnt.values())
    p=[0.0]*37
    for n in range(37):
        s=sector(n)
        p[n]=(cnt[s]/total)/len(groups[s])
    return normalize(p)

def gap(nums,c,cap=180):
    g=0
    for n in reversed(nums[-cap:]):
        if int(n)==c: break
        g+=1
    else: g=cap
    return clamp(g/cap,0,1)

def rel(p):
    return clamp((p/UNIFORM_P-1)/2,-1,1)

def feature_matrix(nums):
    pull_probs,_pull_report=puller_model(nums)
    ps=[
        pull_probs,freq_probs(nums,20),freq_probs(nums,50),
        freq_probs(nums,100),freq_probs(nums,300),freq_probs(nums,1000),
        cluster_probs(nums),terminal_probs(nums),sector_probs(nums)
    ]
    last=int(nums[-1]) if nums else None
    rows=[]
    for c in range(37):
        if last is None:
            near=samet=samec=samed=0
        else:
            near=1-wheel_distance(last,c)/18
            samet=1 if terminal(last)==terminal(c) else 0
            samec=1 if last!=0 and c!=0 and color(last)==color(c) else 0
            samed=1 if dozen(last)!=0 and dozen(last)==dozen(c) else 0
        rows.append([
            1.0,rel(ps[0][c]),rel(ps[1][c]),rel(ps[2][c]),rel(ps[3][c]),
            rel(ps[4][c]),rel(ps[5][c]),rel(ps[6][c]),rel(ps[7][c]),
            gap(nums,c),near,samet,samec,samed,rel(ps[8][c])
        ])
    return rows

def classical_probs(nums):
    models=[
        (trans_probs(nums),.10),(freq_probs(nums,20),.10),(freq_probs(nums,50),.11),
        (freq_probs(nums,100),.13),(freq_probs(nums,300),.11),(freq_probs(nums,1000),.09),
        (cluster_probs(nums),.20),(terminal_probs(nums),.08),(sector_probs(nums),.08)
    ]
    out=[0.0]*37
    for p,w in models:
        for i in range(37): out[i]+=p[i]*w
    return normalize(out)

def window_agreement(nums):
    sets=[]
    for w in (20,50,100,300,1000):
        if len(nums)>=20: sets.append(set(topk(freq_probs(nums,min(w,len(nums))),8)))
    if len(sets)<2: return 0
    vals=[]
    for i in range(len(sets)):
        for j in range(i+1,len(sets)):
            vals.append(len(sets[i]&sets[j])/max(1,len(sets[i]|sets[j])))
    return sum(vals)/len(vals)

# ============================================================
# VALIDADOR WALK-FORWARD DOS PUXADORES
# ============================================================

class PullerValidator:
    KEY="puller_validator_v1"
    def __init__(self,db):
        self.db=db; st=db.get_state(self.KEY,{}) or {}
        self.samples=int(st.get("samples",0)); self.t1=int(st.get("t1",0)); self.t3=int(st.get("t3",0)); self.t5=int(st.get("t5",0))
        self.loss=float(st.get("loss",0.0)); self.bs=float(st.get("brier",0.0)); self.lock=threading.RLock()
        self.bootstrap_samples=int(st.get("bootstrap_samples",0))
    def save(self):
        self.db.set_state(self.KEY,{"samples":self.samples,"t1":self.t1,"t3":self.t3,"t5":self.t5,"loss":self.loss,"brier":self.bs,"bootstrap_samples":self.bootstrap_samples})
    def bootstrap(self,nums):
        """Replay walk-forward, sem futuro, apenas quando ainda não há estado do validador."""
        if self.samples>0 or len(nums)<80:
            return 0
        start=max(60,len(nums)-PULLER_BOOTSTRAP_POINTS)
        before=self.samples
        for i in range(start,len(nums)):
            self.evaluate(nums[:i],int(nums[i]))
        self.bootstrap_samples=self.samples-before
        self.save()
        return self.bootstrap_samples
    def evaluate(self,nums_before,actual):
        if len(nums_before)<30: return
        p,rep=puller_model(nums_before)
        if rep["source_support"]<2: return
        rank=topk(p,5); actual=int(actual)
        with self.lock:
            self.samples+=1; self.t1+=int(actual in rank[:1]); self.t3+=int(actual in rank[:3]); self.t5+=int(actual in rank)
            self.loss+=logloss(p,actual); self.bs+=brier(p,actual)
            if self.samples%5==0: self.save()
    def metrics(self):
        n=self.samples; ll=self.loss/n if n else None
        skill=clamp((UNIFORM_LOG_LOSS-ll)/0.22,0,1) if n>=PULLER_SKILL_MIN_SAMPLES and ll is not None else 0.0
        return {"samples":n,"ready":n>=PULLER_SKILL_MIN_SAMPLES,
                "top1_pct":100*self.t1/n if n else 0.0,"top3_pct":100*self.t3/n if n else 0.0,
                "top5_pct":100*self.t5/n if n else 0.0,"avg_log_loss":ll,"avg_brier":self.bs/n if n else None,
                "skill":skill,"uniform_log_loss":UNIFORM_LOG_LOSS,"bootstrap_samples":self.bootstrap_samples}

# ============================================================
# IA ONLINE EXPLICÁVEL
# ============================================================

class OnlineAI:
    KEY="online_ai_v1"

    def __init__(self,db):
        self.db=db
        state=db.get_state(self.KEY,{}) or {}
        w=state.get("weights")
        if not isinstance(w,list) or len(w)!=len(FEATURES):
            w=[0.0]*len(FEATURES)
            for name,val in {"transicao":.12,"freq50":.06,"freq100":.06,"cluster_roda":.10}.items():
                w[FEATURES.index(name)]=val
        self.weights=[float(x) for x in w]
        self.samples=int(state.get("samples",0))
        self.t1=int(state.get("t1",0)); self.t3=int(state.get("t3",0)); self.t5=int(state.get("t5",0))
        self.loss=float(state.get("loss",0)); self.bs=float(state.get("brier",0))
        self.lock=threading.RLock()

    def save(self):
        self.db.set_state(self.KEY,{"weights":self.weights,"samples":self.samples,
                                    "t1":self.t1,"t3":self.t3,"t5":self.t5,
                                    "loss":self.loss,"brier":self.bs})

    def probs_from_matrix(self,matrix,weights=None):
        weights=self.weights if weights is None else weights
        logits=[sum(w*x for w,x in zip(weights,row)) for row in matrix]
        return softmax(logits)

    def predict(self,nums):
        m=feature_matrix(nums)
        return self.probs_from_matrix(m),m

    def learn(self,nums_before,actual):
        if len(nums_before)<10: return
        with self.lock:
            m=feature_matrix(nums_before)
            p=self.probs_from_matrix(m)
            rank=topk(p,5)
            self.samples+=1
            self.t1+=int(actual in rank[:1]); self.t3+=int(actual in rank[:3]); self.t5+=int(actual in rank)
            self.loss+=logloss(p,actual); self.bs+=brier(p,actual)

            exp=[0.0]*len(self.weights)
            for c in range(37):
                for j,x in enumerate(m[c]): exp[j]+=p[c]*x
            target=m[actual]
            lr=AI_LR/math.sqrt(1+self.samples/250)
            for j in range(len(self.weights)):
                grad=exp[j]-target[j]+AI_L2*self.weights[j]
                self.weights[j]=clamp(self.weights[j]-lr*grad,-3,3)
            if self.samples%5==0: self.save()

    def metrics(self):
        n=self.samples
        return {"samples":n,"ready":n>=AI_MIN_SAMPLES,
                "top1_pct":100*self.t1/n if n else 0,
                "top3_pct":100*self.t3/n if n else 0,
                "top5_pct":100*self.t5/n if n else 0,
                "avg_log_loss":self.loss/n if n else None,
                "avg_brier":self.bs/n if n else None,
                "weights":{FEATURES[i]:round(w,4) for i,w in enumerate(self.weights)}}

    @staticmethod
    def explain(row,weights,topn=5):
        items=[]
        for name,x,w in zip(FEATURES,row,weights):
            if name=="bias": continue
            c=float(x)*float(w)
            items.append({"factor":name,"contribution":round(c,4),
                          "direction":"favorece" if c>0 else "reduz" if c<0 else "neutro"})
        items.sort(key=lambda z:abs(z["contribution"]),reverse=True)
        return items[:topn]

# ============================================================
# GATILHO
# ============================================================

def choose_centers(probs,nums,ai,matrix):
    agreement=window_agreement(nums)
    ai_probs=ai.probs_from_matrix(matrix)
    models=independent_models(nums,ai_probs)
    pull_probs,pull_report=puller_model(nums)

    ranked=sorted(
        range(37),
        key=lambda n:(mass(probs,neighbors(n,2)),probs[n]),
        reverse=True
    )[:22]

    info={}
    for n in ranked:
        zone=neighbors(n,2)
        zp=mass(probs,zone)
        base=len(set(zone))/37
        lift=zp/base if base else 1
        factors=OnlineAI.explain(matrix[n],ai.weights,5)
        pos=sum(max(0,f["contribution"]) for f in factors)
        consensus,consensus_details=consensus_for_center(n,models)

        pull_rank=topk(pull_probs,8)
        pull_zone_lift=mass(pull_probs,zone)/(len(set(zone))/37)
        pull_bonus=clamp(0.55*float(n in pull_rank)+0.45*clamp((pull_zone_lift-1.0)/0.35,0,1),0,1)*float(pull_report.get("reliability",0.0))
        score=(
            25*clamp((lift-.95)/.25,0,1)
            +18*clamp(probs[n]/max(probs),0,1)
            +17*clamp(agreement/.70,0,1)
            +14*clamp(pos/.5,0,1)
            +18*consensus
            +8*pull_bonus
        )
        info[n]={
            "center":n,
            "score":round(clamp(score,0,100),1),
            "zone_probability":zp,
            "zone_lift":lift,
            "ai_factors":factors,
            "model_consensus":consensus,
            "model_support":consensus_details,
            "puller_zone_lift":round(pull_zone_lift,3),
            "puller_support":pull_report.get("source_support",0),
            "puller_reliability":pull_report.get("reliability",0.0),
            "pulled_by_last":n in pull_rank,
        }

    chosen=[]; covered=set()
    while len(chosen)<3:
        best=None; obj=-1e9
        for n in ranked:
            if n in chosen: continue
            z=set(neighbors(n,2))
            overlap=len(z&covered)/5
            cur=info[n]["score"] - 26*overlap
            if cur>obj:
                obj=cur; best=n
        if best is None: break
        chosen.append(best)
        covered.update(neighbors(best,2))

    zones={str(n):neighbors(n,2) for n in chosen}
    coverage=sorted(set(x for z in zones.values() for x in z))
    details=[info[n] for n in chosen]

    overall_consensus=sum(d["model_consensus"] for d in details)/(len(details) or 1)
    return chosen,zones,coverage,details,agreement,overall_consensus

def make_signal(probs,nums,ai,matrix,metrics,calibration):
    centers,zones,coverage,details,agreement,model_consensus=choose_centers(probs,nums,ai,matrix)
    covp=mass(probs,coverage)
    covbase=len(coverage)/37
    covlift=covp/covbase if covbase else 1
    top5=topk(probs,5)
    top5lift=mass(probs,top5)/(5/37)
    entropy=-sum(p*math.log(max(p,1e-12)) for p in probs)/math.log(37)
    am=ai.metrics()
    regime=regime_analysis(nums)

    scores=[d["score"] for d in details]
    center_ok=(
        len(scores)==3
        and scores[0]>=MIN_CENTER_1
        and scores[1]>=MIN_CENTER_2
        and scores[2]>=MIN_CENTER_3
    )

    # Calibração só vira gate depois de amostra suficiente.
    calibration_ok=True
    if calibration["n"]>=CALIBRATION_MIN_N:
        calibration_ok=calibration["gap_pp"]>=-5.0 and calibration["ece"]<=12.0

    quality=(
        18*clamp((covlift-1)/.2,0,1)
        +12*clamp((top5lift-1)/.3,0,1)
        +15*clamp(agreement/.7,0,1)
        +16*model_consensus
        +12*clamp((1-entropy)/.2,0,1)
        +15*(sum(scores)/300 if scores else 0)
        +7*clamp(am["samples"]/AI_MIN_SAMPLES,0,1)
        +5*(1-clamp(regime["tv"]/REGIME_BLOCK_TV,0,1))
    )
    quality=round(clamp(quality,0,100),1)

    threshold=GREEN_SCORE+min(8,3*metrics["green_loss_streak"])
    if metrics["green_count"]>=20 and metrics["green_hit_pct"]<metrics["green_baseline_pct"]:
        threshold+=4
    if calibration["n"]>=CALIBRATION_MIN_N and calibration["gap_pp"] < -3:
        threshold+=2
    if regime["level"]=="MUDANDO":
        threshold+=2
    threshold=round(clamp(threshold,GREEN_SCORE,98),1)

    reasons=[]
    if metrics["resolved"]<GREEN_MIN_RESOLVED:
        reasons.append(f"histórico {metrics['resolved']}/{GREEN_MIN_RESOLVED}")
    if am["samples"]<AI_MIN_SAMPLES:
        reasons.append(f"IA aprendendo {am['samples']}/{AI_MIN_SAMPLES}")
    if agreement<MIN_WINDOW_AGREEMENT:
        reasons.append(f"consenso temporal baixo {agreement*100:.0f}%")
    if model_consensus<MIN_MODEL_CONSENSUS:
        reasons.append(f"modelos discordam {model_consensus*100:.0f}%")
    if covlift<1.03:
        reasons.append("cobertura pouco concentrada")
    if top5lift<1.05:
        reasons.append("TOP5 pouco concentrado")
    if not center_ok:
        reasons.append("um dos 3 centros está fraco")
    if entropy>.985:
        reasons.append("distribuição quase uniforme")
    if metrics["green_loss_streak"]>MAX_GREEN_LOSS_STREAK:
        reasons.append("bloqueio por perdas Green")
    if regime["tv"]>=REGIME_BLOCK_TV:
        reasons.append(f"regime mudou forte TV={regime['tv']:.2f}")
    if not calibration_ok:
        reasons.append("calibração recente não valida confiança")

    green=(
        metrics["resolved"]>=GREEN_MIN_RESOLVED
        and am["samples"]>=AI_MIN_SAMPLES
        and agreement>=MIN_WINDOW_AGREEMENT
        and model_consensus>=MIN_MODEL_CONSENSUS
        and covlift>=1.03
        and top5lift>=1.05
        and center_ok
        and entropy<=.985
        and metrics["green_loss_streak"]<=MAX_GREEN_LOSS_STREAK
        and regime["tv"]<REGIME_BLOCK_TV
        and calibration_ok
        and quality>=threshold
    )

    if green:
        if (
            quality>=GREEN_ELITE_SCORE
            and model_consensus>=.82
            and agreement>=.62
            and regime["tv"]<REGIME_WARN_TV
        ):
            signal="GREEN_ELITE"
        elif quality>=GREEN_PLUS_SCORE and model_consensus>=.72:
            signal="GREEN_PLUS"
        else:
            signal="GREEN"
    elif quality>=YELLOW_SCORE and agreement>=.30:
        signal="YELLOW"
    else:
        signal="RED"

    budget=BANKROLL*(RISK_PCT/100)
    need=len(coverage)*MIN_CHIP
    if signal.startswith("GREEN") and need>budget:
        signal="YELLOW"
        reasons.append(f"stake mínima R$ {need:.2f} > orçamento R$ {budget:.2f}")

    return {
        "signal":signal,
        "quality":quality,
        "threshold":threshold,
        "reason":" | ".join(reasons) if reasons else "todos os filtros de precisão atendidos",
        "centers":centers,
        "zones":zones,
        "coverage":coverage,
        "center_details":details,
        "coverage_probability":covp,
        "coverage_baseline":covbase,
        "coverage_lift":covlift,
        "window_agreement":agreement,
        "model_consensus":model_consensus,
        "entropy":entropy,
        "regime":regime,
        "calibration":calibration,
        "stake_required":round(need,2),
        "stake_budget":round(budget,2),
    }

# ============================================================
# ENGINE / COLETA
# ============================================================

class Engine:
    def __init__(self,db):
        self.db=db
        self.ai=OnlineAI(db)
        self.puller=PullerValidator(db)
        self.history=db.spins(HISTORY_LIMIT)
        if self.puller.samples==0 and len(self.history)>=80:
            try:
                n_boot=self.puller.bootstrap([int(x["number"]) for x in self.history])
                if n_boot:
                    log.info("Puxadores: replay walk-forward inicial com %s previsões",n_boot)
            except Exception:
                log.exception("Falha no bootstrap dos puxadores")
        self.lock=threading.RLock()
        self.collector_running=False
        self.last_error=""
        self.last_fetch=None
        self.last_success=None
        self.last_latency_ms=None
        self.consecutive_errors=0
        self.network_state="INICIANDO"
        self.stop_event=threading.Event()

        # V3.1 Cloud 24x7
        self.last_deep_sync_monotonic=0.0
        self.last_backfill_pages=0
        self.last_backfill_new=0
        self.collector_restarts=0
        self.booted_at=now_iso()

        persisted=self.db.collector_state()
        self.last_success=persisted.get("last_success") or self.last_success

        self.session=requests.Session()
        retry=Retry(
            total=HTTP_RETRIES,
            connect=HTTP_RETRIES,
            read=HTTP_RETRIES,
            status=HTTP_RETRIES,
            backoff_factor=0.45,
            status_forcelist=(429,500,502,503,504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter=HTTPAdapter(max_retries=retry,pool_connections=2,pool_maxsize=4)
        self.session.mount("https://",adapter)
        self.session.mount("http://",adapter)

    def nums(self): return [int(x["number"]) for x in self.history]

    def parse(self,payload):
        if not isinstance(payload,list): raise ValueError("API não retornou lista JSON")
        out=[]
        for item in payload:
            try:
                data=item["data"]
                result=data["result"]
                num=int(result["outcome"]["number"])
                if 0<=num<=36:
                    lucky=[]
                    for x in result.get("luckyNumbersList",[]) or []:
                        try:
                            lucky.append({
                                "number":int(x.get("number")),
                                "roundedMultiplier":int(x.get("roundedMultiplier",0) or 0),
                            })
                        except Exception:
                            pass
                    table=data.get("table") or {}
                    out.append({
                        "id":str(item["id"]),
                        "number":num,
                        "settledAt":data.get("settledAt"),
                        "luckyNumbers":lucky,
                        "tableName":table.get("name","Mega Roulette"),
                        "gameType":data.get("gameType","megaroulette"),
                    })
            except Exception:
                continue
        return out

    def fetch(self, deep=False):
        """
        Coleta normal: página 0.
        Deep sync: pagina para trás até reencontrar um round já salvo.
        Isso permite recuperar lacunas automaticamente após queda/restart.
        """
        recovering=self.consecutive_errors>0
        low_history=self.db.count_spins()<GREEN_MIN_RESOLVED
        deep=bool(deep or recovering or low_history)

        max_pages=MAX_BACKFILL_PAGES if deep else 1
        records={}
        started=time.monotonic()
        pages_used=0
        found_known=False

        for page in range(max_pages):
            url=url_for_page(page)
            r=self.session.get(
                url,
                timeout=(CONNECT_TIMEOUT,READ_TIMEOUT),
                headers={
                    "User-Agent":"Mozilla/5.0 MegaRouletteAI-Cloud-24x7",
                    "Accept":"application/json,text/plain,*/*",
                    "Connection":"keep-alive",
                }
            )
            r.raise_for_status()
            parsed=self.parse(r.json())
            pages_used+=1

            if not parsed:
                break

            # Se encontramos um round já salvo, chegamos à parte conhecida.
            page_has_known=False
            for rec in parsed:
                if self.db.has_spin(rec["id"]):
                    page_has_known=True
                records[rec["id"]]=rec

            if deep and page_has_known:
                found_known=True
                break

            if not deep:
                break

        items=sorted(
            records.values(),
            key=lambda x:(x.get("settledAt") or "",x["id"])
        )

        new=0
        for rec in items:
            if not self.db.has_spin(rec["id"]):
                self.process_spin(rec)
                new+=1

        self.last_latency_ms=round((time.monotonic()-started)*1000)
        self.last_fetch=now_iso()
        self.last_success=self.last_fetch
        self.last_error=""
        self.network_state="ONLINE"
        self.consecutive_errors=0
        self.last_backfill_pages=pages_used
        self.last_backfill_new=new

        if deep:
            self.last_deep_sync_monotonic=time.monotonic()

        self.db.save_collector_state({
            "last_success":self.last_success,
            "last_fetch":self.last_fetch,
            "last_round_id":self.history[-1]["id"] if self.history else None,
            "last_number":self.history[-1]["number"] if self.history else None,
            "last_backfill_pages":pages_used,
            "last_backfill_new":new,
            "deep":deep,
            "found_known_round":found_known,
            "cloud_mode":CLOUD_MODE,
        })

        if self.history:
            self.ensure_prediction()

        return new

    def process_spin(self,rec):
        if self.history:
            prev=self.history[-1]
            old=self.db.unresolved(prev["id"])
            if old:
                try:
                    fm=json.loads(old["features_json"]); weights=json.loads(old["weights_json"])
                    explanation=OnlineAI.explain(fm[int(rec["number"])],weights,6)
                    self.db.resolve(prev["id"],rec["id"],int(rec["number"]),explanation)
                except Exception:
                    log.exception("Erro ao resolver previsão")
            try:
                self.puller.evaluate(self.nums(),int(rec["number"]))
            except Exception:
                log.exception("Erro ao validar puxadores")
            try:
                self.ai.learn(self.nums(),int(rec["number"]))
            except Exception:
                log.exception("Erro no aprendizado")

        if self.db.insert_spin(rec):
            self.history.append(rec)
            if len(self.history)>HISTORY_LIMIT: self.history=self.history[-HISTORY_LIMIT:]
            self.ensure_prediction()

    def ensemble(self):
        nums=self.nums()
        classical=classical_probs(nums)

        aip,matrix=self.ai.predict(nums)
        am=self.ai.metrics()
        ai_ready=clamp(self.ai.samples/AI_MIN_SAMPLES,0,1)
        if self.ai.samples < AI_SKILL_MIN_SAMPLES or am.get("avg_log_loss") is None:
            ai_skill=0.15*ai_ready
        else:
            ai_skill=clamp((UNIFORM_LOG_LOSS-float(am["avg_log_loss"]))/0.22,0,1)

        ai_top=set(topk(aip,8)); base_top=set(topk(classical,8))
        ai_alignment=len(ai_top&base_top)/max(1,len(ai_top|base_top))
        ai_round_factor=clamp(0.45+0.80*ai_alignment,0.35,1.10)
        if ai_alignment<ROUND_AI_ALIGNMENT_MIN:
            ai_round_factor*=0.70
        ai_weight=AI_MAX_BLEND*ai_ready*ai_skill*ai_round_factor

        pp,p_report=puller_model(nums)
        pm=self.puller.metrics()
        local=source_local_validation(nums)
        reliability=float(p_report.get("reliability",0.0))

        if self.puller.samples<PULLER_SKILL_MIN_SAMPLES:
            pull_skill=0.12*clamp(self.puller.samples/max(1,PULLER_SKILL_MIN_SAMPLES),0,1)
        else:
            pull_skill=float(pm.get("skill",0.0))

        local_n=int(local.get("n",0)); local_skill=float(local.get("skill",0.0))
        if local_n<ROUND_LOCAL_MIN_EVENTS:
            local_factor=0.22*clamp(local_n/max(1,ROUND_LOCAL_MIN_EVENTS),0,1)
        else:
            local_factor=clamp(0.20+0.80*local_skill,0.15,1.0)

        pull_weight=PULLER_MAX_BLEND*reliability*pull_skill*local_factor

        special=ai_weight+pull_weight
        if special>ROUND_MAX_SPECIAL_BLEND:
            scale=ROUND_MAX_SPECIAL_BLEND/special
            ai_weight*=scale; pull_weight*=scale

        base=1-ai_weight-pull_weight
        probs=normalize([base*classical[i]+ai_weight*aip[i]+pull_weight*pp[i] for i in range(37)])

        round_meta={
            "ai_alignment":ai_alignment,
            "ai_round_factor":ai_round_factor,
            "local_puller":local,
            "pair_context":pair_context_report(nums),
            "cluster_context":cluster_context_report(nums),
        }
        return probs,matrix,ai_weight,ai_skill,pull_weight,pull_skill,p_report,round_meta

    def build_round_analysis(self,probs,sig,puller_report,round_meta,
                             ai_weight,ai_skill,puller_weight,puller_skill):
        nums=self.nums()
        source=int(nums[-1]) if nums else None
        previous=int(nums[-2]) if len(nums)>=2 else None
        local=round_meta.get("local_puller",{})
        pair=round_meta.get("pair_context",{})
        cluster=round_meta.get("cluster_context",{})
        regime=sig.get("regime",{})

        model_cons=clamp(float(sig.get("model_consensus",0.0)),0,1)
        temporal=clamp(float(sig.get("window_agreement",0.0))/0.70,0,1)
        reliability=clamp(float(puller_report.get("reliability",0.0)),0,1)
        local_skill=clamp(float(local.get("skill",0.0)),0,1)
        cov_lift=clamp((float(sig.get("coverage_lift",1.0))-1.0)/0.20,0,1)
        stability=1-clamp(float(regime.get("tv",0.0))/REGIME_BLOCK_TV,0,1)
        ai_align=clamp(float(round_meta.get("ai_alignment",0.0))/0.45,0,1)

        evidence=round(100*clamp(
            .22*model_cons+.16*temporal+.16*reliability+.15*local_skill+
            .11*cov_lift+.10*stability+.10*ai_align,0,1
        ),1)

        blockers=[]
        if int(local.get("n",0))<ROUND_LOCAL_MIN_EVENTS:
            blockers.append(f"pouco histórico local do {source}: {local.get('n',0)}/{ROUND_LOCAL_MIN_EVENTS}")
        if reliability<.35: blockers.append("puxadores do número atual com baixa confiabilidade")
        if model_cons<MIN_MODEL_CONSENSUS: blockers.append("consenso geral baixo")
        if float(sig.get("window_agreement",0.0))<MIN_WINDOW_AGREEMENT: blockers.append("janelas temporais discordando")
        if regime.get("level")=="MUDANÇA_FORTE": blockers.append("mudança forte de regime")
        if puller_weight<.01: blockers.append("puxadores sem skill validado para influenciar")

        if evidence>=ROUND_STRONG_EVIDENCE and not blockers: label="FORTE"
        elif evidence>=ROUND_GOOD_EVIDENCE and len(blockers)<=1: label="BOA"
        elif evidence>=55: label="MODERADA"
        else: label="FRACA"

        top_pullers=[]
        for item in (puller_report.get("top") or [])[:8]:
            t=int(item["target"]); zone=neighbors(t,2)
            top_pullers.append({
                **item,"zone":zone,
                "zone_probability_pct":100*mass(probs,zone),
                "in_final_centers":t in set(sig.get("centers") or []),
                "in_final_coverage":t in set(sig.get("coverage") or []),
            })

        return {
            "source_round_id":self.history[-1]["id"] if self.history else None,
            "source":source,"previous":previous,
            "evidence_score":evidence,"evidence_label":label,
            "note":"Índice de evidência estatística da rodada; não é porcentagem de chance.",
            "blockers":blockers,"top_pullers":top_pullers,
            "local_validation":local,"pair_context":pair,"cluster_context":cluster,
            "weights":{"ai_pct":100*ai_weight,"ai_skill_pct":100*ai_skill,
                       "puller_pct":100*puller_weight,"puller_global_skill_pct":100*puller_skill,
                       "ai_alignment_pct":100*float(round_meta.get("ai_alignment",0.0))},
            "final":{"signal":sig.get("signal"),"quality":sig.get("quality"),
                     "centers":sig.get("centers"),"coverage":sig.get("coverage"),
                     "coverage_lift":sig.get("coverage_lift"),
                     "model_consensus_pct":100*float(sig.get("model_consensus",0.0)),
                     "temporal_consensus_pct":100*float(sig.get("window_agreement",0.0)),
                     "regime":sig.get("regime")}
        }

    def ensure_prediction(self):
        if not self.history: return
        last=self.history[-1]
        if self.db.prediction_exists(last["id"]): return
        probs,matrix,blend,ai_skill,puller_blend,puller_skill,puller_report,round_meta=self.ensemble()
        metrics=self.db.metrics()
        calibration=self.db.calibration(600)
        sig=make_signal(probs,self.nums(),self.ai,matrix,metrics,calibration)
        round_analysis=self.build_round_analysis(
            probs,sig,puller_report,round_meta,
            blend,ai_skill,puller_blend,puller_skill
        )
        self.db.insert_prediction({
            "source_round_id":last["id"],"source_number":last["number"],"predicted_at":now_iso(),
            "signal":sig["signal"],"quality":sig["quality"],"threshold":sig["threshold"],
            "reason":sig["reason"],"centers":sig["centers"],"zones":sig["zones"],
            "coverage":sig["coverage"],"center_details":sig["center_details"],
            "probs":probs,"features":matrix,"weights":list(self.ai.weights),
            "ai_metrics":{**self.ai.metrics(),"blend_weight":blend,"skill":ai_skill,
                          "puller_blend_weight":puller_blend,"puller_skill":puller_skill,
                          "puller_source":puller_report.get("source"),
                          "puller_support":puller_report.get("source_support",0),
                          "puller_reliability":puller_report.get("reliability",0),
                          "ai_alignment":round_meta.get("ai_alignment",0)},
            "round_analysis":round_analysis
        })

    def status(self):
        if not self.history:
            return {
                "ok":False,
                "error":self.last_error or "Sem histórico",
                "network":{
                    "state":self.network_state,
                    "errors":self.consecutive_errors,
                    "last_success":self.last_success,
                }
            }

        self.ensure_prediction()
        h=self.db.history(1)
        current=dict(h[0]) if h else None

        # Nunca exibe entrada ativa se a fonte estiver velha/offline.
        age=iso_age_seconds(self.last_success)
        stale=(self.network_state!="ONLINE") or (age is not None and age>STALE_DATA_SECONDS)
        if stale and current:
            current["original_signal"]=current.get("signal")
            current["signal"]="RED"
            current["reason"]="DADOS DESATUALIZADOS/OFFLINE • "+str(current.get("reason") or "")

        ai_metrics=self.ai.metrics()
        puller_metrics=self.puller.metrics()
        _pull_probs,current_puller_report=puller_model(self.nums())
        current_ai=(current or {}).get("ai_metrics",{}) if current else {}
        return {
            "ok":True,
            "version":APP_VERSION,
            "game":{
                "type":"megaroulette",
                "table":"Mega Roulette",
                "api_filter":"isLightningNumberMatched=false",
                "filtered_dataset":True,
                "warning":"Lucky numbers/multipliers são monitorados, mas não entram na previsão principal porque a API está filtrada."
            },
            "last":self.history[-1],
            "mega":self.db.mega_stats(MEGA_STATS_WINDOW),
            "spins":self.db.count_spins(),
            "collector":self.collector_running,
            "last_fetch":self.last_fetch,
            "last_error":self.last_error,
            "network":{
                "state":self.network_state,
                "consecutive_errors":self.consecutive_errors,
                "last_success":self.last_success,
                "age_seconds":age,
                "stale":stale,
                "latency_ms":self.last_latency_ms,
            },
            "cloud":{
                "mode":CLOUD_MODE,
                "booted_at":self.booted_at,
                "collector_restarts":self.collector_restarts,
                "last_backfill_pages":self.last_backfill_pages,
                "last_backfill_new":self.last_backfill_new,
                "deep_sync_interval":DEEP_SYNC_INTERVAL,
                "max_backfill_pages":MAX_BACKFILL_PAGES,
                "data_dir":str(DATA_DIR),
                "db_file":str(DB_FILE),
            },
            "current":current,
            "metrics":self.db.metrics(),
            "rolling":{
                "50":self.db.metrics(50),
                "100":self.db.metrics(100),
                "300":self.db.metrics(300),
            },
            "calibration":self.db.calibration(600),
            "ai":{**ai_metrics,
                  "effective_blend_weight":current_ai.get("blend_weight",0),
                  "skill":current_ai.get("skill",0),
                  "uniform_log_loss":UNIFORM_LOG_LOSS},
            "pullers":{
                  "current":current_puller_report,
                  "validator":puller_metrics,
                  "effective_blend_weight":current_ai.get("puller_blend_weight",0),
                  "effective_skill":current_ai.get("puller_skill",0)},
            "round_analysis":(current or {}).get("round_analysis",{}),
            "integrity":self.db.integrity(),
        }

    def loop(self):
        self.collector_running=True
        wait_seconds=COLLECT_INTERVAL

        # Na inicialização sempre tenta preencher a lacuna desde o último round salvo.
        need_deep_sync=True

        while not self.stop_event.is_set():
            try:
                elapsed=(
                    time.monotonic()-self.last_deep_sync_monotonic
                    if self.last_deep_sync_monotonic
                    else DEEP_SYNC_INTERVAL+1
                )
                do_deep=(
                    need_deep_sync
                    or self.consecutive_errors>0
                    or elapsed>=DEEP_SYNC_INTERVAL
                )

                new=self.fetch(deep=do_deep)

                if new:
                    if do_deep:
                        log.info(
                            "Cloud sync recuperou %s novo(s) giro(s) em %s página(s)",
                            new,
                            self.last_backfill_pages,
                        )
                    else:
                        log.info("Coleta recuperou %s novo(s) giro(s)",new)

                need_deep_sync=False
                wait_seconds=COLLECT_INTERVAL

            except requests.RequestException as e:
                self.consecutive_errors+=1
                self.network_state="OFFLINE"
                self.last_error=f"{type(e).__name__}: {e}"

                # Ao voltar, deep sync será obrigatório.
                need_deep_sync=True

                wait_seconds=min(
                    BACKOFF_MAX,
                    max(
                        COLLECT_INTERVAL,
                        COLLECT_INTERVAL*(2**min(self.consecutive_errors-1,4))
                    )
                )

                self.db.save_collector_state({
                    "last_success":self.last_success,
                    "last_error":self.last_error,
                    "network_state":self.network_state,
                    "consecutive_errors":self.consecutive_errors,
                    "next_retry_seconds":wait_seconds,
                    "cloud_mode":CLOUD_MODE,
                })

                if self.consecutive_errors in (1,2,3) or self.consecutive_errors%10==0:
                    log.warning(
                        "API indisponível (%s). Tentativa %s; nova tentativa em %ss. "
                        "O banco permanece preservado.",
                        type(e).__name__,
                        self.consecutive_errors,
                        wait_seconds,
                    )

            except Exception as e:
                self.consecutive_errors+=1
                self.network_state="ERRO"
                self.last_error=f"{type(e).__name__}: {e}"
                need_deep_sync=True
                wait_seconds=min(BACKOFF_MAX,30)
                log.exception("Erro inesperado na coleta cloud")

            self.stop_event.wait(wait_seconds)

        self.collector_running=False

# ============================================================
# FLASK
# ============================================================

db=DB(DB_FILE)
engine=Engine(db)
app=Flask(__name__)

def auth_ok():
    if not PANEL_USER or not PANEL_PASSWORD: return True
    h=request.headers.get("Authorization","")
    if not h.startswith("Basic "): return False
    try:
        raw=base64.b64decode(h.split(" ",1)[1]).decode()
        u,p=raw.split(":",1)
        return u==PANEL_USER and p==PANEL_PASSWORD
    except Exception:
        return False

@app.before_request
def check_auth():
    if auth_ok(): return None
    return Response("Login necessário",401,{"WWW-Authenticate":'Basic realm="Clean AI"'})

HTML = """
<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Mega Roulette Precision AI V3.3 Round Intelligence</title>
<style>
:root{--bg:#070b11;--card:#111821;--line:#29374b;--text:#eef4fb;--muted:#92a1b5;--green:#31d684;--yellow:#f6c94c;--red:#ff6278;--cyan:#58c7ff;--purple:#bd74ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% 0,#17253a 0,#070b11 48%);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}.wrap{max-width:1140px;margin:auto;padding:14px}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:10px}.card{background:rgba(17,24,33,.95);border:1px solid var(--line);border-radius:18px;padding:14px;box-shadow:0 13px 35px rgba(0,0,0,.2)}.s12{grid-column:span 12}.s6{grid-column:span 6}.s4{grid-column:span 4}@media(max-width:760px){.s6,.s4{grid-column:span 12}}h1{margin:2px 0 3px;font-size:24px}h2{margin:0 0 10px;font-size:17px}.muted{color:var(--muted);font-size:12px}.signal{font-size:38px;font-weight:950}.RED{color:var(--red)}.YELLOW{color:var(--yellow)}.GREEN,.GREEN_PLUS,.GREEN_ELITE{color:var(--green);animation:glow 1.3s ease-in-out infinite alternate}@keyframes glow{to{text-shadow:0 0 22px rgba(49,214,132,.85)}}.badge{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:5px 9px;margin:3px;font-size:12px}.online{color:var(--green)}.offline{color:var(--red)}.purple{color:var(--purple)}.centers{display:flex;gap:8px;flex-wrap:wrap}.center{flex:1;min-width:180px;background:#0c121a;border:1px solid var(--line);border-radius:15px;padding:11px}.num{font-size:32px;font-weight:950}.bar{height:8px;background:#253043;border-radius:99px;overflow:hidden;margin:5px 0}.fill{height:100%;background:linear-gradient(90deg,#36d88b,#58c7ff)}.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.metric{background:#0c121a;border-radius:12px;padding:9px}.metric b{font-size:20px}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:8px 6px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}.scroll{max-height:470px;overflow:auto}
.euro-wrap{display:flex;justify-content:center;align-items:center;flex-wrap:wrap;gap:18px}.euro-wheel{position:relative;width:min(88vw,520px);height:min(88vw,520px);border-radius:50%;background:radial-gradient(circle at center,#0d141d 0 25%,#13202d 25% 41%,#0e1620 41% 58%,#0a1017 58% 100%);border:2px solid #34445a;box-shadow:inset 0 0 40px rgba(0,0,0,.55),0 0 28px rgba(0,0,0,.25)}.euro-wheel:after{content:'';position:absolute;inset:112px;border-radius:50%;background:#0d141c;border:1px solid rgba(255,255,255,.08)}.pocket{position:absolute;left:50%;top:50%;width:42px;height:42px;margin:-21px 0 0 -21px;border-radius:50%;display:grid;place-items:center;font-size:11px;font-weight:900;border:1px solid rgba(255,255,255,.18);color:#fff;z-index:2}.redPocket{background:#8d2630}.blackPocket{background:#20252c}.greenPocket{background:#1d8056}.pocket.cover{box-shadow:0 0 0 3px rgba(246,201,76,.9)}.pocket.centerN{background:#146a45!important;box-shadow:0 0 0 3px rgba(49,214,132,.95),0 0 22px rgba(49,214,132,.55)}.pocket.last{outline:3px solid var(--cyan)}.pocket.lucky{border:3px solid var(--purple);box-shadow:0 0 18px rgba(189,116,255,.6)}.hub{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);z-index:3;width:145px;height:145px;border-radius:50%;display:flex;align-items:center;justify-content:center;text-align:center;background:radial-gradient(circle,#1b2a38,#0d141c);border:1px solid rgba(255,255,255,.1)}.lucky-card{display:flex;gap:8px;flex-wrap:wrap}.lucky-item{background:#151020;border:1px solid #553975;border-radius:12px;padding:9px;min-width:90px;text-align:center}.lucky-item b{font-size:22px}.warning{background:#2b2410;border:1px solid #665524;border-radius:12px;padding:10px;color:#f6d979}.mini-track{display:flex;gap:4px;overflow:auto;padding:9px 2px}.wn{min-width:37px;height:37px;border-radius:50%;display:grid;place-items:center;background:#202a39;border:1px solid #34445a;font-size:12px;font-weight:800}.wn.cover{box-shadow:0 0 0 2px #f6c94c inset}.wn.centerN{background:#13613e}.wn.last{outline:2px solid #58c7ff}.wn.lucky{border:2px solid var(--purple)}
.puller-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:7px}.puller-item{background:#0c121a;border:1px solid #2b394d;border-radius:13px;padding:9px}.puller-arrow{font-size:12px;color:#93a2b7}.puller-target{font-size:26px;font-weight:950}.puller-strong{border-color:#31d684;box-shadow:0 0 14px rgba(49,214,132,.12)}@media(max-width:760px){.puller-grid{grid-template-columns:repeat(2,1fr)}}

.round-head{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.round-score{width:92px;height:92px;border-radius:50%;display:grid;place-items:center;background:#0b131c;border:2px solid #34445a;font-size:25px;font-weight:950}
.round-score.FORTE{border-color:#31d684;box-shadow:0 0 22px rgba(49,214,132,.25)}
.round-score.BOA{border-color:#58c7ff}.round-score.MODERADA{border-color:#f6c94c}.round-score.FRACA{border-color:#ff6278}
.round-dossier{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}
.round-cell,.round-dossier .metric{background:#0c121a;border:1px solid #26364b;border-radius:12px;padding:10px}
.blocker{display:inline-block;margin:3px;padding:5px 8px;border-radius:999px;background:#2a1820;color:#ff9cab;font-size:11px}
.puller-zone{font-size:11px;color:#92a1b5;margin-top:4px}
@media(max-width:760px){.round-dossier{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class="wrap">
<h1>⚡ Mega Roulette Precision AI V3.3 Round Intelligence</h1><div class="muted">Mega Roulette • roda europeia • IA com skill adaptativo • consenso • regime • calibração</div><br>
<div class="grid">
<section id="signalCard" class="card s12"><div id="sig" class="signal">CARREGANDO...</div><div id="score"></div><div id="reason" class="muted"></div><div id="net"></div></section>
<section class="card s12"><h2>🎡 Roda europeia Mega</h2><div class="euro-wrap"><div id="euroWheel" class="euro-wheel"><div class="hub"><div><b id="hubTitle">Mega Roulette</b><br><span id="hubSub" class="muted">Aguardando…</span></div></div></div><div style="min-width:250px;flex:1"><div class="badge">🟢 centro</div><div class="badge">🟡 cobertura</div><div class="badge">🔵 último</div><div class="badge purple">🟣 lucky do último giro</div><div id="coverage" class="muted" style="margin-top:10px"></div></div></div><div id="wheelLinear" class="mini-track"></div></section>
<section class="card s12"><h2>⚡ Lucky Numbers do último giro</h2><div id="luckyNow" class="lucky-card"></div><div id="filterWarning" class="warning" style="margin-top:10px"></div></section>
<section class="card s12"><h2>🎯 3 centros validados</h2><div id="centers" class="centers"></div></section>
<section class="card s12">
<h2>🔬 Análise completa da rodada</h2>
<div class="round-head"><div id="roundScore" class="round-score">--</div><div><div id="roundTitle"><b>Aguardando...</b></div><div id="roundNote" class="muted"></div></div></div>
<div id="roundDossier" class="round-dossier"></div><div id="roundBlockers" style="margin-top:9px"></div>
</section>
<section class="card s12"><h2>🧲 Números que se puxam</h2><div id="pullerTitle" class="muted">Calculando relações...</div><div id="pullers" class="puller-grid" style="margin-top:10px"></div><div id="pullerMetrics" class="muted" style="margin-top:10px"></div></section>
<section class="card s4"><h2>🧠 Precisão</h2><div id="precision" class="metrics"></div></section><section class="card s4"><h2>📈 Validação</h2><div id="validation" class="metrics"></div></section><section class="card s4"><h2>⚡ Mega Stats</h2><div id="megaStats" class="metrics"></div></section>
<section class="card s12"><h2>🔥 Lucky mais frequentes</h2><div id="luckyHeat" class="lucky-card"></div></section>
<section class="card s12"><h2>📜 Histórico</h2><div class="scroll"><table><thead><tr><th>Hora</th><th>Sinal</th><th>Centros</th><th>Saiu</th><th>Resultado</th><th>P&L</th><th>Explicação anterior</th></tr></thead><tbody id="hist"></tbody></table></div></section>
</div></div>
<script>
const W=[0,32,15,19,4,21,2,25,17,34,6,27,13,36,11,30,8,23,10,5,24,16,33,1,20,14,31,9,22,18,29,7,28,12,35,3,26];const REDS=new Set([1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]);const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));const factor=a=>(a||[]).slice(0,3).map(x=>esc(x.factor)+' '+(x.contribution>=0?'+':'')+Number(x.contribution).toFixed(2)).join(' • ')||'—';const cls=s=>String(s||'').startsWith('GREEN')?String(s):String(s||'RED');function box(n,v,sub=''){return `<div class="metric"><span class="muted">${n}</span><br><b>${v}</b><br><span class="muted">${sub}</span></div>`}function pc(n){return n===0?'greenPocket':REDS.has(n)?'redPocket':'blackPocket'}
function draw(cov,centers,last,lucky){const root=document.getElementById('euroWheel');root.querySelectorAll('.pocket').forEach(x=>x.remove());const size=root.clientWidth||520,r=size*.405;const luckySet=new Set((lucky||[]).map(x=>Number(x.number)));W.forEach((n,i)=>{const a=(-90)+(360/W.length)*i,rad=a*Math.PI/180,x=Math.cos(rad)*r,y=Math.sin(rad)*r,d=document.createElement('div');d.className=`pocket ${pc(n)} ${cov.has(n)?'cover':''} ${centers.has(n)?'centerN':''} ${Number(last)===n?'last':''} ${luckySet.has(n)?'lucky':''}`;d.style.transform=`translate(${x}px,${y}px)`;d.textContent=n;root.appendChild(d)})}
async function refresh(){const d=await (await fetch('/api/status')).json();if(!d.ok){document.getElementById('sig').textContent='SEM DADOS';return}const c=d.current||{},sig=c.signal||'RED',se=document.getElementById('sig');se.className='signal '+cls(sig);se.textContent=(sig==='GREEN_ELITE'?'🟢⚡ ':sig==='GREEN_PLUS'?'🟢🔥 ':sig==='GREEN'?'🟢 ':sig==='YELLOW'?'🟡 ':'🔴 ')+sig;document.getElementById('score').innerHTML=`<b>Qualidade ${Number(c.quality||0).toFixed(1)}/100</b> • Green exige ${Number(c.threshold||0).toFixed(1)}`;document.getElementById('reason').textContent=c.reason||'';const net=d.network||{};document.getElementById('net').innerHTML=`<span class="badge ${net.state==='ONLINE'?'online':'offline'}">● ${esc(net.state||'')}</span><span class="badge">latência ${net.latency_ms??'—'} ms</span><span class="badge">idade ${net.age_seconds==null?'—':Math.round(net.age_seconds)+'s'}</span>`;
const details=c.center_details||[];document.getElementById('centers').innerHTML=details.map(x=>`<div class="center"><div class="num">${x.center}</div><b>${Number(x.score||0).toFixed(1)}/100</b><div class="bar"><div class="fill" style="width:${Math.min(100,Number(x.score||0))}%"></div></div><span class="muted">zona ${(100*Number(x.zone_probability||0)).toFixed(2)}% • lift ${Number(x.zone_lift||0).toFixed(2)}x • consenso ${(100*Number(x.model_consensus||0)).toFixed(0)}%</span><br><span class="muted">🧲 lift ${Number(x.puller_zone_lift||0).toFixed(2)}x • suporte ${x.puller_support||0} ${x.pulled_by_last?'• puxado pelo último':''}</span><br><span class="muted">${factor(x.ai_factors)}</span></div>`).join('');
const mega=d.mega||{},lucky=mega.last_lucky||[],cov=new Set(c.coverage||[]),centers=new Set(c.centers||[]);draw(cov,centers,d.last.number,lucky);const ls=new Set(lucky.map(x=>Number(x.number)));document.getElementById('wheelLinear').innerHTML=W.map(x=>`<div class="wn ${cov.has(x)?'cover':''} ${centers.has(x)?'centerN':''} ${Number(d.last.number)===x?'last':''} ${ls.has(x)?'lucky':''}">${x}</div>`).join('');document.getElementById('coverage').textContent=`Cobertura: ${(c.coverage||[]).join(' • ')} • ${(c.coverage||[]).length} números`;document.getElementById('hubTitle').textContent=`Saiu ${d.last.number}`;document.getElementById('hubSub').textContent=`Centros ${(c.centers||[]).join(' • ')||'—'}`;
document.getElementById('luckyNow').innerHTML=lucky.length?lucky.map(x=>`<div class="lucky-item"><b>${x.number}</b><br><span class="purple">${x.roundedMultiplier}x</span></div>`).join(''):'<span class="muted">Nenhum lucky registrado neste giro.</span>';document.getElementById('filterWarning').textContent=(d.game||{}).warning||'';
const ra=d.round_analysis||{},rv=ra.local_validation||{},rw=ra.weights||{},rpc=ra.pair_context||{},rcc=ra.cluster_context||{},rf=ra.final||{};
const rs=document.getElementById('roundScore');rs.className='round-score '+(ra.evidence_label||'');rs.textContent=ra.evidence_score==null?'--':Number(ra.evidence_score).toFixed(0);
document.getElementById('roundTitle').innerHTML=`<b>Rodada após ${ra.source??'—'}</b> • evidência ${esc(ra.evidence_label||'—')}`;
document.getElementById('roundNote').textContent=ra.note||'';
document.getElementById('roundDossier').innerHTML=
box('Fonte',ra.source??'—','anterior '+(ra.previous??'—'))+
box('Puxador local',Number(rv.top5_pct||0).toFixed(1)+'% TOP5',(rv.n||0)+' ocorrências • base 13,51%')+
box('Skill local',(100*Number(rv.skill||0)).toFixed(0)+'%','recente '+Number(rv.recent_top5_pct||0).toFixed(1)+'%')+
box('Peso puxadores',Number(rw.puller_pct||0).toFixed(1)+'%','skill global '+Number(rw.puller_global_skill_pct||0).toFixed(0)+'%')+
box('IA rodada',Number(rw.ai_pct||0).toFixed(1)+'%','alinhamento '+Number(rw.ai_alignment_pct||0).toFixed(0)+'%')+
box('Contexto 2 passos',rpc.support||0,'ocorrências '+(rpc.previous??'—')+' → '+(rpc.source??'—'))+
box('Cluster físico',Number(rcc.recent_pct||0).toFixed(1)+'%','justo '+Number(rcc.fair_pct||0).toFixed(1)+'%')+
box('Consenso',Number(rf.model_consensus_pct||0).toFixed(0)+'%','temporal '+Number(rf.temporal_consensus_pct||0).toFixed(0)+'%');
document.getElementById('roundBlockers').innerHTML=(ra.blockers||[]).length?(ra.blockers||[]).map(x=>`<span class="blocker">⚠ ${esc(x)}</span>`).join(''):'<span class="badge online">✓ sem bloqueios estatísticos adicionais</span>';
const pr=d.pullers||{},prep=pr.current||{},pv=pr.validator||{},ptop=(ra.top_pullers&&ra.top_pullers.length?ra.top_pullers:(prep.top||[]));document.getElementById('pullerTitle').innerHTML=`Último <b>${prep.source??'—'}</b> → próximos associados • suporte ${prep.source_support||0} • confiabilidade ${(100*Number(prep.reliability||0)).toFixed(0)}%`;document.getElementById('pullers').innerHTML=ptop.slice(0,10).map(x=>`<div class="puller-item ${Number(x.strength||0)>=60?'puller-strong':''}"><div class="puller-arrow">${prep.source} →</div><div class="puller-target">${x.target}</div><b>${Number(x.strength||0).toFixed(0)}/100</b><br><span class="muted">lift ${Number(x.long_lift||0).toFixed(2)}x • ${x.count}/${x.source_support}</span><br><span class="muted">recente ${Number(x.recent_lift||0).toFixed(2)}x • 2-passos ${Number(x.pair_lift||0).toFixed(2)}x</span><div class="puller-zone">zona ±2: ${(x.zone||[]).join(' • ')}${x.zone_probability_pct==null?'':' • '+Number(x.zone_probability_pct).toFixed(1)+'% modelo'}</div></div>`).join('');document.getElementById('pullerMetrics').innerHTML=`Validação: ${pv.samples||0} giros • TOP5 ${Number(pv.top5_pct||0).toFixed(2)}% • log loss ${pv.avg_log_loss==null?'—':Number(pv.avg_log_loss).toFixed(3)} • skill ${(100*Number(pv.skill||0)).toFixed(0)}% • peso ${(100*Number(pr.effective_blend_weight||0)).toFixed(1)}%`;
const a=d.ai||{},m=d.metrics||{},cal=d.calibration||{},r=c.regime||{};document.getElementById('precision').innerHTML=box('Consenso',(100*Number(c.model_consensus||0)).toFixed(0)+'%','7 modelos')+box('Temporal',(100*Number(c.window_agreement||0)).toFixed(0)+'%','5 janelas')+box('Regime',esc(r.level||'—'),'TV '+Number(r.tv||0).toFixed(3))+box('Peso IA',(100*Number(a.effective_blend_weight||0)).toFixed(1)+'%','skill '+(100*Number(a.skill||0)).toFixed(0)+'%');document.getElementById('validation').innerHTML=box('IA TOP5',Number(a.top5_pct||0).toFixed(2)+'%','aleatório 13,51%')+box('Greens',m.green_count||0,Number(m.green_hit_pct||0).toFixed(1)+'%')+box('Calibração',Number(cal.ece||0).toFixed(1)+' pp','gap '+Number(cal.gap_pp||0).toFixed(1))+box('P&L','R$ '+Number(m.green_pnl||0).toFixed(2),'paper');document.getElementById('megaStats').innerHTML=box('Lucky/giro',Number(mega.avg_lucky_per_round||0).toFixed(2),'últimos '+(mega.rounds||0))+box('Média multi',Number(mega.avg_multiplier||0).toFixed(0)+'x','histórico')+box('Maior multi',Number(mega.max_multiplier||0).toFixed(0)+'x','janela')+box('Dataset','FILTRADO','match=false');document.getElementById('luckyHeat').innerHTML=(mega.top_lucky_numbers||[]).map(x=>`<div class="lucky-item"><b>${x.number}</b><br><span>${x.count}x aparições</span><br><span class="muted">média ${Number(x.avg_multiplier||0).toFixed(0)}x</span></div>`).join('');
const h=await (await fetch('/api/history?limit=80')).json();document.getElementById('hist').innerHTML=(h.items||[]).map(x=>`<tr><td>${esc(x.predicted_at)}</td><td>${esc(x.signal)}<br>${Number(x.quality||0).toFixed(1)}</td><td>${(x.centers||[]).join(' • ')}</td><td>${x.actual==null?'—':x.actual}</td><td>${x.actual==null?'⏳':x.hit?'✅ ACERTOU':'❌ ERROU'}</td><td>${x.paper_pnl==null?'—':'R$ '+Number(x.paper_pnl).toFixed(2)}</td><td>${factor(x.explanation)}</td></tr>`).join('')}
refresh().catch(console.error);setInterval(()=>refresh().catch(console.error),5000);window.addEventListener('resize',()=>refresh().catch(console.error));
</script></body></html>
"""

@app.route("/")
def home(): return Response(HTML,mimetype="text/html")

@app.route("/favicon.ico")
def favicon():
    return Response(status=204)

@app.route("/health")
def health(): return jsonify({"ok":True,"version":APP_VERSION,"collector":engine.collector_running,"integrity":db.integrity()})

@app.route("/api/status")
def status(): return jsonify(engine.status())

@app.route("/api/history")
def history():
    try: limit=max(1,min(500,int(request.args.get("limit","100"))))
    except Exception: limit=100
    return jsonify({"ok":True,"items":db.history(limit)})

@app.route("/api/ai")
def ai(): return jsonify({"ok":True,"metrics":engine.ai.metrics()})


@app.route("/api/diagnostics")
def diagnostics():
    st=engine.status()
    return jsonify({
        "ok":True,
        "version":APP_VERSION,
        "network":st.get("network"),
        "integrity":db.integrity(),
        "spins":db.count_spins(),
        "calibration":db.calibration(600),
        "mega":db.mega_stats(MEGA_STATS_WINDOW),
        "rolling":st.get("rolling"),
    })

@app.route("/api/round-analysis")
def api_round_analysis():
    st=engine.status()
    if not st.get("ok"):
        return jsonify(st),503
    return jsonify({"ok":True,"version":APP_VERSION,"last":st.get("last"),
                    "analysis":st.get("round_analysis",{}),
                    "pullers":st.get("pullers",{}),"current":st.get("current"),
                    "note":"Análise criada antes do próximo resultado; não é garantia de acerto."})


@app.route("/api/pullers")
def api_pullers():
    try:
        raw=request.args.get("source")
        source=None if raw in (None,"") else int(raw)
        if source is not None and not 0<=source<=36: raise ValueError("source deve estar entre 0 e 36")
    except Exception as exc:
        return jsonify({"ok":False,"error":str(exc)}),400
    nums=engine.nums(); probs,report=puller_model(nums,source=source)
    return jsonify({"ok":True,"report":report,"top5":topk(probs,5),"validator":engine.puller.metrics(),
                    "strongest_relations":strongest_puller_relations(nums,15),
                    "note":"Relações estatísticas históricas; não implicam causalidade física entre giros."})

@app.route("/api/cloud-status")
def cloud_status():
    st=engine.status()
    return jsonify({
        "ok":True,
        "version":APP_VERSION,
        "cloud":st.get("cloud"),
        "network":st.get("network"),
        "collector":st.get("collector"),
        "persisted_collector_state":db.collector_state(),
        "spins":db.count_spins(),
        "integrity":db.integrity(),
    })

@app.route("/api/backup",methods=["POST"])
def backup(): return jsonify({"ok":True,"backup":db.backup()})

@app.route("/api/export.csv")
def export():
    rows=db.history(5000); out=io.StringIO(); w=csv.writer(out)
    w.writerow(["predicted_at","signal","quality","centers","coverage","actual","hit","paper_pnl","reason"])
    for x in reversed(rows):
        w.writerow([x.get("predicted_at"),x.get("signal"),x.get("quality"),
                    " ".join(map(str,x.get("centers",[])))," ".join(map(str,x.get("coverage",[]))),
                    x.get("actual"),x.get("hit"),x.get("paper_pnl"),x.get("reason")])
    return Response(out.getvalue(),mimetype="text/csv",headers={"Content-Disposition":'attachment; filename="clean_ai_history.csv"'})

def collector_supervisor():
    """
    Se o loop do collector terminar por qualquer falha inesperada,
    reinicia automaticamente enquanto o processo cloud estiver vivo.
    """
    while not engine.stop_event.is_set():
        try:
            engine.loop()
        except Exception:
            engine.collector_restarts+=1
            log.exception(
                "Collector encerrou inesperadamente. Reiniciando em %ss.",
                COLLECTOR_RESTART_DELAY
            )
            engine.stop_event.wait(COLLECTOR_RESTART_DELAY)

        if not engine.stop_event.is_set():
            engine.collector_restarts+=1
            engine.stop_event.wait(COLLECTOR_RESTART_DELAY)


def backup_loop():
    # Primeiro backup alguns minutos após subir, depois periodicamente.
    if engine.stop_event.wait(180):
        return

    while not engine.stop_event.is_set():
        try:
            path=db.backup()
            log.info("Backup automático criado: %s",path)
        except Exception:
            log.exception("Falha no backup automático")

        if engine.stop_event.wait(BACKUP_INTERVAL):
            break


if RUN_COLLECTOR:
    threading.Thread(
        target=collector_supervisor,
        daemon=True,
        name="collector-supervisor"
    ).start()

threading.Thread(
    target=backup_loop,
    daemon=True,
    name="backup-loop"
).start()

def shutdown():
    try:
        engine.stop_event.set()
    except Exception:
        pass
    try:
        engine.ai.save()
    except Exception:
        pass
    try:
        engine.puller.save()
    except Exception:
        pass
    try:
        db.backup()
    except Exception:
        pass

atexit.register(shutdown)

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False,threaded=True)
