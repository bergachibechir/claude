from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
import pyodbc
import subprocess
import sys
import os
import re
import json
import shutil
import io
import csv
import stat
import tempfile
import unicodedata
from pathlib import Path
from datetime import datetime
import threading
import uuid

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

app = Flask(__name__)
app.secret_key = "pfe_etl_secret_2025"

BASE_DIR = Path(__file__).resolve().parent
ENTITIES_DIR = BASE_DIR / "entities"
ENTITIES_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)
OUTPUT_RULES_DIR = BASE_DIR / "output_regle_de_gestion"
OUTPUT_RULES_DIR.mkdir(exist_ok=True)
OUTPUT_ADVANCED_DIR = BASE_DIR / "output_des_entites_avance"
OUTPUT_ADVANCED_DIR.mkdir(exist_ok=True)

jobs = {}

TARGET_ANALYSIS_TABLES = [
    "ECRITURE", "ANALYTIQ", "TIERS", "JOURNAL", "SECTION", "CSECTION",
    "CSOUSPLAN", "ARTICLE", "PIECE", "LIGNE", "GENERAUX", "RIB",
    "MODEPAIE", "MODEREGL","NATCPTE","BANQUECP","ADRESSES",
]

CUSTOM_FIELD_KEYWORDS = ("TABLE", "LIBRE")

FILTERABLE_COLUMNS = [
    "Compte de tiers", "Compte", "Pays", "Conditions de règlement *",
    "Famille de clients", "Nature du client", "ZoneTaxe", "Famille d ecriture",
    "Etablissement", "Devise", "Lettrage", "Statut",
]

DEFAULT_TRANSFORMATION_OPTIONS = {
    "apply_custom_fields": True,
    "apply_management_rules": False,
}

MULTI_CSV_ENTITIES = {
    "facture_client": [
        "reglement_acompte_client_solde",
        "facture_avoir_client_solde",
    ],
    "facture_fournisseur": [
        "facture_avoir_fournisseur_solde",
        "reglement_acompte_fournisseur_solde",
    ],
    "article": [
        "article_flex_stockes",
        "article_flex_non_stockes",
    ],
}


# =============================================================================
# MODE AVANCE — Consolidation multi-bases (entites eligibles + dedoublonnage)
# =============================================================================
# La consolidation ne concerne PAS toutes les entites du dossier entities/,
# uniquement celles listees ci-dessous (whitelist).
# =============================================================================

CONSOLIDATION_ENTITIES = [
    "ADR_Client", "ADR_Fournisseur", "Analytique", "Contact_Client",
    "Contact_Fournisseur", "Entite_Societe", "Fiche_Client",
    "Fiche_Collectifs_Divers", "Fiche_Compte_Tresorie", "Fiche_Fournisseur",
    "Fiche_Salarie", "Guide_de_saisie", "IMMOBILISATION", "Journal_Comptable",
    "Plan_Comptable", "RIB_Par_Typologie",
    "Section_analytique_Concat_sousPlan",
]

# Cles de dedoublonnage par entite : colonnes du fichier final (CSV/XLSX) qui,
# combinees, identifient une ligne de facon unique.
# A COMPLETER / VALIDER AVEC LE METIER — tant que la liste est vide pour une
# entite, celle-ci est consolidee SANS dedoublonnage (toutes les lignes de
# toutes les bases sont conservees telles quelles).
ENTITY_DEDUP_KEYS: dict = {
    "ADR_Client": [],
    "ADR_Fournisseur": [],
    "Analytique": [],
    "Contact_Client": [],
    "Contact_Fournisseur": [],
    "Entite_Societe": [],
    "Fiche_Client": [],
    "Fiche_Collectifs_Divers": [],
    "Fiche_Compte_Tresorie": [],
    "Fiche_Fournisseur": [],
    "Fiche_Salarie": [],
    "Guide_de_saisie": [],
    "IMMOBILISATION": [],
    "Journal_Comptable": [],
    "Plan_Comptable": [],
    "RIB_Par_Typologie": [],
    "Section_analytique_Concat_sousPlan": [],
}

# Nom du sous-dossier de consolidation cree dans output_<projet>/.
CONSOLIDATION_DIRNAME = "_CONSOLIDE"


# Normalise un identifiant d'entite (accents, casse, separateurs) pour le comparer a la whitelist.
def _normalize_entity_key(name: str) -> str:
    txt = unicodedata.normalize("NFKD", name or "")
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = re.sub(r"[^a-zA-Z0-9]+", "_", txt).strip("_")
    return txt.upper()


_CONSOLIDATION_ENTITY_KEYS = {_normalize_entity_key(e): e for e in CONSOLIDATION_ENTITIES}


# Indique si une entite (identifiee par son id de dossier) fait partie des entites a consolider.
def is_consolidation_entity(entity_id: str) -> bool:
    return _normalize_entity_key(entity_id) in _CONSOLIDATION_ENTITY_KEYS


# Renvoie les colonnes de detection des doublons configurees pour une entite (liste vide = desactive).
def get_dedup_keys_for_entity(entity_id: str) -> list:
    canon = _CONSOLIDATION_ENTITY_KEYS.get(_normalize_entity_key(entity_id))
    if canon is None:
        return []
    return list(ENTITY_DEDUP_KEYS.get(canon, []))


# =============================================================================
# Utilitaire — Input plusieurs base de donnees
# =============================================================================

# Transforme une saisie libre de bases (texte/liste) en liste propre, dédoublonnée et en majuscules.
def parse_databases_input(raw) -> list:
    result = []
    items = [raw] if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
    for item in items:
        parts = re.split(r"[,\s]+", str(item).strip())
        result.extend(p.strip().upper() for p in parts if p.strip())
    seen = set()
    return [x for x in result if not (x in seen or seen.add(x))]


# =============================================================================
# SFTP — Configuration & utilitaires
# =============================================================================

# Lit la configuration SFTP (hôte, port, identifiants) depuis les variables d'environnement (.env).
def get_sftp_config() -> dict:
    host = os.environ.get("SFTP_HOST", "").strip()
    port_raw = os.environ.get("SFTP_PORT", "22").strip()
    username = os.environ.get("SFTP_USERNAME", "").strip()
    password = os.environ.get("SFTP_PASSWORD", "").strip()
    try:
        port = int(port_raw)
    except (ValueError, TypeError):
        port = 22
    missing = [k for k, v in [("SFTP_HOST", host), ("SFTP_USERNAME", username), ("SFTP_PASSWORD", password)] if not v]
    if missing:
        raise EnvironmentError(f"Variables SFTP manquantes dans le .env : {', '.join(missing)}")
    return {"host": host, "port": port, "username": username, "password": password}


# Ouvre une connexion SSH/SFTP vers le serveur distant avec gestion des erreurs d'authentification.
def create_sftp_client(host, port, username, password, timeout=20):
    if not PARAMIKO_AVAILABLE:
        raise ImportError("Paramiko n'est pas installé. Lancez : pip install paramiko")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname=host, port=port, username=username, password=password,
            timeout=timeout, banner_timeout=timeout, auth_timeout=timeout,
            look_for_keys=False, allow_agent=False)
    except paramiko.AuthenticationException:
        raise ConnectionError(f"Authentification SFTP refusée pour '{username}' sur {host}:{port}.")
    except paramiko.SSHException as e:
        raise ConnectionError(f"Erreur SSH lors de la connexion à {host}:{port} -> {e}")
    except OSError as e:
        raise ConnectionError(f"Impossible de joindre le serveur SFTP {host}:{port} -> {e}")
    return ssh, ssh.open_sftp()


# Raccourci : crée un client SFTP directement à partir de la configuration du .env.
def open_sftp_from_env(timeout=20):
    cfg = get_sftp_config()
    return create_sftp_client(**cfg, timeout=timeout)


# Teste si un chemin distant existe déjà sur le serveur SFTP.
def sftp_path_exists(sftp, remote_path: str) -> bool:
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False


# Crée récursivement l'arborescence d'un dossier distant sur le serveur SFTP si elle n'existe pas.
def ensure_remote_dir(sftp, remote_path: str, logs: list) -> None:
    parts = [p for p in remote_path.replace("\\", "/").split("/") if p]
    current = ""
    for part in parts:
        current = ("/" + part) if not current else (current + "/" + part)
        if not sftp_path_exists(sftp, current):
            try:
                sftp.mkdir(current)
                logs.append(f"[SFTP] Dossier créé   : {current}")
            except OSError as e:
                if not sftp_path_exists(sftp, current):
                    raise OSError(f"Impossible de créer {current} : {e}") from e
        else:
            logs.append(f"[SFTP] Dossier existant: {current}")


# Envoie un fichier local vers le serveur SFTP, avec option d'écrasement et journalisation.
def sftp_upload_file(sftp, local_path: Path, remote_path: str, logs: list, overwrite=True) -> bool:
    local_path = Path(local_path)
    if not local_path.exists():
        logs.append(f"[SFTP][ERREUR] Fichier local introuvable : {local_path}")
        return False
    if not overwrite and sftp_path_exists(sftp, remote_path):
        logs.append(f"[SFTP][SKIP] Fichier déjà présent : {remote_path}")
        return True
    try:
        sftp.put(str(local_path), remote_path)
        size_kb = round(local_path.stat().st_size / 1024, 1)
        logs.append(f"[SFTP][OK] {local_path.name} -> {remote_path} ({size_kb} Ko)")
        return True
    except Exception as e:
        logs.append(f"[SFTP][ERREUR] Upload échoué pour {local_path.name} : {e}")
        return False

# Découpe un CSV en deux fichiers (valide / non valide) selon la colonne 'Statut Validation' et génère un rapport de logs.
def _split_valide_non_valide(source_csv: Path, tmp_dir: Path, entity_id: str, logs: list) -> dict:
    entity_clean = entity_id.replace(" ", "_")
    valide_path     = tmp_dir / f"{entity_clean}_valide.csv"
    non_valide_path = tmp_dir / f"{entity_clean}_Non_valide.csv"
    logs_path       = tmp_dir / f"{entity_clean}_logs.csv"

    if not source_csv.exists():
        raise FileNotFoundError(f"CSV source introuvable : {source_csv}")

    with open(source_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows    = list(reader)
        headers = list(reader.fieldnames or [])

    if not headers:
        raise ValueError("Le CSV source ne contient pas d'en-têtes.")

    statut_col = next((h for h in headers if h.strip().upper() == "STATUT VALIDATION"), None)
    motif_col  = next((h for h in headers if h.strip().upper() == "MOTIF REJET"),       None)

    if statut_col is None:
        logs.append(f"[SPLIT][INFO] Colonne 'Statut Validation' absente -> upload fichier brut")
        shutil.copy2(source_csv, valide_path)
        logs.append(f"[SPLIT] {entity_clean}_valide.csv : {len(rows)} lignes (brut, sans split)")
        with open(non_valide_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers, delimiter=";", extrasaction="ignore")
            w.writeheader()
        logs.append(f"[SPLIT] {entity_clean}_Non_valide.csv : 0 lignes (aucun rejet)")
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_rows = [
            {"Rubrique": "=== RAPPORT DE TRAITEMENT ===", "Valeur": ""},
            {"Rubrique": "Entité",          "Valeur": entity_id},
            {"Rubrique": "Fichier source",  "Valeur": source_csv.name},
            {"Rubrique": "Généré le",       "Valeur": generated_at},
            {"Rubrique": "",                "Valeur": ""},
            {"Rubrique": "=== INFO ===",    "Valeur": ""},
            {"Rubrique": "Statut",          "Valeur": "Pas de colonne Statut Validation - fichier uploade tel quel"},
            {"Rubrique": "Total lignes",    "Valeur": str(len(rows))},
            {"Rubrique": "Lignes valides",  "Valeur": str(len(rows))},
            {"Rubrique": "Lignes rejetees", "Valeur": "0"},
        ]
        with open(logs_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Rubrique", "Valeur"], delimiter=";")
            w.writeheader()
            w.writerows(log_rows)
        logs.append(f"[LOGS]  {entity_clean}_logs.csv : rapport généré")
        return {
            "total": len(rows), "ok_count": len(rows), "ko_count": 0,
            "taux_rejet": 0.0, "motif_counter": {},
            "valide_path": valide_path, "non_valide_path": non_valide_path, "logs_path": logs_path,
            "valide_name": valide_path.name, "non_valide_name": non_valide_path.name, "logs_name": logs_path.name,
        }

    ok_rows = [r for r in rows if (r.get(statut_col) or "").strip().upper() != "KO"]
    ko_rows = [r for r in rows if (r.get(statut_col) or "").strip().upper() == "KO"]
    total    = len(rows)
    ok_count = len(ok_rows)
    ko_count = len(ko_rows)

    with open(valide_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter=";", extrasaction="ignore")
        w.writeheader()
        w.writerows(ok_rows)
    logs.append(f"[SPLIT] {entity_clean}_valide.csv     : {ok_count} lignes OK")
    with open(non_valide_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, delimiter=";", extrasaction="ignore")
        w.writeheader()
        w.writerows(ko_rows)
    logs.append(f"[SPLIT] {entity_clean}_Non_valide.csv : {ko_count} lignes KO")

    motif_counter: dict = {}
    if motif_col:
        for row in ko_rows:
            motifs_raw = str(row.get(motif_col) or "").strip()
            if not motifs_raw:
                motifs_raw = "Motif non renseigné"
            for m in motifs_raw.split("|"):
                m = m.strip()
                if m:
                    motif_counter[m] = motif_counter.get(m, 0) + 1

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    taux_rejet   = round((ko_count / total) * 100, 2) if total else 0.0
    taux_valide  = round((ok_count / total) * 100, 2) if total else 0.0

    log_rows = []
    log_rows.append({"Rubrique": "=== RAPPORT DE TRAITEMENT ===", "Valeur": ""})
    log_rows.append({"Rubrique": "Entité",            "Valeur": entity_id})
    log_rows.append({"Rubrique": "Fichier source",    "Valeur": source_csv.name})
    log_rows.append({"Rubrique": "Généré le",         "Valeur": generated_at})
    log_rows.append({"Rubrique": "",                  "Valeur": ""})
    log_rows.append({"Rubrique": "=== STATISTIQUES ===", "Valeur": ""})
    log_rows.append({"Rubrique": "Total lignes",          "Valeur": str(total)})
    log_rows.append({"Rubrique": "Lignes valides (OK)",   "Valeur": f"{ok_count} ({taux_valide} %)"})
    log_rows.append({"Rubrique": "Lignes rejetées (KO)",  "Valeur": f"{ko_count} ({taux_rejet} %)"})
    log_rows.append({"Rubrique": "",                      "Valeur": ""})
    if motif_counter:
        log_rows.append({"Rubrique": "=== DÉTAIL DES MOTIFS DE REJET ===", "Valeur": ""})
        log_rows.append({"Rubrique": "Motif", "Valeur": "Nombre de lignes concernées"})
        for motif, count in sorted(motif_counter.items(), key=lambda x: -x[1]):
            pct = round((count / ko_count) * 100, 1) if ko_count else 0
            log_rows.append({"Rubrique": motif, "Valeur": f"{count} ({pct} % des rejets)"})
    else:
        log_rows.append({"Rubrique": "=== MOTIFS DE REJET ===", "Valeur": ""})
        log_rows.append({"Rubrique": "Info", "Valeur": "Colonne 'Motif Rejet' absente ou vide"})
    log_rows.append({"Rubrique": "", "Valeur": ""})
    log_rows.append({"Rubrique": "=== FICHIERS GÉNÉRÉS ===", "Valeur": ""})
    log_rows.append({"Rubrique": f"{entity_clean}_valide.csv",     "Valeur": f"{ok_count} lignes valides"})
    log_rows.append({"Rubrique": f"{entity_clean}_Non_valide.csv", "Valeur": f"{ko_count} lignes rejetées"})
    log_rows.append({"Rubrique": f"{entity_clean}_logs.csv",       "Valeur": "Ce fichier de synthèse"})

    with open(logs_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Rubrique", "Valeur"], delimiter=";")
        w.writeheader()
        w.writerows(log_rows)
    logs.append(f"[LOGS]  {entity_clean}_logs.csv        : rapport généré")
    return {
        "total": total, "ok_count": ok_count, "ko_count": ko_count,
        "taux_rejet": taux_rejet, "motif_counter": motif_counter,
        "valide_path": valide_path, "non_valide_path": non_valide_path, "logs_path": logs_path,
        "valide_name": valide_path.name, "non_valide_name": non_valide_path.name, "logs_name": logs_path.name,
    }


# Supprime un fichier distant sur le SFTP s'il existe déjà (avant un nouvel upload).
def _sftp_delete_if_exists(sftp, remote_path: str, logs: list) -> None:
    try:
        sftp.stat(remote_path)
        sftp.remove(remote_path)
        logs.append(f"[SFTP] Ancien fichier supprimé : {remote_path}")
    except FileNotFoundError:
        pass
    except Exception as e:
        logs.append(f"[SFTP][WARN] Impossible de supprimer {remote_path} : {e}")


# Retire le suffixe horodaté (_AAAAMMJJ_HHMMSS) d'un nom de fichier pour le comparer à un motif.
def _strip_timestamp(stem: str) -> str:
    return re.sub(r"_\d{8}_\d{6}$", "", stem).lower()


# Filtre une liste de fichiers pour ne garder que ceux correspondant à des motifs donnés (entités multi-CSV).
def _filter_multi(files: list, patterns: list) -> list:
    matched = []
    for pattern in patterns:
        for f in files:
            if _strip_timestamp(f.stem) == pattern.lower() and f not in matched:
                matched.append(f)
    return matched


# Localise les CSV source d'une entité à envoyer en SFTP (priorité : règles > output > transformed).
def find_sftp_source_csv_for_entity(db_name: str, entity_id: str) -> tuple:
    entity_key = entity_id.strip().lower()
    patterns   = MULTI_CSV_ENTITIES.get(entity_key)

    # Sous-fonction : liste tous les fichiers CSV d'un dossier de façon récursive.
    def _all_csvs(folder: Path) -> list:
        if not folder.exists():
            return []
        return sorted([p for p in folder.rglob("*.csv") if p.is_file()])

    # Sous-fonction : sélectionne le(s) bon(s) CSV (multi-fichiers par motif, ou le plus récent).
    def _pick(files: list):
        if not files:
            return []
        if patterns:
            return _filter_multi(files, patterns)
        return [max(files, key=lambda p: p.stat().st_mtime)]

    files = _all_csvs(OUTPUT_RULES_DIR / db_name / entity_id)
    result = _pick(files)
    if result:
        return result, "regles_gestion"
    files = _all_csvs(OUTPUT_DIR / db_name / entity_id)
    result = _pick(files)
    if result:
        return result, "output"
    entity = get_entity(entity_id)
    if entity:
        files = _all_csvs(Path(entity["transformed_dir"]))
        result = _pick(files)
        if result:
            return result, "transformed"
    return [], "aucun"


# Orchestre l'upload SFTP complet d'une entité : split valide/non-valide, création du dossier distant, envoi et résumé.
def upload_entity_to_sftp(project_name, entity_id, source_csvs, sftp_base_dir="", extra_files=None, logs=None, tmp_dir=None):
    if logs is None:
        logs = []
    if tmp_dir is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix="sftp_tmp_"))
    project_clean = project_name.strip().lower().replace(" ", "_")
    entity_clean  = entity_id.replace(" ", "_")
    if sftp_base_dir:
        remote_dir = f"{sftp_base_dir.rstrip('/')}/{project_clean}/{entity_clean}"
    else:
        remote_dir = f"/{project_clean}/{entity_clean}"

    logs.append(f"[SFTP] {'=' * 50}")
    logs.append(f"[SFTP] Entité      : {entity_id}")
    logs.append(f"[SFTP] Projet      : {project_clean}")
    logs.append(f"[SFTP] Dossier     : {remote_dir}")
    logs.append(f"[SFTP] Fichiers    : {len(source_csvs)} CSV(s) à traiter")
    logs.append(f"[SFTP] {'=' * 50}")

    uploaded, failed = [], []
    all_split_stats  = []

    try:
        ssh, sftp = open_sftp_from_env()
        try:
            ensure_remote_dir(sftp, remote_dir, logs)
        finally:
            try: sftp.close()
            except Exception: pass
            try: ssh.close()
            except Exception: pass
    except Exception as e:
        logs.append(f"[SFTP][ERREUR] Impossible de créer le dossier distant : {e}")
        return {"success": False, "error": str(e), "project": project_clean, "entity": entity_id,
                "remote_dir": remote_dir, "uploaded": [], "failed": [], "logs": logs}

    files_to_upload = []
    for source_csv in source_csvs:
        if not source_csv.exists():
            logs.append(f"[WARN] CSV introuvable : {source_csv}")
            continue
        logs.append(f"[INFO] Traitement : {source_csv.name}")
        prefix = source_csv.stem
        try:
            split_stats = _split_valide_non_valide(source_csv, tmp_dir, prefix, logs)
            all_split_stats.append(split_stats)
            files_to_upload += [
                (split_stats["valide_path"],     f"{remote_dir}/{split_stats['valide_name']}"),
                (split_stats["non_valide_path"], f"{remote_dir}/{split_stats['non_valide_name']}"),
                (split_stats["logs_path"],        f"{remote_dir}/{split_stats['logs_name']}"),
            ]
        except Exception as e:
            logs.append(f"[ERREUR] Split échoué pour {source_csv.name} : {e}")
            failed.append(str(e))

    for ef in (extra_files or []):
        ef = Path(ef)
        if ef.exists():
            files_to_upload.append((ef, f"{remote_dir}/{ef.name}"))
        else:
            logs.append(f"[WARN] Fichier extra introuvable : {ef}")

    for local_f, remote_f in files_to_upload:
        try:
            ssh, sftp = open_sftp_from_env()
            try:
                _sftp_delete_if_exists(sftp, remote_f, logs)
                ok = sftp_upload_file(sftp, local_f, remote_f, logs)
            finally:
                try: sftp.close()
                except Exception: pass
                try: ssh.close()
                except Exception: pass
        except Exception as e:
            logs.append(f"[SFTP][ERREUR] Connexion impossible pour {Path(local_f).name} : {e}")
            ok = False
        (uploaded if ok else failed).append(remote_f)

    success = bool(uploaded) and not failed
    agg_total = sum(s.get("total",    0) for s in all_split_stats)
    agg_ok    = sum(s.get("ok_count", 0) for s in all_split_stats)
    agg_ko    = sum(s.get("ko_count", 0) for s in all_split_stats)
    agg_taux  = round((agg_ko / agg_total) * 100, 2) if agg_total else 0.0

    logs.append(f"[SFTP] {'─' * 50}")
    logs.append(f"[SFTP] RÉSUMÉ UPLOAD")
    logs.append(f"[SFTP] Dossier distant  : {remote_dir}")
    logs.append(f"[SFTP] Fichiers envoyés : {len(uploaded)}")
    logs.append(f"[SFTP] Lignes totales   : {agg_total}")
    logs.append(f"[SFTP] Lignes valides   : {agg_ok}")
    logs.append(f"[SFTP] Lignes rejetées  : {agg_ko} ({agg_taux} %)")
    if failed:
        logs.append(f"[SFTP] Échecs           : {len(failed)}")
    logs.append(f"[SFTP] {'─' * 50}")

    return {"success": success, "project": project_clean, "entity": entity_id,
            "remote_dir": remote_dir, "uploaded": uploaded, "failed": failed,
            "split_stats": {"total": agg_total, "ok_count": agg_ok, "ko_count": agg_ko, "taux_rejet": agg_taux},
            "logs": logs}


# Teste la connexion au serveur SFTP et retourne le résultat (succès/échec) avec les logs.
def test_sftp_connection() -> dict:
    logs = []
    try:
        cfg = get_sftp_config()
        logs.append(f"[SFTP] Test connexion -> {cfg['host']}:{cfg['port']} (user: {cfg['username']})")
        ssh, sftp = create_sftp_client(**cfg)
        root_items = sftp.listdir("/")
        sftp.close()
        ssh.close()
        logs.append(f"[SFTP] Connexion réussie. Racine : {root_items}")
        return {"success": True, "host": cfg["host"], "logs": logs}
    except Exception as e:
        logs.append(f"[SFTP][ERREUR] {e}")
        return {"success": False, "error": str(e), "logs": logs}


# =============================================================================
# Fonctions utilitaires
# =============================================================================

# Met un nom de table en majuscules et sans espaces superflus pour comparaison.
def normalize_table_name(name: str) -> str:
    return (name or "").strip().upper()


# Détermine si une colonne est un champ personnalisé (contient TABLE ou LIBRE).
def is_custom_field(column_name: str) -> bool:
    upper = (column_name or "").upper()
    return any(k in upper for k in CUSTOM_FIELD_KEYWORDS)


# Échappe un identifiant SQL (table/colonne) entre crochets pour SQL Server.
def quote_ident(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


# Parcourt le dossier entities/ et détecte pour chaque entité ses scripts d'extraction et de transformation.
def scan_entities():
    entities = []
    for entity_dir in sorted(ENTITIES_DIR.iterdir()):
        if not entity_dir.is_dir():
            continue
        all_py_files = sorted(entity_dir.glob("*.py"))
        if not all_py_files:
            continue
        ignored_stems = {
            "__init__", "transform_patch",
            "script1_scan_fields", "script1_scan_fields_windows_safe",
        }
        py_files = [f for f in all_py_files if f.stem.lower() not in ignored_stems]
        extraction = None
        transformation = None
        for f in py_files:
            low = f.stem.lower()
            if low.startswith(("export_", "extraction_", "extract_")):
                extraction = f
                break
        for f in py_files:
            if f.stem.lower().startswith("trai"):
                transformation = f
                break
        if extraction is None:
            for f in py_files:
                low = f.stem.lower()
                if "export" in low or "extract" in low or "extraction" in low:
                    extraction = f
                    break
        if extraction is None and py_files:
            extraction = py_files[0]
        exports_dir = entity_dir / "exports"
        transformed_dir = entity_dir / "transformed"
        exports_dir.mkdir(exist_ok=True)
        transformed_dir.mkdir(exist_ok=True)
        entities.append({
            "id": entity_dir.name,
            "label": entity_dir.name.replace("_", " ").title(),
            "dir": str(entity_dir),
            "extraction": extraction.name if extraction else None,
            "transformation": transformation.name if transformation else None,
            "exports_dir": str(exports_dir),
            "transformed_dir": str(transformed_dir),
        })
    return entities


# Retrouve la configuration d'une entité précise à partir de son identifiant.
def get_entity(entity_id):
    for e in scan_entities():
        if e["id"] == entity_id:
            return e
    return None


# Convertit une valeur quelconque (texte, nombre...) en vrai booléen, avec valeur par défaut.
def normalize_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    txt = str(value).strip().lower()
    if txt in {"1", "true", "oui", "yes", "on"}:
        return True
    if txt in {"0", "false", "non", "no", "off"}:
        return False
    return default


# Normalise les options de transformation (champs perso, règles de gestion) en booléens fiables.
def normalize_transformation_options(options=None):
    options = options or {}
    return {
        "apply_custom_fields": normalize_bool(
            options.get("apply_custom_fields"),
            DEFAULT_TRANSFORMATION_OPTIONS["apply_custom_fields"],
        ),
        "apply_management_rules": normalize_bool(
            options.get("apply_management_rules"),
            DEFAULT_TRANSFORMATION_OPTIONS["apply_management_rules"],
        ),
    }


# Renvoie le chemin du fichier JSON des options de transformation d'une entité.
def get_transformation_options_path(entity_id: str) -> Path:
    entity = get_entity(entity_id)
    if not entity:
        raise ValueError("Entite introuvable")
    return Path(entity["dir"]) / "transformation_options.json"


# Charge les options de transformation d'une entité depuis son fichier JSON (ou valeurs par défaut).
def load_transformation_options(entity_id: str):
    path = get_transformation_options_path(entity_id)
    if not path.exists():
        return {"generated_at": "", "entity": entity_id, **DEFAULT_TRANSFORMATION_OPTIONS}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {"generated_at": raw.get("generated_at", ""), "entity": entity_id,
            **normalize_transformation_options(raw)}


# Enregistre les options de transformation d'une entité dans son fichier JSON.
def save_transformation_options(entity_id: str, payload: dict):
    path = get_transformation_options_path(entity_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


# Renvoie le CSV le plus récent d'un dossier (par date de modification).
def find_latest_csv_in_folder(folder: Path) -> Path | None:
    if not folder.exists():
        return None
    csv_files = [p for p in folder.rglob("*.csv") if p.is_file()]
    if not csv_files:
        return None
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


# Renvoie le dernier CSV transformé d'une entité.
def find_latest_transformed_csv_for_entity(entity_id: str) -> Path | None:
    entity = get_entity(entity_id)
    if not entity:
        return None
    return find_latest_csv_in_folder(Path(entity["transformed_dir"]))


# Renvoie le dernier CSV présent dans output/ pour une base et une entité données.
def find_latest_output_csv_for_entity(db_name: str, entity_id: str) -> Path | None:
    entity_output_dir = OUTPUT_DIR / db_name / entity_id
    if not entity_output_dir.exists():
        return None
    csv_files = [p for p in entity_output_dir.rglob("*.csv") if p.is_file()]
    if not csv_files:
        return None
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


# Renvoie le CSV final le plus récent d'une entité (entre output/ et transformed/).
def find_latest_source_csv_for_entity(db_name: str, entity_id: str) -> Path | None:
    output_csv = find_latest_output_csv_for_entity(db_name, entity_id)
    transformed_csv = find_latest_transformed_csv_for_entity(entity_id)
    if output_csv and transformed_csv:
        return output_csv if output_csv.stat().st_mtime >= transformed_csv.stat().st_mtime else transformed_csv
    return output_csv or transformed_csv


# Renvoie le dernier CSV généré par les règles de gestion pour une entité.
def find_latest_rules_csv_for_entity(db_name: str, entity_id: str) -> Path | None:
    rules_dir = OUTPUT_RULES_DIR / db_name / entity_id
    if not rules_dir.exists():
        return None
    csv_files = [p for p in rules_dir.rglob("*.csv") if p.is_file()]
    if not csv_files:
        return None
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


# Copie les CSV/XLSX générés vers le dossier output/ (ou un dossier projet) de la base et de l'entité.
def copy_generated_csvs_to_output(db_name: str, entity_id: str, generated_files: list[Path], dest_base: Path = None):
    # dest_base permet au pipeline avance de copier dans output_PROJET/
    # au lieu de OUTPUT_DIR (output/) qui est le dossier standard
    if dest_base is None:
        dest_base = OUTPUT_DIR
    dest_dir = dest_base / db_name / entity_id
    reset_output_entity_dir(dest_dir)
    copied = []
    for csv_file in generated_files:
        dest_file = dest_dir / csv_file.name
        counter = 1
        while dest_file.exists():
            dest_file = dest_dir / f"{csv_file.stem}_{counter}{csv_file.suffix}"
            counter += 1
        shutil.copy2(csv_file, dest_file)
        copied.append(dest_file)
    return copied


# Neutralise temporairement les champs perso pendant une transformation si l'option est désactivée, puis restaure.
def prepare_runtime_files_for_transformation(entity_dir: Path, entity_id: str, runtime_options: dict, logs: list):
    runtime_options = normalize_transformation_options(runtime_options)
    if runtime_options["apply_custom_fields"]:
        return lambda: None

    logs.append("[INFO] Champs personnalises desactives pour cette execution.")
    fields_path   = entity_dir / "fields_selection.json"
    fields_backup = entity_dir / "fields_selection.json.dashboard_backup"
    patch_path    = entity_dir / "transform_patch.py"
    patch_backup  = entity_dir / "transform_patch.py.dashboard_backup"

    for p in (fields_backup, patch_backup):
        if p.exists():
            p.unlink()
    if fields_path.exists():
        fields_path.replace(fields_backup)
    if patch_path.exists():
        patch_path.replace(patch_backup)

    with open(fields_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(), "entity": entity_id,
            "disabled_by_dashboard": True, "total_selected": 0, "fields": [],
        }, f, ensure_ascii=False, indent=2)

    with open(patch_path, "w", encoding="utf-8") as f:
        f.write('\n'.join([
            '"""transform_patch.py temporairement neutralise par le dashboard."""',
            '', 'TABLE_FIELDS_MAPPING = {}', '',
            'def extract_table_fields(row: dict) -> dict:', '    return {}', '',
        ]))

    # Sous-fonction : restaure les fichiers de champs perso d'origine après la transformation.
    def cleanup():
        for p in (fields_path, patch_path):
            try:
                if p.exists(): p.unlink()
            except Exception:
                pass
        if fields_backup.exists():
            fields_backup.replace(fields_path)
        if patch_backup.exists():
            patch_backup.replace(patch_path)

    return cleanup


# Applique les règles de gestion enregistrées sur le dernier CSV final d'une entité et écrit le résultat.
def execute_management_rules_for_entity(db_name: str, entity_id: str):
    rules_config = load_rules_config(entity_id)
    if not rules_config.get("rules"):
        raise ValueError("Aucune regle de gestion enregistree pour cette entite.")
    source_csv = find_latest_source_csv_for_entity(db_name, entity_id)
    if source_csv is None:
        raise ValueError("Aucun fichier CSV final trouve pour cette entite.")
    target_dir = OUTPUT_RULES_DIR / db_name / entity_id
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = target_dir / f"{source_csv.stem}_REGLES_GESTION_{timestamp}{source_csv.suffix}"
    stats = apply_management_rules_to_csv(source_csv, output_csv, rules_config)
    return {
        "entity": entity_id, "source_file": source_csv.name,
        "output_file": output_csv.name,
        "output_path": str(output_csv.relative_to(BASE_DIR)),
        "rows_total": stats["rows_total"], "rows_matched": stats["rows_matched"],
        "updated_cells": stats["updated_cells"], "validation": stats.get("validation", {}),
    }


# Exécute un script (extraction/transformation) en arrière-plan, suit les logs et copie les fichiers générés.
def run_script_job(job_id, script_path, entity_dir, cfg, script_type=None, entity_id=None, runtime_options=None):
    env = os.environ.copy()
    env["DB_SERVER"]   = cfg["server"]
    env["DB_NAME"]     = cfg["database"]
    env["DB_USER"]     = cfg["username"]
    env["DB_PASSWORD"] = cfg["password"]
    runtime_options = normalize_transformation_options(runtime_options)
    env["TS_INCLUDE_CUSTOM_FIELDS"]  = "1" if runtime_options["apply_custom_fields"]    else "0"
    env["TS_APPLY_MANAGEMENT_RULES"] = "1" if runtime_options["apply_management_rules"] else "0"

    transformed_dir    = entity_dir / "transformed"
    transformed_before = snapshot_csv_files(transformed_dir) if script_type == "transformation" else {}
    cleanup_runtime_files = lambda: None

    try:
        if script_type == "transformation" and entity_id:
            jobs[job_id]["logs"].append(
                f"[INFO] Options transformation -> champs perso: {'oui' if runtime_options['apply_custom_fields'] else 'non'} | "
                f"regles auto: {'oui' if runtime_options['apply_management_rules'] else 'non'}"
            )
            cleanup_runtime_files = prepare_runtime_files_for_transformation(
                entity_dir, entity_id, runtime_options, jobs[job_id]["logs"],
            )
        proc = subprocess.Popen(
            [sys.executable, str(script_path), cfg["database"]],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            env=env, cwd=str(entity_dir),
        )
        if proc.stdout is not None:
            for line in proc.stdout:
                jobs[job_id]["logs"].append(line.rstrip())
        proc.wait()
        success = proc.returncode == 0
        jobs[job_id]["returncode"] = proc.returncode

        if success and script_type == "transformation" and entity_id:
            generated_files = diff_generated_csv_files(transformed_before, transformed_dir)
            jobs[job_id]["generated_files"] = [f.name for f in generated_files]
            copied_files = []
            if generated_files:
                copied_files = copy_generated_csvs_to_output(cfg["database"], entity_id, generated_files)
                jobs[job_id]["logs"].append(
                    f"[INFO] {len(copied_files)} fichier(s) copie(s) vers output/{cfg['database']}/{entity_id}/"
                )
            else:
                jobs[job_id]["logs"].append("[WARN] Aucun nouveau fichier detecte dans transformed.")
            jobs[job_id]["output_files"] = [f.name for f in copied_files]

            if runtime_options["apply_management_rules"]:
                try:
                    rules_result = execute_management_rules_for_entity(cfg["database"], entity_id)
                    jobs[job_id]["management_rules_output"] = rules_result
                    jobs[job_id]["logs"].append(
                        f"[INFO] Regles appliquees -> {rules_result['output_file']} | "
                        f"lignes: {rules_result['rows_total']} | matchees: {rules_result['rows_matched']} | "
                        f"cellules maj: {rules_result['updated_cells']}"
                    )
                except Exception as exc:
                    jobs[job_id]["logs"].append(f"[WARN] Application automatique des regles impossible: {exc}")

        jobs[job_id]["status"] = "done" if success else "error"
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["logs"].append(f"ERREUR: {exc}")
    finally:
        try:
            cleanup_runtime_files()
        except Exception as exc:
            jobs[job_id]["logs"].append(f"[WARN] Restauration des fichiers champs perso impossible: {exc}")


# Exécute un script Python de façon synchrone (bloquante) en capturant sa sortie dans les logs.
def run_script_sync(script_path, entity_dir, cfg, logs):
    env = os.environ.copy()
    env["DB_SERVER"]   = cfg["server"]
    env["DB_NAME"]     = cfg["database"]
    env["DB_USER"]     = cfg["username"]
    env["DB_PASSWORD"] = cfg["password"]
    try:
        proc = subprocess.Popen(
            [sys.executable, str(script_path), cfg["database"]],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            env=env, cwd=str(entity_dir),
        )
        if proc.stdout is not None:
            for line in proc.stdout:
                logs.append(line.rstrip())
        proc.wait()
        return proc.returncode == 0
    except Exception as exc:
        logs.append(f"ERREUR: {exc}")
        return False


# =============================================================================
# CORRECTION : snapshot_csv_files et diff_generated_csv_files
# Support .xlsx en plus de .csv
# =============================================================================

# Prend un instantané (taille + date) des CSV et XLSX d'un dossier, pour détecter les fichiers nouveaux ensuite.
def snapshot_csv_files(folder: Path):
    """Prend un instantane des fichiers CSV et XLSX dans un dossier."""
    snapshot = {}
    if not folder.exists():
        return snapshot
    for pat in ("*.csv", "*.xlsx"):
        for f in folder.rglob(pat):
            try:
                s = f.stat()
                snapshot[str(f.resolve())] = {"mtime_ns": s.st_mtime_ns, "size": s.st_size}
            except OSError:
                continue
    return snapshot


# Compare à un instantané pour retourner la liste des CSV/XLSX nouvellement créés ou modifiés.
def diff_generated_csv_files(before_snapshot, folder: Path):
    """Retourne la liste des fichiers CSV et XLSX nouveaux ou modifies."""
    generated = []
    if not folder.exists():
        return generated
    all_files = []
    for pat in ("*.csv", "*.xlsx"):
        all_files.extend(folder.rglob(pat))
    for f in all_files:
        try:
            s = f.stat()
            key = str(f.resolve())
            current_sig = (s.st_mtime_ns, s.st_size)
            previous = before_snapshot.get(key)
            if previous is None:
                generated.append(f)
                continue
            if current_sig != (previous.get("mtime_ns"), previous.get("size")):
                generated.append(f)
        except OSError:
            continue
    generated.sort(key=lambda p: str(p).lower())
    return generated


# Vide et recrée le dossier de sortie d'une entité avant d'y copier de nouveaux fichiers.
def reset_output_entity_dir(dest_dir: Path):
    if dest_dir.exists():
        shutil.rmtree(dest_dir, ignore_errors=True)
    dest_dir.mkdir(parents=True, exist_ok=True)

# Exécute le pipeline complet (extraction + transformation) sur toutes les entités d'une base, en mode simple.
def run_full_pipeline(job_id, cfg):
    db_name  = cfg["database"]
    entities = scan_entities()
    total    = len(entities)
    job      = jobs[job_id]
    job["progress"] = {"done": 0, "total": total, "current": ""}
    db_output = OUTPUT_DIR / db_name
    db_output.mkdir(parents=True, exist_ok=True)
    job["run_started_at"] = datetime.now().isoformat()
    results = []

    for i, entity in enumerate(entities, 1):
        eid             = entity["id"]
        edir            = Path(entity["dir"])
        transformed_dir = Path(entity["transformed_dir"])
        dest_dir        = db_output / eid

        job["progress"]["current"] = eid
        job["logs"].append(f"\n{'=' * 60}")
        job["logs"].append(f"[{i}/{total}]  Entite : {eid}")
        job["logs"].append(f"{'=' * 60}")

        entity_ok          = True
        transformed_before = snapshot_csv_files(transformed_dir)

        if entity["extraction"]:
            script = edir / entity["extraction"]
            job["logs"].append(f"  > Extraction : {entity['extraction']}")
            if not run_script_sync(script, edir, cfg, job["logs"]):
                job["logs"].append(f"  X Extraction echouee pour {eid}")
                entity_ok = False
        else:
            job["logs"].append(f"  ! Pas de script d'extraction pour {eid}")

        if entity_ok and entity["transformation"]:
            script = edir / entity["transformation"]
            job["logs"].append(f"  > Transformation : {entity['transformation']}")
            if not run_script_sync(script, edir, cfg, job["logs"]):
                job["logs"].append(f"  X Transformation echouee pour {eid}")
                entity_ok = False
        elif entity_ok:
            job["logs"].append(f"  ! Pas de script de transformation pour {eid}")

        generated_files = []
        nb_copied = 0

        if entity_ok and transformed_dir.exists():
            generated_files = diff_generated_csv_files(transformed_before, transformed_dir)
            reset_output_entity_dir(dest_dir)
            for csv_file in generated_files:
                dest_file = dest_dir / csv_file.name
                counter = 1
                while dest_file.exists():
                    dest_file = dest_dir / f"{csv_file.stem}_{counter}{csv_file.suffix}"
                    counter += 1
                shutil.copy2(csv_file, dest_file)
                nb_copied += 1
            job["logs"].append(
                f"  OK {nb_copied} fichier(s) copie(s) -> output/{db_name}/{eid}/"
                if nb_copied > 0 else f"  ! Aucun nouveau fichier genere pour {eid}"
            )
        elif entity_ok:
            reset_output_entity_dir(dest_dir)
            job["logs"].append(f"  ! Aucun dossier transformed pour {eid}")

        results.append({
            "entity": eid, "label": entity["label"],
            "success": entity_ok, "files_copied": nb_copied,
            "generated_files": [f.name for f in generated_files],
        })
        job["progress"]["done"] = i

    ok_count   = sum(1 for r in results if r["success"])
    fail_count = total - ok_count
    total_files= sum(r["files_copied"] for r in results)

    job["logs"].append(f"\n{'=' * 60}")
    job["logs"].append(f"TERMINE -- {ok_count}/{total} entites OK | {fail_count} erreur(s)")
    job["logs"].append(f"Fichiers : {total_files} -> output/{db_name}/")
    job["logs"].append(f"{'=' * 60}")

    job["status"]    = "done" if fail_count == 0 else "error"
    job["results"]   = results
    job["output_dir"]= f"output/{db_name}"


# =============================================================================
# BDD DE RÉFÉRENCE — Helpers NT_NATURE / NT_LIBELLE
# =============================================================================

# Construit un dictionnaire NT_NATURE -> NT_LIBELLE à partir d'un CSV de la base de référence.
def build_libelle_lookup_from_csv(csv_path: Path, logs: list) -> dict:
    lookup: dict = {}
    if not csv_path or not csv_path.exists():
        logs.append(f"  [REF][WARN] CSV introuvable : {csv_path}")
        return lookup
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader  = csv.DictReader(f, delimiter=";")
            headers = list(reader.fieldnames or [])
            nature_col  = next((h for h in headers if h.strip().upper() == "NT_NATURE"),  None)
            libelle_col = next((h for h in headers if h.strip().upper() == "NT_LIBELLE"), None)
            if not nature_col or not libelle_col:
                logs.append(f"  [REF][INFO] Colonnes NT_NATURE/NT_LIBELLE absentes dans {csv_path.name} — ignoré")
                logs.append(f"  [REF][INFO] En-têtes trouvées : {headers[:15]}")
                return lookup
            for row in reader:
                code    = str(row.get(nature_col)  or "").strip()
                libelle = str(row.get(libelle_col) or "").strip()
                if code:
                    lookup[code] = libelle
        logs.append(f"  [REF] ✓ Lookup chargé — {len(lookup)} code(s) NT_NATURE depuis {csv_path.name}")
    except Exception as exc:
        logs.append(f"  [REF][WARN] Erreur lecture {csv_path.name} : {exc}")
    return lookup


# Récupère les noms des champs perso retenus dans fields_selection.json pour une entité.
def get_custom_field_names_from_selection(entity_id: str) -> list:
    entity = get_entity(entity_id)
    if not entity:
        return []
    selection_path = Path(entity["dir"]) / "fields_selection.json"
    if not selection_path.exists():
        return []
    try:
        with open(selection_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("disabled_by_dashboard"):
            return []
        fields = data.get("fields", [])
        names = []
        for fld in fields:
            name = (fld.get("final_name") or fld.get("column_name") or "").strip()
            if name:
                names.append(name)
        return names
    except Exception:
        return []


# Ajoute des colonnes de libellés (NT_LIBELLE) à un CSV en s'appuyant sur le lookup de la base de référence.
def enrich_csv_with_libelles(in_path, libelle_lookup, out_path, custom_field_names, logs):
    if not in_path.exists() or not libelle_lookup:
        return 0
    if not custom_field_names:
        logs.append(f"  [REF][INFO] fields_selection.json vide ou absent pour {in_path.name} — enrichissement ignoré")
        return 0
    try:
        with open(in_path, "r", encoding="utf-8-sig", newline="") as f:
            reader  = csv.DictReader(f, delimiter=";")
            rows    = list(reader)
            headers = list(reader.fieldnames or [])
        if not headers:
            return 0
        custom_cols_present = [h for h in headers if h in custom_field_names]
        if not custom_cols_present:
            logs.append(f"  [REF][INFO] Aucune colonne champ perso trouvée dans {in_path.name}")
            return 0
        logs.append(f"  [REF] Champs perso détectés dans {in_path.name} : {custom_cols_present}")
        new_headers: list   = []
        libelle_pairs: list = []
        for h in headers:
            new_headers.append(h)
            if h in custom_cols_present:
                lb_col = f"{h}_NT_LIBELLE"
                new_headers.append(lb_col)
                libelle_pairs.append((h, lb_col))
        if not libelle_pairs:
            return 0
        enriched        = []
        found_in_lookup = 0
        not_found_codes = set()
        for row in rows:
            new_row = dict(row)
            for src_col, lb_col in libelle_pairs:
                code    = str(row.get(src_col) or "").strip()
                libelle = libelle_lookup.get(code, "")
                new_row[lb_col] = libelle
                if libelle:
                    found_in_lookup += 1
                elif code:
                    not_found_codes.add(code)
            enriched.append(new_row)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=new_headers, delimiter=";", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(enriched)
        logs.append(f"  [REF] ✓ {out_path.name} — {len(libelle_pairs)} col. NT_LIBELLE insérée(s) | {found_in_lookup} valeur(s) résolue(s)")
        if not_found_codes:
            logs.append(f"  [REF][INFO] Codes sans correspondance (exemples) : {list(not_found_codes)[:5]}")
        return len(libelle_pairs)
    except Exception as exc:
        logs.append(f"  [REF][WARN] Enrichissement NT_LIBELLE échoué ({in_path.name}) : {exc}")
        return 0


# =============================================================================
# MODE AVANCE — Consolidation multi-bases : lecture/ecriture + dedoublonnage
# =============================================================================

# Detecte le separateur (';' ou ',') d'un CSV a partir de sa premiere ligne.
def _detect_csv_sep(path: Path) -> str:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        first = f.readline()
    return ";" if first.count(";") >= first.count(",") else ","


# Lit un CSV ou XLSX en DataFrame 100% texte (aucune conversion numerique/date) pour preserver les valeurs exactes.
def read_table_preserving_text(path: Path):
    import pandas as pd
    if path.suffix.lower() == ".xlsx":
        df = pd.read_excel(path, dtype=str, engine="openpyxl", keep_default_na=False)
        sep = None
    else:
        sep = _detect_csv_sep(path)
        df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df, sep


# Ecrit un DataFrame en CSV (';' utf-8-sig) ou en XLSX avec le format Texte ('@') force sur toutes les colonnes,
# pour que chaque champ garde sa valeur exacte (codes avec zeros non significatifs, IBAN/RIB, numeros longs...).
def write_table_preserving_text(df, path: Path, as_xlsx: bool, csv_sep: str = ";"):
    path.parent.mkdir(parents=True, exist_ok=True)
    if as_xlsx:
        import pandas as pd
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Consolide")
            ws = writer.sheets["Consolide"]
            for col_cells in ws.iter_cols(min_row=2):
                for cell in col_cells:
                    cell.number_format = "@"
    else:
        df.to_csv(path, sep=csv_sep or ";", index=False, encoding="utf-8-sig")


# Detecte les doublons d'un DataFrame consolide selon la cle de l'entite (comparaison normalisee) et les separe.
def dedupe_dataframe(df, dedup_keys: list):
    missing = [k for k in dedup_keys if k not in df.columns]
    if missing:
        return df, df.iloc[0:0].copy(), missing
    norm_cols = []
    for k in dedup_keys:
        norm_col = f"__norm__{k}"
        df[norm_col] = df[k].fillna("").astype(str).str.strip().str.upper()
        norm_cols.append(norm_col)
    is_dup = df.duplicated(subset=norm_cols, keep="first")
    kept    = df[~is_dup].drop(columns=norm_cols).reset_index(drop=True)
    removed = df[is_dup].drop(columns=norm_cols).reset_index(drop=True)
    return kept, removed, []


# Consolide, pour une entite eligible, les fichiers finaux de toutes les bases en un seul fichier + dedoublonnage.
def consolidate_entity_across_bases(entity_id: str, files_by_db: dict, out_root: Path, logs: list) -> dict:
    import pandas as pd
    entity_clean = entity_id.replace(" ", "_")
    dest_dir = out_root / CONSOLIDATION_DIRNAME / entity_clean
    dest_dir.mkdir(parents=True, exist_ok=True)

    frames    = []
    any_xlsx  = False
    csv_sep   = ";"
    for db_name, files in files_by_db.items():
        for f in files:
            try:
                df, sep = read_table_preserving_text(f)
            except Exception as exc:
                logs.append(f"  [CONSOLIDATION][WARN] Lecture impossible {f.name} ({db_name}) : {exc}")
                continue
            if f.suffix.lower() == ".xlsx":
                any_xlsx = True
            elif sep:
                csv_sep = sep
            df["_Base_Origine"]    = db_name
            df["_Fichier_Origine"] = f.name
            frames.append(df)

    if not frames:
        logs.append(f"  [CONSOLIDATION][WARN] {entity_id} : aucun fichier source lisible, consolidation ignoree.")
        return {"entity": entity_id, "status": "skip"}

    consolidated = pd.concat(frames, ignore_index=True, sort=False)
    total_rows   = len(consolidated)

    dedup_keys = get_dedup_keys_for_entity(entity_id)
    if dedup_keys:
        kept, removed, missing_cols = dedupe_dataframe(consolidated, dedup_keys)
        if missing_cols:
            logs.append(
                f"  [CONSOLIDATION][WARN] {entity_id} : colonne(s) cle absente(s) {missing_cols} "
                f"— dedoublonnage ignore pour cette entite."
            )
            kept, removed = consolidated, consolidated.iloc[0:0]
        else:
            logs.append(
                f"  [CONSOLIDATION] {entity_id} : {len(removed)} doublon(s) detecte(s) sur cle {dedup_keys} "
                f"({total_rows} lignes -> {len(kept)} lignes conservees)"
            )
    else:
        kept, removed = consolidated, consolidated.iloc[0:0]
        logs.append(
            f"  [CONSOLIDATION][INFO] {entity_id} : aucune cle de dedoublonnage configuree "
            f"(ENTITY_DEDUP_KEYS) — fichier consolide sans dedoublonnage."
        )

    out_as_xlsx = any_xlsx
    out_file = dest_dir / f"{entity_clean}_CONSOLIDE.{'xlsx' if out_as_xlsx else 'csv'}"
    write_table_preserving_text(kept, out_file, as_xlsx=out_as_xlsx, csv_sep=csv_sep)
    logs.append(f"  [CONSOLIDATION] [+] {out_file.relative_to(BASE_DIR)}")

    if len(removed):
        dup_file = dest_dir / f"{entity_clean}_DOUBLONS_SUPPRIMES.{'xlsx' if out_as_xlsx else 'csv'}"
        write_table_preserving_text(removed, dup_file, as_xlsx=out_as_xlsx, csv_sep=csv_sep)
        logs.append(f"  [CONSOLIDATION] [+] {dup_file.relative_to(BASE_DIR)}")

    return {
        "entity": entity_id, "status": "ok",
        "bases": list(files_by_db.keys()), "total_rows": total_rows,
        "kept_rows": len(kept), "removed_duplicates": len(removed),
        "dedup_keys": dedup_keys, "output_file": str(out_file.relative_to(BASE_DIR)),
        "output_format": "xlsx" if out_as_xlsx else "csv",
    }


# =============================================================================
# MODE AVANCE — Pipeline multi-bases  *** CORRIGÉ ***
# =============================================================================

# Exécute le pipeline avancé multi-bases avec base de référence, enrichissement des libellés et règles de gestion.
def run_advanced_pipeline(job_id: str, cfg: dict, databases: list,
                          entity_configs: list, project_name: str,
                          reference_db: str = ""):
    job       = jobs[job_id]
    safe_proj = re.sub(r"[^a-zA-Z0-9_\-]", "_", project_name.strip()) or "avance"
    out_dir   = BASE_DIR / f"output_{safe_proj}"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    ref_upper = (reference_db or "").strip().upper()
    ordered_dbs: list = []
    if ref_upper:
        ordered_dbs.append(ref_upper)
    for db in databases:
        db_clean = db.strip().upper()
        if db_clean and db_clean != ref_upper:
            ordered_dbs.append(db.strip())

    libelle_lookup: dict = {}
    total_steps = len(ordered_dbs) * len(entity_configs)
    done_steps  = 0

    job["progress"]   = {"done": 0, "total": total_steps, "current": ""}
    job["output_dir"] = f"output_{safe_proj}"
    job["results"]    = []
    job["consolidation"] = []

    # Fichiers finaux par entite eligible / par base, collectes au fil du pipeline
    # pour la consolidation multi-bases effectuee en fin de traitement.
    consolidation_sources: dict = {}

    for db_idx, db_name in enumerate(ordered_dbs):
        is_ref  = bool(ref_upper) and (db_name.strip().upper() == ref_upper)
        db_cfg  = {**cfg, "database": db_name}
        db_output = out_dir / db_name
        db_output.mkdir(parents=True, exist_ok=True)

        ref_tag = "  ★ BASE DE RÉFÉRENCE" if is_ref else ""
        job["logs"].append("")
        job["logs"].append("=" * 60)
        job["logs"].append(f"BASE [{db_idx + 1}/{len(ordered_dbs)}]  =>  {db_name}{ref_tag}")
        job["logs"].append("=" * 60)

        ref_transf_csv_paths: list = []

        for e_conf in entity_configs:
            entity_id = e_conf.get("id", "").strip()
            entity    = get_entity(entity_id)

            job["progress"]["current"] = f"{db_name} / {entity_id}"
            job["logs"].append("")
            job["logs"].append(f"  [{done_steps + 1}/{total_steps}]  Entite : {entity_id}")
            job["logs"].append(f"  {'-' * 44}")

            if not entity:
                job["logs"].append(f"  [SKIP] Entite introuvable : {entity_id}")
                done_steps += 1
                job["progress"]["done"] = done_steps
                job["results"].append({"db": db_name, "entity": entity_id, "status": "skip", "is_reference": is_ref})
                continue

            edir            = Path(entity["dir"])
            exports_dir     = Path(entity["exports_dir"])
            transformed_dir = Path(entity["transformed_dir"])

            entity_base = db_output / entity_id
            run_folder  = entity_base / run_ts
            transf_out  = run_folder / "transformation"
            transf_out.mkdir(parents=True, exist_ok=True)

            runtime_options = normalize_transformation_options({
                "apply_custom_fields":    e_conf.get("apply_custom_fields",    True),
                "apply_management_rules": e_conf.get("apply_management_rules", False),
            })

            entity_ok       = True
            new_transformed = []

            # ── EXTRACTION ────────────────────────────────────────────────
            if entity["extraction"]:
                script = edir / entity["extraction"]
                job["logs"].append(f"  > Extraction     :  {entity['extraction']}")
                if not run_script_sync(script, edir, db_cfg, job["logs"]):
                    job["logs"].append(f"  [ERR] Extraction echouee pour {entity_id}")
                    entity_ok = False
            else:
                job["logs"].append(f"  [!] Aucun script d extraction pour {entity_id}")
            # Fichiers d'extraction non copiés dans le run_folder (transformation uniquement)

            # ── TRANSFORMATION ────────────────────────────────────────────
            t_before_transf = datetime.now().timestamp()
            cleanup_fn  = lambda: None

            if entity["transformation"]:
                script = edir / entity["transformation"]
                job["logs"].append(f"  > Transformation :  {entity['transformation']}")
                cleanup_fn = prepare_runtime_files_for_transformation(
                    edir, entity_id, runtime_options, job["logs"]
                )
                try:
                    if not run_script_sync(script, edir, db_cfg, job["logs"]):
                        job["logs"].append(f"  [ERR] Transformation echouee pour {entity_id}")
                        entity_ok = False
                finally:
                    cleanup_fn()
            else:
                job["logs"].append(f"  [!] Aucun script de transformation pour {entity_id}")

            if entity_ok:
                # Collecter tous les fichiers csv/xlsx créés/modifiés après t_before_transf
                new_transformed = []
                for pat in ("*.csv", "*.xlsx"):
                    for f in transformed_dir.rglob(pat):
                        try:
                            if f.stat().st_mtime >= t_before_transf:
                                new_transformed.append(f)
                        except OSError:
                            pass
                new_transformed.sort(key=lambda p: str(p).lower())

                for f in new_transformed:
                    shutil.copy2(f, transf_out / f.name)
                    job["logs"].append(f"  [+] transformation/{f.name}")
                if new_transformed:
                    # Copier dans output_PROJET/ ET dans output/ (standard)
                    copy_generated_csvs_to_output(db_name, entity_id, new_transformed, dest_base=out_dir)
                    copy_generated_csvs_to_output(db_name, entity_id, new_transformed)

                # ── Collecter les fichiers de la BDD de référence ─────────
                if is_ref:
                    # 1er choix : fichiers dans le run_folder courant
                    ref_from_run = list(transf_out.glob("*.csv")) + list(transf_out.glob("*.xlsx"))
                    if ref_from_run:
                        ref_transf_csv_paths.extend(ref_from_run)
                        job["logs"].append(
                            f"  [REF] {len(ref_from_run)} fichier(s) collecté(s) depuis run_folder pour le lookup"
                        )
                    else:
                        # Fallback 1 : output/<ref_db>/<entity>/
                        out_ref_dir = OUTPUT_DIR / db_name / entity_id
                        if out_ref_dir.exists():
                            ref_from_out = list(out_ref_dir.glob("*.csv")) + list(out_ref_dir.glob("*.xlsx"))
                            if ref_from_out:
                                ref_transf_csv_paths.extend(ref_from_out)
                                job["logs"].append(
                                    f"  [REF] {len(ref_from_out)} fichier(s) collecté(s) depuis output/ (fallback)"
                                )
                        # ── BUG 1 CORRIGÉ : indentation et if not ref_transf_csv_paths ──
                        # Fallback 2 : transformed/ de l'entité
                        if not ref_transf_csv_paths:
                            ref_from_transf = (
                                [p for p in transformed_dir.glob("*.csv") if p.is_file()] +
                                [p for p in transformed_dir.glob("*.xlsx") if p.is_file()]
                            )
                            if ref_from_transf:
                                ref_transf_csv_paths.extend(ref_from_transf)
                                job["logs"].append(
                                    f"  [REF] {len(ref_from_transf)} fichier(s) collecté(s) depuis transformed/ (fallback 2)"
                                )

            # ── Enrichissement NT_LIBELLE ─────────────────────────────────
            if entity_ok and not is_ref and libelle_lookup:
                custom_field_names = get_custom_field_names_from_selection(entity_id)

                if not custom_field_names:
                    job["logs"].append(
                        f"  [REF][INFO] fields_selection.json absent/vide pour {entity_id} — détection automatique TABLE/LIBRE"
                    )
                    csv_to_probe = list(transf_out.glob("*.csv"))
                    if not csv_to_probe:
                        out_ent_probe = OUTPUT_DIR / db_name / entity_id
                        if out_ent_probe.exists():
                            csv_to_probe = list(out_ent_probe.glob("*.csv"))
                    for probe in csv_to_probe[:1]:
                        try:
                            with open(probe, "r", encoding="utf-8-sig", newline="") as pf:
                                hdrs = list(csv.DictReader(pf, delimiter=";").fieldnames or [])
                            custom_field_names = [h for h in hdrs if is_custom_field(h)]
                            if custom_field_names:
                                job["logs"].append(
                                    f"  [REF][AUTO] {len(custom_field_names)} champ(s) détecté(s) : {custom_field_names}"
                                )
                        except Exception:
                            pass

                if custom_field_names:
                    job["logs"].append(
                        f"  > Enrichissement NT_LIBELLE (réf: {ref_upper}) — {len(custom_field_names)} champ(s) perso : {custom_field_names}"
                    )
                    total_enriched = 0
                    csvs_to_enrich = []
                    for c in transf_out.glob("*.csv"):
                        csvs_to_enrich.append(c)
                    out_ent_dir = OUTPUT_DIR / db_name / entity_id
                    if out_ent_dir.exists():
                        for c in out_ent_dir.glob("*.csv"):
                            csvs_to_enrich.append(c)

                    if not csvs_to_enrich:
                        job["logs"].append(
                            f"  [REF][WARN] Aucun CSV trouvé pour {entity_id} dans {db_name} — enrichissement ignoré."
                        )
                    else:
                        for csv_file in csvs_to_enrich:
                            n = enrich_csv_with_libelles(
                                csv_file, libelle_lookup, csv_file, custom_field_names, job["logs"]
                            )
                            total_enriched += n
                        if total_enriched:
                            job["logs"].append(
                                f"  [REF] ✓ Enrichissement terminé — {total_enriched} colonne(s) NT_LIBELLE insérée(s) sur {len(csvs_to_enrich)} fichier(s)"
                            )
                        else:
                            job["logs"].append(
                                f"  [REF][INFO] Aucune valeur résolue — vérifiez que les codes correspondent aux NT_NATURE de la BDD de référence."
                            )
                else:
                    job["logs"].append(
                        f"  [REF][INFO] Aucun champ perso détecté pour {entity_id} — ni dans fields_selection.json ni via TABLE/LIBRE."
                    )

            # ── RÈGLES DE GESTION ─────────────────────────────────────────
            if entity_ok and runtime_options["apply_management_rules"] and new_transformed:
                job["logs"].append("  > Regles de gestion  :  application...")
                try:
                    res    = execute_management_rules_for_entity(db_name, entity_id)
                    rdir   = OUTPUT_RULES_DIR / db_name / entity_id
                    rfiles = sorted(
                        [p for p in rdir.rglob("*.csv") if p.is_file()],
                        key=lambda p: p.stat().st_mtime, reverse=True,
                    )
                    if rfiles:
                        regles_out = run_folder / "regles"
                        regles_out.mkdir(exist_ok=True)
                        shutil.copy2(rfiles[0], regles_out / rfiles[0].name)
                        job["logs"].append(f"  [+] regles/{rfiles[0].name}")
                    job["logs"].append(
                        f"  [OK] {res['rows_matched']} lignes matchees / {res['updated_cells']} cellules mises a jour"
                    )
                except Exception as exc:
                    job["logs"].append(f"  [WARN] Regles ignorees : {exc}")

            # ── Collecte pour la consolidation multi-bases (entites eligibles uniquement) ──
            if entity_ok and is_consolidation_entity(entity_id):
                regles_out = run_folder / "regles"
                final_files = []
                if regles_out.exists():
                    final_files = [p for p in regles_out.glob("*") if p.is_file() and p.suffix.lower() in (".csv", ".xlsx")]
                if not final_files:
                    final_files = [p for p in transf_out.glob("*") if p.is_file() and p.suffix.lower() in (".csv", ".xlsx")]
                if final_files:
                    consolidation_sources.setdefault(entity_id, {})[db_name] = final_files
                else:
                    job["logs"].append(f"  [CONSOLIDATION][WARN] {entity_id} : aucun fichier final trouve pour {db_name}.")

            files_created = [p.name for p in run_folder.rglob("*") if p.is_file()]
            status = "ok" if entity_ok else "error"
            job["results"].append({
                "db": db_name, "entity": entity_id, "status": status,
                "run_folder": str(run_folder.relative_to(BASE_DIR)),
                "files": files_created, "is_reference": is_ref,
            })
            if entity_ok:
                job["logs"].append(f"  [OK] {entity_id}  termine pour  {db_name}")

            done_steps += 1
            job["progress"]["done"] = done_steps

        # ── Construction du lookup APRÈS traitement complet de la BDD de référence ──
        if is_ref:
            job["logs"].append("")
            job["logs"].append("  " + "─" * 50)
            job["logs"].append(
                f"  [REF] Construction du lookup NT_NATURE → NT_LIBELLE ({len(ref_transf_csv_paths)} fichier(s) analysé(s))"
            )
            for ref_csv in ref_transf_csv_paths:
                partial = build_libelle_lookup_from_csv(ref_csv, job["logs"])
                if partial:
                    libelle_lookup.update(partial)
            if libelle_lookup:
                job["logs"].append(f"  [REF] ✓ Lookup prêt : {len(libelle_lookup)} code(s) NT_NATURE")
                job["logs"].append("  [REF]   Toutes les bases suivantes bénéficieront du NT_LIBELLE.")
            else:
                job["logs"].append("  [REF][WARN] Aucune colonne NT_NATURE/NT_LIBELLE trouvée dans les fichiers de la BDD de référence.")
                job["logs"].append("  [REF][INFO] Vérifiez que le script de transformation produit un fichier avec NT_NATURE et NT_LIBELLE en en-têtes.")
            job["logs"].append("  " + "─" * 50)

    # ── CONSOLIDATION MULTI-BASES (entites eligibles uniquement) ──────────────
    if consolidation_sources:
        job["logs"].append("")
        job["logs"].append("=" * 60)
        job["logs"].append("CONSOLIDATION MULTI-BASES")
        job["logs"].append("=" * 60)
        for entity_id, files_by_db in consolidation_sources.items():
            try:
                result = consolidate_entity_across_bases(entity_id, files_by_db, out_dir, job["logs"])
            except Exception as exc:
                job["logs"].append(f"  [CONSOLIDATION][ERREUR] {entity_id} : {exc}")
                result = {"entity": entity_id, "status": "error", "error": str(exc)}
            job["consolidation"].append(result)
        requested = {e_conf.get("id", "").strip() for e_conf in entity_configs
                     if is_consolidation_entity(e_conf.get("id", "").strip())}
        missing = sorted(requested - set(consolidation_sources.keys()))
        if missing:
            job["logs"].append(
                f"  [CONSOLIDATION][WARN] Entite(s) eligible(s) sans fichier consolide (echec ou aucun fichier genere) : {missing}"
            )
        job["logs"].append(f"  Dossier  =>  output_{safe_proj}/{CONSOLIDATION_DIRNAME}/")
        job["logs"].append("=" * 60)

    ok_ct  = sum(1 for r in job["results"] if r["status"] == "ok")
    err_ct = sum(1 for r in job["results"] if r["status"] == "error")
    skp_ct = sum(1 for r in job["results"] if r["status"] == "skip")

    job["logs"].append("")
    job["logs"].append("=" * 60)
    job["logs"].append("PIPELINE AVANCE  TERMINE")
    job["logs"].append(f"  OK: {ok_ct}   ERR: {err_ct}   SKIP: {skp_ct}")
    job["logs"].append(f"  Output  =>  output_{safe_proj}/")
    if ref_upper:
        job["logs"].append(f"  BDD Ref  =>  {ref_upper}  ({len(libelle_lookup)} codes NT_NATURE dans le lookup)")
    job["logs"].append("=" * 60)

    job["status"]      = "done" if err_ct == 0 else "error"
    job["finished_at"] = datetime.now().isoformat()


# =============================================================================
# Fonctions DB / Analyse
# =============================================================================

# Ouvre une connexion ODBC vers SQL Server avec les identifiants fournis.
def get_connection(server, database, username, password, timeout=15):
    cs = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=15;"
    )
    return pyodbc.connect(cs, timeout=timeout)


# Calcule les statistiques d'un champ perso (taux de remplissage, valeurs distinctes, top valeurs).
def fetch_custom_field_stats(cursor, schema_name, table_name, column_name, table_row_count):
    q_schema = quote_ident(schema_name)
    q_table  = quote_ident(table_name)
    q_col    = quote_ident(column_name)

    cursor.execute(f"""
        SELECT
            COUNT(1) AS total_rows,
            SUM(CASE WHEN {q_col} IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {q_col}))) = '' THEN 0 ELSE 1 END) AS filled_rows,
            COUNT(DISTINCT CASE WHEN {q_col} IS NULL OR LTRIM(RTRIM(CONVERT(NVARCHAR(4000), {q_col}))) = ''
                THEN NULL ELSE LEFT(CONVERT(NVARCHAR(4000), {q_col}), 4000) END) AS distinct_non_empty
        FROM {q_schema}.{q_table}
    """)
    row = cursor.fetchone()
    total_rows         = int(row[0] or table_row_count or 0)
    filled_rows        = int(row[1] or 0)
    distinct_non_empty = int(row[2] or 0)
    empty_rows         = max(total_rows - filled_rows, 0)
    fill_rate          = round((filled_rows / total_rows) * 100, 2) if total_rows else 0.0

    distinct_values = []
    try:
        cursor.execute(f"""
            SELECT TOP 10 LEFT(CONVERT(NVARCHAR(4000), {q_col}), 250) AS val, COUNT(1) AS cnt
            FROM {q_schema}.{q_table}
            WHERE {q_col} IS NOT NULL AND LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {q_col}))) <> ''
            GROUP BY LEFT(CONVERT(NVARCHAR(4000), {q_col}), 250)
            ORDER BY COUNT(1) DESC, LEFT(CONVERT(NVARCHAR(4000), {q_col}), 250)
        """)
        for r in cursor.fetchall():
            distinct_values.append({"value": str(r[0]) if r[0] is not None else "", "count": int(r[1] or 0)})
    except Exception:
        pass

    return {
        "column_name": column_name, "filled_rows": filled_rows,
        "empty_rows": empty_rows, "distinct_non_empty": distinct_non_empty,
        "fill_rate": fill_rate, "top_values": distinct_values,
    }


# Analyse une base : tables ciblées, comptage de lignes, colonnes, aperçus et champs personnalisés.
def analyse_single_db(server, database, username, password):
    conn   = get_connection(server, database, username, password, timeout=15)
    cursor = conn.cursor()
    target_set = {t.upper() for t in TARGET_ANALYSIS_TABLES}

    cursor.execute("""
        SELECT s.name, t.name,
            ISNULL(SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END), 0)
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE UPPER(t.name) IN ({})
        GROUP BY s.name, t.name ORDER BY t.name, s.name
    """.format(",".join("?" for _ in target_set)), tuple(sorted(target_set)))
    tables_raw = cursor.fetchall()

    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_NAME) IN ({})
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """.format(",".join("?" for _ in target_set)), tuple(sorted(target_set)))
    columns_raw = cursor.fetchall()

    columns_by_table = {}
    for row in columns_raw:
        key = f"{row[0]}.{row[1]}"
        columns_by_table.setdefault(key, []).append({
            "name": row[2], "type": row[3], "nullable": row[4], "max_length": row[5],
        })

    table_map   = {}
    found_names = set()

    for row in tables_raw:
        schema_name, table_name, row_count = row[0], row[1], int(row[2] or 0)
        found_names.add(normalize_table_name(table_name))
        key  = f"{schema_name}.{table_name}"
        cols = columns_by_table.get(key, [])

        preview_rows, preview_columns = [], []
        try:
            cursor.execute(f"SELECT TOP 5 * FROM {quote_ident(schema_name)}.{quote_ident(table_name)}")
            preview_columns = [desc[0] for desc in cursor.description] if cursor.description else []
            for prow in cursor.fetchall():
                preview_rows.append([str(v)[:120] if v is not None else "" for v in prow])
        except Exception:
            pass

        custom_fields = []
        for col in cols:
            if is_custom_field(col["name"]):
                try:
                    custom_fields.append(fetch_custom_field_stats(cursor, schema_name, table_name, col["name"], row_count))
                except Exception as exc:
                    custom_fields.append({
                        "column_name": col["name"], "filled_rows": 0, "empty_rows": row_count,
                        "distinct_non_empty": 0, "fill_rate": 0.0, "top_values": [], "error": str(exc),
                    })

        table_map[normalize_table_name(table_name)] = {
            "schema": schema_name, "name": table_name, "full_name": key,
            "exists": True, "row_count": row_count, "column_count": len(cols),
            "columns": cols, "preview_columns": preview_columns, "preview_rows": preview_rows,
            "custom_fields": custom_fields, "custom_fields_count": len(custom_fields),
        }

    tables = []
    total_rows = populated = empty = total_custom_fields = tables_with_custom = 0

    for expected in TARGET_ANALYSIS_TABLES:
        item = table_map.get(expected)
        if item:
            total_rows += item["row_count"]
            if item["row_count"] > 0: populated += 1
            else: empty += 1
            total_custom_fields += item["custom_fields_count"]
            if item["custom_fields_count"] > 0: tables_with_custom += 1
            tables.append(item)
        else:
            tables.append({
                "schema": "-", "name": expected, "full_name": expected,
                "exists": False, "row_count": 0, "column_count": 0,
                "columns": [], "preview_columns": [], "preview_rows": [],
                "custom_fields": [], "custom_fields_count": 0,
            })

    conn.close()
    return {
        "database": database, "server": server,
        "target_tables": TARGET_ANALYSIS_TABLES,
        "summary": {
            "requested_tables": len(TARGET_ANALYSIS_TABLES),
            "found_tables": len(found_names),
            "missing_tables": len(TARGET_ANALYSIS_TABLES) - len(found_names),
            "total_rows": total_rows, "populated_tables": populated, "empty_tables": empty,
            "tables_with_custom_fields": tables_with_custom, "custom_fields_total": total_custom_fields,
        },
        "tables": tables,
    }


# Met une valeur de pays en majuscules sans espaces, pour comparaison dans les règles.
def normalize_country_value(value: str) -> str:
    return (value or "").strip().upper()


# Renvoie le chemin du fichier JSON des règles de gestion d'une entité.
def get_rules_file_path(entity_id: str) -> Path:
    entity = get_entity(entity_id)
    if not entity:
        raise ValueError("Entite introuvable")
    return Path(entity["dir"]) / "regles_gestion.json"


# Charge la configuration des règles de gestion d'une entité (ou une config vide par défaut).
def load_rules_config(entity_id: str):
    path = get_rules_file_path(entity_id)
    if not path.exists():
        return {
            "generated_at": "", "entity": entity_id, "rule_type": "pays",
            "match_column": "Pays*", "overwrite_existing": False, "rules": [],
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Enregistre la configuration des règles de gestion d'une entité dans son fichier JSON.
def save_rules_config(entity_id: str, payload: dict):
    path = get_rules_file_path(entity_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


# Normalise un nom d'en-tête (majuscules, sans espaces) pour faire correspondre les colonnes.
def safe_norm_header(name: str) -> str:
    return (name or "").strip().upper()


# Construit un index {en-tête normalisé -> en-tête réel} pour retrouver les vraies colonnes du CSV.
def build_header_index(headers):
    return {safe_norm_header(h): h for h in headers}


# Teste une condition (égal, commence par, finit par, contient) entre une valeur de cellule et une valeur cible.
def match_condition(cell_value, operator, condition_value):
    cell = (cell_value or "").strip().upper()
    cond = (condition_value or "").strip().upper()
    if not cond:
        return False
    if operator == "egal_a":         return cell == cond
    elif operator == "commence_par": return cell.startswith(cond)
    elif operator == "finit_par":    return cell.endswith(cond)
    elif operator == "contient":     return cond in cell
    return False


# Vérifie qu'une ligne respecte TOUTES les conditions d'une règle filtre (logique ET).
def row_matches_conditions(row, conditions, header_index):
    for cond in conditions:
        real_col = header_index.get(safe_norm_header(cond.get("column", "")))
        if not real_col:
            return False
        if not match_condition(row.get(real_col, ""), cond.get("operator", "egal_a"), cond.get("value", "")):
            return False
    return True


# Applique les règles de gestion (par pays ou par filtre) sur un CSV et écrit le fichier corrigé.
def apply_management_rules_to_csv(input_csv: Path, output_csv: Path, rules_config: dict):
    rules = rules_config.get("rules", [])
    if not rules:
        raise ValueError("Aucune regle de gestion definie.")

    rule_type          = rules_config.get("rule_type", "pays")
    match_column_cfg   = rules_config.get("match_column", "Pays*")
    overwrite_existing = bool(rules_config.get("overwrite_existing", False))

    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader  = csv.DictReader(f, delimiter=";")
        rows    = list(reader)
        headers = reader.fieldnames or []

    if not headers:
        raise ValueError("Le fichier CSV source ne contient pas d'en-tetes.")

    header_index      = build_header_index(headers)
    final_headers     = list(headers)
    validation_errors = []
    prepared_rules    = []

    # Sous-fonction : retrouve le vrai nom d'une colonne demandée dans le CSV.
    def resolve_existing_header(requested_name: str):
        return header_index.get(safe_norm_header(requested_name))

    # Sous-fonction : valide et résout les colonnes cibles d'une règle vers les vraies colonnes du CSV.
    def resolve_values_dict(values: dict, rule_label: str):
        resolved = {}
        for target_col, target_val in (values or {}).items():
            requested = str(target_col or "").strip()
            if not requested:
                continue
            real_col = resolve_existing_header(requested)
            if not real_col:
                validation_errors.append(f"{rule_label} -> colonne cible introuvable : {requested}")
                continue
            val_str = "" if target_val is None else str(target_val)
            if val_str.strip():
                resolved[real_col] = val_str
        return resolved

    if rule_type == "filtre":
        for idx, rule in enumerate(rules, 1):
            conditions = rule.get("conditions", []) or []
            if not conditions:
                continue
            prepared_conditions = []
            for cond in conditions:
                col_name = (cond.get("column") or "").strip()
                operator = (cond.get("operator") or "egal_a").strip()
                value    = (cond.get("value") or "").strip()
                if not col_name or not value:
                    continue
                real_col = resolve_existing_header(col_name)
                if not real_col:
                    validation_errors.append(f"Regle {idx} -> colonne de condition introuvable : {col_name}")
                    continue
                prepared_conditions.append({"column": real_col, "operator": operator, "value": value})
            resolved_values = resolve_values_dict(rule.get("values", {}) or {}, f"Regle {idx}")
            if prepared_conditions and resolved_values:
                prepared_rules.append({"conditions": prepared_conditions, "values": resolved_values})
    else:
        match_column_real = resolve_existing_header(match_column_cfg)
        if not match_column_real:
            validation_errors.append(f"Colonne de correspondance introuvable : {match_column_cfg}")
        for idx, rule in enumerate(rules, 1):
            match_value     = normalize_country_value(rule.get("match_value", ""))
            if not match_value:
                continue
            resolved_values = resolve_values_dict(rule.get("values", {}) or {}, f"Regle {idx} [{match_value}]")
            if resolved_values:
                prepared_rules.append({"match_value": match_value, "values": resolved_values})

    if validation_errors:
        raise ValueError(" ; ".join(validation_errors))
    if not prepared_rules:
        raise ValueError("Aucune regle exploitable trouvee apres validation.")

    updated_count = matched_rows = 0

    if rule_type == "filtre":
        for row in rows:
            for rule in prepared_rules:
                if row_matches_conditions(row, rule["conditions"], {safe_norm_header(k): k for k in row.keys()}):
                    matched_rows += 1
                    for real_col, val_str in rule["values"].items():
                        current_val = str(row.get(real_col, "")).strip()
                        if overwrite_existing or current_val == "":
                            if str(row.get(real_col, "")) != val_str:
                                row[real_col] = val_str
                                updated_count += 1
    else:
        match_column_real  = resolve_existing_header(match_column_cfg)
        normalized_rules   = {rule["match_value"]: rule["values"] for rule in prepared_rules}
        for row in rows:
            row_match_value = normalize_country_value(row.get(match_column_real, ""))
            if row_match_value not in normalized_rules:
                continue
            matched_rows += 1
            for col_name, col_value in normalized_rules[row_match_value].items():
                current_val = str(row.get(col_name, "")).strip()
                if overwrite_existing or current_val == "":
                    if str(row.get(col_name, "")) != col_value:
                        row[col_name] = col_value
                        updated_count += 1

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=final_headers, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            for h in final_headers:
                row.setdefault(h, "")
            writer.writerow(row)

    return {
        "rows_total": len(rows), "rows_matched": matched_rows,
        "updated_cells": updated_count, "output_file": output_csv.name,
        "validation": {
            "headers_count": len(final_headers),
            "prepared_rules": len(prepared_rules),
            "overwrite_existing": overwrite_existing,
        },
    }


# =============================================================================
# Routes Flask — Application existante
# =============================================================================

# Route '/' : redirige vers la connexion ou le dashboard selon l'état de la session.
@app.route("/")
def index():
    return redirect(url_for("login") if "db_config" not in session else url_for("accueil"))


# Route '/login' : affiche la page de connexion et valide les identifiants de la base SQL Server.
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        d        = request.get_json() or {}
        server   = d.get("server",   "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "").strip()
        if not all([server, database, username, password]):
            return jsonify({"success": False, "error": "Tous les champs sont obligatoires."})
        try:
            c = get_connection(server, database, username, password, timeout=10)
            c.close()
            session["db_config"] = {"server": server, "database": database,
                                    "username": username, "password": password}
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
    return render_template("login.html")


# Route '/logout' : vide la session et renvoie vers la page de connexion.
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# Route '/accueil' : page d'accueil affichée juste après la connexion.
# Donne accès aux différents modules : extraction par entité, mode avancé, analyse, IA, SFTP.
@app.route("/accueil")
def accueil():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("accueil.html", db_config=session["db_config"], entities=scan_entities())


# Route '/entites' : grille des entités (une carte par entité). Le clic ouvre le pipeline correspondant.
@app.route("/entites")
def entites_page():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("entites.html", db_config=session["db_config"], entities=scan_entities())


# Route '/dashboard' : affiche le tableau de bord principal avec la liste des entités.
@app.route("/dashboard")
def dashboard():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", db_config=session["db_config"], entities=scan_entities())


# Route '/pipeline' : affiche la page du flux ETL.
@app.route("/pipeline")
def pipeline_page():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("pipeline.html", db_config=session["db_config"], entities=scan_entities())


# Route '/analyse' : affiche la page d'analyse des bases de données.
@app.route("/analyse")
def analyse_page():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("analyse.html", db_config=session["db_config"], target_tables=TARGET_ANALYSIS_TABLES)


# API : renvoie la liste des entités détectées (JSON).
@app.route("/api/entities")
def api_entities():
    return jsonify(scan_entities())


# API : lance un script (extraction, transformation ou scan de champs) pour une entité, en arrière-plan.
@app.route("/api/run", methods=["POST"])
def api_run():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    d           = request.get_json() or {}
    entity_id   = d.get("entity")
    script_type = d.get("type")
    entity      = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    entity_dir  = Path(entity["dir"])

    if script_type == "scan_fields":
        candidates  = [BASE_DIR / "script1_scan_fields.py", BASE_DIR / "script1_scan_fields_windows_safe.py"]
        script_path = next((p for p in candidates if p.exists()), None)
        if script_path is None:
            return jsonify({"error": "Aucun script de scan trouve a la racine"}), 404
        job_id = str(uuid.uuid4())
        jobs[job_id] = {"status": "running", "logs": [], "started": datetime.now().isoformat()}
        threading.Thread(
            target=run_script_job,
            args=(job_id, script_path, entity_dir, session["db_config"]),
            kwargs={"script_type": "scan_fields", "entity_id": entity_id},
            daemon=True,
        ).start()
        return jsonify({"job_id": job_id})

    if script_type not in ("extraction", "transformation"):
        return jsonify({"error": "Type de script invalide"}), 400
    script_name = entity.get(script_type)
    if not script_name:
        return jsonify({"error": f"Pas de script '{script_type}' pour cette entite"}), 404
    script_path = entity_dir / script_name
    if not script_path.exists():
        return jsonify({"error": f"Script introuvable : {script_name}"}), 404

    runtime_options = None
    if script_type == "transformation":
        if isinstance(d.get("options"), dict):
            runtime_options = normalize_transformation_options(d.get("options"))
        else:
            runtime_options = normalize_transformation_options(load_transformation_options(entity_id))
        save_transformation_options(entity_id, {
            "generated_at": datetime.now().isoformat(), "entity": entity_id, **runtime_options,
        })

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "logs": [], "started": datetime.now().isoformat()}
    threading.Thread(
        target=run_script_job,
        args=(job_id, script_path, entity_dir, session["db_config"]),
        kwargs={"script_type": script_type, "entity_id": entity_id, "runtime_options": runtime_options},
        daemon=True,
    ).start()
    return jsonify({"job_id": job_id, "options": runtime_options})


# API : renvoie l'état et les logs d'un job en cours (suivi de progression).
@app.route("/api/job/<job_id>")
def api_job(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job introuvable"}), 404
    return jsonify(jobs[job_id])


# API : liste les fichiers CSV d'un dossier (exports ou transformed) d'une entité.
@app.route("/api/files/<entity_id>/<folder>")
def api_files(entity_id, folder):
    if folder not in ("exports", "transformed"):
        return jsonify([]), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify([]), 404
    target = Path(entity[f"{folder}_dir"])
    files  = []
    for f in sorted(target.rglob("*.csv"), reverse=True):
        rel = f.relative_to(BASE_DIR)
        files.append({
            "name": f.name, "path": str(rel),
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
        })
    return jsonify(files[:100])


# API : télécharge un fichier du projet (vérifie qu'il reste dans le dossier autorisé).
@app.route("/api/download")
def api_download():
    if "db_config" not in session:
        return redirect(url_for("login"))
    rel  = request.args.get("path", "")
    full = BASE_DIR / rel
    if not full.exists() or not str(full.resolve()).startswith(str(BASE_DIR.resolve())):
        return "Fichier introuvable", 404
    return send_file(full, as_attachment=True)


# API : supprime un fichier CSV (uniquement dans les dossiers autorisés).
@app.route("/api/delete_file", methods=["POST"])
def api_delete_file():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    data     = request.get_json() or {}
    rel_path = data.get("path", "").strip()
    if not rel_path:
        return jsonify({"error": "Chemin manquant."}), 400
    full = (BASE_DIR / rel_path).resolve()
    if not (
        str(full).startswith(str(ENTITIES_DIR.resolve()))
        or str(full).startswith(str(OUTPUT_DIR.resolve()))
        or str(full).startswith(str(OUTPUT_RULES_DIR.resolve()))
    ):
        return jsonify({"error": "Chemin non autorise."}), 403
    if not full.exists():
        return jsonify({"error": "Fichier introuvable."}), 404
    if not full.is_file():
        return jsonify({"error": "Ce n'est pas un fichier."}), 400
    if full.suffix.lower() != ".csv":
        return jsonify({"error": "Seuls les fichiers CSV peuvent etre supprimes."}), 400
    try:
        filename = full.name
        full.unlink()
        return jsonify({"success": True, "deleted": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : renvoie le catalogue des champs perso détectés (fields_catalog.json) d'une entité.
@app.route("/api/fields_catalog/<entity_id>")
def api_fields_catalog(entity_id):
    if ".." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    catalog_path = Path(entity["dir"]) / "fields_catalog.json"
    if not catalog_path.exists():
        return jsonify({"error": "Aucun catalog - lancez un scan d'abord"}), 404
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : enregistre les champs perso sélectionnés et génère le transform_patch.py associé.
@app.route("/api/save_fields_selection", methods=["POST"])
def api_save_fields_selection():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body      = request.get_json(force=True) or {}
    entity_id = body.get("entity", "")
    database  = body.get("database", "")
    fields    = body.get("fields", [])
    if not entity_id or ".." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    entity_dir     = Path(entity["dir"])
    selection_data = {
        "generated_at": datetime.now().isoformat(), "database": database,
        "entity": entity_id, "total_selected": len(fields), "fields": fields,
    }
    selection_path = entity_dir / "fields_selection.json"
    with open(selection_path, "w", encoding="utf-8") as f:
        json.dump(selection_data, f, ensure_ascii=False, indent=2)

    lines = [
        '"""', "transform_patch.py", "===================",
        "Genere automatiquement par le Dashboard Timsoft.",
        f"Genere le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Entite    : {entity_id}", f"Base      : {database}", "",
        "Integrez ce code dans votre script de transformation.", '"""', "",
        "# -- CHAMPS TABLE SELECTIONNES --", "TABLE_FIELDS_MAPPING = {",
    ]
    for fld in fields:
        src  = fld.get("column_name", "")
        dest = fld.get("final_name", src)
        lines.append(f'    "{src}": "{dest}",')
    lines += [
        "}", "", "",
        "def extract_table_fields(row: dict) -> dict:",
        '    """Extrait et renomme les champs TABLE."""',
        "    result = {}",
        "    for src_col, dest_col in TABLE_FIELDS_MAPPING.items():",
        '        val = (row.get(src_col.upper()) or row.get(src_col) or "").strip()',
        "        result[dest_col] = val",
        "    return result", "",
    ]
    patch_path = entity_dir / "transform_patch.py"
    with open(patch_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return jsonify({
        "ok": True,
        "selection_path": str(selection_path.relative_to(BASE_DIR)),
        "patch_path": str(patch_path.relative_to(BASE_DIR)),
        "total_selected": len(fields),
    })


# API : renvoie les options de transformation enregistrées pour une entité.
@app.route("/api/transformation_options/<entity_id>")
def api_transformation_options(entity_id):
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    try:
        return jsonify(load_transformation_options(entity_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : enregistre les options de transformation d'une entité.
@app.route("/api/save_transformation_options", methods=["POST"])
def api_save_transformation_options():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body      = request.get_json(force=True) or {}
    entity_id = (body.get("entity") or "").strip()
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    normalized = normalize_transformation_options(body)
    payload    = {"generated_at": datetime.now().isoformat(), "entity": entity_id, **normalized}
    try:
        path = save_transformation_options(entity_id, payload)
        return jsonify({"ok": True, "options_path": str(path.relative_to(BASE_DIR)), **normalized})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : renvoie la configuration des règles de gestion d'une entité.
@app.route("/api/management_rules/<entity_id>")
def api_management_rules(entity_id):
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    try:
        return jsonify(load_rules_config(entity_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : valide et enregistre les règles de gestion (pays ou filtre) d'une entité.
@app.route("/api/save_management_rules", methods=["POST"])
def api_save_management_rules():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body               = request.get_json(force=True) or {}
    entity_id          = (body.get("entity") or "").strip()
    rule_type          = (body.get("rule_type") or "pays").strip()
    match_column       = (body.get("match_column") or "Pays*").strip()
    overwrite_existing = bool(body.get("overwrite_existing", False))
    rules              = body.get("rules", [])

    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    if not isinstance(rules, list):
        return jsonify({"error": "Le champ 'rules' doit etre une liste."}), 400

    available_headers = []
    available_index   = {}
    source_csv        = find_latest_source_csv_for_entity(session["db_config"]["database"], entity_id)
    if source_csv is not None:
        try:
            with open(source_csv, "r", encoding="utf-8-sig") as f:
                reader            = csv.reader(f, delimiter=";")
                available_headers = [h.strip() for h in next(reader, []) if h.strip()]
                available_index   = build_header_index(available_headers)
        except Exception:
            available_headers = []
            available_index   = {}

    validation_errors = []
    if available_index and match_column and not available_index.get(safe_norm_header(match_column)):
        validation_errors.append(f"Colonne de correspondance introuvable : {match_column}")

    cleaned_rules = []
    for rule in rules:
        values = rule.get("values", {}) or {}
        if not isinstance(values, dict):
            continue
        cleaned_values = {str(k): str(v) for k, v in values.items() if str(v).strip() != ""}
        if available_index:
            for target_col in cleaned_values.keys():
                if not available_index.get(safe_norm_header(target_col)):
                    validation_errors.append(f"Colonne cible introuvable : {target_col}")

        if rule_type == "filtre":
            conditions = rule.get("conditions", [])
            if not conditions or not isinstance(conditions, list):
                continue
            cleaned_conditions = []
            for c in conditions:
                col = (c.get("column") or "").strip()
                op  = (c.get("operator") or "").strip()
                val = (c.get("value") or "").strip()
                if col and op and val:
                    if available_index and not available_index.get(safe_norm_header(col)):
                        validation_errors.append(f"Colonne de condition introuvable : {col}")
                    cleaned_conditions.append({"column": col, "operator": op, "value": val})
            if not cleaned_conditions:
                continue
            cleaned_rules.append({"conditions": cleaned_conditions, "values": cleaned_values})
        else:
            match_value = (rule.get("match_value") or "").strip()
            if not match_value:
                continue
            cleaned_rules.append({"match_value": match_value, "values": cleaned_values})

    payload = {
        "generated_at": datetime.now().isoformat(),
        "database": session["db_config"]["database"],
        "entity": entity_id, "rule_type": rule_type,
        "match_column": match_column, "overwrite_existing": overwrite_existing,
        "rules": cleaned_rules,
    }
    try:
        path = save_rules_config(entity_id, payload)
        return jsonify({"ok": True, "rules_path": str(path.relative_to(BASE_DIR)), "rules_count": len(cleaned_rules)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : applique les règles de gestion enregistrées sur le dernier CSV final d'une entité.
@app.route("/api/apply_management_rules", methods=["POST"])
def api_apply_management_rules():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body      = request.get_json(force=True) or {}
    entity_id = (body.get("entity") or "").strip()
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    try:
        result = execute_management_rules_for_entity(session["db_config"]["database"], entity_id)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : liste les fichiers générés par les règles de gestion pour la base courante.
@app.route("/api/output_management_rules_files")
def api_output_management_rules_files():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    db_name   = session["db_config"]["database"]
    db_output = OUTPUT_RULES_DIR / db_name
    if not db_output.exists():
        return jsonify({"database": db_name, "entities": []})
    result = []
    for entity_dir in sorted(db_output.iterdir()):
        if not entity_dir.is_dir():
            continue
        files = []
        for f in sorted(entity_dir.rglob("*.csv"), reverse=True):
            rel = f.relative_to(BASE_DIR)
            files.append({
                "name": f.name, "path": str(rel),
                "size_kb": round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
            })
        result.append({"entity": entity_dir.name, "label": entity_dir.name.replace("_", " ").title(), "files": files})
    return jsonify({"database": db_name, "entities": result})


# API : renvoie la liste des colonnes filtrables prédéfinies.
@app.route("/api/filterable_columns")
def api_filterable_columns():
    return jsonify(FILTERABLE_COLUMNS)


# API : renvoie les colonnes réelles du dernier CSV final d'une entité.
@app.route("/api/csv_columns/<entity_id>")
def api_csv_columns(entity_id):
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    db_name  = session["db_config"]["database"]
    csv_path = find_latest_source_csv_for_entity(db_name, entity_id)
    if csv_path is None:
        return jsonify({"error": "Aucun CSV final trouve.", "columns": [], "file": ""})
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader  = csv.reader(f, delimiter=";")
            headers = next(reader, [])
        return jsonify({"columns": [h.strip() for h in headers if h.strip()], "file": csv_path.name})
    except Exception as e:
        return jsonify({"error": str(e), "columns": [], "file": ""})


# API : lance le pipeline complet (toutes les entités) pour la base courante.
@app.route("/api/run_all", methods=["POST"])
def api_run_all():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    cfg    = session["db_config"]
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status":   "running",
        "logs":     [f"Pipeline demarre pour la base : {cfg['database']}"],
        "started":  datetime.now().isoformat(),
        "progress": {"done": 0, "total": 0, "current": ""},
    }
    threading.Thread(target=run_full_pipeline, args=(job_id, cfg), daemon=True).start()
    return jsonify({"job_id": job_id, "output_dir": f"output/{cfg['database']}"})


# API : liste tous les fichiers de sortie (output/) par entité pour la base courante.
@app.route("/api/output_files")
def api_output_files():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    db_name   = session["db_config"]["database"]
    db_output = OUTPUT_DIR / db_name
    if not db_output.exists():
        return jsonify({"database": db_name, "entities": []})
    result = []
    for entity_dir in sorted(db_output.iterdir()):
        if not entity_dir.is_dir():
            continue
        files = []
        for f in sorted(entity_dir.rglob("*.csv"), reverse=True):
            rel = f.relative_to(BASE_DIR)
            files.append({
                "name": f.name, "path": str(rel),
                "size_kb": round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
            })
        if files:
            result.append({"entity": entity_dir.name, "label": entity_dir.name.replace("_", " ").title(), "files": files})
    return jsonify({"database": db_name, "entities": result})


# API : lance l'analyse d'une ou plusieurs bases et renvoie un résumé global.
@app.route("/api/analyse_db", methods=["POST"])
def api_analyse_db():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    cfg     = session["db_config"]
    body    = request.get_json(force=True) or {}
    db_list = [d.strip() for d in body.get("databases", [cfg["database"]]) if d.strip()]
    if not db_list:
        return jsonify({"error": "Aucune base de donnees specifiee."}), 400

    all_results, errors = [], []
    for db_name in db_list:
        try:
            result = analyse_single_db(cfg["server"], db_name, cfg["username"], cfg["password"])
            result["analysed_at"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            all_results.append(result)
        except Exception as e:
            errors.append({"database": db_name, "error": str(e)})

    global_summary = {
        "total_databases": len(db_list), "analysed_ok": len(all_results), "analysed_errors": len(errors),
        "requested_tables": len(TARGET_ANALYSIS_TABLES),
        "found_tables":    sum(r["summary"]["found_tables"]    for r in all_results),
        "missing_tables":  sum(r["summary"]["missing_tables"]  for r in all_results),
        "total_rows":      sum(r["summary"]["total_rows"]      for r in all_results),
        "total_populated": sum(r["summary"]["populated_tables"]for r in all_results),
        "total_empty":     sum(r["summary"]["empty_tables"]    for r in all_results),
        "tables_with_custom_fields": sum(r["summary"]["tables_with_custom_fields"] for r in all_results),
        "custom_fields_total":       sum(r["summary"]["custom_fields_total"]        for r in all_results),
    }
    return jsonify({
        "server": cfg["server"],
        "analysed_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "keywords_used": list(CUSTOM_FIELD_KEYWORDS),
        "target_tables": TARGET_ANALYSIS_TABLES,
        "global_summary": global_summary,
        "databases": all_results,
        "errors": errors,
    })


# API : génère un rapport PDF complet d'analyse (tables, champs perso, graphiques) avec ReportLab.
@app.route("/api/generate_report", methods=["POST"])
def api_generate_report():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.units import cm, mm
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            PageBreak, Image as RLImage,
        )
        from reportlab.lib.enums import TA_CENTER
    except ImportError as e:
        return jsonify({"error": f"Librairie manquante : {e}. pip install reportlab matplotlib"}), 500

    # Sous-fonction : formate un nombre avec séparateurs de milliers.
    def fmt_num(v):
        return f"{int(v):,}".replace(",", " ")

    # Sous-fonction : tronque un texte trop long avec des points de suspension.
    def ellipsize(text, max_len=60):
        text = "" if text is None else str(text)
        return text if len(text) <= max_len else text[:max_len - 3] + "..."

    # Sous-fonction : construit un tableau stylé pour le rapport PDF.
    def make_table(data, col_widths, header_color="#0047FF", alt="#f7f9fc", font_size=7):
        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, 0), colors.HexColor(header_color)),
            ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
            ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, -1), font_size),
            ("GRID",         (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d9e6")),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, colors.HexColor(alt)]),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING",  (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING",   (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ]))
        return tbl

    data           = request.get_json(force=True) or {}
    databases      = data.get("databases", [])
    global_summary = data.get("global_summary", {})
    server         = data.get("server", "")
    if not databases:
        return jsonify({"error": "Aucune donnee d'analyse fournie."}), 400

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_names  = "_".join(d["database"][:15] for d in databases[:3])
    if len(databases) > 3:
        db_names += f"_+{len(databases)-3}"
    pdf_name = f"Rapport_tables_ciblees_{db_names}_{timestamp}.pdf"
    pdf_path = REPORTS_DIR / pdf_name

    styles = getSampleStyleSheet()
    for name, kwargs in [
        ("CoverTitle",   dict(fontName="Helvetica-Bold", fontSize=26, alignment=TA_CENTER, spaceAfter=10, textColor=colors.HexColor("#0047FF"))),
        ("CoverSub",     dict(fontName="Helvetica",      fontSize=13, alignment=TA_CENTER, spaceAfter=6,  textColor=colors.HexColor("#64748b"))),
        ("SectionTitle", dict(fontName="Helvetica-Bold", fontSize=16, spaceAfter=10, spaceBefore=16, textColor=colors.HexColor("#1e293b"))),
        ("DBTitle",      dict(fontName="Helvetica-Bold", fontSize=18, spaceAfter=8,  spaceBefore=12, textColor=colors.HexColor("#0047FF"))),
        ("SubSection",   dict(fontName="Helvetica-Bold", fontSize=11, spaceAfter=6,  spaceBefore=10, textColor=colors.HexColor("#2563eb"))),
        ("SmallText",    dict(fontName="Helvetica",      fontSize=8,  textColor=colors.HexColor("#94a3b8"))),
    ]:
        styles.add(ParagraphStyle(name=name, **kwargs))

    doc        = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                                   leftMargin=1.6*cm, rightMargin=1.6*cm,
                                   topMargin=1.7*cm,  bottomMargin=1.7*cm)
    story      = []
    page_width = A4[0] - 3.2 * cm

    story.append(Spacer(1, 2.6 * cm))
    story.append(Paragraph("RAPPORT D'ANALYSE", styles["CoverTitle"]))
    story.append(Paragraph("TABLES CIBLEES ET CHAMPS PERSONNALISES", styles["CoverSub"]))
    story.append(Spacer(1, 0.7 * cm))
    for db in databases:
        story.append(Paragraph(f"<b>{db['database']}</b>",
            ParagraphStyle(name=f"db_{db['database']}", fontName="Helvetica-Bold", fontSize=16,
                           alignment=TA_CENTER, textColor=colors.HexColor("#059669"))))
    story.append(Spacer(1, 0.4 * cm))
    story.append(Paragraph(f"Serveur : {server}", styles["CoverSub"]))
    story.append(Paragraph(f"Date : {data.get('analysed_at', '')}", styles["CoverSub"]))
    story.append(Paragraph("Tables ciblees : " + ", ".join(data.get("target_tables", TARGET_ANALYSIS_TABLES)), styles["SmallText"]))
    story.append(Paragraph("Champs personnalises recherches : TABLE / LIBRE", styles["SmallText"]))
    story.append(Spacer(1, 0.9 * cm))

    cover_data = [
        ["Indicateur", "Valeur"],
        ["Bases analysees",                   str(global_summary.get("analysed_ok",              0))],
        ["Tables demandees",                  str(global_summary.get("requested_tables",          0))],
        ["Tables trouvees",                   str(global_summary.get("found_tables",              0))],
        ["Tables manquantes",                 str(global_summary.get("missing_tables",            0))],
        ["Tables alimentees",                 str(global_summary.get("total_populated",           0))],
        ["Lignes totales",                    fmt_num(global_summary.get("total_rows",            0))],
        ["Tables avec champs personnalises",  str(global_summary.get("tables_with_custom_fields", 0))],
        ["Champs personnalises detectes",     str(global_summary.get("custom_fields_total",       0))],
    ]
    story.append(make_table(cover_data, [9 * cm, 6 * cm], font_size=10))
    story.append(Spacer(1, 0.8 * cm))
    story.append(Paragraph("Genere automatiquement par Timsoft ETL Dashboard", styles["SmallText"]))
    story.append(PageBreak())

    for db_data in databases:
        db_name_local = db_data["database"]
        summary       = db_data["summary"]
        tables        = db_data.get("tables", [])

        story.append(Paragraph(f"Base : {db_name_local}", styles["DBTitle"]))
        story.append(Paragraph(
            f"{summary['found_tables']} tables trouvees sur {summary['requested_tables']} demandees | "
            f"{summary['populated_tables']} alimentees | {summary['missing_tables']} manquantes | "
            f"{fmt_num(summary['total_rows'])} lignes | {summary['custom_fields_total']} champs personnalises",
            styles["SmallText"]))
        story.append(Spacer(1, 0.35 * cm))

        found     = summary.get("found_tables",     0)
        missing   = summary.get("missing_tables",   0)
        populated = summary.get("populated_tables", 0)
        empty_    = summary.get("empty_tables",     0)

        fig1, ax1 = plt.subplots(figsize=(5.2, 3.2))
        if found + missing > 0:
            ax1.pie([found, missing],
                    labels=[f"Trouvees ({found})", f"Manquantes ({missing})"],
                    colors=["#059669", "#dc2626"], autopct="%1.0f%%", startangle=90,
                    textprops={"fontsize": 9})
            ax1.set_title(f"{db_name_local} - Presence des tables", fontsize=11, fontweight="bold")
        buf1 = io.BytesIO()
        fig1.tight_layout()
        fig1.savefig(buf1, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig1)
        buf1.seek(0)
        story.append(RLImage(buf1, width=10.5 * cm, height=7.0 * cm))
        story.append(Spacer(1, 0.2 * cm))

        fig2, ax2 = plt.subplots(figsize=(6.3, 2.8))
        ax2.bar(["Alimentees", "Vides"], [populated, empty_], color=["#0047FF", "#EA580C"])
        ax2.set_title(f"{db_name_local} - Statut des tables trouvees", fontsize=11, fontweight="bold")
        ax2.set_ylabel("Nombre de tables")
        for spine in ["top", "right"]:
            ax2.spines[spine].set_visible(False)
        buf2 = io.BytesIO()
        fig2.tight_layout()
        fig2.savefig(buf2, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig2)
        buf2.seek(0)
        story.append(RLImage(buf2, width=page_width, height=6.3 * cm))
        story.append(PageBreak())

        story.append(Paragraph(f"{db_name_local} - Resume des tables ciblees", styles["SectionTitle"]))
        recap_data = [["#", "Table", "Schema", "Existe", "Lignes", "Colonnes", "Champs perso"]]
        for i, t in enumerate(tables, 1):
            recap_data.append([str(i), t["name"], t["schema"],
                "OUI" if t.get("exists") else "NON",
                fmt_num(t.get("row_count", 0)), str(t.get("column_count", 0)),
                str(t.get("custom_fields_count", 0))])
        story.append(make_table(recap_data,
            [0.8*cm, 3.6*cm, 2.5*cm, 1.8*cm, 2.1*cm, 2.0*cm, 2.2*cm], font_size=7))
        story.append(PageBreak())

        for t in tables:
            story.append(Paragraph(f"{t['name']}", styles["SubSection"]))
            if not t.get("exists"):
                story.append(Paragraph("Table absente de la base analysee.", styles["SmallText"]))
                story.append(Spacer(1, 5 * mm))
                continue
            story.append(Paragraph(
                f"Schema : {t['schema']} | Lignes : {fmt_num(t['row_count'])} | "
                f"Colonnes : {t['column_count']} | Champs personnalises : {t['custom_fields_count']}",
                styles["SmallText"]))
            story.append(Spacer(1, 2 * mm))

            col_data = [["Colonne", "Type", "Nullable"]]
            for c in t.get("columns", [])[:18]:
                col_data.append([ellipsize(c["name"], 34), c["type"], c["nullable"]])
            if len(col_data) > 1:
                story.append(make_table(col_data, [8.0*cm, 3.2*cm, 2.2*cm], header_color="#2563eb", alt="#f0f5ff"))
                story.append(Spacer(1, 2 * mm))

            if t.get("preview_columns") and t.get("preview_rows"):
                pcols = t["preview_columns"][:6]
                pdata = [[ellipsize(c, 20) for c in pcols]]
                for prow in t["preview_rows"][:5]:
                    pdata.append([ellipsize(v, 24) for v in prow[:6]])
                preview_widths = [page_width / len(pcols)] * len(pcols)
                story.append(Paragraph("Apercu - 5 lignes", styles["SmallText"]))
                story.append(make_table(pdata, preview_widths, header_color="#059669", alt="#f0fdf4", font_size=6))
                story.append(Spacer(1, 2 * mm))

            if t.get("custom_fields"):
                story.append(Paragraph("Analyse des champs personnalises (TABLE / LIBRE)", styles["SmallText"]))
                cf_data = [["Champ", "Alimente", "Vide", "%", "Distincts"]]
                for cf in t["custom_fields"]:
                    cf_data.append([ellipsize(cf["column_name"], 28), fmt_num(cf["filled_rows"]),
                        fmt_num(cf["empty_rows"]), f"{cf['fill_rate']:.2f}%", fmt_num(cf["distinct_non_empty"])])
                story.append(make_table(cf_data, [6.0*cm, 2.1*cm, 2.1*cm, 1.7*cm, 2.1*cm], header_color="#7c3aed", alt="#f5f3ff"))
                story.append(Spacer(1, 2 * mm))
                for cf in t["custom_fields"][:6]:
                    top_values = cf.get("top_values", [])
                    if not top_values:
                        continue
                    story.append(Paragraph(f"Valeurs distinctes - {cf['column_name']}", styles["SmallText"]))
                    val_data = [["Valeur", "Occur."]]
                    for item in top_values[:8]:
                        val_data.append([ellipsize(item["value"], 70), fmt_num(item["count"])])
                    story.append(make_table(val_data, [11.5*cm, 2.5*cm], header_color="#0f766e", alt="#ecfeff", font_size=7))
                    story.append(Spacer(1, 2 * mm))

            story.append(Spacer(1, 4 * mm))
            if t != tables[-1]:
                story.append(PageBreak())

    doc.build(story)
    return jsonify({"ok": True, "pdf_name": pdf_name, "pdf_path": str(pdf_path.relative_to(BASE_DIR))})


# API : télécharge un rapport PDF déjà généré.
@app.route("/api/download_report")
def api_download_report():
    if "db_config" not in session:
        return redirect(url_for("login"))
    name = request.args.get("name", "")
    if not name or ".." in name or "/" in name or "\\" in name:
        return "Fichier invalide", 400
    full = REPORTS_DIR / name
    if not full.exists():
        return "Rapport introuvable", 404
    return send_file(full, as_attachment=True, download_name=name)


# =============================================================================
# Routes Flask — SFTP
# =============================================================================

# API : renvoie l'état de la configuration SFTP (paramiko installé, .env configuré).
@app.route("/api/sftp/status")
def api_sftp_status():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    status = {"paramiko_installed": PARAMIKO_AVAILABLE, "env_configured": False, "host": "", "port": 22, "username": ""}
    try:
        cfg = get_sftp_config()
        status["env_configured"] = True
        status["host"]     = cfg["host"]
        status["port"]     = cfg["port"]
        status["username"] = cfg["username"]
    except EnvironmentError as e:
        status["error"] = str(e)
    return jsonify(status)


# API : teste la connexion au serveur SFTP.
@app.route("/api/sftp/test", methods=["POST"])
def api_sftp_test():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    return jsonify(test_sftp_connection())


# API : envoie en SFTP les fichiers finaux d'une entité (split valide/non-valide inclus).
@app.route("/api/sftp/upload_entity", methods=["POST"])
def api_sftp_upload_entity():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body          = request.get_json(force=True) or {}
    entity_id     = (body.get("entity")        or "").strip()
    project       = (body.get("project")       or "").strip()
    sftp_base_dir = (body.get("sftp_base_dir") or "").strip()
    if not entity_id or "." in entity_id or "/" in entity_id or "\\" in entity_id:
        return jsonify({"error": "entity_id invalide"}), 400
    if not project:
        return jsonify({"error": "Le champ 'project' est obligatoire."}), 400
    entity = get_entity(entity_id)
    if not entity:
        return jsonify({"error": "Entite introuvable"}), 404
    db_name = session["db_config"]["database"]
    source_csvs, source_origin = find_sftp_source_csv_for_entity(db_name, entity_id)
    if not source_csvs:
        return jsonify({"error": f"Aucun fichier CSV trouvé pour '{entity_id}'."}), 404
    logs = [f"[INFO] Démarrage upload SFTP", f"[INFO] Entité : {entity_id}", f"[INFO] Projet SFTP : {project}",
            f"[INFO] Source : {source_origin}", f"[INFO] CSV(s) : {len(source_csvs)} fichier(s) détecté(s)", f"[INFO] Base : {db_name}"]
    for csv_f in source_csvs:
        logs.append(f"[INFO]   -> {csv_f.name}")
    with tempfile.TemporaryDirectory(prefix="sftp_tmp_") as tmp_str:
        result = upload_entity_to_sftp(project_name=project, entity_id=entity_id, source_csvs=source_csvs,
                                       sftp_base_dir=sftp_base_dir, logs=logs, tmp_dir=Path(tmp_str))
    ss = result.get("split_stats", {})
    return jsonify({"ok": result["success"], "project": result["project"], "entity": entity_id,
                    "source_files": [f.name for f in source_csvs], "remote_dir": result["remote_dir"],
                    "uploaded": result["uploaded"], "failed": result["failed"],
                    "stats": {"total": ss.get("total", 0), "ok_count": ss.get("ok_count", 0),
                              "ko_count": ss.get("ko_count", 0), "taux_rejet": ss.get("taux_rejet", 0)},
                    "logs": result["logs"]})


# API : envoie en SFTP un fichier précis du projet vers le dossier distant.
@app.route("/api/sftp/upload_file", methods=["POST"])
def api_sftp_upload_file():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body            = request.get_json(force=True) or {}
    rel_path        = (body.get("path")            or "").strip()
    project         = (body.get("project")         or "").strip()
    remote_filename = (body.get("remote_filename") or "").strip()
    if not rel_path:
        return jsonify({"error": "Le champ 'path' est obligatoire."}), 400
    if not project:
        return jsonify({"error": "Le champ 'project' est obligatoire."}), 400
    local_file = (BASE_DIR / rel_path).resolve()
    if not str(local_file).startswith(str(BASE_DIR.resolve())):
        return jsonify({"error": "Chemin non autorisé."}), 403
    if not local_file.exists():
        return jsonify({"error": "Fichier introuvable."}), 404
    fname         = remote_filename or local_file.name
    project_clean = project.strip().lower().replace(" ", "_")
    remote_dir    = f"/{project_clean}"
    remote_path   = f"{remote_dir}/{fname}"
    logs = [f"[INFO] Upload manuel : {local_file.name} -> {remote_path}"]
    try:
        ssh, sftp = open_sftp_from_env()
        try:
            ensure_remote_dir(sftp, remote_dir, logs)
            ok = sftp_upload_file(sftp, local_file, remote_path, logs)
        finally:
            sftp.close(); ssh.close()
    except Exception as e:
        logs.append(f"[SFTP][ERREUR] {e}")
        return jsonify({"ok": False, "error": str(e), "logs": logs}), 500
    return jsonify({"ok": ok, "local_file": local_file.name, "remote_path": remote_path, "logs": logs})


# API : envoie en SFTP les fichiers finaux de toutes les entités de la base courante.
@app.route("/api/sftp/upload_all", methods=["POST"])
def api_sftp_upload_all():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body          = request.get_json(force=True) or {}
    project       = (body.get("project")       or "").strip()
    sftp_base_dir = (body.get("sftp_base_dir") or "").strip()
    if not project:
        return jsonify({"error": "Le champ 'project' est obligatoire."}), 400
    db_name     = session["db_config"]["database"]
    entities    = scan_entities()
    results     = []
    global_logs = [f"[INFO] Upload global SFTP -> projet '{project}' | base '{db_name}'",
                   f"[INFO] {len(entities)} entité(s) détectée(s)"]
    for entity in entities:
        eid = entity["id"]
        source_csvs, source_origin = find_sftp_source_csv_for_entity(db_name, eid)
        if not source_csvs:
            global_logs.append(f"[SKIP] {eid} : aucun CSV trouvé.")
            results.append({"entity": eid, "skipped": True, "reason": "Aucun CSV disponible"})
            continue
        global_logs.append(f"[INFO] {eid} : source = {source_origin} | {len(source_csvs)} CSV(s)")
        entity_logs = []
        with tempfile.TemporaryDirectory(prefix=f"sftp_{eid}_") as tmp_str:
            result = upload_entity_to_sftp(project_name=project, entity_id=eid, source_csvs=source_csvs,
                                           sftp_base_dir=sftp_base_dir, logs=entity_logs, tmp_dir=Path(tmp_str))
        global_logs.extend(entity_logs)
        ss = result.get("split_stats", {})
        results.append({"entity": eid, "skipped": False, "success": result["success"],
                        "uploaded": result["uploaded"], "failed": result["failed"],
                        "stats": {"total": ss.get("total", 0), "ok_count": ss.get("ok_count", 0),
                                  "ko_count": ss.get("ko_count", 0), "taux_rejet": ss.get("taux_rejet", 0)}})
    total_ok   = sum(1 for r in results if not r.get("skipped") and     r.get("success"))
    total_fail = sum(1 for r in results if not r.get("skipped") and not r.get("success"))
    total_skip = sum(1 for r in results if     r.get("skipped"))
    global_logs.append(f"[INFO] Terminé : {total_ok} succès | {total_fail} échec(s) | {total_skip} ignoré(s)")
    return jsonify({"ok": total_fail == 0, "project": project.strip().lower().replace(" ", "_"),
                    "summary": {"total_entities": len(entities), "success": total_ok, "failed": total_fail, "skipped": total_skip},
                    "results": results, "logs": global_logs})


# =============================================================================
# Routes Flask — MODE AVANCE
# =============================================================================

# Route '/mode_avance' : affiche la page du pipeline avancé multi-bases.
@app.route("/mode_avance")
def mode_avance_page():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("mode_avance.html", db_config=session["db_config"], entities=scan_entities())


# API : lance le pipeline avancé multi-bases (avec base de référence optionnelle).
@app.route("/api/advanced/run", methods=["POST"])
def api_advanced_run():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body         = request.get_json(force=True) or {}
    databases    = parse_databases_input(body.get("databases", []))
    entity_cfgs  = body.get("entities", [])
    project_name = (body.get("project_name") or "avance").strip()
    reference_db = (body.get("reference_db") or "").strip()
    if not databases:
        return jsonify({"error": "Au moins une base de donnees est requise."}), 400
    if not entity_cfgs:
        return jsonify({"error": "Selectionnez au moins une entite."}), 400
    if not project_name or re.search(r'[/\\<>|*?"]', project_name):
        return jsonify({"error": "Nom de projet invalide."}), 400
    ref_upper = reference_db.strip().upper()
    all_dbs: list = []
    if ref_upper:
        all_dbs.append(ref_upper)
    for db in databases:
        if db.strip().upper() != ref_upper:
            all_dbs.append(db.strip())
    total_steps = len(all_dbs) * len(entity_cfgs)
    cfg    = session["db_config"]
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "running",
        "logs": [
            "Pipeline Avance  Demarrage",
            f"Bases      :  {', '.join(databases)}",
            f"BDD Ref    :  {reference_db if reference_db else '(aucune)'}",
            f"Entites    :  {', '.join(e.get('id','?') for e in entity_cfgs)}",
            f"Projet     :  output_{project_name}/",
            f"Heure      :  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        ],
        "started":  datetime.now().isoformat(),
        "progress": {"done": 0, "total": total_steps, "current": ""},
        "results":  [],
    }
    threading.Thread(
        target=run_advanced_pipeline,
        args=(job_id, cfg, databases, entity_cfgs, project_name),
        kwargs={"reference_db": reference_db},
        daemon=True,
    ).start()
    return jsonify({"job_id": job_id, "output_dir": f"output_{project_name}",
                    "total_steps": total_steps, "reference_db": reference_db})


# API : liste les fichiers de sortie du mode avancé, organisés par projet/base/entité/run.
@app.route("/api/advanced/output_files")
def api_advanced_output_files():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    project_name = request.args.get("project", "").strip()
    if project_name:
        safe  = re.sub(r"[^a-zA-Z0-9_\-]", "_", project_name)
        dirs_ = [BASE_DIR / f"output_{safe}"]
    else:
        dirs_ = [
            d for d in BASE_DIR.iterdir()
            if d.is_dir()
            and d.name.startswith("output_")
            and d.name not in ("output", "output_regle_de_gestion")
        ]
    projects = []
    for odir in sorted(dirs_):
        if not odir.exists():
            continue
        dbs = []
        for db_dir in sorted(odir.iterdir()):
            if not db_dir.is_dir():
                continue
            ents = []
            for ent_dir in sorted(db_dir.iterdir()):
                if not ent_dir.is_dir():
                    continue
                runs = []
                for run_dir in sorted(ent_dir.iterdir(), reverse=True):
                    if not run_dir.is_dir():
                        continue
                    run_files = []
                    for subdir_name in ("transformation", "regles"):
                        subdir = run_dir / subdir_name
                        if not subdir.exists():
                            continue
                        # ── BUG 2 CORRIGÉ : indentation + : + continue ──
                        for f in sorted(subdir.glob("*"), reverse=True):
                            if f.suffix.lower() not in (".csv", ".xlsx"):
                                continue
                            rel = f.relative_to(BASE_DIR)
                            run_files.append({
                                "name":    f.name,
                                "path":    str(rel),
                                "size_kb": round(f.stat().st_size / 1024, 1),
                                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
                                "type":    subdir_name,
                            })
                    if run_files:
                        runs.append({"run": run_dir.name, "files": run_files})
                if runs:
                    ents.append({"entity": ent_dir.name, "runs": runs})
            if ents:
                dbs.append({"database": db_dir.name, "entities": ents})
        projects.append({"project": odir.name, "databases": dbs})
    return jsonify({"projects": projects})


# =============================================================================
# DÉPLOIEMENT MACHINE LEARNING — Détection d'anomalies & doublons
# =============================================================================
# Cette section déploie les deux modèles ML entraînés (fichiers .joblib à la
# racine du projet) derrière une interface web : page /ml + API d'upload,
# de détection et de téléchargement des résultats.
# =============================================================================

# Dossiers de travail du module ML : fichiers importés et résultats produits.
ML_DIR     = BASE_DIR / "ml_deploy"
ML_UPLOADS = ML_DIR / "uploads"
ML_RESULTS = ML_DIR / "results"
for _d in (ML_DIR, ML_UPLOADS, ML_RESULTS):
    _d.mkdir(parents=True, exist_ok=True)

# Chemins des deux modèles entraînés (sauvegardés via joblib).
MODELE_ANOMALIES = BASE_DIR / "modele_anomalies_conditions_reglement.joblib"
MODELE_DOUBLONS  = BASE_DIR / "modele_doublons_tfidf.joblib"


# Lecture robuste d'un CSV importé (détecte le séparateur ; ou , et lit tout en texte).
def _read_ml_csv(path):
    import pandas as pd
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        first = f.readline()
    sep = ";" if first.count(";") >= first.count(",") else ","
    df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df, sep


# Prépare l'aperçu (colonnes + premières lignes) d'un DataFrame pour l'interface.
def _ml_preview(df, n=20):
    cols = list(df.columns)
    rows = df.head(n).astype(str).values.tolist()
    return cols, rows


# Normalise un nom de tiers exactement comme à l'entraînement du modèle de doublons.
def _normalize_name(texte, formes):
    s = str(texte).upper().strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    if formes:
        if isinstance(formes, (list, tuple, set)):
            pattern = r"\b(" + "|".join(re.escape(str(x)) for x in formes) + r")\b"
            s = re.sub(pattern, " ", s)
        else:
            try:
                s = re.sub(formes, " ", s)
            except re.error:
                pass
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Retrouve le vrai nom d'une colonne dans le fichier en ignorant accents, casse et espaces.
def _match_col(df_columns, wanted):
    def norm(s):
        s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"\s+", " ", s).strip().lower()
    cible = norm(wanted)
    for c in df_columns:
        if norm(c) == cible:
            return c
    return None


# Objectif 1 — Applique le Random Forest pour signaler les conditions de règlement incohérentes.
def run_anomaly_detection(csv_path, seuil=0.30):
    import joblib, pandas as pd
    if not MODELE_ANOMALIES.exists():
        raise FileNotFoundError("Modèle d'anomalies introuvable : modele_anomalies_conditions_reglement.joblib")

    b = joblib.load(MODELE_ANOMALIES)
    modele            = b["modele"]
    enc_cible         = b["encodeur_cible"]
    cible             = b["cible"]
    predicteurs       = b["predicteurs"]
    colonnes_encodees = b["colonnes_encodees"]

    df, sep = _read_ml_csv(csv_path)
    logs = [f"[INFO] {len(df)} tiers chargés", f"[INFO] Cible : {cible}"]

    # Résolution tolérante des colonnes (accents/casse/espaces) vers les vrais noms du fichier.
    cible_reel = _match_col(df.columns, cible)
    if cible_reel is None:
        raise ValueError(f"Colonne cible manquante : {cible}")
    pred_map = {p: _match_col(df.columns, p) for p in predicteurs}
    manquantes = [p for p, r in pred_map.items() if r is None]
    if manquantes:
        raise ValueError(f"Colonnes prédictrices manquantes : {manquantes}")

    # Renommer les colonnes du fichier vers les noms attendus par le modèle.
    rename = {r: p for p, r in pred_map.items() if r != p}
    if cible_reel != cible:
        rename[cible_reel] = cible
    work = df.rename(columns=rename).copy()
    for c in predicteurs + [cible]:
        work[c] = work[c].fillna("").astype(str)

    X = pd.get_dummies(work[predicteurs], prefix_sep="=")
    X = X.reindex(columns=colonnes_encodees, fill_value=0)
    logs.append(f"[INFO] Encodage one-hot aligné sur {len(colonnes_encodees)} colonnes")

    proba   = modele.predict_proba(X)
    classes = list(modele.classes_)

    pred_idx        = proba.argmax(axis=1)
    pred_enc        = [classes[i] for i in pred_idx]
    valeur_attendue = enc_cible.inverse_transform(pred_enc)
    proba_attendue  = proba.max(axis=1)

    proba_vraie = []
    for i, val in enumerate(work[cible]):
        if val in enc_cible.classes_:
            enc = enc_cible.transform([val])[0]
            if enc in classes:
                proba_vraie.append(float(proba[i, classes.index(enc)]))
                continue
        proba_vraie.append(0.0)

    df["proba_coherence"] = [round(p, 3) for p in proba_vraie]
    df["valeur_attendue"] = valeur_attendue
    df["proba_attendue"]  = [round(float(p), 3) for p in proba_attendue]
    df["anomalie"]        = ["OUI" if p < seuil else "NON" for p in proba_vraie]

    nb = int(sum(1 for p in proba_vraie if p < seuil))
    taux = round(nb / len(df) * 100, 2) if len(df) else 0.0
    logs.append(f"[OK] {nb} anomalie(s) détectée(s) sur {len(df)} tiers ({taux}%)")

    df_anom = df[df["anomalie"] == "OUI"].copy()
    return df, df_anom, sep, {"total": len(df), "anomalies": nb, "taux": taux}, logs


# Objectif 2 — Applique le TF-IDF pour regrouper les tiers en doublons (similarité des noms).
def run_duplicate_detection(csv_path, seuil=None):
    import joblib, pandas as pd, collections
    from sklearn.neighbors import NearestNeighbors
    if not MODELE_DOUBLONS.exists():
        raise FileNotFoundError("Modèle de doublons introuvable : modele_doublons_tfidf.joblib")

    b = joblib.load(MODELE_DOUBLONS)
    vectoriseur = b["vectoriseur"]
    champs      = b["champs"]
    seuil       = float(seuil) if seuil is not None else float(b.get("seuil", 0.85))
    k_voisins   = int(b.get("k_voisins", 6))
    formes      = b.get("formes_juridiques")

    df, sep = _read_ml_csv(csv_path)
    logs = [f"[INFO] {len(df)} tiers chargés", f"[INFO] Champs comparés : {champs}", f"[INFO] Seuil : {seuil}"]

    champ_map = {c: _match_col(df.columns, c) for c in champs}
    manquantes = [c for c, r in champ_map.items() if r is None]
    if manquantes:
        raise ValueError(f"Colonnes manquantes : {manquantes}. Le modèle attend : {champs}")
    # Renommer vers les noms attendus par le modèle (tolérance accents/casse).
    df = df.rename(columns={r: c for c, r in champ_map.items() if r != c})

    df["__cle"] = df[champs].apply(lambda r: _normalize_name(" ".join(str(x) for x in r.values), formes), axis=1)
    work = df[df["__cle"].str.strip() != ""].reset_index(drop=True)
    n = len(work)
    if n < 2:
        raise ValueError("Pas assez de lignes exploitables après normalisation.")

    M  = vectoriseur.transform(work["__cle"])
    k  = min(k_voisins, n)
    nn = NearestNeighbors(n_neighbors=k, metric="cosine").fit(M)
    dist, idx = nn.kneighbors(M)
    logs.append(f"[INFO] Recherche des plus proches voisins ({k} voisins)")

    paires = []
    for a in range(n):
        for d, bb in zip(dist[a], idx[a]):
            bb = int(bb)
            if a < bb and (1 - d) >= seuil:
                paires.append((a, bb))
    logs.append(f"[INFO] {len(paires)} paire(s) au-dessus du seuil")

    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    for a, bb in paires:
        ra, rb = find(a), find(bb)
        if ra != rb:
            parent[ra] = rb

    groupes = collections.defaultdict(list)
    for i in range(n):
        groupes[find(i)].append(i)
    groupes = {g: m for g, m in groupes.items() if len(m) > 1}

    code_col = next((c for c in work.columns if c.strip().lower() in ("code client *", "code client", "code")), None)
    rows = []
    for gid, membres in enumerate(groupes.values(), start=1):
        for m in membres:
            row = {"groupe_doublon": gid}
            if code_col:
                row["Code client"] = work.loc[m, code_col]
            for ch in champs:
                row[ch] = work.loc[m, ch]
            rows.append(row)

    res = pd.DataFrame(rows)
    fiches = int(sum(len(m) for m in groupes.values()))
    logs.append(f"[OK] {len(groupes)} groupe(s) de doublons ({fiches} fiches concernées)")
    return res, sep, {"total": n, "groupes": len(groupes), "fiches": fiches}, logs


# Route '/ml' : affiche la page de déploiement des deux modèles (interface 2 blocs).
@app.route("/ml")
def ml_page():
    if "db_config" not in session:
        return redirect(url_for("login"))
    return render_template("ml_deployment.html", db_config=session["db_config"])


# API : reçoit un CSV importé, le stocke et renvoie un aperçu (colonnes + premières lignes).
@app.route("/api/ml/upload", methods=["POST"])
def api_ml_upload():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    if "file" not in request.files:
        return jsonify({"error": "Aucun fichier reçu."}), 400
    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Seuls les fichiers CSV sont acceptés."}), 400
    token = uuid.uuid4().hex
    dest  = ML_UPLOADS / f"{token}.csv"
    f.save(dest)
    try:
        df, _ = _read_ml_csv(dest)
        cols, rows = _ml_preview(df, 20)
        return jsonify({"token": token, "filename": f.filename,
                        "rows": len(df), "columns": cols, "preview_rows": rows})
    except Exception as e:
        return jsonify({"error": f"Lecture impossible : {e}"}), 500


# API : lance la détection d'anomalies sur le CSV importé et renvoie stats + résultat.
@app.route("/api/ml/run_anomalies", methods=["POST"])
def api_ml_run_anomalies():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body  = request.get_json(force=True) or {}
    token = (body.get("token") or "").strip()
    seuil = float(body.get("seuil", 0.30))
    src   = ML_UPLOADS / f"{token}.csv"
    if not token or not src.exists():
        return jsonify({"error": "Fichier source introuvable (ré-importez le CSV)."}), 404
    try:
        df_full, df_anom, sep, stats, logs = run_anomaly_detection(src, seuil)
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        out  = ML_RESULTS / f"anomalies_{token}_{ts}.csv"
        df_full.to_csv(out, sep=sep, index=False, encoding="utf-8-sig")
        cols, rows = _ml_preview(df_anom if len(df_anom) else df_full, 20)
        return jsonify({"logs": logs, "stats": stats,
                        "result_path": str(out.relative_to(BASE_DIR)),
                        "result_columns": cols, "result_preview": rows,
                        "summary": f"{stats['anomalies']} anomalie(s) sur {stats['total']} tiers"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : lance la détection de doublons sur le CSV importé et renvoie stats + résultat.
@app.route("/api/ml/run_doublons", methods=["POST"])
def api_ml_run_doublons():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body  = request.get_json(force=True) or {}
    token = (body.get("token") or "").strip()
    seuil = body.get("seuil", None)
    src   = ML_UPLOADS / f"{token}.csv"
    if not token or not src.exists():
        return jsonify({"error": "Fichier source introuvable (ré-importez le CSV)."}), 404
    try:
        res, sep, stats, logs = run_duplicate_detection(src, seuil)
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = ML_RESULTS / f"doublons_{token}_{ts}.csv"
        res.to_csv(out, sep=sep, index=False, encoding="utf-8-sig")
        cols, rows = _ml_preview(res, 20)
        return jsonify({"logs": logs, "stats": stats,
                        "result_path": str(out.relative_to(BASE_DIR)),
                        "result_columns": cols, "result_preview": rows,
                        "summary": f"{stats['groupes']} groupe(s) de doublons ({stats['fiches']} fiches)"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# API : télécharge un fichier de résultat ML (vérifie qu'il reste dans ml_deploy/results).
@app.route("/api/ml/download")
def api_ml_download():
    if "db_config" not in session:
        return redirect(url_for("login"))
    rel  = request.args.get("path", "")
    full = (BASE_DIR / rel).resolve()
    if not str(full).startswith(str(ML_RESULTS.resolve())) or not full.exists():
        return "Fichier introuvable", 404
    return send_file(full, as_attachment=True)


# API : supprime un fichier de résultat ML (uniquement dans ml_deploy/results).
@app.route("/api/ml/delete", methods=["POST"])
def api_ml_delete():
    if "db_config" not in session:
        return jsonify({"error": "Non connecte"}), 401
    body = request.get_json(force=True) or {}
    rel  = (body.get("path") or "").strip()
    full = (BASE_DIR / rel).resolve()
    if not str(full).startswith(str(ML_RESULTS.resolve())):
        return jsonify({"error": "Chemin non autorisé."}), 403
    if not full.exists():
        return jsonify({"error": "Fichier introuvable."}), 404
    try:
        name = full.name
        full.unlink()
        return jsonify({"success": True, "deleted": name})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    app.run(debug=True, port=5000, use_reloader=False)