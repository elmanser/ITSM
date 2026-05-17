from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"

RAW_INCIDENTS_DIR = DATA_DIR / "batch"
RAW_DEMANDES_DIR = DATA_DIR / "batch"
PROCESSED_INCIDENTS_DIR = DATA_DIR / "processed" / "incidents"
PROCESSED_DEMANDES_DIR = DATA_DIR / "processed" / "demandes"
CURATED_DIR = DATA_DIR / "curated"
QUALITY_DIR = DATA_DIR / "quality"
ML_DIR = DATA_DIR / "ml"
KPI_DIR = CURATED_DIR / "kpis"

# Ensure all output directories exist
for d in [PROCESSED_INCIDENTS_DIR, PROCESSED_DEMANDES_DIR, CURATED_DIR, QUALITY_DIR, ML_DIR, KPI_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Expected Columns
INCIDENTS_EXPECTED_COLUMNS = [
    "Date d'émission", "N° d'Incident", "Priorité", "Urgence", "Date de fin",
    "Sujet (complet)", "Localisation (dernier niveau)", "Entité", "Description",
    "Résolu par (groupe)", "Résolu par (intervenant)", "Statut", "Origine",
    "Dernière mise à jour"
]

DEMANDES_EXPECTED_COLUMNS = [
    "Date d'émission", "N° de demande", "Statut", "Libellé du service",
    "Libellé complet", "Description", "Date de fin", "Résolu par (groupe)",
    "Résolu par (intervenant)", "Localisation", "Entité du bénéficiaire"
]

# Normalization maps
STATUS_CLOSED = [
    "résolu", "clôturé", "closed", "resolved", "terminé", "fermé"
]

STATUS_OPEN = [
    "en cours", "escaladé", "suspendu", "en attente", "open", "in progress", 
    "nouveau", "attente", "a prendre en compte"
]

LONG_RESOLUTION_HOURS = 24.0
