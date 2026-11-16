import csv
import time
import requests

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SP.POP.TOTL": "Population, total",
    "SL.UEM.TOTL.ZS": "Unemployment (% of labor force)",
    "SP.DYN.LE00.IN": "Life expectancy at birth (years)",
}
BASE = "https://api.worldbank.org/v2/country/{c}/indicator/{i}"


def get_json(url, params, tries=4):
    """GET avec retries sur les erreurs réseau (l'API World Bank est lente)."""
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, timeout=180)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            if attempt == tries:
                raise
            wait = 5 * attempt
            print(f"Tentative {attempt} échouée ({type(e).__name__}), nouvel essai dans {wait}s")
            time.sleep(wait)


def fetch_indicator(code):
    payload = get_json(
        BASE.format(c=";".join(COUNTRIES), i=code),
        {"format": "json", "per_page": 1000, "date": "2015:2024"},
    )
    # HTTP 200 mais erreur JSON -> on lève explicitement (point 7 du PDF)
    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        raise ValueError(f"Réponse World Bank invalide pour {code}: {str(payload)[:300]}")
    return payload[1]


def build_csv(ds, path):
    n = 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["country_code", "country_name", "indicator_code",
                    "indicator_label", "year", "value", "load_date"])
        for code, label in INDICATORS.items():
            for d in fetch_indicator(code):
                if d["value"] is None:
                    continue
                w.writerow([d["countryiso3code"], d["country"]["value"], code,
                            label, int(d["date"]), d["value"], ds])
                n += 1
    if n == 0:
        raise ValueError("Aucune ligne extraite")
    return n


if __name__ == "__main__":
    print(build_csv("2026-11-16", "/tmp/indicators.csv"), "lignes")
