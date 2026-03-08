#!/usr/bin/env python3
"""
kununu Bewertungen Scraper

Scrapt Mitarbeiter- und Bewerberbewertungen eines Unternehmens von kununu.com
und exportiert sie als CSV-Dateien (employee_rows.csv, candidates_rows.csv).

Nutzung:
    python kununu_bewertungen_scraper.py "PLEdoc"
    python kununu_bewertungen_scraper.py --url https://www.kununu.com/de/pledoc
    python kununu_bewertungen_scraper.py "Deutsche Post" --max-seiten 10

Voraussetzungen:
    pip install requests beautifulsoup4 playwright
    python -m playwright install chromium
"""

import csv
import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_VERFUEGBAR = True
except ImportError:
    PLAYWRIGHT_VERFUEGBAR = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.kununu.com"

# ---------------------------------------------------------------------------
# CSV-Feldnamen (exakt wie in den Beispieldateien)
# ---------------------------------------------------------------------------

MITARBEITER_FELDER = [
    "titel", "status", "datum", "update_datum",
    "durchschnittsbewertung", "gerundete_durchschnittsbewertung",
    "jobbeschreibung", "gut_am_arbeitgeber_finde_ich",
    "schlecht_am_arbeitgeber_finde_ich", "verbesserungsvorschlaege",
    "sternebewertung_arbeitsatmosphaere", "sternebewertung_image",
    "sternebewertung_work_life_balance",
    "sternebewertung_karriere_weiterbildung",
    "sternebewertung_gehalt_sozialleistungen",
    "sternebewertung_kollegenzusammenhalt",
    "sternebewertung_umwelt_sozialbewusstsein",
    "sternebewertung_vorgesetztenverhalten",
    "sternebewertung_kommunikation",
    "sternebewertung_interessante_aufgaben",
    "sternebewertung_umgang_mit_aelteren_kollegen",
    "sternebewertung_arbeitsbedingungen",
    "sternebewertung_gleichberechtigung",
    "created_at", "updated_at", "company_id",
    "arbeitsatmosphaere", "image", "work_life_balance",
    "karriere_weiterbildung", "gehalt_sozialleistungen",
    "kollegenzusammenhalt", "umwelt_sozialbewusstsein",
    "vorgesetztenverhalten", "kommunikation", "interessante_aufgaben",
    "umgang_mit_aelteren_kollegen", "arbeitsbedingungen", "gleichberechtigung",
]

BEWERBER_FELDER = [
    "titel", "status", "datum", "update_datum",
    "durchschnittsbewertung", "gerundete_durchschnittsbewertung",
    "stellenbeschreibung", "verbesserungsvorschlaege",
    "sternebewertung_erklaerung_der_weiteren_schritte",
    "sternebewertung_zufriedenstellende_reaktion",
    "sternebewertung_vollstaendigkeit_der_infos",
    "sternebewertung_zufriedenstellende_antworten",
    "sternebewertung_angenehme_atmosphaere",
    "sternebewertung_professionalitaet_des_gespraechs",
    "sternebewertung_wertschaetzende_behandlung",
    "sternebewertung_erwartbarkeit_des_prozesses",
    "sternebewertung_zeitgerechte_zu_oder_absage",
    "sternebewertung_schnelle_antwort",
    "created_at", "updated_at", "company_id",
]

# ---------------------------------------------------------------------------
# Kategorie-Mappings  (kununu Label → CSV-Feld-Fragment)
# ---------------------------------------------------------------------------

MITARBEITER_KATEGORIEN = {
    "Arbeitsatmosphäre": "arbeitsatmosphaere",
    "Image": "image",
    "Work-Life-Balance": "work_life_balance",
    "Karriere/Weiterbildung": "karriere_weiterbildung",
    "Gehalt/Sozialleistungen": "gehalt_sozialleistungen",
    "Kollegenzusammenhalt": "kollegenzusammenhalt",
    "Umwelt-/Sozialbewusstsein": "umwelt_sozialbewusstsein",
    "Vorgesetztenverhalten": "vorgesetztenverhalten",
    "Kommunikation": "kommunikation",
    "Interessante Aufgaben": "interessante_aufgaben",
    "Umgang mit älteren Kollegen": "umgang_mit_aelteren_kollegen",
    "Arbeitsbedingungen": "arbeitsbedingungen",
    "Gleichberechtigung": "gleichberechtigung",
}

BEWERBER_KATEGORIEN = {
    "Erklärung der weiteren Schritte": "erklaerung_der_weiteren_schritte",
    "Zufriedenstellende Reaktion": "zufriedenstellende_reaktion",
    "Vollständigkeit der Infos": "vollstaendigkeit_der_infos",
    "Zufriedenstellende Antworten": "zufriedenstellende_antworten",
    "Angenehme Atmosphäre": "angenehme_atmosphaere",
    "Professionalität des Gesprächs": "professionalitaet_des_gespraechs",
    "Wertschätzende Behandlung": "wertschaetzende_behandlung",
    "Erwartbarkeit des Prozesses": "erwartbarkeit_des_prozesses",
    "Zeitgerechte Zu- oder Absage": "zeitgerechte_zu_oder_absage",
    "Schnelle Antwort": "schnelle_antwort",
}

# Übersetzung der reviewer-Rollen auf kununu → CSV-Werte
MITARBEITER_TYP_MAP = {
    "Angestellte/r oder Arbeiter/in": "employee",
    "Angestellte/r": "employee",
    "Arbeiter/in": "employee",
    "Werkstudent/in": "student",
    "Auszubildende:r": "trainee",
    "Führungskraft": "manager",
    "Zeitarbeit": "temp",
    "Freelancer": "freelancer",
    "Praktikant/in": "intern",
}

BEWERBER_STATUS_MAP = {
    "Zusage": "hired",
    "Absage": "deferred",
    "Eingestellt": "hired",
    "Hat sich beworben": "",
    "Bewerber/in hat sich selbst anders entschieden": "",
}


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def pause(min_sec: float = 2.0, max_sec: float = 5.0) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def runde_auf_halbe(wert: float) -> float:
    """Runde auf die nächste 0.5-Stufe (z.B. 4.8 → 5.0, 2.2 → 2.0)."""
    return round(wert * 2) / 2


def jetzt_iso() -> str:
    """Aktuelle Zeit als ISO-String mit Zeitzone."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f+00")


def text_bereinigen(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def cookie_banner_schliessen(page) -> None:
    for sel in [
        "button:has-text('Akzeptieren')",
        "button:has-text('Alle akzeptieren')",
        "button:has-text('Accept All')",
        "#onetrust-accept-btn-handler",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.click()
                page.wait_for_timeout(1000)
                return
        except Exception:
            continue


MONATE = {
    "Januar": "01", "Februar": "02", "März": "03", "April": "04",
    "Mai": "05", "Juni": "06", "Juli": "07", "August": "08",
    "September": "09", "Oktober": "10", "November": "11", "Dezember": "12",
}


def datum_parsen(text: str) -> str:
    """Parst ein deutsches Datum und gibt 'YYYY-MM-DD 00:00:00+00' zurück."""
    if not text:
        return ""
    text = text.strip()

    # "17. Juli 2024"
    for name, num in MONATE.items():
        m = re.search(rf"(\d{{1,2}})\.\s*{re.escape(name)}\s*(\d{{4}})", text)
        if m:
            return f"{m.group(2)}-{num}-{m.group(1).zfill(2)} 00:00:00+00"

    # "Juli 2024"
    for name, num in MONATE.items():
        m = re.search(rf"{re.escape(name)}\s*(\d{{4}})", text)
        if m:
            return f"{m.group(1)}-{num}-01 00:00:00+00"

    # ISO: 2024-07-17
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return f"{m.group(1)} 00:00:00+00"

    return ""


def score_aus_text(text: str) -> float | None:
    """Extrahiert einen numerischen Score (z.B. '4,80') aus Text."""
    m = re.search(r"(\d)[.,](\d{1,2})", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d)", text)
    if m:
        return float(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Firmensuche auf kununu
# ---------------------------------------------------------------------------

def firmen_url_finden(firmenname: str, page) -> str | None:
    """Sucht ein Unternehmen auf kununu und gibt die Profil-URL zurück."""
    search_url = f"{BASE_URL}/de/search#/?q={quote_plus(firmenname)}"
    log.info("Suche '%s' auf kununu …", firmenname)

    page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    cookie_banner_schliessen(page)
    page.wait_for_timeout(4000)

    soup = BeautifulSoup(page.content(), "html.parser")
    links = soup.find_all("a", href=re.compile(r"^/de/[\w-]+/?$"))

    skip = {
        "/de/search", "/de/login", "/de/gehalt", "/de/jobs",
        "/de/beste-arbeitgeber", "/de/user", "/de/insights",
    }

    for link in links:
        href = link.get("href", "").rstrip("/")
        if href in skip or "/kommentare" in href or "/bewerbung" in href:
            continue
        name_lower = firmenname.lower()
        href_clean = href.replace("/de/", "").replace("-", " ").lower()
        aria = link.get("aria-label", "").lower()
        link_text = link.get_text(strip=True).lower()

        if name_lower in href_clean or name_lower in aria or name_lower in link_text:
            url = f"{BASE_URL}{href}"
            log.info("Unternehmen gefunden: %s", url)
            return url

    # Fallback: erstes Ergebnis
    for link in links:
        href = link.get("href", "").rstrip("/")
        if href not in skip and len(href) > 5:
            url = f"{BASE_URL}{href}"
            log.info("Erstes Suchergebnis gewählt: %s", url)
            return url

    log.error("Unternehmen '%s' nicht gefunden.", firmenname)
    return None


# ---------------------------------------------------------------------------
# Seite laden (Playwright)
# ---------------------------------------------------------------------------

def seite_laden(page, url: str) -> BeautifulSoup:
    """Lädt eine Seite mit Playwright und gibt ein BeautifulSoup-Objekt zurück."""
    for versuch in range(3):
        try:
            # Erst domcontentloaded abwarten (schneller), dann manuell warten
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Warten bis Inhalte gerendert sind
            page.wait_for_timeout(3000)
            cookie_banner_schliessen(page)
            page.wait_for_timeout(1000)
            break
        except Exception as e:
            if versuch < 2:
                log.warning("  Timeout bei %s (Versuch %d/3), erneuter Versuch …", url, versuch + 1)
                pause(3.0, 6.0)
            else:
                log.error("  Seite konnte nicht geladen werden: %s", e)
                return BeautifulSoup("", "html.parser")

    # Runterscrollen um Lazy-Loading-Inhalte zu triggern
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)

    return BeautifulSoup(page.content(), "html.parser")


# ---------------------------------------------------------------------------
# __NEXT_DATA__ Extraktion  (zuverlässigste Methode)
# ---------------------------------------------------------------------------

def next_data_extrahieren(soup: BeautifulSoup) -> dict | None:
    """Versucht __NEXT_DATA__ JSON aus der Seite zu extrahieren."""
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        try:
            return json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def bewertungen_aus_next_data(data: dict, typ: str) -> list[dict] | None:
    """Extrahiert Bewertungen aus __NEXT_DATA__ falls vorhanden."""
    if not data:
        return None

    # Rekursiv nach reviews/applications suchen
    reviews = _rekursiv_suchen(data, typ)
    if reviews:
        return reviews
    return None


def _rekursiv_suchen(obj, typ: str, tiefe: int = 0) -> list[dict] | None:
    """Durchsucht verschachtelte Datenstruktur nach Bewertungsdaten."""
    if tiefe > 15:
        return None

    if isinstance(obj, dict):
        # Typische Schlüssel für Bewertungslisten
        suchbegriffe = (
            ["reviews", "commentList", "comments", "employeeReviews"]
            if typ == "mitarbeiter"
            else ["applications", "applicationReviews", "candidateReviews",
                  "bewerbungen"]
        )
        for key in suchbegriffe:
            if key in obj and isinstance(obj[key], list) and obj[key]:
                return obj[key]

        for val in obj.values():
            result = _rekursiv_suchen(val, typ, tiefe + 1)
            if result:
                return result

    elif isinstance(obj, list):
        for item in obj:
            result = _rekursiv_suchen(item, typ, tiefe + 1)
            if result:
                return result

    return None


def mitarbeiter_aus_json(raw_reviews: list[dict], company_id: int) -> list[dict]:
    """Konvertiert rohe JSON-Bewertungsdaten in das CSV-Format (Mitarbeiter)."""
    ergebnisse = []
    jetzt = jetzt_iso()

    for r in raw_reviews:
        row = {f: "" for f in MITARBEITER_FELDER}
        row["company_id"] = company_id
        row["created_at"] = jetzt
        row["updated_at"] = jetzt

        # Titel
        row["titel"] = r.get("title", "") or r.get("titel", "")

        # Datum
        date_str = r.get("date", "") or r.get("createdAt", "") or r.get("datum", "")
        row["datum"] = datum_parsen(str(date_str)) if date_str else ""

        # Status (1.0 = aktuell, 0.0 = ehemalig)
        reviewer_type = r.get("reviewerType", "") or r.get("employeeType", "")
        if isinstance(reviewer_type, str) and "ex" in reviewer_type.lower():
            row["status"] = 0.0
        elif reviewer_type:
            row["status"] = 1.0

        # Jobbeschreibung: "typ, abteilung"
        typ_label = reviewer_type
        for prefix in ("Ex-", "ex-"):
            if isinstance(typ_label, str):
                typ_label = typ_label.removeprefix(prefix)
        mitarbeiter_typ = MITARBEITER_TYP_MAP.get(typ_label, "employee")
        abteilung = r.get("department", "") or r.get("abteilung", "") or "Sonstige"
        row["jobbeschreibung"] = f"{mitarbeiter_typ}, {abteilung}"

        # Textfelder
        row["gut_am_arbeitgeber_finde_ich"] = r.get("pro", "") or r.get("positive", "")
        row["schlecht_am_arbeitgeber_finde_ich"] = r.get("contra", "") or r.get("negative", "")
        row["verbesserungsvorschlaege"] = r.get("suggestions", "") or r.get("improvement", "")

        # Sternebewertungen
        ratings = r.get("ratings", {}) or r.get("categories", {})
        if isinstance(ratings, dict):
            _map_sterne_json(ratings, row, MITARBEITER_KATEGORIEN, "sternebewertung_")

        # Gesamtbewertung
        overall = r.get("score", None) or r.get("rating", None) or r.get("totalScore", None)
        if overall is not None:
            try:
                val = float(overall)
                row["durchschnittsbewertung"] = f"{val:.2f}"
                row["gerundete_durchschnittsbewertung"] = f"{runde_auf_halbe(val):.2f}"
            except (ValueError, TypeError):
                pass
        else:
            _berechne_durchschnitt(row, MITARBEITER_KATEGORIEN)

        # Kategorie-Textkommentare
        comments = r.get("categoryComments", {}) or r.get("categoryTexts", {})
        if isinstance(comments, dict):
            for label, feld in MITARBEITER_KATEGORIEN.items():
                text = comments.get(label, "") or comments.get(feld, "")
                if text:
                    row[feld] = text

        ergebnisse.append(row)

    return ergebnisse


def bewerber_aus_json(raw_reviews: list[dict], company_id: int) -> list[dict]:
    """Konvertiert rohe JSON-Bewertungsdaten in das CSV-Format (Bewerber)."""
    ergebnisse = []
    jetzt = jetzt_iso()

    for r in raw_reviews:
        row = {f: "" for f in BEWERBER_FELDER}
        row["company_id"] = company_id
        row["created_at"] = jetzt
        row["updated_at"] = jetzt

        row["titel"] = r.get("title", "") or r.get("titel", "")

        date_str = r.get("date", "") or r.get("createdAt", "")
        row["datum"] = datum_parsen(str(date_str)) if date_str else ""

        # Status: hired / deferred
        status_raw = r.get("status", "") or r.get("result", "")
        row["status"] = BEWERBER_STATUS_MAP.get(status_raw, status_raw)

        row["stellenbeschreibung"] = r.get("position", "") or r.get("jobTitle", "")
        row["verbesserungsvorschlaege"] = r.get("suggestions", "") or r.get("improvement", "")

        ratings = r.get("ratings", {}) or r.get("categories", {})
        if isinstance(ratings, dict):
            _map_sterne_json(ratings, row, BEWERBER_KATEGORIEN, "sternebewertung_")

        overall = r.get("score", None) or r.get("rating", None)
        if overall is not None:
            try:
                val = float(overall)
                row["durchschnittsbewertung"] = f"{val:.2f}"
                row["gerundete_durchschnittsbewertung"] = f"{runde_auf_halbe(val):.2f}"
            except (ValueError, TypeError):
                pass
        else:
            _berechne_durchschnitt(row, BEWERBER_KATEGORIEN)

        ergebnisse.append(row)

    return ergebnisse


def _map_sterne_json(ratings: dict, row: dict, kategorien: dict, prefix: str):
    """Mappt Bewertungen aus JSON in die CSV-Felder."""
    for label, feld in kategorien.items():
        csv_key = f"{prefix}{feld}"
        val = ratings.get(label) or ratings.get(feld)
        if val is not None:
            try:
                row[csv_key] = f"{float(val):.2f}"
            except (ValueError, TypeError):
                pass


def _berechne_durchschnitt(row: dict, kategorien: dict):
    """Berechnet den Durchschnitt aller Sternebewertungen."""
    werte = []
    for feld in kategorien.values():
        key = f"sternebewertung_{feld}"
        val = row.get(key, "")
        if val:
            try:
                werte.append(float(val))
            except ValueError:
                pass
    if werte:
        avg = sum(werte) / len(werte)
        row["durchschnittsbewertung"] = f"{avg:.2f}"
        row["gerundete_durchschnittsbewertung"] = f"{runde_auf_halbe(avg):.2f}"


# ---------------------------------------------------------------------------
# HTML-Parsing  (Fallback wenn kein JSON verfügbar)
# ---------------------------------------------------------------------------

def _finde_bewertungs_container(soup: BeautifulSoup) -> list:
    """Findet Bewertungs-Container im HTML."""
    # Strategie 1: article-Elemente
    articles = soup.find_all("article")
    if articles:
        return articles

    # Strategie 2: data-testid mit review
    containers = soup.find_all(attrs={"data-testid": re.compile(r"review", re.I)})
    if containers:
        return containers

    # Strategie 3: Elemente die Score + Titel enthalten
    all_divs = soup.find_all("div", recursive=True)
    review_divs = []
    for div in all_divs:
        # Ein Review-Container hat typischerweise einen Titel (h-Element)
        # und Eine Bewertung (Sterne oder Score-Zahl)
        has_heading = div.find(["h2", "h3"]) is not None
        text = div.get_text()
        has_score = bool(re.search(r"\d[.,]\d{1,2}\s*(?:von|/)\s*5", text))
        if has_heading and has_score:
            # Prüfe ob es kein Kind eines anderen gefundenen Containers ist
            is_child = any(div in d.descendants for d in review_divs)
            if not is_child:
                review_divs.append(div)

    return review_divs


def _score_aus_element(elem) -> float | None:
    """Extrahiert einen Score-Wert aus einem HTML-Element."""
    if not elem:
        return None

    # aria-label: "4 von 5 Sternen"
    for attr_name in ("aria-label", "title", "data-score", "data-value"):
        attr_val = elem.get(attr_name, "")
        if attr_val:
            m = re.search(r"(\d[.,]?\d*)\s*(?:von|out of|/)\s*\d", attr_val)
            if m:
                return float(m.group(1).replace(",", "."))
            m = re.search(r"(\d[.,]\d{1,2})", attr_val)
            if m:
                return float(m.group(1).replace(",", "."))

    # Textinhalt: "4,00" oder "4.8"
    text = elem.get_text(strip=True)
    m = re.match(r"^(\d[.,]\d{1,2})$", text)
    if m:
        return float(m.group(1).replace(",", "."))

    return None


def _sterne_zaehlen(container) -> float | None:
    """Zählt gefüllte Sterne (SVG/Icons) in einem Container."""
    # Suche nach SVG-Elementen oder Icon-Spans die Sterne darstellen
    sterne = container.find_all(["svg", "i", "span"],
                                class_=re.compile(r"star|rating|icon", re.I))
    if not sterne:
        return None

    gefuellt = 0
    gesamt = len(sterne)
    for stern in sterne:
        classes = " ".join(stern.get("class", []))
        fill = stern.get("fill", "")
        if ("filled" in classes or "active" in classes or "full" in classes
                or fill in ("#ffc107", "#ffb400", "currentColor")):
            gefuellt += 1

    if gesamt > 0:
        return float(gefuellt)
    return None


def _kategorie_bewertungen_extrahieren(container, kategorien: dict) -> dict:
    """Extrahiert Kategorie-Bewertungen (Sterne) aus einem Review-Container."""
    ergebnis = {}
    text_full = container.get_text()

    for label, feld in kategorien.items():
        # Suche nach dem Kategorie-Label im Container
        label_elem = container.find(
            string=re.compile(re.escape(label), re.I)
        )
        if not label_elem:
            # Versuche mit normalisiertem Text
            label_elem = container.find(
                string=re.compile(label.replace("/", r"[/\s]").replace("-", r"[-\s]"), re.I)
            )

        if label_elem:
            # Eltern-Element finden das Label + Score enthält
            parent = label_elem.parent
            if parent:
                # Score im Eltern- oder Geschwister-Element suchen
                for search_scope in [parent, parent.parent]:
                    if not search_scope:
                        continue
                    score = _score_aus_element(search_scope)
                    if score is not None:
                        ergebnis[f"sternebewertung_{feld}"] = f"{score:.2f}"
                        break
                    # Score in Kind-Elementen suchen
                    for child in search_scope.find_all(True, recursive=False):
                        score = _score_aus_element(child)
                        if score is not None:
                            ergebnis[f"sternebewertung_{feld}"] = f"{score:.2f}"
                            break
                    if f"sternebewertung_{feld}" in ergebnis:
                        break

        if f"sternebewertung_{feld}" not in ergebnis:
            # Fallback: Score direkt vor oder nach dem Label-Text suchen
            pattern = rf"(?:(\d[.,]\d{{1,2}})\s*{re.escape(label)}|{re.escape(label)}\s*(\d[.,]\d{{1,2}}))"
            m = re.search(pattern, text_full, re.I)
            if m:
                val = (m.group(1) or m.group(2)).replace(",", ".")
                ergebnis[f"sternebewertung_{feld}"] = f"{float(val):.2f}"

    return ergebnis


def _kategorie_texte_extrahieren(container, kategorien: dict) -> dict:
    """Extrahiert Kategorie-Textkommentare aus einem Review-Container."""
    ergebnis = {}

    for label, feld in kategorien.items():
        label_elem = container.find(
            string=re.compile(re.escape(label), re.I)
        )
        if label_elem and label_elem.parent:
            # Text im nächsten Geschwister-Element oder -Paragraph suchen
            sibling = label_elem.parent.find_next_sibling(["p", "div", "span"])
            if sibling:
                text = text_bereinigen(sibling.get_text())
                if text and text != label:
                    ergebnis[feld] = text

    return ergebnis


def mitarbeiter_bewertung_aus_html(container) -> dict:
    """Parst eine einzelne Mitarbeiter-Bewertung aus einem HTML-Container."""
    row = {f: "" for f in MITARBEITER_FELDER}
    text_full = container.get_text()

    # Titel: erstes h2/h3
    titel_elem = container.find(["h2", "h3"])
    if titel_elem:
        row["titel"] = text_bereinigen(titel_elem.get_text())

    # Datum: time-Element oder Text mit Monat+Jahr
    time_elem = container.find("time")
    if time_elem:
        row["datum"] = datum_parsen(time_elem.get("datetime", "") or time_elem.get_text())
    else:
        for name in MONATE:
            m = re.search(rf"(\d{{1,2}}\.\s*)?{re.escape(name)}\s*\d{{4}}", text_full)
            if m:
                row["datum"] = datum_parsen(m.group())
                break

    # Gesamtscore
    score_elem = container.find(attrs={"aria-label": re.compile(r"\d.*(?:von|out of)", re.I)})
    if score_elem:
        overall = _score_aus_element(score_elem)
    else:
        # Erstes Score-Element im Container
        m = re.search(r"(\d[.,]\d{1,2})", text_full[:200])
        overall = float(m.group(1).replace(",", ".")) if m else None

    # Status und Jobbeschreibung
    for typ_label, typ_code in MITARBEITER_TYP_MAP.items():
        if typ_label in text_full:
            is_ex = f"Ex-{typ_label}" in text_full or f"ex-{typ_label}" in text_full
            row["status"] = 0.0 if is_ex else 1.0
            # Abteilung suchen (Text nach dem Typ-Label)
            after_typ = text_full.split(typ_label)[-1][:100]
            abt_match = re.search(r"(?:in\s+)?([A-ZÄÖÜa-zäöü/\s&-]{3,30})", after_typ)
            abteilung = text_bereinigen(abt_match.group(1)) if abt_match else "Sonstige"
            row["jobbeschreibung"] = f"{typ_code}, {abteilung}"
            break

    # Pro / Contra / Verbesserungsvorschläge
    pro_labels = ["Gut am Arbeitgeber", "Pro", "Positiv"]
    contra_labels = ["Schlecht am Arbeitgeber", "Contra", "Negativ"]
    suggest_labels = ["Verbesserungsvorschläge", "Vorschläge"]

    for labels, feld in [
        (pro_labels, "gut_am_arbeitgeber_finde_ich"),
        (contra_labels, "schlecht_am_arbeitgeber_finde_ich"),
        (suggest_labels, "verbesserungsvorschlaege"),
    ]:
        for lbl in labels:
            elem = container.find(string=re.compile(re.escape(lbl), re.I))
            if elem and elem.parent:
                sibling = elem.parent.find_next_sibling(["p", "div", "span"])
                if sibling:
                    row[feld] = text_bereinigen(sibling.get_text())
                    break

    # Kategorie-Sternebewertungen
    sterne = _kategorie_bewertungen_extrahieren(container, MITARBEITER_KATEGORIEN)
    row.update(sterne)

    # Kategorie-Textkommentare
    texte = _kategorie_texte_extrahieren(container, MITARBEITER_KATEGORIEN)
    row.update(texte)

    # Gesamtbewertung berechnen
    if overall is not None:
        row["durchschnittsbewertung"] = f"{overall:.2f}"
        row["gerundete_durchschnittsbewertung"] = f"{runde_auf_halbe(overall):.2f}"
    else:
        _berechne_durchschnitt(row, MITARBEITER_KATEGORIEN)

    return row


def bewerber_bewertung_aus_html(container) -> dict:
    """Parst eine einzelne Bewerber-Bewertung aus einem HTML-Container."""
    row = {f: "" for f in BEWERBER_FELDER}
    text_full = container.get_text()

    # Titel
    titel_elem = container.find(["h2", "h3"])
    if titel_elem:
        row["titel"] = text_bereinigen(titel_elem.get_text())

    # Datum
    time_elem = container.find("time")
    if time_elem:
        row["datum"] = datum_parsen(time_elem.get("datetime", "") or time_elem.get_text())
    else:
        for name in MONATE:
            m = re.search(rf"(\d{{1,2}}\.\s*)?{re.escape(name)}\s*\d{{4}}", text_full)
            if m:
                row["datum"] = datum_parsen(m.group())
                break

    # Status (Zusage/Absage)
    for label, code in BEWERBER_STATUS_MAP.items():
        if label in text_full:
            row["status"] = code
            break

    # Stellenbeschreibung
    stellen_patterns = [
        r"(?:als|für|auf)\s+([A-ZÄÖÜa-zäöü/\s&()-]{3,60})",
        r"(?:Position|Stelle|Job):\s*(.+?)(?:\n|$)",
    ]
    for pat in stellen_patterns:
        m = re.search(pat, text_full)
        if m:
            row["stellenbeschreibung"] = text_bereinigen(m.group(1))
            break

    # Verbesserungsvorschläge
    for lbl in ["Verbesserungsvorschläge", "Vorschläge"]:
        elem = container.find(string=re.compile(re.escape(lbl), re.I))
        if elem and elem.parent:
            sibling = elem.parent.find_next_sibling(["p", "div", "span"])
            if sibling:
                row["verbesserungsvorschlaege"] = text_bereinigen(sibling.get_text())
                break

    # Gesamtscore
    score_elem = container.find(attrs={"aria-label": re.compile(r"\d.*(?:von|out of)", re.I)})
    overall = _score_aus_element(score_elem) if score_elem else None
    if overall is None:
        m = re.search(r"(\d[.,]\d{1,2})", text_full[:200])
        if m:
            overall = float(m.group(1).replace(",", "."))

    # Kategorie-Sternebewertungen
    sterne = _kategorie_bewertungen_extrahieren(container, BEWERBER_KATEGORIEN)
    row.update(sterne)

    # Gesamtbewertung
    if overall is not None:
        row["durchschnittsbewertung"] = f"{overall:.2f}"
        row["gerundete_durchschnittsbewertung"] = f"{runde_auf_halbe(overall):.2f}"
    else:
        _berechne_durchschnitt(row, BEWERBER_KATEGORIEN)

    return row


# ---------------------------------------------------------------------------
# Seiten-Parsing (kombiniert JSON + HTML)
# ---------------------------------------------------------------------------

def bewertungen_von_seite(soup: BeautifulSoup, typ: str, company_id: int) -> list[dict]:
    """Parst alle Bewertungen einer Seite (JSON oder HTML Fallback)."""
    kategorien = MITARBEITER_KATEGORIEN if typ == "mitarbeiter" else BEWERBER_KATEGORIEN

    # Strategie 1: __NEXT_DATA__ JSON
    next_data = next_data_extrahieren(soup)
    if next_data:
        raw = bewertungen_aus_next_data(next_data, typ)
        if raw:
            log.info("  → %d Bewertungen aus JSON-Daten extrahiert", len(raw))
            if typ == "mitarbeiter":
                return mitarbeiter_aus_json(raw, company_id)
            else:
                return bewerber_aus_json(raw, company_id)

    # Strategie 2: Eingebettetes JSON suchen (andere Script-Tags)
    for script in soup.find_all("script", type="application/json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            raw = bewertungen_aus_next_data(data, typ)
            if raw:
                log.info("  → %d Bewertungen aus eingebettetem JSON", len(raw))
                if typ == "mitarbeiter":
                    return mitarbeiter_aus_json(raw, company_id)
                else:
                    return bewerber_aus_json(raw, company_id)
        except (json.JSONDecodeError, TypeError):
            continue

    # Strategie 3: HTML-Parsing
    log.info("  → Verwende HTML-Parsing")
    containers = _finde_bewertungs_container(soup)
    if not containers:
        log.debug("  Keine Bewertungs-Container im HTML gefunden.")
        return []

    ergebnisse = []
    for container in containers:
        if typ == "mitarbeiter":
            row = mitarbeiter_bewertung_aus_html(container)
        else:
            row = bewerber_bewertung_aus_html(container)

        # Nur hinzufügen wenn Titel vorhanden
        if row.get("titel"):
            row["company_id"] = company_id
            row["created_at"] = jetzt_iso()
            row["updated_at"] = jetzt_iso()
            ergebnisse.append(row)

    return ergebnisse


def hat_naechste_seite(soup: BeautifulSoup, aktuelle_seite: int) -> bool:
    """Prüft ob es eine nächste Seite gibt."""
    text = soup.get_text()

    # Pagination-Links: a-Elemente mit Seitenzahlen
    naechste = str(aktuelle_seite + 1)
    pag_links = soup.find_all("a", href=re.compile(r"/\d+$"))
    for link in pag_links:
        href = link.get("href", "")
        if href.endswith(f"/{naechste}"):
            return True

    # "Nächste"-Button oder Link suchen (verschiedene Varianten)
    for pattern in [
        r"(?:nächste|next|weiter|vorwärts)",
        r"›",
        r"»",
    ]:
        next_link = soup.find("a", attrs={
            "aria-label": re.compile(pattern, re.I)
        })
        if next_link:
            return True
        next_link = soup.find("a", string=re.compile(pattern, re.I))
        if next_link:
            return True
        next_btn = soup.find("button", attrs={
            "aria-label": re.compile(pattern, re.I)
        })
        if next_btn and not next_btn.get("disabled"):
            return True

    # Pagination: aktuelle Seite + 1 als Link-Text suchen
    pag_link = soup.find("a", string=re.compile(rf"^\s*{naechste}\s*$"))
    if pag_link:
        return True

    # "Seite X von Y" Pattern
    m = re.search(r"(?:Seite|Page)\s*(\d+)\s*(?:von|of)\s*(\d+)", text)
    if m and int(m.group(1)) < int(m.group(2)):
        return True

    # Pagination-Container mit nav-Element prüfen
    nav = soup.find("nav", attrs={"aria-label": re.compile(r"paginat|seite|page", re.I)})
    if nav:
        links = nav.find_all("a")
        for link in links:
            link_text = link.get_text(strip=True)
            if link_text == naechste:
                return True

    # __NEXT_DATA__ basierte Prüfung: totalPages oder pageCount
    next_data = next_data_extrahieren(soup)
    if next_data:
        total = _finde_wert(next_data, ["totalPages", "pageCount", "lastPage", "maxPage"])
        if total is not None and aktuelle_seite < int(total):
            return True
        total_items = _finde_wert(next_data, ["totalCount", "total", "count", "totalReviews"])
        if total_items is not None and int(total_items) > aktuelle_seite * 10:
            return True

    return False


def _finde_wert(obj, schluessel: list[str], tiefe: int = 0):
    """Sucht rekursiv nach einem Schlüssel in verschachtelten Dicts."""
    if tiefe > 10:
        return None
    if isinstance(obj, dict):
        for key in schluessel:
            if key in obj:
                return obj[key]
        for val in obj.values():
            result = _finde_wert(val, schluessel, tiefe + 1)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _finde_wert(item, schluessel, tiefe + 1)
            if result is not None:
                return result
    return None


# ---------------------------------------------------------------------------
# Haupt-Scraping-Logik
# ---------------------------------------------------------------------------

def bewertungen_scrapen(
    firmen_url: str,
    typ: str,
    page,
    max_seiten: int,
    company_id: int,
    debug: bool = False,
) -> list[dict]:
    """Scrapt alle Bewertungen eines Typs (mitarbeiter/bewerber) mit Paginierung."""
    pfad = "kommentare" if typ == "mitarbeiter" else "bewerbung"
    alle = []
    gesehene_titel = set()  # Duplikaterkennung

    for seite_nr in range(1, max_seiten + 1):
        url = f"{firmen_url}/{pfad}" if seite_nr == 1 else f"{firmen_url}/{pfad}/{seite_nr}"
        log.info("[%s] Seite %d: %s", typ.upper(), seite_nr, url)

        soup = seite_laden(page, url)

        if debug:
            debug_datei = Path(f"debug_{typ}_seite_{seite_nr}.html")
            debug_datei.write_text(soup.prettify(), encoding="utf-8")
            log.info("  → Debug-HTML gespeichert: %s", debug_datei)

        bewertungen = bewertungen_von_seite(soup, typ, company_id)

        if not bewertungen:
            if seite_nr == 1:
                log.warning("  Keine %s-Bewertungen gefunden.", typ)
            else:
                log.info("  Keine weiteren Bewertungen auf Seite %d.", seite_nr)
            break

        # Duplikate filtern (falls gleiche Seite erneut geladen wird)
        neue = []
        for b in bewertungen:
            key = (b.get("titel", ""), b.get("datum", ""))
            if key not in gesehene_titel:
                gesehene_titel.add(key)
                neue.append(b)

        if not neue and seite_nr > 1:
            log.info("  Keine neuen Bewertungen auf Seite %d (Duplikate). Ende.", seite_nr)
            break

        alle.extend(neue)
        log.info("  %d Bewertungen gefunden (gesamt: %d)", len(neue), len(alle))

        # Prüfe ob es eine nächste Seite gibt
        naechste_seite = hat_naechste_seite(soup, seite_nr)

        if not naechste_seite:
            # Sicherheitsversuch: wenn wir eine volle Seite (≥5) bekommen haben,
            # versuche trotzdem die nächste Seite zu laden
            if len(bewertungen) >= 5 and seite_nr < max_seiten:
                log.info("  Paginierung nicht erkannt, versuche nächste Seite trotzdem …")
            else:
                log.info("  Letzte Seite erreicht.")
                break

        if seite_nr < max_seiten:
            pause()

    return alle


# ---------------------------------------------------------------------------
# CSV-Export
# ---------------------------------------------------------------------------

def als_csv_speichern(bewertungen: list[dict], felder: list[str], dateiname: str) -> None:
    """Speichert Bewertungen als CSV-Datei."""
    if not bewertungen:
        log.warning("Keine Daten zum Speichern für %s.", dateiname)
        return

    with open(dateiname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=felder, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(bewertungen)

    log.info("CSV gespeichert: %s (%d Einträge)", dateiname, len(bewertungen))


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

def main():
    if not PLAYWRIGHT_VERFUEGBAR:
        log.error(
            "Playwright ist nicht installiert. Installiere es mit:\n"
            "  pip install playwright && python -m playwright install chromium"
        )
        sys.exit(1)

    print("=" * 60)
    print("  kununu Bewertungen Scraper")
    print("=" * 60)
    print()

    # --- Interaktive Eingabe ---
    eingabe = input("Firmenname oder kununu-URL: ").strip()
    if not eingabe:
        print("Keine Eingabe. Abbruch.")
        sys.exit(1)

    # Erkennen ob URL oder Name
    ist_url = eingabe.startswith("http://") or eingabe.startswith("https://")

    print()
    log.info("=" * 60)
    log.info("kununu Bewertungen Scraper")
    log.info("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="de-DE",
        )
        page = context.new_page()

        # Firmen-URL ermitteln
        if ist_url:
            firmen_url = eingabe.rstrip("/")
        else:
            firmen_url = firmen_url_finden(eingabe, page)
            if not firmen_url:
                browser.close()
                sys.exit(1)

        # Firmenname für Dateinamen
        slug = firmen_url.rstrip("/").split("/")[-1]

        log.info("Firma: %s", firmen_url)
        log.info("-" * 60)

        # Mitarbeiter-Bewertungen
        log.info("Starte Mitarbeiter-Bewertungen …")
        mitarbeiter = bewertungen_scrapen(
            firmen_url, "mitarbeiter", page,
            max_seiten=50, company_id=1,
        )
        datei = f"{slug}_employee_rows.csv"
        als_csv_speichern(mitarbeiter, MITARBEITER_FELDER, datei)
        log.info("")

        # Bewerber-Bewertungen
        log.info("Starte Bewerber-Bewertungen …")
        bewerber = bewertungen_scrapen(
            firmen_url, "bewerber", page,
            max_seiten=50, company_id=1,
        )
        datei = f"{slug}_candidates_rows.csv"
        als_csv_speichern(bewerber, BEWERBER_FELDER, datei)

        browser.close()

    log.info("-" * 60)
    log.info("Fertig!")


if __name__ == "__main__":
    main()
