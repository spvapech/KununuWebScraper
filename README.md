# kununu Scraper

Web-Scraper für [kununu.com](https://www.kununu.com/) – extrahiert Mitarbeiter- und Bewerberbewertungen beliebiger Unternehmen und exportiert sie als CSV oder Excel. Enthält ein Flask-basiertes Web-Frontend mit Live-Fortschrittsanzeige und direktem Browser-Download sowie eine Terminal-Variante mit interaktiver Eingabe. Nutzt Playwright für zuverlässiges Rendering JavaScript-basierter Inhalte mit automatischer Paginierung und Duplikaterkennung.

Scrapt Unternehmensdaten und Bewertungen von [kununu.com](https://www.kununu.com/) und exportiert sie als **CSV** und/oder **Excel (XLSX)**.

Das Projekt besteht aus drei Komponenten:

| Komponente                        | Beschreibung                                              |
|-----------------------------------|-----------------------------------------------------------|
| `app.py`                          | **Web-Frontend** (Flask) – benutzerfreundliche Oberfläche |
| `kununu_bewertungen_scraper.py`   | Bewertungen-Scraper (Terminal)                            |
| `kununu_scraper.py`               | Unternehmens-Scraper (Terminal)                           |

## Installation

```bash
# Python 3.10+ erforderlich
cd kununu_scraper
pip install -r requirements.txt
python -m playwright install chromium
```

---

## Web-Frontend (empfohlen)

Die einfachste Art den Scraper zu nutzen – mit grafischer Oberfläche im Browser.

### Starten

```bash
python app.py
```

Dann im Browser öffnen: **http://localhost:5000**

### Funktionen

- Firmenname oder kununu-URL eingeben
- Bewertungstyp wählen: **Beide**, **Mitarbeiter** oder **Bewerber**
- Format wählen: **CSV** oder **Excel (XLSX)**
- Live-Fortschrittsanzeige während des Scrapings
- Download-Buttons für die fertigen Dateien direkt im Browser

---

## Bewertungen Scraper (Terminal)

Scrapt **Mitarbeiter-** und **Bewerberbewertungen** eines Unternehmens über das Terminal.

### Verwendung

```bash
python kununu_bewertungen_scraper.py
```

Das Skript fragt interaktiv nach dem Firmennamen oder der kununu-URL.

### Ausgabe-Dateien

| Datei                            | Inhalt                        |
|----------------------------------|-------------------------------|
| `{firma}_employee_rows.csv`      | Mitarbeiterbewertungen        |
| `{firma}_candidates_rows.csv`    | Bewerberbewertungen           |

---

## Unternehmens-Scraper (Terminal)

Scrapt Unternehmensübersichten aus den kununu-Suchergebnissen.

### Verwendung

```bash
python kununu_scraper.py
python kununu_scraper.py --seiten 5 --details --browser
python kununu_scraper.py --branche IT --ort Berlin
```

```
python kununu_scraper.py --help
```

## Ausgabedateien

- **`{firma}_employee_rows.csv`** — Mitarbeiterbewertungen (CSV)
- **`{firma}_candidates_rows.csv`** — Bewerberbewertungen (CSV)
- **`{firma}_employee_rows.xlsx`** / `{firma}_candidates_rows.xlsx` — Excel-Format (über Web-Frontend)
- **`kununu_unternehmen.csv`** / `.xlsx` — Unternehmensübersicht (kununu_scraper.py)

## Hinweise

- Der Scraper macht **automatische Pausen** zwischen den Anfragen, um den Server nicht zu überlasten.
- Die Option `--details` scrapt jedes Unternehmensprofil einzeln und dauert daher länger.
- **Bitte beachte die Nutzungsbedingungen von kununu.com** und scrape verantwortungsvoll.
- Webseiten ändern sich regelmäßig — der Scraper muss ggf. angepasst werden, wenn sich die Seitenstruktur ändert.
