#!/usr/bin/env python3
"""
Verbesserter Scraper für https://alainreinickeimmobilien.de/aktuelle-angebote/
Extrahiert ALLE Immobilienangebote (außer reservierte) mit ALLEN Bildern

Version 3.0 - Vollständige Erfassung
"""

import os
import re
import sys
import csv
import json
import time
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("[ERROR] Fehlende Module. Bitte installieren:")
    print("  pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

BASE = "https://alainreinicke.landingpage.immobilien"
LIST_URL = f"{BASE}/public"

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# REGEX PATTERNS
# ===========================================================================

RE_OBJEKTNR = re.compile(r"(?:Objekt[:\s\-]*Nr|ImmoNr|ID)[:\s\-]+(\S+)", re.IGNORECASE)
RE_PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)")
RE_PRICE = re.compile(r"([\d.,]+)\s*€")

# ===========================================================================
# STOPWORDS
# ===========================================================================

STOP_STRINGS = [
    "Cookie", "Datenschutz", "Impressum", "Kontakt",
    "Tel:", "Fax:", "E-Mail:", "www.", "http",
    "© ", "JavaScript", "Alle Rechte", "Footer",
    "Geldwäscheprävention", "Weitergabeverbot", "Maklervertrag"
]

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clean_desc_lines(lines: List[str]) -> List[str]:
    """Bereinige Beschreibungszeilen"""
    cleaned = []
    seen = set()
    
    for line in lines:
        line = _norm(line)
        if not line or len(line) < 10:
            continue
        
        # Filtere Stopwords
        if any(stop in line for stop in STOP_STRINGS):
            continue
        
        # Dedupliziere
        line_lower = line.lower()
        if line_lower in seen:
            continue
        seen.add(line_lower)
        cleaned.append(line)
    
    return cleaned

def soup_get(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup:
    """Hole HTML und parse mit BeautifulSoup"""
    time.sleep(delay)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    """Gibt base/table Segment für Airtable API zurück"""
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    """Airtable API Headers"""
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
    """Liste alle Records aus Airtable"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    all_records = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)
    
    ids = [rec["id"] for rec in all_records]
    fields = [rec.get("fields", {}) for rec in all_records]
    return ids, fields

def airtable_existing_fields() -> set:
    """Ermittle existierende Felder"""
    _, all_fields = airtable_list_all()
    if not all_fields:
        return set()
    return set(all_fields[0].keys())

def airtable_batch_create(records: List[dict]):
    """Erstelle Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    """Update Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    """Lösche Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    """Bereinige Record für Airtable"""
    if not allowed_fields:
        return record
    return {k: v for k, v in record.items() if k in allowed_fields or not allowed_fields}

# ===========================================================================
# EXTRACTION FUNCTIONS
# ===========================================================================

def extract_all_images(soup: BeautifulSoup, detail_url: str) -> List[str]:
    """Extrahiere ALLE Bilder von einer Detailseite"""
    images = []
    seen = set()
    
    # Suche alle img Tags
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if not src:
            continue
            
        # Filtere Logo/Icon aus
        if any(x in src.lower() for x in ["logo", "icon", "favicon", "avatar"]):
            continue
        
        # Prüfe auf typische Bild-Dateien
        if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            # Mache absolute URL
            if not src.startswith("http"):
                src = urljoin(detail_url, src)
            
            # Dedupliziere
            if src not in seen:
                seen.add(src)
                images.append(src)
    
    # Suche auch in data-src Attributen (Lazy Loading)
    for img in soup.find_all("img"):
        data_src = img.get("data-src", "")
        if data_src and any(ext in data_src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            if not data_src.startswith("http"):
                data_src = urljoin(detail_url, data_src)
            if data_src not in seen:
                seen.add(data_src)
                images.append(data_src)
    
    # Suche in CSS Background Images
    for elem in soup.find_all(style=True):
        style = elem.get("style", "")
        bg_urls = re.findall(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
        for bg_url in bg_urls:
            if any(ext in bg_url.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                if not bg_url.startswith("http"):
                    bg_url = urljoin(detail_url, bg_url)
                if bg_url not in seen:
                    seen.add(bg_url)
                    images.append(bg_url)
    
    print(f"  [BILDER] Gefunden: {len(images)} Bilder")
    return images

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    """Extrahiere Preis"""
    # Suche nach verschiedenen Preis-Patterns
    for pattern in [
        r"Kaufpreis[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Kaltmiete[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Miete[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Preis[:\s]+€?\s*([\d.,]+)\s*€?",
        r"€\s*([\d.,]+)",
        r"EUR\s*([\d.,]+)"
    ]:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            preis_str = m.group(1).replace(".", "").replace(",", ".")
            try:
                preis_num = float(preis_str)
                if preis_num > 100:  # Plausibilitätsprüfung
                    return f"€{int(preis_num):,}".replace(",", ".")
            except:
                pass
    
    return ""

def parse_price_to_number(preis_str: str) -> Optional[float]:
    """Konvertiere Preis-String zu Nummer"""
    if not preis_str:
        return None
    
    clean = re.sub(r"[^0-9.,]", "", preis_str)
    clean = clean.replace(".", "").replace(",", ".")
    
    try:
        return float(clean)
    except:
        return None

def extract_plz_ort(text: str, title: str = "") -> str:
    """Extrahiere PLZ und Ort"""
    blacklist = [
        "mietvertrag", "kaufvertrag", "zimmer", "kaufen", "mieten",
        "javascript", "cookie", "datenschutz", "telefon"
    ]
    
    # Suche im Text
    for match in RE_PLZ_ORT.finditer(text):
        plz = match.group(1)
        ort = _norm(match.group(2))
        
        # Filtere Blacklist
        if any(b in ort.lower() for b in blacklist):
            continue
        
        # Plausibilitätsprüfung
        if len(ort) > 3 and ort[0].isupper():
            return f"{plz} {ort}"
    
    # Suche im Titel
    if title:
        m = RE_PLZ_ORT.search(title)
        if m:
            return f"{m.group(1)} {_norm(m.group(2))}"
    
    return ""

def extract_description(soup: BeautifulSoup, structured_data: dict, page_text: str) -> str:
    """Extrahiere Beschreibung"""
    sections = []
    
    # Objektdaten
    obj_lines = []
    for key, val in structured_data.items():
        if val:
            obj_lines.append(f"{key}: {val}")
    
    if obj_lines:
        sections.append("=== OBJEKTDATEN ===\n\n" + "\n\n".join(obj_lines))
    
    # Beschreibung aus bestimmten Sections
    desc_selectors = [
        "div.property-description",
        "div.beschreibung",
        "section.description",
        "div.expose-text",
        "div.object-description"
    ]
    
    desc_lines = []
    for selector in desc_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text("\n", strip=True)
            lines = [_norm(l) for l in text.split("\n")]
            desc_lines.extend(lines)
    
    # Falls keine spezifischen Sections gefunden
    if not desc_lines:
        # Suche nach längeren Textblöcken
        for p in soup.find_all(["p", "div"]):
            text = p.get_text(strip=True)
            if len(text) > 50:
                desc_lines.append(text)
    
    # Bereinige Zeilen
    desc_lines = _clean_desc_lines(desc_lines)
    
    if desc_lines:
        sections.append("=== BESCHREIBUNG ===\n\n" + "\n\n".join(desc_lines[:20]))  # Limit auf 20 Zeilen
    
    return "\n\n".join(sections)

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_detail_links() -> List[str]:
    """
    Sammle ALLE Immobilien-Links von der Listenseite
    Verbesserte Logik um auch dynamisch geladene Links zu finden
    """
    print(f"[LIST] Lade: {LIST_URL}")
    soup = soup_get(LIST_URL, delay=2.0)
    page_text = soup.get_text()
    html_content = str(soup)
    
    links = []
    seen = set()
    
    # Methode 1: Suche <a> Tags mit href
    print("[DEBUG] Methode 1: Suche <a> Tags...")
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "exposee" in href.lower():
            # Mache absolute URL
            if not href.startswith("http"):
                href = urljoin(BASE, href)
            if href not in seen:
                seen.add(href)
                links.append(href)
    
    print(f"[DEBUG] Nach Methode 1: {len(links)} Links")
    
    # Methode 2: Suche in onclick Events
    print("[DEBUG] Methode 2: Suche onclick Events...")
    for elem in soup.find_all(onclick=True):
        onclick = elem.get("onclick", "")
        # Suche URLs in onclick
        urls = re.findall(r'["\']([^"\']*exposee[^"\']*)["\']', onclick)
        for url in urls:
            if not url.startswith("http"):
                url = urljoin(BASE, url)
            if url not in seen:
                seen.add(url)
                links.append(url)
    
    print(f"[DEBUG] Nach Methode 2: {len(links)} Links")
    
    # Methode 3: Suche in data-Attributen
    print("[DEBUG] Methode 3: Suche data-Attribute...")
    for elem in soup.find_all(attrs={"data-url": True}):
        url = elem.get("data-url", "")
        if "exposee" in url.lower():
            if not url.startswith("http"):
                url = urljoin(BASE, url)
            if url not in seen:
                seen.add(url)
                links.append(url)
    
    for elem in soup.find_all(attrs={"data-href": True}):
        url = elem.get("data-href", "")
        if "exposee" in url.lower():
            if not url.startswith("http"):
                url = urljoin(BASE, url)
            if url not in seen:
                seen.add(url)
                links.append(url)
    
    print(f"[DEBUG] Nach Methode 3: {len(links)} Links")
    
    # Methode 4: Regex-Suche im gesamten HTML
    print("[DEBUG] Methode 4: Regex-Suche im HTML...")
    # Suche nach propstack URLs
    patterns = [
        r'https://alainreinicke\.landingpage\.immobilien/public/exposee/[^\s"\'\)<>]+',
        r'/public/exposee/[^\s"\'\)<>]+',
        r'href=["\']([^"\']*exposee[^"\']*)["\']',
    ]
    
    for pattern in patterns:
        found_urls = re.findall(pattern, html_content)
        for url in found_urls:
            # Bereinige URL
            url = url.rstrip('",\'};])')
            if not url.startswith("http"):
                url = urljoin(BASE, url)
            if url not in seen:
                seen.add(url)
                links.append(url)
    
    print(f"[DEBUG] Nach Methode 4: {len(links)} Links")
    
    # Methode 5: Suche in JavaScript/JSON Blöcken
    print("[DEBUG] Methode 5: Suche in Scripts...")
    for script in soup.find_all("script"):
        script_text = script.string if script.string else ""
        if "exposee" in script_text:
            # Suche URLs
            urls = re.findall(r'["\']([^"\']*exposee[^"\']*)["\']', script_text)
            for url in urls:
                url = url.rstrip('",\'};])')
                if not url.startswith("http"):
                    url = urljoin(BASE, url)
                if url not in seen and len(url) > 30:  # Filter zu kurze URLs
                    seen.add(url)
                    links.append(url)
    
    print(f"[DEBUG] Nach Methode 5: {len(links)} Links")
    
    # Filtere "reserviert" aus
    filtered_links = []
    for link in links:
        # Prüfe ob Link "reserviert" enthält
        if "reserviert" not in link.lower():
            filtered_links.append(link)
        else:
            print(f"  [SKIP] Reserviert: {link[:80]}...")
    
    print(f"\n[LIST] Gesamt: {len(links)} Links gefunden")
    print(f"[LIST] Gefiltert (ohne reserviert): {len(filtered_links)} Links")
    
    # Debug Output
    if len(filtered_links) < 8:
        print(f"\n[WARN] Nur {len(filtered_links)} Links gefunden!")
        print("[HINT] Die Website lädt möglicherweise mehr Inhalte via JavaScript.")
        print("[HINT] Prüfe die Website manuell im Browser.")
    
    return filtered_links

def parse_detail(detail_url: str) -> dict:
    """Parse Detailseite und extrahiere ALLE Daten inklusive ALLER Bilder"""
    print(f"  [PARSE] {detail_url[:80]}...")
    
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)
    
    # Titel
    title = ""
    for selector in ["h1", ".property-title", ".immobilie-titel", "h2"]:
        elem = soup.select_one(selector)
        if elem:
            title = _norm(elem.get_text(strip=True))
            if title:
                break
    
    # Objektnummer
    m_obj = RE_OBJEKTNR.search(page_text)
    objektnummer = m_obj.group(1).strip() if m_obj else ""
    
    # Preis
    preis = extract_price(soup, page_text)
    
    # PLZ/Ort
    ort = extract_plz_ort(page_text, title)
    
    # ALLE Bilder extrahieren
    all_images = extract_all_images(soup, detail_url)
    
    # Erstes Bild für Kompatibilität
    image_url = all_images[0] if all_images else ""
    
    # Alle Bilder als komma-separierte Liste
    all_images_str = ", ".join(all_images) if all_images else ""
    
    # Vermarktungsart
    vermarktungsart = "Kaufen"
    if re.search(r"\b(zu\s+vermieten|miete|zur\s+miete|kaltmiete)\b", page_text, re.IGNORECASE):
        vermarktungsart = "Mieten"
    
    # Objekttyp
    objekttyp = ""
    for pattern in [
        r"Objekttyp[:\s]+([^\n]+)",
        r"Objektart[:\s]+([^\n]+)",
        r"Immobilientyp[:\s]+([^\n]+)"
    ]:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            objekttyp = _norm(m.group(1))
            break
    
    # Wohnfläche
    wohnflaeche = ""
    m = re.search(r"Wohnfläche[:\s]+ca\.\s*([\d.,]+)\s*m²", page_text, re.IGNORECASE)
    if m:
        wohnflaeche = f"ca. {m.group(1)} m²"
    
    # Grundstück
    grundstueck = ""
    m = re.search(r"Grundstücksgröße[:\s]+ca\.\s*([\d.,]+)\s*m²", page_text, re.IGNORECASE)
    if m:
        grundstueck = f"ca. {m.group(1)} m²"
    
    # Zimmer
    zimmer = ""
    m = re.search(r"(?:Anzahl\s+)?Zimmer[:\s]+([\d.,]+)", page_text, re.IGNORECASE)
    if m:
        zimmer = m.group(1)
    
    # Baujahr
    baujahr = ""
    m = re.search(r"Baujahr[:\s]+(\d{4})", page_text, re.IGNORECASE)
    if m:
        baujahr = m.group(1)
    
    # Strukturierte Daten
    structured_data = {
        "Objekttyp": objekttyp,
        "Vermarktungsart": vermarktungsart,
        "Wohnfläche": wohnflaeche,
        "Grundstücksgröße": grundstueck,
        "Zimmer": zimmer,
        "Baujahr": baujahr,
    }
    
    # Beschreibung
    description = extract_description(soup, structured_data, page_text)
    
    return {
        "Titel": title,
        "URL": detail_url,
        "Beschreibung": description,
        "Objektnummer": objektnummer,
        "Kategorie": vermarktungsart,
        "Preis": preis,
        "Ort": ort,
        "Bild_URL": image_url,
        "Alle_Bilder": all_images_str,
        "Anzahl_Bilder": len(all_images),
    }

def make_record(row: dict) -> dict:
    """Erstelle Airtable-Record"""
    preis_value = parse_price_to_number(row["Preis"])
    
    # Für Airtable - verwende nur erstes Bild im "Bild" Feld
    # aber speichere alle Bilder im "Alle_Bilder" Feld
    return {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Alle_Bilder": row["Alle_Bilder"],
        "Anzahl_Bilder": row["Anzahl_Bilder"],
        "Preis": preis_value,
        "Standort": row["Ort"],
    }

def unique_key(fields: dict) -> str:
    """Eindeutiger Key für Record"""
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ===========================================================================
# MAIN
# ===========================================================================

def run():
    """Hauptfunktion"""
    print("[REINICKE v3] Starte Scraper für alainreinickeimmobilien.de")
    print("[INFO] Ziel: ALLE Immobilien (außer reservierte) mit ALLEN Bildern\n")
    
    # Sammle Links
    try:
        detail_links = collect_detail_links()
    except Exception as e:
        print(f"[ERROR] Fehler beim Sammeln der Links: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if not detail_links:
        print("[WARN] Keine Links gefunden!")
        return
    
    # Scrape Details
    all_rows = []
    for i, url in enumerate(detail_links, 1):
        try:
            print(f"\n[SCRAPE] {i}/{len(detail_links)}")
            row = parse_detail(url)
            record = make_record(row)
            
            # Zeige Vorschau
            print(f"  → {record['Kategorie']:8} | {record['Titel'][:50]}")
            print(f"  → Bilder: {record['Anzahl_Bilder']} | {record.get('Standort', 'N/A')}")
            
            all_rows.append(record)
        except Exception as e:
            print(f"[ERROR] Fehler bei {url}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return
    
    # Speichere CSV
    csv_file = "reinicke_immobilien_v3.csv"
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Beschreibung", 
            "Bild", "Alle_Bilder", "Anzahl_Bilder", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    
    print(f"\n{'='*70}")
    print(f"[✓] Erfolgreich abgeschlossen!")
    print(f"[✓] Gespeichert: {csv_file}")
    print(f"[✓] Immobilien: {len(all_rows)}")
    print(f"[✓] Gesamt Bilder: {sum(r['Anzahl_Bilder'] for r in all_rows)}")
    print(f"{'='*70}\n")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        allowed = airtable_existing_fields()
        all_ids, all_fields = airtable_list_all()
        
        existing = {}
        for rec_id, f in zip(all_ids, all_fields):
            k = unique_key(f)
            existing[k] = (rec_id, f)
        
        desired = {}
        for r in all_rows:
            k = unique_key(r)
            if k in desired:
                if len(r.get("Beschreibung", "")) > len(desired[k].get("Beschreibung", "")):
                    desired[k] = sanitize_record_for_airtable(r, allowed)
            else:
                desired[k] = sanitize_record_for_airtable(r, allowed)
        
        to_create, to_update, keep = [], [], set()
        for k, fields in desired.items():
            if k in existing:
                rec_id, old = existing[k]
                diff = {fld: val for fld, val in fields.items() if old.get(fld) != val}
                if diff:
                    to_update.append({"id": rec_id, "fields": diff})
                keep.add(k)
            else:
                to_create.append(fields)
        
        to_delete_ids = [rec_id for k, (rec_id, _) in existing.items() if k not in keep]
        
        print(f"\n[SYNC] Gesamt → create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
        
        if to_create:
            print(f"[Airtable] Erstelle {len(to_create)} neue Records...")
            airtable_batch_create(to_create)
        if to_update:
            print(f"[Airtable] Aktualisiere {len(to_update)} Records...")
            airtable_batch_update(to_update)
        if to_delete_ids:
            print(f"[Airtable] Lösche {len(to_delete_ids)} Records...")
            airtable_batch_delete(to_delete_ids)
        
        print("[Airtable] Synchronisation abgeschlossen.\n")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

if __name__ == "__main__":
    run()
