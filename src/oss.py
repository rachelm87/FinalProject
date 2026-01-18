# Outer Space Security Project:

# This file contains code for mapping recent outer space security - related developments, with a focus on space security.

# The code is structured as follows:

# 1. Consolidation:
#     - SpaceFlightNews API
#     - GDELT DOC 2.0 API
#     - Google News RSS

# 2. Categorization:
#     - Filtering records based on keywords (space / security)
#     - Categorizing records by event type (space policies, launches, satellites, etc)
#     - Tagging records based on countries / institutions / entities mentioned (Germany, ESA, Rocket Lab)

# 3. Integration:
#     - Deduplicates and integrates new records into space_records.csv file
#     - Connects csv file to PostgreSQL 'AstraWatch' Database (via .env and connection.py files)

# 4. Machine Learning:
#     - Google Colab imports space_records.csv file to cluster and suggest key headlines
#     - ML_Weekly_Highlights.ipynb generates a weekly_headlines.csv file
#     - PostgreSQL imports csv file and generates view: space_headlines_period

# 5. Analytics / Preparation:
#     - SQL queries create 10 different SQL views, including space_headlines_period
#     - Due to Tableau trial limitations, SQL views are consolidated into an Excel file for future Tableau consumption.

import requests
import pandas as pd
#from sqlalchemy import create_engine
import time
import re
import unicodedata
import os, shutil
from datetime import datetime
from pygooglenews import GoogleNews
from connection import connection, cursor
from pathlib import Path

#keyword groupings to identify relevant information based on API source
space_words = ["satellite", "space", "spaceport", "spacecraft", "orbit", "asat", "gnss", "launch", "rocket"]

security_words = ["military", "defense", "defence", "missile", "ballistic", "weaponization", "weaponisation", "space warfare", "deterrence", "counterspace", "cyber", "cyberattack", "jamming", "spoofing", "interference", "hacking", "attack", "threat", "vulnerability", "exploit", "malicious", "investigation", "risk", "dual-use", "kinetic strike"]

adversary_words = ["terrorism", "terrorist", "crime", "criminal", "smuggling", "trafficking", "extremist", "armed group"]

categories = {
    "security_event": ["attack", "jamming", "spoofing", "counterspace","threat", "interference", "missile", "situational awareness", "reconnaissance", "surveillance", "counterspace"],
    "launch": ["launch", "launched", "liftoff", "rocket", "deploy", "mission", "flight"],
    "ground_infrastructure": ["spaceport", "ground station", "launch site", "infrastructure"],
    "satellite_deployment": ["satellite", "deploy", "deployment", "constellation", "payload"],
    "policy_or_corporate": ["defense", "legislation", "space policy", "deal", "regulation", "contract", "strategy", "strategic", "review", "program", "plan", "security", "space force"]
} #categorize incoming SpaceFlightNews API records

record_cols = ["title", "summary", "source", "source_api", "published_date", "event_type", "is_space_related", "is_security_related", "countries", "entities", "raw_source", "time_classified"] #incoming records are structured according to database columns

exclude = ["hubble", "nebula", "galaxy", "exoplanet","astrophysics", "cosmic", "game", "gaming", "casino", "slot", "holiday", "christmas", "santa", "in 2017, our annual celebration", "telescope"]

countries = {
    "Afghanistan": ["afghanistan", "afghan"],
    "Albania": ["albania", "albanian"],
    "Algeria": ["algeria", "algerian"],
    "Argentina": ["argentina", "argentinian", "conae"],
    "Armenia": ["armenia", "armenian"],
    "Australia": ["australia", "australian", "australian space agency", "defence space command"],
    "Austria": ["austria", "austrian"],
    "Azerbaijan": ["azerbaijan", "azerbaijani"],
    "Bahamas": ["bahamas", "bahamian"],
    "Bahrain": ["bahrain", "bahraini"],
    "Bangladesh": ["bangladesh", "bangladeshi"],
    "Belarus": ["belarus", "belarusian"],
    "Belgium": ["belgium", "belgian"],
    "Belize": ["belize", "belizean"],
    "Benin": ["benin", "beninese"],
    "Bhutan": ["bhutan", "bhutanese"],
    "Bolivia": ["bolivia", "bolivian"],
    "Bosnia and Herzegovina": ["bosnia", "bosnian", "herzegovina"],
    "Brazil": ["brazil", "brazilian", "aeb", "brazilian space agency"],
    "Brunei": ["brunei", "bruneian"],
    "Bulgaria": ["bulgaria", "bulgarian"],
    "Burkina Faso": ["burkina faso"],
    "Cambodia": ["cambodia", "cambodian"],
    "Cameroon": ["cameroon", "cameroonian"],
    "Canada": ["canada", "canadian", "canadian space agency", "csa"],
    "Central African Republic": ["central african republic"],
    "Chad": ["chad", "chadian"],
    "Chile": ["chile", "chilean"],
    "China": ["china", "chinese", "cnsa", "beijing", "pla", "strategic support force"],
    "Colombia": ["colombia", "colombian"],
    "Costa Rica": ["costa rica", "costa rican"],
    "Croatia": ["croatia", "croatian"],
    "Cuba": ["cuba", "cuban"],
    "Cyprus": ["cyprus", "cypriot"],
    "Czech Republic": ["czech republic", "czech"],
    "Denmark": ["denmark", "danish"],
    "Dominican Republic": ["dominican republic", "dominican"],
    "Ecuador": ["ecuador", "ecuadorian"],
    "Egypt": ["egypt", "egyptian", "egyptian space agency"],
    "El Salvador": ["el salvador", "salvadoran"],
    "Estonia": ["estonia", "estonian"],
    "Ethiopia": ["ethiopia", "ethiopian"],
    "Finland": ["finland", "finnish"],
    "France": ["france", "french", "cnes", "arianegroup", "dga"],
    "Georgia": ["georgia", "georgian"],
    "Germany": ["germany", "german", "dlr", "bundeswehr"],
    "Ghana": ["ghana", "ghanaian"],
    "Greece": ["greece", "greek"],
    "Guatemala": ["guatemala", "guatemalan"],
    "Haiti": ["haiti", "haitian"],
    "Honduras": ["honduras", "honduran"],
    "Hungary": ["hungary", "hungarian"],
    "Iceland": ["iceland", "icelandic"],
    "India": ["india", "indian", "isro", "indian space research organisation"],
    "Indonesia": ["indonesia", "indonesian"],
    "Iran": ["iran", "iranian", "iranian space agency", "irgc"],
    "Iraq": ["iraq", "iraqi"],
    "Ireland": ["ireland", "irish"],
    "Israel": ["israel", "israeli", "israel space agency"],
    "Italy": ["italy", "italian", "asi", "telespazio"],
    "Japan": ["japan", "japanese", "jaxa"],
    "Jordan": ["jordan", "jordanian"],
    "Kazakhstan": ["kazakhstan", "kazakh", "baikonur"],
    "Kenya": ["kenya", "kenyan"],
    "Kuwait": ["kuwait", "kuwaiti"],
    "Latvia": ["latvia", "latvian"],
    "Lebanon": ["lebanon", "lebanese"],
    "Lithuania": ["lithuania", "lithuanian"],
    "Luxembourg": ["luxembourg", "luxembourgish"],
    "Malaysia": ["malaysia", "malaysian"],
    "Maldives": ["maldives"],
    "Mexico": ["mexico", "mexican", "aem"],
    "Morocco": ["morocco", "moroccan"],
    "Netherlands": ["netherlands", "dutch"],
    "New Zealand": ["new zealand", "new zealander"],
    "Nigeria": ["nigeria", "nigerian"],
    "North Korea": ["north korea", "dprk", "jong un", "north korean"],
    "Norway": ["norway", "norwegian"],
    "Oman": ["oman", "omani"],
    "Pakistan": ["pakistan", "pakistani", "suparco"],
    "Philippines": ["philippines", "philippine"],
    "Poland": ["poland", "polish"],
    "Portugal": ["portugal", "portuguese"],
    "Qatar": ["qatar", "qatari"],
    "Romania": ["romania", "romanian"],
    "Russia": ["russia", "russian", "roscosmos", "soyuz"],
    "Saudi Arabia": ["saudi arabia", "saudi", "ssa"],
    "Singapore": ["singapore", "singaporean"],
    "Somalia": ["somalia", "somali"],
    "South Africa": ["south africa", "south african", "sansa"],
    "South Korea": ["south korea", "kasa"],
    "Spain": ["spain", "spanish", "spainsat"],
    "Sweden": ["sweden", "swedish"],
    "Switzerland": ["switzerland", "swiss"],
    "Syria": ["syria", "syrian"],
    "Taiwan": ["taiwan"],
    "Thailand": ["thailand", "thai"],
    "Turkey": ["turkey", "turkish", "turkiye", "türkiye"],
    "Ukraine": ["ukraine", "ukrainian"],
    "United Arab Emirates": ["united arab emirates", "uae", "uae space agency"],
    "United Kingdom": ["united kingdom", "uk", "u.k.", "britain", "british", "uk space agency", "ministry of defence", "mod"],
    "United States of America": ["united states", "us", "u.s.", "american", "nasa", "darpa", "pentagon", "department of defense", "dod", "space development agency", "sda", "space force", "us space force"],
    "Venezuela": ["venezuela", "venezuelan"],
    "Vietnam": ["vietnam", "vietnamese"],
    "Yemen": ["yemen", "yemeni"]
}

entities = {
    "SpaceX": ["spacex", "starlink"],
    "Blue Origin": ["blue origin"],
    "United Launch Alliance": ["ula", "united launch alliance"],
    "Arianespace": ["arianespace"],
    "Rocket Lab": ["rocket lab"],
    "Firefly Aerospace": ["firefly aerospace"],
    "Relativity Space": ["relativity space"],
    "Stoke Space": ["stoke space"],
    "Isar Aerospace": ["isar aerospace"],
    "Avio": ["avio"],
    "Telesat": ["telesat"],
    "SES": ["ses"],
    "Inmarsat": ["inmarsat"],
    "Iridium": ["iridium"],
    "Viasat": ["viasat"],
    "Intelsat": ["intelsat"],
    "Eutelsat": ["eutelsat"],
    "OneWeb": ["oneweb"],
    "Globalstar": ["globalstar"],
    "Lockheed Martin": ["lockheed martin"],
    "Northrop Grumman": ["northrop grumman"],
    "Raytheon": ["raytheon"],
    "L3Harris": ["l3harris"],
    "Boeing Defense": ["boeing defense"],
    "Airbus Defence and Space": ["airbus defence", "airbus defense", "airbus space"],
    "Thales": ["thales"],
    "Leonardo": ["leonardo"],
    "BAE Systems": ["bae systems"],
    "Rheinmetall": ["rheinmetall"],
    "Hanwha Aerospace": ["hanwha aerospace"],
    "Maxar": ["maxar"],
    "Planet Labs": ["planet labs"],
    "BlackSky": ["blacksky"],
    "Capella Space": ["capella space"],
    "HawkEye 360": ["hawkeye 360"],
    "Iceye": ["iceye"],
    "Satellogic": ["satellogic"],
    "NASA": ["nasa"],
    "ESA": ["esa", "european space agency", "european", "europe"],
    "JAXA": ["jaxa"],
    "ISRO": ["isro"],
    "Roscosmos": ["roscosmos"],
    "CNSA": ["cnsa"],
    "US Space Force": ["u.s. space force", "space force"],
    "Missile Defense Agency": ["missile defense agency", "mda"],
    "Amazon": ["amazon", "kuiper"],
    "NATO": ["nato", "diana"],
    "Sidus Space": ["sidus", "sidus space"],
    "York Space Systems": ["york space"],
    "Muon Space": ["muon space"],
    "Frontgrade": ["frontgrade"],
    "DARPA": ["darpa"],
    "Pentagon": ["pentagon"],
    "Space Development Agency": ["space development agency", "sda"],
    "CACI": ["caci"],
    "Arka": ["arka"],
    "LeoLabs": ["leolabs"],
    "Lodestar Space": ["lodestar space"],
    "Odyssey Space Research": ["odyssey space"],
    "ACME Space": ["acme space"],
    "Varda Space": ["varda space"],
    "Voyager Technologies": ["voyager technologies"],
    "Other": ["anysignal", "sli", "ascendarc", "ursa major", "max space", "starfighters", "b2space", "kymeta","synspective", "eartheye", "arche orbital", "kratos", "indra group", "exolaunch",  "k2 space" ],
}

#ensures that records with these terms aren't included in list of records
na_phrases = [
"launch investigation","launches investigation","launched investigation","launch probe","launches probe","launched probe","launch inquiry","launches inquiry","launched inquiry","launch campaign","launches campaign","launched campaign","launch initiative","launches initiative","launched initiative","launch program","launches program","launched program","launch new program","launch website","launches website","launched website","launch new website","launch alert system","launches alert system","launched alert system",
"police launch","police launches","police launched","feds launch","feds launches","feds launched","authorities launch","authorities launched","federal investigation","state investigation","placed on leave","fatal shooting","homicide investigation",
"safe space program","safe space","launch awareness month","human trafficking awareness","community leaders launch","mayor elect","crime victims services website","attorney general announces launch","un toolkit launches","services website",
"rocket pharmaceuticals","rocket pharma","rocket mortgage","rocket company","rocket companies","rocket stock","rkt stock","shares rocket","stock rockets","prices rocket","stocks rocket","crypto rocket","memecoin rocket","dogecoin rocket","pepe rocket","shares skyrocket","stock soars","shares surge",
"houston rockets","rockets vs","rockets face","rocket city","rocket city half marathon","spengler cup","rare strike","80th minute rocket","rocket of a shot","rocket sealed","rocket cancelled out",
"satellite bar","satellite campus","satellite clinic","satellite location","satellite branch", "satellite store","safe space initiative","personal space","space in your home","storage space","parking space",
]


#Categorization / Tagging - Related Functions
def contains(text: str, keywords: list) -> bool:
    """
    Ensures that incoming records include space-security content
    """
    if not text:
        return False

    text = address_text_issues(text)

    for word in keywords:
        if re.search(rf"\b{re.escape(word)}\b", text):
            return True
    return False

def classify_event(text: str) -> str:
    """
    categorizes relevant SpaceFlight records
    """
    if not text:
        return "other"

    text = address_text_issues(text)
    
    if any(p in text for p in na_phrases):
        return "other"
    
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in text:
                return category

    return "other" 

def standardize_dates(value):
    """
    Standardize all incoming date formats to pd datetime for PostgreSQL.
    """
    try:
        return pd.to_datetime(value, utc=True)
    except Exception:
        return None

def address_text_issues(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"<[^>]+>", " ", text)       # strip HTML
    text = re.sub(r"(’s|'s)\b", "", text)      # remove possessives FIRST
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()

    return text
    
def classify_countries(text: str) -> list[str]:
    """
    Used for tagging countries mentioned in the record.
    """
    text = address_text_issues(text)
    matches = set()

    for country, keywords in countries.items():
        for k in keywords:
            if re.search(rf"\b{re.escape(k)}\b", text):
                matches.add(country)
                break
    return sorted(matches)

def classify_entity(text: str) -> list[str]:
    """
    Used for tagging companies/organizations/institutions in the record.
    """
    text = address_text_issues(text)
    matches = []
    for entity, keywords in entities.items():
        for k in keywords:
            if re.search(rf"\b{re.escape(k)}\b", text):
                matches.append(entity)
                break
    return sorted(set(matches))

#Archived Functions (used once, no longer needed)
# EXCEL_FILE = "GPI_public_release_2025.xlsx"

# def clean_base(df):
#     """
#     Ensure both tabs in Excel file are easily digestible.
#     """
#     df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
#     df["country"] = df["country"].astype(str).str.strip()
#     return df.dropna(subset=["iso3"])

# df_2025 = (
#     pd.read_excel(EXCEL_FILE, sheet_name="2025", skiprows=5)
#       .rename(columns={"geocode": "iso3", "overall score": "gpi_score"})
#       .assign(year=2025)
#       [["iso3", "country", "year", "gpi_score"]]
#       .pipe(clean_base)
#       .dropna(subset=["gpi_score"])
# )
# #merge the '2025' and 'overall scores' tabs into one table.
# df_overall = pd.read_excel(
#     EXCEL_FILE, sheet_name="Overall Scores", skiprows=7
# ).rename(columns={"geocode": "iso3"})

# year_cols = [c for c in df_overall.columns if "_score" in c or "_rank" in c]

# df_long = (
#     df_overall
#     .melt(id_vars=["iso3", "country"], value_vars=year_cols,
#           var_name="year_metric", value_name="value")
#     .assign(
#         year=lambda x: x["year_metric"].str.extract(r"(\d{4})").astype(int),
#         metric=lambda x: x["year_metric"].str.extract(r"(score|rank)")
#     )
#     .pivot_table(index=["iso3", "country", "year"],
#                  columns="metric", values="value", aggfunc="first")
#     .reset_index()
#     .rename(columns={"score": "gpi_score", "rank": "gpi_rank"})
#     .pipe(clean_base)
#     .dropna(subset=["gpi_score"])
# )
# #insert into Postgres
# df_final = (
#     pd.concat([df_long, df_2025])
#       .drop_duplicates(subset=["iso3", "year"])
# )
# engine = create_engine("postgresql+psycopg2://", creator=lambda: connection)
# df_final.to_sql("peace_index", engine, if_exists="append", index=False)
# print(f"Inserted {len(df_final)} rows into peace_index")

#Consolidating Space News - Related Functions

def get_spaceflight_articles(max_records: int = 1000):
    """
    Access SpaceFlightNews API Records
    """
    url = "https://api.spaceflightnewsapi.net/v4/articles"
    all_articles = []
    offset = 0
    limit = 100

    while len(all_articles) < max_records:
        params = {
            "limit": limit,
            "offset": offset,
            "ordering": "-published_at"
        }

        response = requests.get(url, params=params, timeout=20)

        if response.status_code != 200:
            print(f"Error {response.status_code}")
            break

        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        all_articles.extend(results)
        offset += limit

    return all_articles[:max_records]


def make_spaceflight_df(articles):
    """
    Consolidate SpaceFlightNews API results into dataframe
    """
    if not articles:
        return pd.DataFrame()
    df = pd.DataFrame(articles)

    target_cols = ["id", "title", "summary", "news_site", "published_at", "updated_at"]
    keep_cols = []
    
    for col in target_cols:
        if col in df.columns:
            keep_cols.append(col)

    df = df[keep_cols]
    df["source"] = "Spaceflight News API"
    return df

def get_gdelt_articles(query: str, max_records: int = 200):
    """
    Access GDELT API Records
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"

    params = {
        "query": query,
        "mode": "artlist",
        "timespan": "1month",
        "format": "json",
        "maxrecords": max_records,
        "language": "eng"
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        print(f"GDELT error {response.status_code}")
        return []

    try:
        data = response.json()
    except ValueError:
        print("GDELT returned non-JSON")
        return []

    return data.get("articles", [])

def make_gdelt_df(articles):
    """
    Consolidate GDELT API results into dataframe
    """
    if not articles:
        return pd.DataFrame()

    df = pd.DataFrame(articles)

    target_cols = ["title", "sourcecountry", "seendate", "url"]
    df = df[[col for col in target_cols if col in df.columns]]

    df["source"] = "GDELT DOC 2.0"
    return df

def get_google_articles():
    """
    Google Functions (via pygooglenews RSS from github)
    """
    gn = GoogleNews(lang="en", country="US")
    rows = []

    for space_kw in space_words:
        for risk_kw in (security_words + adversary_words):
            query = f"{space_kw} {risk_kw}"
            
            try:
                feed = gn.search(query, when="7d")
                entries = feed.get("entries", [])

                for entry in entries[:5]:
                    rows.append({
                        "space_keyword": space_kw,
                        "risk_keyword": risk_kw,
                        "query": query,
                        "title": entry.get("title", ""),
                        "summary": entry.get("summary", ""),
                        "url": entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "collected_at": datetime.utcnow().isoformat(),
                        "source": "Google News"
                    })

                time.sleep(1.5)

            except Exception as e:
                print("Google News error:", e)

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset="url")
    return df

def categorize_records(df: pd.DataFrame, source_api: str) -> pd.DataFrame:
    """
    Categorize and standardize all records by defined columns for database
    """
    records = []
    for _, row in df.iterrows():
        if source_api == "spaceflight_news":

            text = address_text_issues(f"{row['title']} {row.get('summary','')}")
            if any(p in text for p in na_phrases):
                continue
            
            if not contains(text, space_words):
                continue

            if any(a in text for a in exclude):
                continue

            event_type = classify_event(text)
            
            if event_type == "other":
                continue

            countries = classify_countries(text)
            entities = classify_entity(text)

            records.append({
                "title": row["title"],
                "summary": row.get("summary"),
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["published_at"]),
                "event_type": event_type,
                "is_space_related": True,
                "is_security_related": contains(text, security_words),
                "countries": countries,
                "entities": entities,
                "raw_source": row.get("id"),
                "time_classified": datetime.utcnow()
            })
        elif source_api == "gdelt":
            text = address_text_issues(row["title"])
            if any(p in text for p in na_phrases):
                continue

            if not row.get("has_space"):
                continue
            
            countries = classify_countries(text)
            event_type = classify_event(text)
            if event_type == "other":
                continue

            entities = classify_entity(text)

            if row.get("sourcecountry"):
                countries.append(row["sourcecountry"])

            countries = sorted(set(countries))

            records.append({
                "title": row["title"],
                "summary": None,
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["seendate"]),
                "event_type": event_type,
                "is_space_related": row["has_space"],
                "is_security_related": row["has_security"] or row["has_adversary"],
                "countries": countries,
                "entities": entities,
                "raw_source": row["url"],
                "time_classified": datetime.utcnow()
            })

        elif source_api == "google_news":
            text = address_text_issues(f"{row['title']} {row.get('summary','')}")
            
            if any(p in text for p in na_phrases):
                continue

            if not contains(text, space_words):
                continue

            if any(a in text for a in exclude):
                continue

            event_type = classify_event(text)
            if event_type == "other":
                continue

            countries = classify_countries(text)
            entities = classify_entity(text)

            records.append({
                "title": row["title"],
                "summary": row.get("summary"),
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["published"]),
                "event_type": event_type,
                "is_space_related": True,
                "is_security_related": contains(text, security_words),
                "countries": countries,
                "entities": entities,
                "raw_source": row["url"],
                "time_classified": datetime.utcnow()
            })

    df_out = pd.DataFrame(records, columns=record_cols)
    return df_out

#Database Management - Related Functions
def insert_records(df):
    """
    Insert records into database for SQL queries
    """
    inserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO space_records (
                published_date,
                time_classified,
                source_api,
                source,
                title,
                summary,
                event_type,
                is_space_related,
                is_security_related,
                countries,
                entities,
                raw_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_api, raw_source) DO NOTHING
            RETURNING event_id;
        """, (
            row["published_date"],
            row["time_classified"],
            row["source_api"],
            row["source"],
            row["title"],
            row["summary"],
            row["event_type"],
            row["is_space_related"],
            row["is_security_related"],
            row["countries"],   # list → TEXT[]
            row["entities"],    # list → TEXT[]
            row["raw_source"]
        ))

        if cursor.fetchone():
            inserted += 1

    connection.commit()
    print(f"Inserted {inserted} new records into space_records.")

def close_connection():
    cursor.close()
    connection.close()

def count_records():
    cursor.execute("SELECT COUNT(*) FROM space_records;")
    return cursor.fetchone()[0]

def update_older_records():
    """
    Used only when records were not tagged (must be null)
    """
    cursor.execute("""
        SELECT event_id, title, summary
        FROM space_records
        WHERE countries = '{}'
        AND entities = '{}';
    """)
    rows = cursor.fetchall()

    updated = 0

    for event_id, title, summary in rows:
        text = address_text_issues(f"{title} {summary or ''}")
        
        if any(p in text for p in na_phrases):
            continue
        countries = classify_countries(text)
        entities = classify_entity(text)

        cursor.execute("""
            UPDATE space_records
            SET countries = %s,
                entities = %s
            WHERE event_id = %s;
        """, (countries, entities, event_id))

        updated += 1

    connection.commit()
    print(f"Retagged {updated} existing records.")

def retag_all_records():
    """
    Used only when records are mislabeled and requires wiping all tagged records from the database.
    """
    cursor.execute("""
        SELECT event_id, title, summary
        FROM space_records;
    """)
    rows = cursor.fetchall()

    updated = 0

    for event_id, title, summary in rows:
        text = f"{title} {summary or ''}"
        countries = classify_countries(text)
        entities = classify_entity(text)

        cursor.execute("""
            UPDATE space_records
            SET countries = %s,
                entities = %s
            WHERE event_id = %s;
        """, (countries, entities, event_id))

        updated += 1

    connection.commit()
    print(f"Retagged {updated} records.")


def export_views_to_excel(output_file="/Users/rachel/Desktop/DI-Bootcamp/FinalProject/data/tableau_data.xlsx"):
    """
    Export SQL views to a single Excel workbook for Tableau.
    Writes to a temp file first, then atomically replaces the target file.
    """
    from sqlalchemy import create_engine
    import pandas as pd
    from connection import connection

    engine = create_engine("postgresql+psycopg2://", creator=lambda: connection)

    views = {
        "tableau_space_records": "public.v_tableau_space_records",
        "tableau_space_counts": "public.v_tableau_space_counts",
        "baseline_filter": "public.v_baseline_filter",
        "baseline_space_security": "public.v_baseline_space_security",
        "security_by_country": "public.v_security_events_by_country",
        "security_by_entity": "public.v_security_events_by_entity",
        "weekly_country_trends": "public.v_weekly_country_trends",
        "weekly_entity_trends": "public.v_weekly_entity_trends",
        "country_cooccurrence": "public.v_country_cooccurrence",
        "entity_cooccurrence": "public.v_entity_cooccurrence",
        "country_entity_shared": "public.v_country_entity_mentions",
        "headlines_ml": "public.v_space_headlines_period",
        "security_by_country_with_gpi": "public.v_security_by_country_with_gpi",
    }

    output_path = Path(output_file)
    tmp_path = output_path.with_suffix(".tmp.xlsx")

    # Ensure folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        for sheet, view in views.items():
            print(f"Exporting {view} → {sheet}")

            df = pd.read_sql(f"SELECT * FROM {view};", engine)

            # Remove timezone info for Excel compatibility
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_convert(None)

            # Excel sheet names max 31 chars
            df.to_excel(writer, sheet_name=sheet[:31], index=False)

    # Atomic replace so Tableau never reads a partial file
    os.replace(tmp_path, output_path)

    print(f"\nExport reflected here: {output_path}")

#avoid manually moving /saving Colab-generated downloads to the Project Folder
def replace_colab_records(filename, dest_dir):
    src = max(
        (Path.home() / "Downloads").glob(filename),
        key=lambda p: p.stat().st_mtime
    )

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    shutil.copy2(src, tmp)
    os.replace(tmp, dest)

    print(f"Updated: {dest}")

#Main Program
if __name__ == "__main__":
    print("Fetching SpaceFlight News articles...")

    articles = get_spaceflight_articles(max_records=1000)
    df_spaceflight = make_spaceflight_df(articles)

    print("Fetching GDELT articles...")
    gdelt_query = "satellite"

    gdelt_articles = get_gdelt_articles(gdelt_query, max_records=200)
    df_gdelt = make_gdelt_df(gdelt_articles)
    
    #Deduplicate and tag GDELT records
    df_gdelt = df_gdelt.drop_duplicates(subset="url")

    if not df_gdelt.empty and "title" in df_gdelt.columns:
        has_space = []
        has_security = []
        has_adversary = []
        
        for title in df_gdelt["title"]:
            has_space.append(contains(title, space_words))
            has_security.append(contains(title, security_words))
            has_adversary.append(contains(title, adversary_words))

        df_gdelt["has_space"] = has_space
        df_gdelt["has_security"] = has_security
        df_gdelt["has_adversary"] = has_adversary

    #pull google rss
    print("Fetching Google News articles...")

    df_google = get_google_articles()

    #categorize information from incoming records by columns
    df_space_events = categorize_records(df_spaceflight, "spaceflight_news")
    df_gdelt_events = categorize_records(df_gdelt, "gdelt")
    df_google_events = categorize_records(df_google, "google_news")

    df_events = pd.concat(
        [df_space_events, df_gdelt_events, df_google_events],
        ignore_index=True
    )
    
    #consolidate all new records into space_records.csv and replaces the older file.
    space_csv_update = Path(
    "/Users/rachel/Desktop/DI-Bootcamp/FinalProject/data/space_records.csv"
)
    TMP_PATH = space_csv_update.with_suffix(".tmp.csv")

    space_csv_update.parent.mkdir(parents=True, exist_ok=True)
    
    df_events.to_csv(TMP_PATH, index=False)
    os.replace(TMP_PATH, space_csv_update)

    print(f"space_records.csv replaced at: {space_csv_update}")

    #insert into PostgreSQL
    #update_older_records()
    insert_records(df_events)
    print("Total new records: ", count_records())

    #integrate Colab - generated files
    # source = Path("/Users/rachel/Desktop/DI-Bootcamp/FinalProject")

    # replace_colab_records(
    # "ML_Weekly_Highlights.ipynb",
    # source / "notebooks"
    # )

    # replace_colab_records(
    # "weekly_headlines.csv",
    # source / "data"
    # )

    #export SQL data into excel for Tableau
    export_views_to_excel()

    #print("Retagging all existing records...")
    # retag_all_records()
    close_connection()