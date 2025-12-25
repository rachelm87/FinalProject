#Outer Space Security Project:
##This file takes a layered approach:
###Task 1: First, we run the SpaceFlightNews API to establish a baseline understanding of what's happening in outer space.
###Task 2: Then, we want to know what's happening around the world from a space security standpoint through the GDELT API.
###Task 3: Integrated Google RSS (using github code) because GDELT was yielding limited results


###refine with keywords and other filtering methods;
###train it to begin picking up other datapoints / classify it correctly using ML.

####Types of Stats: seeing a list of most recent news articles, 
####attacks by type and location, 
###new national developments (like germany, space policy, russia new spaceport)
####tailored view of international product/policy/milestone
####space companies by money and attributed to which country?

import requests
import pandas as pd
import time
from datetime import datetime
from pygooglenews import GoogleNews
from connection import connection, cursor


#keyword groupings to identify relevant information based on API source
space_words = ["satellite", "space", "spaceport", "spacecraft", "orbit", "asat", "gnss", "launch", "rocket"]

security_words = ["military", "defense", "defence", "missile", "ballistic", "weaponization", "weaponisation", "warfare", "deterrence", "counterspace", "cyber", "cyberattack", "jamming", "spoofing", "interference", "hacking", "attack", "threat", "vulnerability", "exploit", "malicious", "investigation", "risk", "dual-use"]

adversary_words = ["terrorism", "terrorist", "crime", "criminal", "smuggling", "trafficking", "extremist", "armed group"]

categories = {
    "security_event": ["attack", "jamming", "spoofing", "counterspace","threat", "interference", "missile", "situational awareness", "reconnaisance", "surveillance", "counterspace"],
    "launch": ["launch", "launched", "liftoff", "rocket", "deploy", "mission", "flight"],
    "ground_infrastructure": ["spaceport", "ground station", "launch site", "infrastructure"],
    "satellite_deployment": ["satellite", "deploy", "deployment", "constellation", "payload"],
    "policy_or_corporate": ["defense", "legislation", "space policy", "deal", "regulation", "contract", "strategy", "strategic", "review", "program", "plan", "security", "space force"]
} #categorize incoming SpaceFlightNews API records

record_cols = ["title", "summary", "source", "source_api", "published_date", "event_type", "is_space_related", "is_security_related", "country", "region", "raw_source", "time_classified"] #incoming records are structured according to database columns

#Functions applicable to all records 
def contains(text: str, keywords: list) -> bool:
    """
    Ensures that incoming records include space-security content
    """
    if not text:
        return False
    
    text = text.lower()

    for word in keywords:
        if word in text:
            return True

    return False

def classify_event(text: str) -> str:
    """
    categorizes relevant SpaceFlight records
    """
    if not text:
        return "other"

    text = text.lower()

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

#Task 1: Access SpaceFlightNews API Records
def get_spaceflight_articles(max_records: int = 1000):
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

#Task 1.1: Consolidate SpaceFlightNews API results into dataframe
def make_spaceflight_df(articles):
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

#Task 2: Access GDELT API Records
def get_gdelt_articles(query: str, max_records: int = 200):
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

#Task 2.1: Consolidate GDELT API results into dataframe
def make_gdelt_df(articles):
    if not articles:
        return pd.DataFrame()

    df = pd.DataFrame(articles)

    target_cols = ["title", "sourcecountry", "seendate", "url"]
    df = df[[col for col in target_cols if col in df.columns]]

    df["source"] = "GDELT DOC 2.0"
    return df

#Task 3: Google Functions (via pygooglenews RSS from github)
def get_google_articles():
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

            except Exception:
                pass

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset="url")
    return df

#Task 4: Categorize and standardize all records by defined columns for database
def categorize_records(df: pd.DataFrame, source_api: str) -> pd.DataFrame:
    records = []

    for _, row in df.iterrows():

        if source_api == "spaceflight_news":
            text = f"{row['title']} {row.get('summary', '')}"

            records.append({
                "title": row["title"],
                "summary": row.get("summary"),
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["published_at"]),
                "event_type": classify_event(text),
                "is_space_related": True,
                "is_security_related": contains(text, security_words),
                "country": None,
                "region": None,
                "raw_source": row.get("id"),
                "time_classified": datetime.utcnow()
            })

        elif source_api == "gdelt":
            if not row.get("has_space"):
                continue

            text = row["title"]

            records.append({
                "title": row["title"],
                "summary": None,
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["seendate"]),
                "event_type": classify_event(text),
                "is_space_related": row["has_space"],
                "is_security_related": row["has_security"] or row["has_adversary"],
                "country": row.get("sourcecountry"),
                "region": None,
                "raw_source": row["url"],
                "time_classified": datetime.utcnow()
            })

        elif source_api == "google_news":
            text = f"{row['title']} {row.get('summary', '')}"

            records.append({
                "title": row["title"],
                "summary": row.get("summary"),
                "source": row["source"],
                "source_api": source_api,
                "published_date": standardize_dates(row["published"]),
                "event_type": classify_event(text),
                "is_space_related": contains(text, space_words),
                "is_security_related": contains(text, security_words),
                "country": None,
                "region": None,
                "raw_source": row["url"],
                "time_classified": datetime.utcnow()
            })

    df_out = pd.DataFrame(records)
    return df_out[record_cols]

#Task 6: Insert records into database for SQL queries
def insert_records(df):
    inserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO space_records
            (published_date, time_classified, source_api, source,
             title, summary, event_type, is_space_related,
             is_security_related, country, region, raw_source)
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
            row["country"],
            row["region"],
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

#Main Program
if __name__ == "__main__":
    print("Fetching SpaceFlight News articles...")

    articles = get_spaceflight_articles(max_records=1000)
    df_spaceflight = make_spaceflight_df(articles)
    df_spaceflight.to_csv("spaceflight_news_raw.csv", index=False)

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

    # Save raw GDELT with tags
    df_gdelt.to_csv("gdelt_data_raw.csv", index=False)

    #pull google rss
    print("Fetching Google News articles...")

    df_google = get_google_articles()

    df_google.to_csv("google_news_raw.csv", index=False)

    #categorize information from incoming records by columns
    df_space_events = categorize_records(df_spaceflight, "spaceflight_news")
    df_gdelt_events = categorize_records(df_gdelt, "gdelt")
    df_google_events = categorize_records(df_google, "google_news")

    df_events = pd.concat(
        [df_space_events, df_gdelt_events, df_google_events],
        ignore_index=True
    )

    df_events.to_csv("space_records.csv", index=False)

    #insert into PostgreSQL
    insert_records(df_events)
    print("Total new records: ", count_records())
    close_connection()