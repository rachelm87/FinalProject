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

#keyword groupings to identify relevant information based on API source
space_words = ["satellite", "space", "spaceport", "spacecraft", "orbit", "asat", "gnss", "launch", "rocket"]

security_words = ["military", "defense", "defence", "missile", "ballistic", "weaponization", "weaponisation", "warfare", "deterrence", "counterspace", "cyber", "cyberattack", "jamming", "spoofing", "interference", "hacking", "attack", "threat", "vulnerability", "exploit", "malicious", "investigation", "risk", "dual-use"]

adversary_words = ["terrorism", "terrorist", "crime", "criminal", "smuggling", "trafficking", "extremist", "armed group"]

#function identifies records with a space-security intersection
def space_sec_nexus(text: str) -> bool:
    if not text:
        return False

    text = text.lower()

    has_space = any(w in text for w in space_words)
    has_security = any(w in text for w in security_words)
    has_adversary = any(w in text for w in adversary_words)

    return has_space and (has_security or has_adversary)

def contains(text: str, keywords: list) -> bool:
    if not text:
        return False
    
    text = text.lower()

    for word in keywords:
        if word in text:
            return True

    return False


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
        print("No new articles.")
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


def filter_gdelt_space_security(df):
    if df.empty:
        return df

# Combine title + description if available
    text_series = df["title"].fillna("")

    if "description" in df.columns:
        text_series = text_series + " " + df["description"].fillna("")

    mask = text_series.apply(space_sec_nexus)
    return df[mask]

#Task 2.1: Consolidate GDELT API results into dataframe
def make_gdelt_df(articles):
    if not articles:
        print("No GDELT articles.")
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

    print("Fetching Records...\n")

    for space_kw in space_words:
        for risk_kw in (security_words + adversary_words):
            query = f"{space_kw} {risk_kw}"
            
            try:
                feed = gn.search(query, when="7d")
                entries = feed.get("entries", [])

                print(f"Query '{query}' → {len(entries)} articles")

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
                print(f"Query '{query}' failed: {e}")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset="url")
    return df

#Main Program
if __name__ == "__main__":
    print("Fetching SpaceFlight News articles...")

    articles = get_spaceflight_articles(max_records=1000)
    df_spaceflight = make_spaceflight_df(articles)
    df_spaceflight.to_csv("spaceflight_news_raw.csv", index=False)
    print("Saved spaceflight_news_raw.csv")

    print("Fetching GDELT articles...")

    gdelt_query = "satellite"

    gdelt_articles = get_gdelt_articles(gdelt_query, max_records=200)
    df_gdelt = make_gdelt_df(gdelt_articles)

    # Deduplicate
    df_gdelt = df_gdelt.drop_duplicates(subset="url")

    # 🔹 TAG (do NOT filter)
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

    print("Total GDELT articles (unfiltered):", len(df_gdelt))
    print("Saved gdelt_data_raw.csv")

    #pull google rss
    print("Fetching Google News articles...")

    df_google = get_google_articles()

    print("Total Google News articles:", len(df_google))
    df_google.to_csv("google_news_raw.csv", index=False)
    print("Saved google_news_raw.csv")
    