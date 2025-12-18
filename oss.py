#Outer Space Security Project:
##This file will grow over time as we:
###gradually add different data sources;
###integrate RSS and webscrapping; 
###refine with keywords and other filtering methods;
###train it to begin picking up other datapoints / classify it correctly using ML.

####Types of Stats: seeing a list of most recent news articles, 
####attacks by type and location, 
###new national developments (like germany, space policy, russia new spaceport)
####tailored view of international product/policy/milestone
####space companies by money and attributed to which country?

import requests
import pandas as pd
from tabulate import tabulate

security_keywords:list[str] = ["attack", "threat", "terror", "terrorism",
    "crime", "criminal", "jamming", "spoofing",
    "interference", "cyber", "cyberattack", "hacking",
    "military", "defense", "ASAT", "anti-satellite",
    "missile", "debris", "incident", "vulnerability",
    "GPS", "GNSS", "outage"]

#access Spaceflight News API articles
def get_articles(limit: int = 10):

    url = 'https://api.spaceflightnewsapi.net/v4/articles'
    params = {"limit": limit,
              "ordering": "-published_at"}
              #"published_at_gt": "published_at_gt"} #get newest first
    
    response = requests.get(url, params=params, timeout=20)
    print(f"Status Code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error. {response.text}.")
        return []
    
    data = response.json()

    articles = data.get("results", [])

    return articles

#Consolidate into dataframe
def make_df(articles):
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
    return df



#Filter articles for relevance
def get_security_keywords(df, security_keywords):

    if df.empty:
        print("Nothing to filter.")
        return df
    
    filtered_df = pd.DataFrame(columns=df.columns)

    for index, row in df.iterrows():

        title_text = str(row.get("title", "")).lower()
        summary_text = str(row.get("summary", "")).lower()

        for word in security_keywords:
            word_lower = word.lower()

            if word_lower in title_text or word_lower in summary_text:
                filtered_df = pd.concat([filtered_df, row.to_frame().T])
                break #don't check for additional keywords

    return filtered_df

def print_pretty_table(df):
    if df.empty:
        print("No results to display.")
        return
    
    table = PrettyTable()
    table.field_names = df.columns.tolist()

    for _, row in df.iterrows():
        table.add_row(row.tolist())

    print(table)

#Main Program (so we can use for future)

if __name__ == "__main__":
    print("Fetching the articles.")

    #Step 1: get_articles

    articles = get_articles(limit=30)

    #Step 2: make_df

    df_all = make_df(articles)
    print(df_all.head())

    #Step 3: get_security_keywords

    df_filtered = get_security_keywords(df_all, security_keywords)
print("\nFiltered Articles (Pretty Table):")
print(tabulate(df_filtered, headers='keys', tablefmt='psql'))

# Save only filtered security-relevant articles
df_filtered.to_csv("filtered_articles.csv", index=False)

print("\nSaved all_articles.csv and filtered_articles.csv")