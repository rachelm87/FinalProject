import pandas as pd
from datetime import datetime


categories = {
    "security_event": ["attack", "jamming", "spoofing", "counterspace","threat", "interference", "missile", "situational awareness", "reconnaisance", "surveillance", "counterspace"],
    "launch": ["launch", "launched", "liftoff", "rocket", "deploy", "mission", "flight"],
    "ground_infrastructure": ["spaceport", "ground station", "launch site", "infrastructure"],
    "satellite_deployment": ["satellite", "deploy", "deployment", "constellation", "payload"],
    "policy_or_corporate": ["defense", "legislation", "space policy", "deal", "regulation", "contract", "strategy", "strategic", "review", "program", "plan", "security", "space force"]
}

def classify(text):
    if not text:
        return "other"

    text = text.lower()

    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in text:
                return category

    return "other"



# ----------------------------
# BUILD SPACE EVENTS
# ----------------------------

if __name__ == "__main__":

    df = pd.read_csv("spaceflight_news_raw.csv")

    events = []

    for _, row in df.iterrows():
        text = f"{row['title']} {row.get('summary', '')}"

        event_type = classify(text)

        events.append({
            "event_date": row["published_at"],
            "event_type": event_type,
            "description": row["title"],
            "source": row["source"],
            "created_at": datetime.utcnow().isoformat()
        })

    events_df = pd.DataFrame(events)

    events_df.to_csv("space_events.csv", index=False)

    print("Saved space_events.csv")


other_df = events_df[events_df["event_type"] == "other"]

print("\n--- SAMPLE OF 'OTHER' ITEMS ---")
print(f"Total 'other' records: {len(other_df)}\n")

# Print first 10 titles
for i, row in other_df.head(400).iterrows():
    print("-", row["description"])