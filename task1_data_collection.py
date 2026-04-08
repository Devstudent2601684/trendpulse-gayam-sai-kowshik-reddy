import requests
import json
import time
import os
from datetime import datetime

TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

HEADERS = {"User-Agent": "TrendPulse/1.0"}

# category keywords to find best stories
CATEGORIES = {
    "technology": [
        "ai","software","tech","code","computer","data","cloud",
        "api","gpu","llm","openai","github","linux","developer"
    ],
    "worldnews": [
        "war","government","country","president","election",
        "climate","attack","global","china","india","russia",
        "uk","eu","policy","military"
    ],
   "sports": [
    "sport","game","match","team","player","league",
    "football","cricket","tennis","olympic","championship",
    "score","season","coach","tournament",
    "nba","nfl","fifa","mlb","world cup",
    "club","goal","cup","athlete"
    ],

    "science": [
    "research","study","space","physics","biology",
    "discovery","nasa","scientists","quantum","experiment",
    "astronomy","genome","medicine","neural",
    "brain","cell","protein","particle","mars",
    "telescope","lab","scientific","satellite"
    ],
    "entertainment": [
        "movie","film","music","netflix","game","book",
        "show","award","streaming","tv","series",
        "hollywood","anime","video"
    ]
}
MAX_PER_CATEGORY = 25


# to help the keyword search
def get_category(title):
    title = title.lower()

    for category, keywords in CATEGORIES.items():
        if any(keyword in title for keyword in keywords):
            return category

    return None


def fetch_json(url):
    """Safe API call"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Request failed: {e}")
        return None


# main code for the category search
def main():
    print("Fetching top stories...")

    story_ids = fetch_json(TOP_STORIES_URL)

    if not story_ids:
        print("Failed to fetch story IDs")
        return

    story_ids = story_ids[:1000]

    results = []
    category_counts = {cat: 0 for cat in CATEGORIES}

    for story_id in story_ids:

        # stop only when all categories 
        if all(count >= MAX_PER_CATEGORY for count in category_counts.values()):
            break

        story = fetch_json(ITEM_URL.format(story_id))
        if not story:
            continue

        title = story.get("title", "")
        if not title:
            continue

        category = get_category(title)

        if category is None:
            category = min(category_counts, key=category_counts.get)

        # skip if more than 25
        if category_counts[category] >= MAX_PER_CATEGORY:
            continue

        record = {
            "post_id": story.get("id"),
            "title": title.strip(),
            "category": category,
            "score": story.get("score", 0),
            "num_comments": story.get("descendants", 0),
            "author": story.get("by"),
            "collected_at": datetime.now().isoformat()
        }

        results.append(record)
        category_counts[category] += 1

    # to know which categories have 25 and less
    print("\nCategory counts:")
    for k, v in category_counts.items():
        print(f"{k}: {v}")

    # file saved as json
    os.makedirs("data", exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    output_file = f"data/trends_{date_str}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"\nCollected {len(results)} stories. Saved to {output_file}")


if __name__ == "__main__":
    main()
print("\nCategory counts:")
