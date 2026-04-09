import requests
import json
import os
from datetime import datetime

# hn api to find stories link provided
hn_top = "https://hacker-news.firebaseio.com/v0/topstories.json"
hn_item = "https://hacker-news.firebaseio.com/v0/item/{}.json"

req_headers = {"User-Agent": "TrendScraper-personal/0.1"}

#selected keywords for better search 
topic_map = {
    "technology": [
        "ai", "software", "tech", "code", "computer", "data", "cloud",
        "api", "gpu", "llm", "openai", "github", "linux", "developer"
    ],
    "worldnews": [
        "war", "government", "country", "president", "election",
        "climate", "attack", "global", "china", "india", "russia",
        "uk", "eu", "policy", "military"
    ],
    "sports": [
        "sport", "match", "team", "player", "league",
        "football", "cricket", "tennis", "olympic", "championship",
        "score", "season", "coach", "tournament",
        "nba", "nfl", "fifa", "mlb", "world cup",
        "club", "goal", "cup", "athlete"
    ],
    "science": [
        "research", "study", "space", "physics", "biology",
        "discovery", "nasa", "scientists", "quantum", "experiment",
        "astronomy", "genome", "medicine", "neural",
        "brain", "cell", "protein", "particle", "mars",
        "telescope", "lab", "scientific", "satellite"
    ],
    "entertainment": [
        "movie", "film", "music", "netflix", "book",
        "show", "award", "streaming", "tv", "series",
        "hollywood", "anime", "video"
    ]
}

# 25 per topic beacuse of given constraint
stories_per_topic = 25


def guess_topic(title):
    t = title.lower()
    for topic in topic_map:
        for kw in topic_map[topic]:
            if kw in t:
                return topic
    return None


def hit_hn(url):
    try:
        r = requests.get(url, headers=req_headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        print("no internet or hn is down?")
        return None
    except requests.exceptions.Timeout:
        print("took too long:", url)
        return None
    except requests.exceptions.HTTPError as e:
        print("bad response", e)
        return None
    except Exception as e:
        print("unexpected error:", e)
        return None


def run():
    print("pulling top story ids...")

    id_list = hit_hn(hn_top)

    if id_list is None:
        print("failed at first step, giving up")
        return

    # hn for top500 as to be taken
    id_list = id_list[:1000]

    collected = []
    topic_tally = {}
    for t in topic_map:
        topic_tally[t] = 0

    done_count = 0

    for item_id in id_list:

        buckets_full = 0
        for t in topic_tally:
            if topic_tally[t] >= stories_per_topic:
                buckets_full += 1
        if buckets_full == len(topic_tally):
            print("all topics full, stopping early")
            break

        raw = hit_hn(hn_item.format(item_id))

        if raw is None:
            continue

        if raw.get("type") != "story":
            continue

        heading = raw.get("title", "").strip()
        if not heading:
            continue

        topic = guess_topic(heading)

        if topic is None:
            least_filled = None
            min_so_far = 99999
            for t in topic_tally:
                if topic_tally[t] < min_so_far:
                    min_so_far = topic_tally[t]
                    least_filled = t
            topic = least_filled

        if topic_tally[topic] >= stories_per_topic:
            continue

        entry = {
            "post_id":      raw.get("id"),
            "title":        heading,
            "topic":        topic,
            "points":       raw.get("score", 0),
            "comments":     raw.get("descendants", 0),
            "posted_by":    raw.get("by", "unknown"),
            "url":          raw.get("url", ""),
            "scraped_at":   datetime.now().isoformat()
        }

        collected.append(entry)
        topic_tally[topic] = topic_tally[topic] + 1
        done_count += 1

    print("\nstories per topic:")
    for t in topic_tally:
        bar = "#" * topic_tally[t]  
        print(f"  {t:15} {topic_tally[t]:3}  {bar}")

    # creating folder
    if not os.path.exists("data"):
        os.makedirs("data")

    stamp = datetime.now().strftime("%Y%m%d_%H%M")  # added time so reruns dont overwrite
    save_path = "data/hn_trends_" + stamp + ".json"

    fout = open(save_path, "w", encoding="utf-8")
    json.dump(collected, fout, indent=2)
    fout.close()

    print("\ndone! saved", done_count, "stories to", save_path)


run()
