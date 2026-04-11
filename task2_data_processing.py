import pandas as pd
import os
import glob

# finding all json files created by task1
all_jsons = glob.glob("data/hn_trends_*.json")

if len(all_jsons) == 0:
    print("no json found - task1 probably didnt finish or saved somewhere else")
    exit()

# for multiple runs select only latest one
all_jsons.sort()
todays_file = all_jsons[-1]

# loading json safely
try:
    loaded = pd.read_json(todays_file)
except ValueError as e:
    print("couldnt parse json, file might be broken:", e)
    exit()
except FileNotFoundError:
    print("file disappeared between glob and read, weird")
    exit()

print("Loaded", len(loaded), "stories from", todays_file)

# working on copy so no problems arise
stories = loaded.copy()

# renaming columns to match processing pipeline
stories = stories.rename(columns={
    "points": "score",
    "comments": "num_comments",
    "topic": "category"
})

# dropping duplicates using post_id
orig_len = len(stories)
stories = stories.drop_duplicates(subset="post_id")
dupes_removed = orig_len - len(stories)

if dupes_removed > 0:
    print("found", dupes_removed, "duplicate post_ids, removed them")

print("After removing duplicates:", len(stories))


# these columns must exist
must_have = ["post_id", "title", "score"]

# removing rows missing critical values
stories = stories.dropna(subset=must_have)
print("After removing nulls:", len(stories))


# changing score to int
stories["score"] = stories["score"].astype(int)

# fixing comments column
if "num_comments" in stories.columns:
    stories["num_comments"] = stories["num_comments"].fillna(0)
    stories["num_comments"] = stories["num_comments"].astype(int)
else:
    # full column empty make it zero
    stories["num_comments"] = 0


# removing weak stories
weak = stories["score"] < 5
print("dropping", weak.sum(), "stories under score 5")
stories = stories[weak == False]

print("After removing low scores:", len(stories))


# cleaning trailing spaces
stories["title"] = stories["title"].str.strip()


# saving cleaned file
output_path = "data/trends_clean.csv"

try:
    stories.to_csv(output_path, index=False)
except PermissionError:
    print("cant write to csv - is trends_clean.csv open somewhere?")
    exit()
except Exception as e:
    print("save failed for some reason:", e)
    exit()

print("\nSaved", len(stories), "rows to", output_path)


# category report
print("\nStories per category:")
topic_breakdown = stories["category"].value_counts()

for t in topic_breakdown.index:
    print(" ", t, " ", topic_breakdown[t])


# checking empty categories
empty_topics = []
for t in topic_breakdown.index:
    if topic_breakdown[t] == 0:
        empty_topics.append(t)

if len(empty_topics) > 0:
    print("warning: these categories ended up empty:", empty_topics)
