
import pandas as pd
import numpy as np


# read csv file for data
df = pd.read_csv("data/trends_clean.csv")

# print shape
print("Loaded data:", df.shape)

# print first 5 rows
print("\nFirst 5 rows:")
print(df.head())



avg_score = df["score"].mean()

avg_comments = df["num_comments"].mean()

print("\nAverage score :", round(avg_score, 3))
print("Average comments:", round(avg_comments))


# convert columns to numpy arrays
score_array = df["score"].to_numpy()
comments_array = df["num_comments"].to_numpy()

# numpy calculations
mean_score = np.mean(score_array)
median_score = np.median(score_array)
std_score = np.std(score_array)

max_score = np.max(score_array)
min_score = np.min(score_array)

print("\n--- NumPy Stats ---")
print("Mean score :", round(mean_score, 3))
print("Median score :", round(median_score, 3))
print("Std deviation :", round(std_score, 3))
print("Max score :", max_score)
print("Min score :", min_score)


category_counts = df["category"].value_counts()

#top category found
top_category = category_counts.idxmax()
top_count = category_counts.max()

print("\nMost stories in:", top_category, f"({top_count} stories)")


max_comment_index = df["num_comments"].idxmax()

most_commented = df.loc[max_comment_index]

print("\nMost commented story:",
      f"\"{most_commented['title']}\"",
      "-", most_commented["num_comments"], "comments")

#engagement column
df["engagement"] = df["num_comments"] / (df["score"] + 1)

#popular col
df["is_popular"] = df["score"] > avg_score

#saving file
df.to_csv("data/trends_analysed.csv", index=False)

print("\nSaved to data/trends_analysed.csv")