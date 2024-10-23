# Databricks notebook source
!pip install Levenshtein

# COMMAND ----------

import Levenshtein

def fuzzy_match(expected_word, current_word, threshold):
    for word in current_word:
        distance = Levenshtein.distance(expected_word.lower(), word.lower())
        max_length = max(len(expected_word), len(word))
        similarity = 1 - (distance / max_length)

        # return similarity >= threshold
        if similarity >= threshold:
            print(f"'{expected_word}' and '{word}' are similar.")
            current_word[current_word.index(word)] = expected_word
        else:
            print(f"'{expected_word}' and '{word}' are not similar.")

# Example usage:
expected_word = "Redislabs"
current_word = ["RedisLabs", "Redisabs", "Redis abs", "edisLabs", "cdp", "contenthub"]
threshold = 0.85

fuzzy_match(expected_word, current_word, threshold)
print(current_word)

# COMMAND ----------


