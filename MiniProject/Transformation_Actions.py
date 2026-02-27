from pyspark.sql import SparkSession
import re

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Sherlock Holmes Analytics") \
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("sherlock_holmes.txt")

# Load file

#T1 Text Normalization
def normalize_text(line):
    line = line.lower()
    line = re.sub(r"[^\w\s]", "", line)   
    return line

normalized_lines = lines.map(normalize_text)

#T2 Tokenization
words = normalized_lines.flatMap(lambda line: line.split())

#T3 Filter Words
stopwords = {
    "the","a","an","is","am","are","was","were",
    "of","to","in","on","for","and","or","but",
    "i","you","he","she","it","we","they"
}
# Broadcast stopwords
broadcast_stopwords = sc.broadcast(stopword_list)

filtered_words = words.filter(lambda word: word not in stopwords)

#T4 Character Count
total_characters = lines.map(lambda l: len(l)).sum()

#T5 Line Length Analysis
longest_len, longest_line = lines.map(lambda l: (len(l), l)).max()

#T6 Word Search
watson_lines = lines.filter(lambda l: "watson" in l.lower())
results = watson_lines.collect()

#T7 Unique Vocabulary Count
unique_word_count = filtered_words.distinct().count()

#T8 Top 10 frequent Words
top_10_words = (filtered_words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
.takeOrdered(10, key=lambda x: -x[1]))

#T9 Sentences Start Distrbution
first_words = (normalized_lines.filter(lambda line: line.strip() !="").map(lambda line: line.split()[0]))

#count
count_word = (first_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b))

#T10 Distributions
# Total number of words
total_words = filtered_words.count()

# Total number of characters across all words
total_word_characters = filtered_words.map(lambda w: len(w)).sum()

# Average word length
average_word_length = total_word_characters / total_words

#T11 Distribution of word lengths
word_length_counts = (filtered_words.map(lambda w: (len(w), 1)).reduceByKey(lambda a, b: a + b))
sorted_length_counts = word_length_counts.sortByKey()

#T12 Chapter Extraction
start_title = "A SCANDAL IN BOHEMIA"
end_title = "THE RED-HEADED LEAGUE"

inside_section = False
section_lines = []

for line in all_lines:
    if start_title in line:
        inside_section = True
        continue

    if end_title in line:
        break
    if inside_section:
        section_lines.append(line)

#O1 Stopword filtering (broadcast)
filtered_words = words.filter(lambda word: word not in broadcast_stopwords.value)

spark.stop()