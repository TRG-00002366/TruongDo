from pyspark.sql import SparkSession
import re
import os 
import shutil

#O6 Loading from multiple sources
def load_text_file(sc, path):
    if not os.path.isfile(path):
        print(f"ERROR: File not found -> {path}")
        raise SystemExit(1)

    rdd = sc.textFile(path)

    # force Spark to read immediately
    rdd.take(1)

    return rdd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Sherlock Holmes Analytics") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "/home/truongdo/TruongDo/MiniProject/sherlock_holmes.txt"
lines = load_text_file(sc, input_path)

#O2 Global Error (Accumulator) 
blank_line_acc = sc.accumulator(0)

def count_blank_lines(line):
    if line.strip() == "":
        blank_line_acc.add(1)
    return line

lines = lines.map(count_blank_lines)
lines.count()
print("Blank lines:", blank_line_acc.value)

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
# Broadcast stopwords O1 & T1
broadcast_stopwords = sc.broadcast(stopwords)
filtered_words = words.filter(lambda word: word not in broadcast_stopwords.value)

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
total_chars, total_words = (filtered_words.map(lambda w: (len(w),1)).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])))

# Average word length
average_word_length = total_chars / total_words

#T11 Distribution of word lengths
word_length_counts = (filtered_words.map(lambda w: (len(w), 1)).reduceByKey(lambda a, b: a + b))

#T12 Chapter Extraction
all_lines = lines.collect()

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


#O3 Word-Length Pairing
length_word_pairs = filtered_words.map(lambda w: (len(w), w))

#O4 Character Fequency
letter_counts = (lines.map(lambda l: l.lower()).flatMap(lambda line: list(line)).filter(lambda c: c.isalpha())
    .map(lambda c: (c, 1)).reduceByKey(lambda a, b: a + b).sortByKey())

#O5 Grouped Word Lists
grouped_words = (filtered_words.map(lambda w: (w[0], w)).groupByKey().mapValues(list))

#O7 Saving Analytics Results
word_counts = filtered_words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
output_rdd = word_counts.map(lambda x: f"{x[0]}\t{x[1]}")

output_path = "Holmes_WordCount_Results"

if os.path.exists(output_path):
    shutil.rmtree(output_path)

print("Total characters:", total_characters)
print("Longest line length:", longest_len)
print("Watson lines:", len(results))
print("Unique vocab:", unique_word_count)
print("Avg word length:", average_word_length)
print("Top 10 words:", top_10_words)

output_rdd.saveAsTextFile(output_path)
print("Saved word count to:", output_path)

spark.stop()