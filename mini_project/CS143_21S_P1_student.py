# CS 143 - SPRING 2021 -- MINIPROJECT
# Skeleton Code

# AT THE BEGINNING OF EACH SESSION, COPY AND PASTE ALL OF THE FOLLOWING CODE
# UNTIL YOU REACH "STOP COPY/PASTE"

from pyspark.sql import SparkSession

import string

import pyspark.sql.functions as F

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import broadcast, col, date_format, desc, explode, from_unixtime, log, to_date, udf

spark = SparkSession.\
        builder.\
        appName("solution").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1024m").\
        getOrCreate()
sc = spark.sparkContext

_PUNCTUATION = "".join(string.punctuation + "’")

# You don't need to do anything with the stopwords, so don't be intimidated.
stopwords = frozenset(["'ll", "'ve", "I", "a", "a's", "able", "about", "above",
    "abst", "accordance", "according", "accordingly", "across", "act",
    "actually", "added", "adj", "affected", "affecting", "affects", "after",
    "afterwards", "again", "against", "ah", "ain't", "all", "allow", "allows", 
    "almost", "alone", "along", "already", "also", "although", "always", "am",
    "among", "amongst", "an", "and", "announce", "another", "any", "anybody",
    "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere",
    "apart", "apparently", "appear", "appreciate", "appropriate",
    "approximately", "are", "aren", "aren't", "arent", "arise", "around", "as",
    "aside", "ask", "asking", "associated", "at", "auth", "available", "away",
    "awfully", "b", "back", "be", "became", "because", "become", "becomes",
    "becoming", "been", "before", "beforehand", "begin", "beginning",
    "beginnings", "begins", "behind", "being", "believe", "below", "beside",
    "besides", "best", "better", "between", "beyond", "biol", "both", "brief",
    "briefly", "but", "by", "c", "c'mon", "c's", "ca", "came", "can", "can't",
    "cannot", "cant", "cause", "causes", "certain", "certainly", "changes",
    "clearly", "co", "com", "come", "comes", "concerning", "consequently",
    "consider", "considering", "contain", "containing", "contains",
    "corresponding", "could", "couldn't", "couldnt", "course", "currently", "d",
    "date", "definitely", "described", "despite", "did", "didn't", "different",
    "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards",
    "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty",
    "either", "else", "elsewhere", "end", "ending", "enough", "entirely",
    "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody",
    "everyone", "everything", "everywhere", "ex", "exactly", "example",
    "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix",
    "followed", "following", "follows", "for", "former", "formerly", "forth",
    "found", "four", "from", "further", "furthermore", "g", "gave", "get",
    "gets", "getting", "give", "given", "gives", "giving", "go", "goes",
    "going", "gone", "got", "gotten", "greetings", "h", "had", "hadn't",
    "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he",
    "he'd", "he'll", "he's", "hed", "hello", "help", "hence", "her", "here",
    "here's", "hereafter", "hereby", "herein", "heres", "hereupon", "hers",
    "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home",
    "hopefully", "how", "how's", "howbeit", "however", "hundred", "i", "i'd",
    "i'll", "i'm", "i've", "id", "ie", "if", "ignored", "im", "immediate", 
    "immediately", "importance", "important", "in", "inasmuch", "inc", "indeed",
    "index", "indicate", "indicated", "indicates", "information", "inner", 
    "insofar", "instead", "into", "invention", "inward", "is", "isn't", "it",
    "it'd", "it'll", "it's", "itd", "its", "itself", "j", "just", "k", "keep",
    "keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely",
    "last", "lately", "later", "latter", "latterly", "least", "less", "lest",
    "let", "let's", "lets", "like", "liked", "likely", "line", "little", "look",
    "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many",
    "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely",
    "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly",
    "mr", "mrs", "much", "mug", "must", "mustn't", "my", "myself", "n", "na",
    "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary",
    "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine",
    "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor",
    "normally", "nos", "not", "noted", "nothing", "novel", "now", "nowhere",
    "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok",
    "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or",
    "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves",
    "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages",
    "part", "particular", "particularly", "past", "per", "perhaps", "placed",
    "please", "plus", "poorly", "possible", "possibly", "potentially", "pp",
    "predominantly", "present", "presumably", "previously", "primarily",
    "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly",
    "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", 
    "reasonably", "recent", "recently", "ref", "refs", "regarding",
    "regardless", "regards", "related", "relatively", "research",
    "respectively", "resulted", "resulting", "results", "right", "run", "s",
    "said", "same", "saw", "say", "saying", "says", "sec", "second",
    "secondly", "section", "see", "seeing", "seem", "seemed", "seeming",
    "seems", "seen", "self", "selves", "sensible", "sent", "serious",
    "seriously", "seven", "several", "shall", "shan't", "she", "she'd",
    "she'll", "she's", "shed", "shes", "should", "shouldn't", "show", "showed",
    "shown", "showns", "shows", "significant", "significantly", "similar",
    "similarly", "since", "six", "slightly", "so", "some", "somebody",
    "somehow", "someone", "somethan", "something", "sometime", "sometimes",
    "somewhat", "somewhere", "soon", "sorry", "specifically", "specified",
    "specify", "specifying", "still", "stop", "strongly", "sub",
    "substantially", "successfully", "such", "sufficiently", "suggest", "sup",
    "sure", "t", "t's", "take", "taken", "taking", "tell", "tends", "th",
    "than", "thank", "thanks", "thanx", "that", "that'll", "that's", "that've",
    "thats", "the", "their", "theirs", "them", "themselves", "then", "thence",
    "there", "there'll", "there's", "there've", "thereafter", "thereby",
    "thered", "therefore", "therein", "thereof", "therere", "theres",
    "thereto", "thereupon", "these", "they", "they'd", "they'll", "they're",
    "they've", "theyd", "theyre", "think", "third", "this", "thorough",
    "thoroughly", "those", "thou", "though", "thoughh", "thousand", "three",
    "throug", "through", "throughout", "thru", "thus", "til", "tip", "to",
    "together", "too", "took", "toward", "towards", "tried", "tries", "truly",
    "try", "trying", "ts", "twice", "two", "u", "un", "under", "unfortunately",
    "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us",
    "use", "used", "useful", "usefully", "usefulness", "uses", "using",
    "usually", "v", "value", "various", "very", "via", "viz", "vol", "vols",
    "vs", "w", "want", "wants", "was", "wasn't", "wasnt", "way", "we", "we'd",
    "we'll", "we're", "we've", "wed", "welcome", "well", "went", "were",
    "weren't", "werent", "what", "what'll", "what's", "whatever", "whats",
    "when", "when's", "whence", "whenever", "where", "where's", "whereafter",
    "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever",
    "whether", "which", "while", "whim", "whither", "who", "who'll", "who's",
    "whod", "whoever", "whole", "whom", "whomever", "whos", "whose", "why",
    "why's", "widely", "will", "willing", "wish", "with", "within", "without",
    "won't", "wonder", "wont", "words", "world", "would", "wouldn't",
    "wouldnt", "www", "x", "y", "yes", "yet", "you", "you'd", "you'll",
    "you're", "you've", "youd", "your", "youre", "yours", "yourself",
    "yourselves", "z", "zero", "ucla", "class", "people", "classes", "lol",
    "thanks", "dm"])

@udf(ArrayType(StringType()))
def cleantext(x):
    # Convert to lower case.
    text = x.lower()
    # Split on space
    tokens = text.split(' ')
    # Remove stopwords
    nostops = ' '.join([token for token in tokens if token not in _STOPWORDS.value])
    # Remove punctuation.
    tokens = ''.join([c for c in nostops if c not in _PUNCTUATION]).split(' ')
    # Remove empty tokens.
    tokens = [token for token in tokens if token]
    return tokens


# UDF is automatically sent to all workers, but stopwords frozenset is not.
# We have to distribute it to the workers ourself:
_STOPWORDS = sc.broadcast(stopwords)


####################
# STOP COPY/PASTE
####################

##################################
# Tips for Writing and Debugging #
##################################
# 1. If you want to check that you are on the right track, use the
#    head(10) method. For example:
#    ucla.select("subreddit").show()
#    or ucla.select("subreddit").head(10)
#    will output the subreddit field to the screen.
#    Remember what we discussed about LAZY EVALUATION.
# 2. You can print the schema at any time by adding .printSchema()
# 3. Words in asterisks ** give a clue as to what operator to use. (use documentation)
#    You can probably use other operators as well if you find a better one.
# 4. The imports come from my solution, and this should also give a clue. This does NOT
#    mean you can only use my operators. You may input your own.
# 5. I've prefixed each step with a variable containing your output. You should only need
#    to write one line for each. You are free to use more, but note that you may also be doing
#    something wrong, it just depends on your method.
# 6. Be careful with aliases.
# 8. The usual aggregation functions are implemented: count, countDistinc, min, max, avg etc. These
#    are imported as "F".
# 9. TF-IDF is pretty naive for this problem. Don't expect to be impressed with the results.
##################################

###########################################
# YOU WILL WRITE CODE FOR STEPS 3, 5-14
###########################################

# STEP 1/2: Load the data and subset to only the three columns that we will
# use in this prokect. "body", the text of the comment, "created_utc" which
# is the  Unix timestamp when it was submitted, and "id" which is a unique
# identifier for each comment. We will also drop duplicates because the
# researcher fetched some comments multipe times.
ucla = spark.read.json("data/*.json")  # creates a DataFrame
ucla = ucla.select("id", "created_utc", "body").dropDuplicates()


# **TODO** STEP 3A (originally Step 6): Remove comments whose entire text is 
# "[deleted]", "deleted", "[removed]" and "removed" which just 
# means that the comment was deleted or removed and is not interesting to us.
# What operator would we use to find these?
result = ... 


# **TODO** STEP 3B: Use the UDF to clean the text of each comment by converting
# to lower case and splitting on a space after removing stopwords.  Note that
# splitting on a space gives us a LIST of tokens into the column. Note that
# cleantext is NOT perfect, you will see some bugs.
# HINT: Research withColumn operator, which creates a new column.
# NOTE: You may need to filter out empty rows first.
result = ... 

# STEP 4: There is a Unix timestamp (a long) in the field "created_utc". Let's
# create a new column "month" that converts the timestamp to a Python datetime object,
# and also only retains the MONTH and YEAR as "2019-11".
# Asking you to write code to convert dates/times would be cruel and unusual punishment,
# so I have done it for you.
result = result.withColumn("month", date_format(to_date(from_unixtime("created_utc"),
    "yyyy-MM-dd HH:mm:SS"), "yyyy-MM")).drop(result.created_utc)


# **TODO** STEP 5: Recall that each row of the column cleaned_text contains a list of
# tokens. We can't do anything with this. *Explode* this list so that each word appears
# as a new row. Then *drop* the cleaned_text column since we don't need it anymore.
result = ... 


# **TODO** STEP 7: Count the number of  times a token appears in each comment.
# How would you do it in SQL? If you're stuck, write out the SQL query and find the proper
# algebraic operators in Spark. *Alias* the resulting column "tf_num"
token_count_in_comments = ... 


# **TODO** STEP 8: Count the unique number of tokens in each comment. *Alias* the resulting
# column "tf_den"
tokens_in_comment = ...


# **TODO** STEP 9: Count the number of comments in each month-year and alias the resulting
# column as "idf_num".
comments_in_month = ... 


# **TODO** STEP 10: For each month-year, calculate the number of comments each unique token appears in.
# This is the denominator of the IDF term, so alias the resulting column "idf_den". IDF can be very
# noisy, so filter so only tokens that have an idf_den > 7 are included. This also speeds up execution.
comments_with_token = ... 


# **TODO** STEP 11: You now have 4 different tables containing the components of TF-IDF. You need
# to join them together. This data is small, but we still need to be very careful.
# Before believing your join is correct, add .explain() at the end of your statement to make sure
# Spark does the right thing. If it doesn't, you will may need to change a default (see Lecture 15)
# and also provide a join hint to the optimizer. Hint: you probably don't need to worry about this,
# if you do, use *broadcast*. The data is small enough that this shouldn't be an issue.
# NOTE: This should take about 5-10 minutes on a modern system once you call .collect(), .head(),
# or .show(). Remember, due to LAZY EVALUATION, the join command will return automatically.
joined = ...


# **TODO** STEP 12: Compute TF-IDF. See the formula in the spec. The log function is imported for you.
# There should be one TF-IDF value for each month-year/comment/token triple. Call the new column "tfidf".
tfidf_df = ... 


# **TODO** STEP 13: Having a TF-IDF score for each token in each comment in each time period is correct;
# however, it doesn't get us what we want. Let's now compute the average TF-IDF value of each token per month-year.
# *Alias* the column "avg_tfidf".
avg_df = ... 


# **TODO** STEP 13: Find the token(s) that have maximal average-TF-IDF within each month-year.
# This is a mathematical argmax, and can be implemented as a join.
# You will need to use a join on table avg_tfidf. Again, see the cautions in Step 11.
max_tfidf = ... 
max_joined = ... 
final_result = ... 


# Note that Spark may give you some problems in Step 13. If you get errors that you are referring to a non-existent
# column, particularly in the join to create max_joined, what's happening is that Spark sees multiple copies
# of the same column and isn't sure which column you're referring to. Use the following snippet (or something
# similar to it) if that happens, before moving on to the final join.
# max_tfidf = max_tfidf.select(F.col("month").alias("month"), "max")
# Depending on how you wrote your code, you may not run into this issue.

# **TODO** STEP 14: Finally, order the results by month and output just the month-year and token.
# Note that some months may be missing due to the earlier filter on IDF.
final_result = ...
final_result.show()

### EXECUTING MY SOLUTION TOOK APPROXIMATELY 10-15 MINUTES FROM TOP TO BOTTOM.
# You should make sure a step works correctly before moving to the next.
# This will save you time.
