-- Load data from a CSV file
books = LOAD '/home/hadoop/IST3134/asg/Books_rating.csv' USING PigStorage(',') AS (
    Id:chararray, Title:chararray, Price:float, User_id:chararray, profileName:chararray,
    review_helpfulness:chararray, review_score:float, review_time:long,
    review_summary:chararray, review_text:chararray
);

-- Clean and tokenize the review summaries
cleaned_books = FOREACH books GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(review_summary), '[^a-zA-Z\\s]', ''))) AS word;

-- Filter out empty words and 1-character words
filtered_books = FILTER cleaned_books BY word != '' AND SIZE(word) > 1;

-- Remove words with repeated characters (e.g., "aa", "aaaa")
grouped_words = GROUP filtered_books BY word;
no_repeated_characters = FOREACH grouped_words {
    -- Filter words with no repeated characters (single characters or no repetitions)
    repeated_chars = FILTER filtered_books BY (SIZE(TOKENIZE(REPLACE(word, '(.)(?=\\1)', ''))) == 1);
    GENERATE FLATTEN(repeated_chars) AS word;
}

-- Perform word count
word_counts = GROUP no_repeated_characters BY word;
word_counts = FOREACH word_counts GENERATE group AS word, COUNT(no_repeated_characters) AS count;

-- Order word count in descending order
ordered_word_counts = ORDER word_counts BY count DESC;

-- Store the word count
STORE ordered_word_counts INTO '/home/hadoop/IST3134/asg/Pig_wordcount' USING PigStorage(',');
