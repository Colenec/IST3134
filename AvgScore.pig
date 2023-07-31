-- Load data from a CSV file and define schema for each column
books = LOAD '/home/hadoop/IST3134/asg/Books_rating.csv' USING PigStorage(',') AS (
    Id:chararray, Title:chararray, Price:float, User_id:chararray, profileName:chararray,
    review_helpfulness:chararray, review_score:float, review_time:long,
    review_summary:chararray, review_text:chararray
);

-- Group all records together to calculate the average score
grouped_data = GROUP books ALL;

-- Calculate the average review score using the 'AVG' function
avg_score = FOREACH grouped_data GENERATE AVG(books.review_score) AS average_score;

-- Store the average score to a new file
STORE avg_score INTO '/home/hadoop/IST3134/asg/Pig_avgscore' USING PigStorage(',');
