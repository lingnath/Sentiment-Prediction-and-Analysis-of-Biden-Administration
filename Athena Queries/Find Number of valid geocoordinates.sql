-- Find the number of rows where geocoordinates exist for the tweet
SELECT count(*)-
       (SELECT count(*) FROM twitter_analysis.twitter_raw
        where geocoordinates='None' or geocoordinates is NULL)
FROM twitter_analysis.twitter_raw;