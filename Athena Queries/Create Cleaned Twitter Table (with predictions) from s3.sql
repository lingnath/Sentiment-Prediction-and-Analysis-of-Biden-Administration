CREATE TABLE "twitter_analysis"."clean_table_with_predictions"
WITH (
   format='TEXTFILE',
   external_location='s3://tweets-data-nl/athenaoutput/clean_table_with_predictions/'
) AS
select username,
    screenname,
    tweetdirty,
    -- Find the tweet length
    length(tweetdirty) as tweet_length,
    -- Create new column of which 1 represents that there is a retweet, and 0 being not a retweet
    CASE WHEN tweetdirty LIKE '%RT%' THEN 1 ELSE 0 END as retweet,
    -- Extracting the username of the retweet user (minus the 'RT @')
    substr(REGEXP_EXTRACT(tweetdirty, 'RT @[a-zA-Z0-9_.-]+'),5, length(REGEXP_EXTRACT(tweetdirty, 'RT @[a-zA-Z0-9_.-]+'))-5+1) as retweet_users,
    followerscount,
    -- Bucketing the location into US states
    CASE WHEN location LIKE '%California%' OR location LIKE '%CA%' THEN 'CA'
     WHEN location LIKE '%Alabama%' OR location LIKE '%AL%' THEN 'AL'
     WHEN location LIKE '%Alaska%' OR location LIKE '%AK%' THEN 'AK'
     WHEN location LIKE '%Arizona%' OR location LIKE '%AZ%' THEN 'AZ'
     WHEN location LIKE '%Arkansas%' OR location LIKE '%AR%' THEN 'AR'
     WHEN location LIKE '%Colorado%' OR location LIKE '%CO%' THEN 'CO'
     WHEN location LIKE '%Connecticut%' OR location LIKE '%CT%' THEN 'CT'
     WHEN location LIKE '%Deleware%' OR location LIKE '%DE%' THEN 'DE'
     WHEN location LIKE '%Arkansas%' OR location LIKE '%AR%' THEN 'AR'
     WHEN location LIKE '%District of Co%' OR location LIKE '%DC%' THEN 'DC'
     WHEN location LIKE '%Florida%' OR location LIKE '%FL%' THEN 'FL'
     WHEN location LIKE '%Georgia%' OR location LIKE '%GA%' THEN 'GA'
     WHEN location LIKE '%Hawaii%' OR location LIKE '%HI%' THEN 'HI'
     WHEN location LIKE '%Idaho%' OR location LIKE '%ID%' THEN 'ID'
     WHEN location LIKE '%Illinois%' OR location LIKE '%IL%' THEN 'IL'
     WHEN location LIKE '%Indiana%' OR location LIKE '%IN%' THEN 'IN'
     WHEN location LIKE '%Iowa%' OR location LIKE '%IA%' THEN 'IA'
     WHEN location LIKE '%Kansas%' OR location LIKE '%KS%' THEN 'KS'
     WHEN location LIKE '%Kentucky%' OR location LIKE '%KY%' THEN 'KY'
     WHEN location LIKE '%Louisiana%' OR location LIKE '%LA%' THEN 'LA'
     WHEN location LIKE '%Maine%' OR location LIKE '%ME%' THEN 'ME'
     WHEN location LIKE '%Maryland%' OR location LIKE '%MD%' THEN 'MD'
     WHEN location LIKE '%Massachusetts%' OR location LIKE '%MA%' THEN 'MA'
     WHEN location LIKE '%Michigan%' OR location LIKE '%MI%' THEN 'MI'
     WHEN location LIKE '%Missouri%' OR location LIKE '%MO%' THEN 'MO'
     WHEN location LIKE '%Montana%' OR location LIKE '%MT%' THEN 'MT'
     WHEN location LIKE '%Nebraska%' OR location LIKE '%NE%' THEN 'NE'
     WHEN location LIKE '%Nevada%' OR location LIKE '%NV%' THEN 'NV'
     WHEN location LIKE '%New Hampshire%' OR location LIKE '%NH%' THEN 'NH'
     WHEN location LIKE '%New Jersey%' OR location LIKE '%NJ%' THEN 'NJ'
     WHEN location LIKE '%New Mexico%' OR location LIKE '%NM%' THEN 'NM'
     WHEN location LIKE '%New York%' OR location LIKE '%NY%' THEN 'NY'
     WHEN location LIKE '%North Carolina%' OR location LIKE '%NC%' THEN 'NC'
     WHEN location LIKE '%North Dakota%' OR location LIKE '%ND%' THEN 'ND'
     WHEN location LIKE '%Ohio%' OR location LIKE '%OH%' THEN 'OH'
     WHEN location LIKE '%Oklahoma%' OR location LIKE '%OK%' THEN 'OK'
     WHEN location LIKE '%Oregon%' OR location LIKE '%OR%' THEN 'OR'
     WHEN location LIKE '%Pennsylvania%' OR location LIKE '%PA%' THEN 'PA'
     WHEN location LIKE '%Rhode Island%' OR location LIKE '%RI%' THEN 'RI'
     WHEN location LIKE '%South Carolina%' OR location LIKE '%SC%' THEN 'SC'
     WHEN location LIKE '%South Dakota%' OR location LIKE '%SD%' THEN 'SD'
     WHEN location LIKE '%Tennessee%' OR location LIKE '%TN%' THEN 'TN'
     WHEN location LIKE '%Texas%' OR location LIKE '%TX%' THEN 'TX'
     WHEN location LIKE '%Arkansas%' OR location LIKE '%AR%' THEN 'AR'
     WHEN location LIKE '%Utah%' OR location LIKE '%UT%' THEN 'UT'
     WHEN location LIKE '%Vermont%' OR location LIKE '%VT%' THEN 'VT'
     WHEN location LIKE '%Washington%' OR location LIKE '%WA%' THEN 'WA'
     WHEN location LIKE '%West Virginia%' OR location LIKE '%WV%' THEN 'WV'
     WHEN location LIKE '%Wisconsin%' OR location LIKE '%WI%' THEN 'WI'
     WHEN location LIKE '%Wyoming%' OR location LIKE '%WY%' THEN 'WY'
     ELSE 'Not in US'
        END as cleaned_location,
    -- Parsing the tweet timestamp into a SQL timestamp data type
    try_cast(concat('2021-12-',substr(createdate, 9,11)) as timestamp) as created_at_new,
    prediction
    from twitter_labelled
-- Ordering by date from oldest to newest
order by 9 asc;