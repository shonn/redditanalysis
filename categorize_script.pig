subreddit_bodytext = foreach reddit generate $0#'subreddit' as subreddit, $0#'body' as bodytext; 
subreddit_categorybag = foreach subreddit_bodytext generate subreddit, (CATEGORIZE(bodytext)) as categorybag;
filtered_bag = filter subreddit_categorybag by not IsEmpty(categorybag);                                     
store filtered_bag into 'category_output' using PigStorage();
