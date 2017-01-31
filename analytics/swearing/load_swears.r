library(dplyr)

set_global <- function(x){
  for (name in names(x))
    assign(name,x[[name]],envir=globalenv())
}

load_data <- function(x){
  swear_s = read.csv(paste0(x,'_specific.csv'))
  swear_g = read.csv(paste0(x,'_general.csv'))
  swear_s_summary = read.csv(paste0(x,'_specific_summary.csv'))
  swear_g_summary = read.csv(paste0(x,'_general_summary.csv'))

  #calculate confidence intervals via Wilson score

  z = qnorm(0.975)

  swear_s_summary = swear_s_summary %>%
    mutate(n = round(total_swears/percentage_swears), 
    upper_p = 1/(1+1/n*z^2)*(percentage_swears + 1/(2*n)*z^2 + z*sqrt(1/n*percentage_swears*(1-percentage_swears) + 
      1/(4*n^2)*z^2)),
    lower_p = 1/(1+1/n*z^2)*(percentage_swears + 1/(2*n)*z^2 - z*sqrt(1/n*percentage_swears*(1-percentage_swears) + 
      1/(4*n^2)*z^2))
    )
    
  swear_g_summary = swear_g_summary %>%
    mutate(n = round(total_swears/percentage_swears), 
    upper_p = 1/(1+1/n*z^2)*(percentage_swears + 1/(2*n)*z^2 + z*sqrt(1/n*percentage_swears*(1-percentage_swears) + 
      1/(4*n^2)*z^2)),
    lower_p = 1/(1+1/n*z^2)*(percentage_swears + 1/(2*n)*z^2 - z*sqrt(1/n*percentage_swears*(1-percentage_swears) + 
      1/(4*n^2)*z^2))
    )
    
  swear_s= swear_s %>%
    mutate(n = round(count/relative), 
    upper_p = 1/(1+1/n*z^2)*(relative + 1/(2*n)*z^2 + z*sqrt(1/n*relative*(1-relative) + 
      1/(4*n^2)*z^2)),
    lower_p = 1/(1+1/n*z^2)*(relative + 1/(2*n)*z^2 - z*sqrt(1/n*relative*(1-relative) + 
      1/(4*n^2)*z^2))
    )
    
  swear_g = swear_g %>%
    mutate(n = round(count/relative), 
    upper_p = 1/(1+1/n*z^2)*(relative + 1/(2*n)*z^2 + z*sqrt(1/n*relative*(1-relative) + 
      1/(4*n^2)*z^2)),
    lower_p = 1/(1+1/n*z^2)*(relative + 1/(2*n)*z^2 - z*sqrt(1/n*relative*(1-relative) + 
      1/(4*n^2)*z^2))
    )
      

  #create union of old and new 

  summary_union = rbind(swear_s_summary, swear_g_summary)
  summary_union$source = c(rep(c('Specific','Overzealous'),each=nrow(swear_s_summary)))



  #filter out low-count words

  s_words = (swear_s %>% 
    group_by(word) %>% 
    summarise(total = sum(count)) %>% 
    filter(total > 30))$word
  swear_s = swear_s %>% 
    filter(word %in% s_words)
    
  g_words = (swear_g %>% 
    group_by(word) %>% 
    summarise(total = sum(count)) %>% 
    filter(total > 30))$word
  swear_g = swear_g %>% 
    filter(word %in% g_words)

  #summarize specific/general swears by average frequency  

  s_averages = swear_s %>% 
    group_by(word) %>% 
    summarise(score=mean(relative)) %>% 
    arrange(-score)
    
  g_averages = swear_g %>% 
    group_by(word) %>% 
    summarise(score=mean(relative)) %>% 
    arrange(-score)
    
  swear_s$word = factor(swear_s$word, levels=as.character(s_averages$word))
  swear_g$word = factor(swear_g$word, levels=as.character(g_averages$word))

  swear_s$subreddit = factor(swear_s$subreddit, levels=as.character(swear_s_summary$subreddit))
  swear_g$subreddit = factor(swear_g$subreddit, levels=as.character(swear_g_summary$subreddit))

  swear_s_summary$subreddit = factor(swear_s_summary$subreddit, levels=rev(as.character(swear_s_summary$subreddit)))
  swear_g_summary$subreddit = factor(swear_g_summary$subreddit, levels=rev(as.character(swear_g_summary$subreddit)))
  summary_union$subreddit = factor(summary_union$subreddit, levels=rev(as.character(swear_g_summary$subreddit)))

  #create word-averaged ratios to get the relative likelihood of words for subreddits 

  swear_s = swear_s %>% group_by(word) %>% mutate(ratio=relative/mean(relative))
  swear_g = swear_g %>% group_by(word) %>% mutate(ratio=relative/mean(relative))
  return(list(swear_s=swear_s, swear_g=swear_g, swear_g_summary=swear_g_summary,swear_s_summary=swear_s_summary,
  summary_union=summary_union))
}

set_global_data <- function(x){
  set_global(load_data(x))
}

swear_prawdata = data.frame(subreddit = c('gonewild',
					  'tifu',
					  'TwoXChromosomes',
					  'WritingPrompts',
					  'news',
					  'AskReddit',
					  'videos',
					  'The_Donald',
					  'worldnews',
					  'todayilearned',
					  'funny',
					  'movies',
					  'Showerthoughts',
					  'pics',
					  'gifs',
					  'television',
					  'nottheonion',
					  'gaming',
					  'Jokes',
					  'LifeProTips',
					  'IAmA',
					  'mildlyinteresting',
					  'Music',
					  'Futurology',
					  'OldSchoolCool',
					  'aww', 
					  'sports',
					  'Documentaries',
					  'explainlikeimfive',
					  'books',
					  'dataisbeautiful',
					  'creepy',
					  'UpliftingNews',
					  'personalfinance',
					  'gadgets',
					  'food',
					  'nosleep',
					  'space',
					  'GetMotivated',
					  'DIY',
					  'Art',
					  'science',
					  'history',
					  'EarthPorn',
					  'listentothis',
					  'philosophy',
					  'photoshopbattles',
					  'askscience',
					  'InternetIsBeautiful',
					  'blog',
					  'announcements')
)
