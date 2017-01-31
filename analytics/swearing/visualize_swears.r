library(ggplot2)
library(dplyr)
library(scales)

setwd('/hddl/workspace/TreeGrabForReddit/analytics/swearing')
source('load_swears.r')

#swear_s = read.csv('default_specific.csv')
#swear_g = read.csv('default_general.csv')
#swear_s_summary = read.csv('default_specific_summary.csv')
#swear_g_summary = read.csv('default_general_summary.csv')

##images
#frequency and relative ratio
ggplot(swear_s, aes(x=word, y=subreddit, fill=relative)) +
  geom_tile() + scale_fill_continuous('Proportion',label=percent) + 
  theme(axis.text.x = element_text(angle=20, vjust=0.8), plot.title = element_text(hjust = 0.5)) + 
  ggtitle('Proportion of Comments in Subreddits Containing Swear') + ylab('Subreddit') + xlab('Swear')
  
ggplot(swear_s, aes(x=word, y=subreddit, fill=pmax(0.03, ratio))) +
  geom_tile() + scale_fill_gradient2('Ratio',trans='log', breaks=c(0.1,0.2,0.5,1.0,2,5,10)) + 
  ggtitle(bquote(atop('Ratio of Comments in Subreddits Containing Swear Compared to other Subreddits',
		      atop('blue (>1) indicates more frequent use of swear compared to other subreddits',
		      'red (<1) indicates less frequent use of swear compared to other subreddits')))) + 
  theme(axis.text.x = element_text(angle=20, vjust=0.8), plot.title = element_text(hjust = 0.5)) +
    ylab('Subreddit') + xlab('Swear')
  
  
#plot of word frequencies
#add names of subreddits for high-frequency ones
ggplot(swear_s, aes(y=relative, x=word)) +
  geom_boxplot(notch=FALSE) + coord_flip() +
  scale_y_continuous(label=percent, breaks=0:12/100, limits=c(0,0.12)) + 
  theme_gray() +
  geom_text(aes(label=subreddit, alpha=ifelse(ratio>2.5 & relative > 0.015, 1, 0)), nudge_y=-0.001,nudge_x=0.22) + 
  scale_alpha_identity() + 
  ggtitle('Swear Frequencies Across Subreddits') + xlab('Swear') + ylab("Percentage of Comments Containing Swear")
  
ggplot(swear_s, aes(y=ratio, x=word)) +
  geom_boxplot(notch=FALSE) + coord_flip() +
  scale_y_continuous(breaks = 0:6*5) + 
  theme_gray() +
  geom_text(aes(label=paste(subreddit, percent(relative)), alpha=ifelse(ratio>4 & relative > 0.001, 1, 0)), 
     size=2.5,angle=25,color='red') + 
  scale_alpha_identity() + 
  ggtitle('Swear Usage Ratios Across Subreddits') + 
  ylab('Ratio of Swear Frequency to Subreddit Average') + 
  xlab('Swear')

#overall subreddit swear frequencies

ggplot(swear_s_summary, aes(y=percentage_swears, x=subreddit)) + 
  geom_bar(stat='identity') + coord_flip() + scale_y_continuous(label=percent, expand=c(0,0))+ 
  geom_errorbar(aes(ymin=lower_p,ymax=upper_p,color='red')) + 
  scale_color_identity() + ggtitle('Frequency of Swear Presence in Subreddit Comments') + 
  labs(subtitle='error bars indicate 95% confidence interval') + xlab('Subreddit') + ylab('Swear Frequency') +
  theme(plot.title=element_text(hjust=0.5), plot.subtitle=element_text(hjust=0.5))
  
ggplot(swear_g_summary, aes(y=percentage_swears, x=subreddit)) + 
  geom_bar(stat='identity') + coord_flip() + scale_y_continuous(label=percent, expand=c(0,0)) + 
  geom_errorbar(aes(ymin=lower_p,ymax=upper_p,color='red')) + 
  scale_color_identity() + ggtitle('Frequency of Swear Presence in Subreddit Comments Using Overly Zealous List') + 
  labs(subtitle='error bars indicate 95% confidence interval') + xlab('Subreddit') + ylab('Swear Frequency') +
  theme(plot.title=element_text(hjust=0.5), plot.subtitle=element_text(hjust=0.5))
  
#compare two figures
ggplot(summary_union, aes(x=subreddit,weight=percentage_swears,fill=source,ymin=lower_p,ymax=upper_p))+
  geom_bar(stat='identity',position='dodge',aes(y=percentage_swears)) + 
  coord_flip() + geom_errorbar(aes(group=source),position='dodge',color='gray40') + 
  guides(fill=guide_legend('Swear List Source')) + 
  ggtitle('Comparison of Swearing Estimates with Different Swear Lists') +
  xlab("Subreddit") + ylab("Percentage of Comments Containing Swears") +
  scale_y_continuous(label=percent,expand=c(0,0)) + 
  theme(axis.ticks.y = element_line(), plot.subtitle=element_text(hjust=0.5), plot.title=element_text(hjust=0.5)) + 
  labs(subtitle='with 95% confidence interval error bars')

#simple numeric count of comments 

countframe = swear_s_summary %>% select(subreddit, n) %>%
  arrange(-n) %>%
  mutate(subreddit = factor(subreddit, levels=as.character(subreddit)))
  

ggplot(countframe) + geom_bar(aes(x=subreddit, y=n), fill='#44DD55',stat='identity') + 
  geom_text(aes(x=subreddit,y=n + 500,label=n),hjust=0) + coord_flip() + 
  scale_y_continuous(expand=c(0.0,100),limits=c(0,82000),breaks=0:8*10000) + 
  ggtitle("Total Counts of Comments from Sample") + xlab("Subreddit") + ylab("Count") + 
  labs(subtitle='comments by AutoModerator and WritingPromptsRobot omitted from sample') + 
  theme(plot.title=element_text(hjust=0.5), plot.subtitle=element_text(hjust=0.5, color='gray60'))