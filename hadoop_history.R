library(utils)
library(ggplot2)

setwd("~/src/modules/data-pipeline")
jobs1 <- read.csv("work/temp/hadoop-history-2016-01-31.csv", as.is= TRUE)
jobs2 <- read.csv("work/temp/hadoop-history-2016-02-29.csv", as.is= TRUE)
jobs3 <- read.csv("work/temp/hadoop-history-2016-03-31.csv", as.is= TRUE)
jobs4 <- read.csv("work/temp/hadoop-history-2016-04-15.csv", as.is= TRUE)
jobs <- rbind(jobs1, jobs2, jobs3, jobs4)              

jobs$date <- strptime(jobs$dt, "%Y-%m-%d %H:%M:%S")
jobs <- jobs[jobs$date>'2016-01-01',]
jobs$runtime <- (jobs$mapSlotMs + jobs$reduceSlotMs + jobs$frameworkMs) / (3600*1000)
jobs$month <- format(jobs$date, "%b")

# aggregation - where is time spent over the whole month?
month_jobs <- aggregate(runtime ~ jobName + month, jobs, sum)

# longJobNames <- month_jobs$jobName[month_jobs$runtime > 10]
longJobNames <- month_jobs[ with(month_jobs, order(-runtime)), ][1:100,]$jobName


# plot total time spent over the month
g1 <- ggplot(month_jobs[month_jobs$jobName %in% longJobNames,], aes(x=jobName, y=runtime, fill=month)) + geom_bar(stat="identity") + 
  ylab("Total runtime during month (hours)") +
  coord_flip()
ggsave('metrics/data/hadoop-month-runtimes.pdf', plot=g1, width=12, height=15)
print(g1)

# plot distribution of jobs over the month
g2 <- ggplot(jobs[jobs$jobName %in% longJobNames,], aes(x=jobName, y=runtime, fill=month)) + geom_boxplot() + 
  ylab("Runtime (hours)") +
  coord_flip()
ggsave('metrics/data/hadoop-runtimes.pdf', plot=g2, width=8, height=20)
print(g2)






