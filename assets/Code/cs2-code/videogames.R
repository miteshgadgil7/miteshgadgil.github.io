Sweave("videogames.Rnw")

setwd("C:/Users/Kyle/Documents/Math189_Data/HW2")
data = read.table("data.txt", header = TRUE, stringsAsFactors = F)

# Scenario 1
N = 314
dat1c <- data[which(data$time!=99),]
p_last_Week <- dat1c[which(dat1c$time>0),]
dnp_last_Week <- dat1c[which(dat1c$time==0),]
n = nrow(p_last_Week)

## point estimate
prop_point_est = nrow(p_last_Week)/nrow(dat1c)

## standard error calculation
SE_prop = (prop_point_est*(1-prop_point_est)*(N-n)/((n-1)*N))^0.5

## interval estimate 95% confidence interval
c(prop_point_est-1.96*(SE_prop),prop_point_est+1.96*(SE_prop))

# Scenario 2

## segregation into 4 vectors based on frequency of playing as reported
dat1c <- data[which(data$time!=99 & data$freq!=99),]
p.daily <- dat1c[which(dat1c$freq==1),]$time
p.weekly <- dat1c[which(dat1c$freq==2),]$time
p.monthly <- dat1c[which(dat1c$freq==3),]$time
p.semesterly <- dat1c[which(dat1c$freq==4),]$time

## displaying summaries of the 4 frequency categories
summary(p.daily)
summary(p.weekly)
summary(p.monthly)
summary(p.semesterly)

## displaying time data samples of each frequency category
p.daily
p.weekly
p.monthly
p.semesterly

###Scenario 3###
## Clean Data
dat_clean <- data[which(data$like != 99),] #Cleaned Data
dat_like <- dat_clean[which((dat_clean$like==2)|(dat_clean$like==3)),]
dat_notlike <- dat_clean[which((dat_clean$like == 1)|(dat_clean$like==4)|(dat_clean$like==5)),]


######################################################
## Histogram
library(ggplot2)
hist_like = ggplot(dat_like,aes(x=dat_like$like))+geom_histogram(binwidth=1 ,fill = "royalblue3")+ggtitle("Histogram: Like Types")+labs(x = "Like",y = "Count")


######################################################
## Scenario 3
######################################################

####### Interval Estimation using Bootstrap ##########
## Writing the bootstrap function

# @param x is a sample, B is the number of bootstrap times, width is the width of histogram
boot.statistics = function(x, B, breaks=NULL, adjust.density=TRUE){
  par(mfrow=c(1,2))
  n = length(x)
  boot.samples = matrix(sample(x, size=B*n, replace=TRUE), B, n)
  k = length(boot.samples)
  boot.mean = apply(boot.samples, 1, mean)
  se = sd(boot.mean)
  if (is.null(breaks)){
    breaks = 30 # 30 is how many breaks you want
  }
  hist(boot.mean, breaks = breaks, freq=FALSE)
  if (adjust.density){
    lines(density(boot.mean, adjust=2), col="red")
  }else{
    lines(density(boot.mean), col="red")
  }
  qqnorm(boot.mean)
  qqline(boot.mean)
  CI_norm = mean(x) + c(-1,1)*1.96*se
  print(CI_norm)
  
  return (list(boot.mean = boot.mean, CI_norm = CI_norm, se = se, n = n))
}

## Interval Estimation using Bootstrap on whole data
boot.video = with(dat_clean, boot.statistics(time, B=1000, breaks=50))
boot.video$CI
boot.video$se

##################### Using FPCF ##################
## Using Finite population factor
N = 314
n = 90
fpf <- sqrt((N-n)/(n*N))
s1 = sd(dat_clean$time)
se1 = s1*fpf
mn_time = mean(dat_clean$time)
CI_fpf = mn_time + c(-1,1)*1.96*se1
CI_fpf

##################### T test ######################
## Interval Estimate using T-test
## Data need not be Normal in nature. So to find conf. interval for data with unknown variance
Ttest_total = t.test(dat_clean$time)
Ttest_total$conf.int

###################################################

############### Scenario 4 ###########################

#install.packages("tree",repos="http://cran.us.r-project.org")

dat_clean2 <- dat_clean#[which((dat_clean$like!= 99)),]
dat_clean2[which(dat_clean2$like==2),]$like=1
dat_clean2[which(dat_clean2$like==3),]$like=1
dat_clean2[which(dat_clean2$like==4),]$like=0
dat_clean2[which(dat_clean2$like==5),]$like=0
as.factor(dat_clean2$like)

################## Trees #############################
dev.off()
require(tree)

## Create Tree
# overfitting

tree.fit = tree((as.factor(dat_clean2$like)) ~ (dat_clean2$own) + (dat_clean2$busy) + (dat_clean2$sex), data=dat_clean2, mindev = 0.001)
plot(tree.fit) # Plot the tree
text(tree.fit, cex=0.75) # Text
title('CART (Leaf Class: Like (TRUE),Dislike(FALSE))')
summary(tree.fit) # Summary

# We wont need to prune the tree


############ Bar plots ###############################
## Like playing on a computer or not
dev.off()
dat_where <- dat_clean[which((dat_clean$where!= 99)),]
ggplot(dat_where, aes(factor(dat_where$where))) + labs(x="Types",y="Count",title="Bar plot: Preferred place of playing video games")+geom_bar(fill="royalblue3") # bar plot
dat_where[which(dat_where$where==4),]$where=3
dat_where[which(dat_where$where==5),]$where=3
dat_where[which(dat_where$where==6),]$where=3
dat_where[which(dat_where$where==2),]$where=0
dat_where[which(dat_where$where==1),]$where=0
dat_where[which(dat_where$where==3),]$where=1
ggplot(dat_where, aes(as.logical(dat_where$where))) + labs(x="Types",y="Count",title="Bar plot: Like playing games on computer")+geom_bar(fill="royalblue3") # bar plot


## Find games educational or not
dat_educ <- dat_clean[which((dat_clean$educ!= 99)),]
ggplot(dat_educ, aes(as.logical(dat_educ$educ))) + labs(x="Types",y="Count",title="Bar plot: Playing educational")+geom_bar(fill="darkgreen") # bar plot


## Sex a factor?
lg_m = dat_like[which(dat_like$sex==1),]
lg_m = lg_m$sex
lg_f = dat_like[which(dat_like$sex==0),]
lg_f = lg_f$sex
nlg_m = dat_notlike[which(dat_notlike$sex==1),]
nlg_m = nlg_m$sex
nlg_f = dat_notlike[which(dat_notlike$sex==0),]
nlg_f = nlg_f$sex
f<-cbind(v1=c(length(lg_m),length(nlg_m)),v2=c(length(lg_f),length(nlg_f)))

barplot(f, main="Group Barplots: Like Video Games vs Sex", ylab= "Count",names.arg=c("Male","Female"),beside=TRUE, col=c("red","darkblue"))
legend("topright", fill= c("red","darkblue"), legend=c("Like", "Dislike"))


###Scenario 5###
which(data$like == 99)
which(data$sex == 99)
which(data$work == 99)
which(data$own == 99)

data.table <- data
data.table$like[which(data.table$like == 99)] <- "Dislike"
data.table$like[which(data.table$like == 1)] <- "Dislike"
data.table$like[which(data.table$like > 3)] <- "Dislike"
data.table$like[which(data.table$like <= 3)] <- "Like"

like.gender <- table(data.table$like, data$sex)
colnames(like.gender) <- c("Female", "Male")
round(prop.table(like.gender) * 100, 2)
barplot(like.gender, main = "Video Game Preference vs. Gender",
        ylab= "Count",
        names.arg=c("Female","Male"),
        beside=TRUE,
        col=c("red","darkblue"))
legend("topright", fill = c("red", "darkblue"), legend = c("Dislike", "Like"))

data.table$work[which(data.table$work == 99)] <- 0
data.table$work[which(data.table$work > 0)] <- 1
like.work <- table(data.table$like, data.table$work)
colnames(like.work) <- c("Nonworking", "Work")
round(prop.table(like.work) * 100, 2)
barplot(like.gender, main = "Video Game Preference vs. Working",
        ylab= "Count",
        names.arg=c("Nonworking","Working"),
        beside=TRUE,
        col=c("red","darkblue"))
legend("topright", fill = c("red", "darkblue"), legend = c("Dislike", "Like"))

like.own <- table(data.table$like, data.table$own)
colnames(like.own) <- c("Lack", "Own")
round(prop.table(like.own) * 100, 2)
barplot(like.gender, main = "Video Game Preference vs. Owning a PC",
        ylab= "Count",
        names.arg=c("Lack","Own"),
        beside=TRUE,
        col=c("red","darkblue"))
legend("topright", fill = c("red", "darkblue"), legend = c("Dislike", "Like"))


###Graphs Section 5###
dat1p = na.omit(data)
image(is.na(data), main = "Visualization for Missing Values", xlab = "Observation", ylab = "Variable", xaxt = "n", yaxt = "n", bty = "n")
axis(1, seq(0, 1, length.out = nrow(data)), 1:nrow(data), col = "white")
axis(2, (0:(length(names(data))-1))/(length(names(data))-1), names(data), col = "white", las = 2)

# like:0 not like:1
dat1p[which(dat1p$like==1 | dat1p$like==4 | dat1p$like==5),]$like =1
dat1p[which(dat1p$like==2 | dat1p$like==3),]$like =0

m_nl<- dat1p$sex[which(dat1p$sex==1 & dat1p$like==1)]
m_l <- dat1p$sex[which(dat1p$sex==1 & dat1p$like==0)]
f_nl <- dat1p$sex[which(dat1p$sex==0 & dat1p$like==1)]
f_l <- dat1p$sex[which(dat1p$sex==0 & dat1p$like==0)]
f<- cbind(v1=c(length(m_l),length(f_l)),v2=c(length(m_nl),length(f_nl)))
barplot(f, main="Group Barplots: Gender comparison", ylab= "Count",names.arg=c("Like","Don't Like"),beside=TRUE, col=c("red","darkblue"))
legend("topright", fill= c("red","darkblue"), legend=c("Male", "Female")  )
dev.off()

c_nl<- dat1p$home[which(dat1p$home==1 & dat1p$like==1)]
c_l <- dat1p$home[which(dat1p$home==1 & dat1p$like==0)]
nc_nl <- dat1p$home[which(dat1p$home==0 & dat1p$like==1)]
nc_l <- dat1p$home[which(dat1p$home==0 & dat1p$like==0)]
f<- cbind(v1=c(length(c_l),length(nc_l)),v2=c(length(c_nl),length(nc_nl)))
barplot(f, main="Group Barplots: Computer at home comparison", ylab= "Count",names.arg=c("Like","Don't Like"),beside=TRUE, col=c("red","darkblue"))
legend("topright", fill= c("red","darkblue"), legend=c("Have computer at home", "Don't have computer at home")  )

o_nl<- dat1p$own[which(dat1p$own==1 & dat1p$like==1)]
o_l <- dat1p$own[which(dat1p$own==1 & dat1p$like==0)]
no_nl <- dat1p$own[which(dat1p$own==0 & dat1p$like==1)]
no_l <- dat1p$own[which(dat1p$own==0 & dat1p$like==0)]
f<- cbind(v1=c(length(o_l),length(no_l)),v2=c(length(o_nl),length(no_nl)))
barplot(f, main="Group Barplots: Own PC comparison", ylab= "Count",names.arg=c("Like","Don't Like"),beside=TRUE, col=c("red","darkblue"))
legend("topright", fill= c("red","darkblue"), legend=c("Own PC", "Dont Own PC")  )

e_nl<- dat1p$educ[which(dat1p$educ==1 & dat1p$like==1)]
e_l <- dat1p$educ[which(dat1p$educ==1 & dat1p$like==0)]
ne_nl <- dat1p$educ[which(dat1p$educ==0 & dat1p$like==1)]
ne_l <- dat1p$home[which(dat1p$educ==0 & dat1p$like==0)]
f<- cbind(v1=c(length(e_l),length(ne_l)),v2=c(length(e_nl),length(ne_nl)))
barplot(f, main="Group Barplots: Find Playing educational? comparison", ylab= "Count",names.arg=c("Like","Don't Like"),beside=TRUE, col=c("red","darkblue"))
legend("topright", fill= c("red","darkblue"), legend=c("Find Educational", "Don't find Educational")  )


###Section 6###
which(data.table$grade == 99)
data.table$grade[which(data.table$grade == 1)] <- "D/F"
data.table$grade[which(data.table$grade == 2)] <- "C"
data.table$grade[which(data.table$grade == 3)] <- "B"
data.table$grade[which(data.table$grade == 4)] <- "A"
grade <- table(data.table$grade)
grade <- c(grade, 0)
names(grade) <- c("A", "B", "C", "D/F")
round(prop.table(grade) * 100, 2)
