# Case Study 1 - Basic Analysis
# Using Data babies..txt

# Change directory
setwd('C:/Users/Saurabh K/Desktop/CodingWD/RCodes')

# Use ggplot2 library
library(ggplot2)

# Get table
dat1 = read.table("babies..txt",header=TRUE)
#header = TRUE tells R that first line is the headings line 

attach(dat1) #To use data from that particular table

## Scatterplot
x <- dat1[,"gestation"]
y <- dat1[,"bwt"]
plot(x,y)	# scatterplot

## Classified Scatterplot
dat1c = dat1[which(dat1$gestation != 999),] #Cleaned Data
dat1c = dat1c[which(dat1c$smoke != 9),] #Cleaned Data
dat1c.smoke = dat1c[which(dat1c$smoke == 1), ] # Data with women who smoked
dat1c.smoke.unh = dat1c.smoke[which(dat1c.smoke$bwt<88),]#Unhealthy babies
dat1c.smoke.h = dat1c.smoke[which(dat1c.smoke$bwt>88),]#Unhealthy babies
dat1c.non = dat1c[which(dat1c$smoke != 1), ] # Data with women who did not smoked
dat1c.non.unh = dat1c.non[which(dat1c.non$bwt<88),]#Unhealthy babies
dat1c.non.h = dat1c.non[which(dat1c.non$bwt>88),]#Unhealthy babies
x <- dat1c[,"gestation"]
y <- dat1c[,"bwt"]
z <- dat1c[,"smoke"] + 1
DF <- data.frame(x,y,z)
plot(DF$x,DF$y,col = c("blue","red")[DF$z],main="Bwt vs Gestation ", xlab="Gestation", ylab="Bwt",pch=1)
legend("topleft", pch=c(1,1), col=c("blue", "red"), c("Non-smokers", "Smokers"), bty="o",  box.col="black", cex=.8)

## Histogram Plot
nbin <- seq(40,180,10)
nbin2 <- seq(45,175,10)
hist_bwt <- hist(bwt,breaks = nbin,xlim = c(40,180), ylim = c(0,300), xlab="Body Weight", ylab="Count")
lines(nbin2, hist_bwt$counts, pch=16, col="red", lty=2)
## Box Plot
library(ggplot2)
ggplot(dat1c.smoke,aes(x=bwt))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 88)+ggtitle("Histogram:Baby Bwt among Smokers")+labs(x = "Weight in Ounces",y = "Count") #hist
ggplot(dat1c.non,aes(x=bwt))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 88)+ggtitle("Histogrom:Baby Bwt among Non-smokers")+labs(x = "Weight in Ounces",y = "Count") #hist
ggplot(dat1c,aes(x=bwt))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 88)+ggtitle("Histogrom:Baby Bwt among All data")+labs(x = "Weight in Ounces",y = "Count") #hist

ggplot(dat1c.smoke,aes(x=gestation))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 259)+ggtitle("Histogram:Baby Gestation time among Smokers")+labs(x = "Days",y = "Count") #hist
ggplot(dat1c.non,aes(x=gestation))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 259)+ggtitle("Histogrom:Baby Gestation time among Non-smokers")+labs(x = "Days",y = "Count") #hist
ggplot(dat1c,aes(x=gestation))+geom_histogram(binwidth=10 ,fill = "royalblue3")+geom_vline(xintercept = 259)+ggtitle("Histogrom:Baby gestation time among All data")+labs(x = "Days",y = "Count") #hist



## Box plot
boxplot(bwt~smoke,data=dat1c, main="Box Plot of Bwt for both categories", 
        xlab="Category: 0=Non-smokers, 1=Smokers", ylab="Weight in Ounces") 	# boxplot for the variable bwt

## Q-Q plot
# For the entire Dataset
qqnorm(dat1c$bwt,main = "Bwt for Cleaned Dataset",pch=1)
qqline(dat1c$bwt,col="red") 	# quantile-quantile reference line
# For the Smoking Mothers
qqnorm(dat1c.smoke$bwt,main="Bwt for Smoking Mothers",pch=1) 	# quantile-quantile plot
qqline(dat1c.smoke$bwt,col="red") 	# quantile-quantile reference line
# For Non-smoking Mothers
qqnorm(dat1c.non$bwt,main="Bwt for Non-Smoking Mothers",pch=1) 	# quantile-quantile plot
qqline(dat1c.non$bwt,col="red") 	# quantile-quantile reference line


## Numerical Tests using Loops
sampsmoke = 430 # Appx 0.9*480 smoking samples
sampnon = 660 # Appx 0.9*733 smoking samples
samptrials = 70 # Number of trials
bwt_mn_smoke <- matrix(0,nrow=samptrials,ncol=1)
bwt_mn_non <- matrix(0,nrow=samptrials,ncol=1)
bwt_sd_smoke <- bwt_mn_smoke
bwt_sd_non <- bwt_mn_smoke
frq_low_smoke <- matrix(0,nrow=samptrials,ncol=1)
frq_low_non <- matrix(0,nrow=samptrials,ncol=1)
k_s <- matrix(0,nrow=samptrials,ncol=1)
k_s  <- matrix(0,nrow=samptrials,ncol=1)
k_ns  <- matrix(0,nrow=samptrials,ncol=1)
for (j in 1:samptrials){
  df  <- matrix(0,nrow=sampsmoke,ncol=1)
  for (i in 1:sampsmoke) {
    a <- dat1c.smoke[sample(nrow(dat1c.smoke),1),]
    df[i] <- a$bwt
  }
  frq_low_smoke[j] <- sum(df<88)*100/sampsmoke
  bwt_mn_smoke[j] <- mean(df)
  bwt_sd_smoke[j] <- sd(df)
  k_s[j]<- mean(((a$bwt-mean(df))/sd(a$bwt))^4)
  df  <- matrix(0,nrow=sampnon,ncol=1)
  
  for (i in 1:sampnon) {
    a <- dat1c.non[sample(nrow(dat1c.non),1),]
    df[i] <- a$bwt
  }
  frq_low_non[j] <- sum(df<88)*100/sampnon
  bwt_mn_non[j] <- mean(df)
  bwt_sd_non[j] <- sd(df)
  k_ns<- mean(((a$bwt-mean(a$bwt))/sd(a$bwt))^4)
}
k_s =  mean(((bwt_mn_smoke-mean(bwt_mn_smoke))/sd(bwt_mn_smoke))^4)

hist_low_bwr_smoke = hist(frq_low_smoke,xlim = c(4,15), ylim = c(0,25), xlab="Percentage(%)", ylab="Count",main = "Histogram: % of Unhealthy Babies for Smokers")
hist_low_bwr_non = hist(frq_low_non,xlim = c(0,6), ylim = c(0,30), xlab="Percentage(%)", ylab="Count",main = "Histogram: % of Unhealthy Babies for NonSmokers")
ggplot(dat1c,aes=(factor(smoke))) + geom_bar(fill="royalblue3") #barplot
ggplot(,aes(x=frq_low_smoke))+geom_histogram(binwidth=1 ,fill = "royalblue3")+ggtitle("% of unhealthy babies among Smokers")+labs(x = "Percentage (%)",y = "Count") #hist
ggplot(,aes(x=frq_low_non))+geom_histogram(binwidth=1 ,fill = "royalblue3")+ggtitle("% of unhealthy babies among Non-smokers")+labs(x = "Percentage (%)",y = "Count") #hist
ggplot(,aes(x=bwt_mn_smoke))+geom_histogram(binwidth=1 ,fill = "royalblue3")+ggtitle(" Bwt means among Smokers")+labs(x = "Means of bwt",y = "Count") #hist
ggplot(,aes(x=bwt_mn_non))+geom_histogram(binwidth=1 ,fill = "royalblue3")+ggtitle(" Bwt means among NonSmokers")+labs(x = "Means of bwt",y = "Count") #hist
