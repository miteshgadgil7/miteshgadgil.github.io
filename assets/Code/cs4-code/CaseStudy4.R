############# MATH 289C: CASE STUDY 4 ##########################
# Linear Regression
# Team: Saurabh Kulkarni, Mitesh Gadgil, Kyle Kole

rm(list=ls())

# Loading dataset
gauge <- read.csv("gauge.txt", sep="")
head(gauge)
density <- gauge$density
gain <- gauge$gain

# 9 Unique values of density for which observations have been recorded
obs.density <- unique(gauge$density)
# gain observations for each unique density stored as a row vector
obs.gain <- matrix(nrow = length(obs.density),ncol = nrow(gauge)/length(obs.density))
for (i in 1:length(obs.density)){
  obs.gain[i,] <- gauge$gain[which(gauge$density==unique(gauge$density)[i])]
}
# summary statistics for gain observations of each unique density
obs.mean <- numeric(length(obs.density))
obs.sd <- numeric(length(obs.density))
for (i in 1:length(obs.density)){
  obs.mean[i] <- mean(obs.gain[i,])
  obs.sd[i] <- sd(obs.gain[i,])
}
print(matrix(c(obs.density,obs.mean,obs.sd),nrow = 9,ncol=3,dimnames = list(1:9,c("Density","Mean(gain)"," Std.dev(gain)"))))

# Part 2

str(gauge)
library(ggplot2)

ggplot(gauge,aes(x = density, y=gain))+geom_point(col="blue",size=2)+labs(y="Gain",x="Density",title="Scatter plot of Observed Data (90 observations)")+geom_smooth(mapping = aes(x = density, y=(gain)),col='black',se=FALSE)+geom_smooth(method = "lm", se = FALSE,col='red')


#Part 3

ggplot(gauge,aes(x = density, y=log(gain)))+geom_point(col="blue",size=2)+labs(y="Log(Gain)",x="Density",title="Scatter plot of Log-Transformed Observed Data (90 observations)")+geom_smooth(mapping = aes(x = density, y=log(gain)),col='black',se= FALSE)

# Part 4

ggplot(gauge,aes(x = density, y=log(gain)))+geom_point(col="blue",size=2)+labs(y="Log(Gain)",x="Density",title="Scatter plot of Log-Transformed Observed Data (90 observations)")+geom_smooth(method = "lm", se = FALSE,col='red')+geom_smooth(mapping = aes(x = density, y=log(gain)),col='black',se= FALSE)

# Part 5

fit.log <- lm(density~I(log(gain)) )
fit.linear <- lm( density~I(gain) )
library(ellipse)
plotcorr(cor(gauge))

# Part 6

print("The correlation matrix of the given data is: ")
cor(gauge)

# Part 7

# F-value and R-square
summary(fit.linear)
summary(fit.log)

# Part 8

# Adjusted R-squared ~ 1 => Good model
# confidence interval for the coefficients
confint(fit.log,level=0.95)


# Part 9

res.linear <- as.numeric(residuals(fit.linear))
res.log <- as.numeric(residuals(fit.log))

# To check if residuals are nearly normal
a<-fortify(fit.linear)
sres.linear <- as.numeric(a$.stdresid)


b<-fortify(fit.log)
sres.log <- as.numeric(b$.stdresid)
ggplot(b, aes(sres.log))+geom_histogram(binwidth = diff(range(sres.log))/8)+labs(x="Standardized Residuals of Log-linear Fit Model",y="Counts",title="Histogram of Residuals of Log-linear Fit Model")+geom_smooth(aes(y=45*dnorm(sres.log,mean=mean(sres.log),sd=sd(sres.log))),se = FALSE)


# Part 10

#ggplot(fit.linear, aes(density, .resid)) +geom_point() +  geom_hline(yintercept = 0) +  geom_smooth(se = FALSE)+labs(x="Density",y="Residuals",title="Residuals for linear model")
#ggplot(fit.linear, aes(.fitted, .stdresid)) +geom_point() +  geom_hline(yintercept = 0) +  geom_smooth(se = FALSE)+labs(x="Density",y="Standardized Residuals",title="Standardized Residuals for linear model")


ggplot(fit.log, aes(density, .resid)) +geom_point() +  geom_hline(yintercept = 0) +  geom_smooth(se = FALSE)+labs(x="Density",y="Residuals",title="Residuals for Log-linear model")

ggplot(fit.log, aes(density, .stdresid)) +geom_point() +  geom_hline(yintercept = 0) +  geom_smooth(se = FALSE)+labs(x="Density",y="Standardized Residuals",title="Standardized Residuals for Log-linear model")

qqplot <- function(y,distribution=qnorm,t) {
  require(ggplot2)
  x <- distribution(ppoints(y))
  d <- data.frame(Theoretical_Quantiles=x, Sample_Quantiles=sort(y))
  p <- ggplot(d, aes(x=Theoretical_Quantiles, y=Sample_Quantiles)) +geom_point() + geom_line(aes(x=x, y=x)) +labs(title=t)
  return(p)
}

qqplot(sres.linear,t="Q-Q plot for residuals of Linear Fit")
qqplot(sres.log,t="Q-Q plot for residuals of Log-linear Fit")

# install.packages('moments')
library(moments)
kurt_res = kurtosis(res.log)
cat("The kurtosis of the distribution of residuals is: ",kurt_res)


# Part 11

boxplot(res.linear,main="Box plot of residuals for Linear Fit")
boxplot(res.log,main="Box plot of residuals for Log-linear Fit")
ggplot(b,aes(x=1:90,y=.resid))+geom_point()+labs(title="Residuals for Log-Linear fit",x="index",y="Residuals")+ylim(c(-1,1))


# Part 12

ggplot(b,aes(x=1:90,y=.stdresid))+geom_point(col='blue')+ylim(c(-4,4))+geom_hline(yintercept = 3)+geom_hline(yintercept = -3)+labs(title="Residuals for Log-linear Fit",y="Standardised residuals",x="index")

ggplot(gauge,aes(y = density, x=gain))+geom_point(col="blue",size=2)+labs(x="Gain",y="Density",title="Scatter plot of Observed Data (90 observations)")+stat_smooth(method = "lm", se = TRUE,col='red',level=0.95)
ggplot(gauge,aes(y = density, x=log(gain)))+geom_point(col="blue",size=2)+labs(x="Log(Gain)",y="Density",title="Scatter plot of Log-Transformed Observed Data (90 observations)")+stat_smooth(method = "lm", se = TRUE,col='red',level=0.95)

mean_res = mean(res.log)
var_res = var(res.log)
range_res = c(min(res.log), max(res.log))

# Part 13

# Mean Square Error
rmse <- function(error){
  return(sqrt(mean(error^2)))
}
# RMSE for linear model
RMSE.linear <- rmse(res.log)
cat("The RMS error of the regression fit is: ",RMSE.linear)
    
abs_res_error = sum(abs(res.log))
    
# Part 14

require(plotrix)


fit_lm<-function(d,t){d.train<-unique(d$density)
g.train <- numeric(length(d.train))
for (i in 1:length(d.train)){
  g.train[i]<-mean(d$gain[which(d$density==d.train[i])])
}
model_x <- g.train
fit.model<-lm(d.train~log(model_x))
pred.density <- predict(fit.model, data.frame(model_x<-t))
r <- list("model"=fit.model,"prediction"=pred.density)
return (r)
}

# Part 15

data.train1 <- gauge[which(density!=0.508),]
data.train1 <- cbind.data.frame(data.train1,"id"=rep(x = 1,nrow(data.train1)))
test.data1 <- gain[density==0.508]

data.train2 <- gauge[which(density!=0.001),]
data.train2 <- cbind.data.frame(data.train2,"id"=rep(x = 1,nrow(data.train2)))
test.data2 <- gain[density==0.001]

ret1<-fit_lm(data.train1,test.data1)
ret2<-fit_lm(data.train2,test.data2)

# PArt 16

model1 <- ret1$model
density.prediction1 <- ret1$prediction
df_temp <- cbind.data.frame("density"=density.prediction1,"gain"=test.data1,"id"=rep(x=0,length(test.data1)))
df1<- rbind.data.frame(data.train1,df_temp)

model2 <- ret2$model
density.prediction2 <- ret2$prediction
df_temp <- cbind.data.frame("density"=density.prediction2,"gain"=test.data2,"id"=rep(x=0,length(test.data2)))
df2<- rbind.data.frame(data.train2,df_temp)

ggplot(df1,aes(y = density, x=log(gain),col=factor(id)))+geom_point(size=3.5)+labs(x="Log(Gain)",y="Density",title="Predicted Data (density=0.508) with fitted Log-Linear Model")+geom_smooth(method = "lm", se = FALSE,col='gray')
ggplot(df2,aes(y = density, x=log(gain),col=factor(id)))+geom_point(size=3.5)+labs(x="Log(Gain)",y="Density",title="Predicted Data (density=0.001) with fitted Log-Linear Model")+geom_smooth(method = "lm", se = FALSE,col='gray')

# Part 17

pred.density.CI1 <- predict(model1, data.frame(x.model=test.data1), interval="confidence")
pred.density.CI2 <- predict(model2, data.frame(x.model=test.data2), interval="confidence")

lbd1 <- pred.density.CI1[,2]
ubd1 <- pred.density.CI1[,3]
lbd2 <- pred.density.CI2[,2]
ubd2 <- pred.density.CI2[,3]

true.density1 = 0.508
true.density2 = 0.001

check1 <- (true.density1 >= lbd1) & (true.density1 <= ubd1)
check2 <- (true.density2 >= lbd2) & (true.density2 <= ubd2)
check1
check2

# Part 18

plotCI(test.data1, density.prediction1, ui=ubd1, li=lbd1,main="CI of predicted density & True density (d = 0.508)",sub="Predicted values(Black)   True values(Red)",xlab="Gain",ylab="Density")
points(test.data1, rep(x=true.density1,length(test.data1)), col="red")

plotCI(test.data2, density.prediction2, ui=ubd2, li=lbd2,main="CI of predicted density & True density (d = 0.001)",sub="Predicted values(Black)   True values(Red)",xlab="Gain",ylab="Density")
points(test.data2, rep(x=true.density2,length(test.data2)), col="red")


# Part 19
plot(data.train1$density ~ log(data.train1$gain), type = 'n',main="Prediction results for density 0.508",xlab="log(gain)",ylab="Density",sub="shaded region is interval")
x.val <- seq(2.5, 6.5, length.out = 100)
preds1 <- predict(model1,  data.frame(model_x =exp(x.val)),interval = 'prediction')
lines(x.val, preds1[ ,3], lty = 'dashed', col = 'red')
lines(x.val, preds1[ ,2], lty = 'dashed', col = 'red')
polygon(c(rev(x.val), x.val), c(rev(preds1[ ,3]), preds1[ ,2]), col = 'grey80', border = NA)
abline(model1)
points(log(test.data1),rep(x=true.density1,length(test.data1)), col="red")



plot(data.train1$density ~ log(data.train1$gain), type = 'n',main="Prediction results for density 0.001",xlab="log(gain)",ylab="Density",sub="shaded region is interval")
preds2 <- predict(model2,  data.frame(model_x =exp(x.val)), interval = 'prediction')
lines(x.val, preds2[ ,3], lty = 'dashed', col = 'red')
lines(x.val, preds2[ ,2], lty = 'dashed', col = 'red')
polygon(c(rev(x.val), x.val), c(rev(preds2[ ,3]), preds2[ ,2]), col = 'grey80', border = NA)
abline(model2)
points(log(test.data2),rep(x=true.density2,length(test.data2)), col="red")

# End of Code
