## KAGGLE CONTEST: BNPP DATASET
## MATH 289 FINAL REPORT
## AUTHORS: MITESH GADGIL & sAURABH KULKARNI
## EXPLORATORY DATA ANALYSIS OF DATASET

# Inputting data
test <- read.csv("test.csv")
train <- read.csv("train.csv")
dim(train)
dim(test)

# Type of variables
str(train)
str(test)

# Checking length of unique ID's to check for duplication of records
length(unique(train$ID))
length(unique(test$ID))

# Checking proportion of data points with target=1 and target=0
table(train$target)
prop.table(table(train$target))

# to remove the "" level of categorical variables and replace by NA
levels(train$v3)[1] <- NA 
levels(train$v22)[1] <- NA
levels(train$v30)[1] <- NA
levels(train$v31)[1] <- NA
levels(train$v52)[1] <- NA
levels(train$v56)[1] <- NA
levels(train$v91)[1] <- NA
levels(train$v107)[1] <- NA
levels(train$v112)[1] <- NA
levels(train$v113)[1] <- NA
levels(train$v125)[1] <- NA

# removing data points with missing data
dim(na.omit(train)) 

# Look at the percentage of missing data for each feature
Count <- apply(train, 2, function(x){sum(is.na(x))})
Percentage <- apply(train, 2, function(x){sum(is.na(x))/length(x)})
missingsvariables <- cbind(Count, Percentage)
missingsvariables[order(missingsvariables[,"Count"],decreasing = F),]

#Is there a relation between missing data and the target value of that data point
target0 <- train[train$target==0,]
target1 <- train[train$target==1,]

Count_0 <- apply(target0, 2, function(x){sum(is.na(x))})
Percentage_0 <- apply(target0, 2, function(x){sum(is.na(x))/length(x)})
Count_1 <- apply(target1, 2, function(x){sum(is.na(x))})
Percentage_1 <- apply(target1, 2, function(x){sum(is.na(x))/length(x)})
missingvariables <- cbind(Count_0, Percentage_0*100, Count_1, Percentage_1*100)
missingvariables

# Summary: Continuous variables
cont.var.names <- c(paste("v", c(1, 2, 4:21, 23, 25:29, 32:46, 48:51, 53:55, 57:65, 67:70, 72, 73, 76:78, 80:90, 92:106, 108, 109, 111, 114:124, 126:131), sep=""))
summary(train[cont.var.names])

# Histograms of continuous variables comparing distributions for target=0 and target=1
for (i in 1:length(train[cont.var.names])) {
  par(mfrow=c(1,2))
  hist(target0[cont.var.names][[i]], breaks=100, main="Histogram target 0", xlab=colnames(target0[cont.var.names][i]))
  hist(target1[cont.var.names][[i]], breaks=100, main="Histogram target 1", xlab=colnames(target1[cont.var.names][i]))
}
dev.off()

# Correlogram of continuous variables depicting correlation
library(corrgram)
library(ellipse)
set.seed(1)
mysample <- train[cont.var.names][sample(1:nrow(train[cont.var.names]), 1000, replace=FALSE),]
corrgram(mysample, order=F, upper.panel=NULL, lower.panel=panel.shade, cex.labels=1)

# checking max and min correlation
df <- as.data.frame(cor(train[cont.var.names], use="pairwise.complete.obs"))
df[df==1] <- NA 
apply(df, 2, min, na.rm=T)
apply(df, 2, max, na.rm=T)
apply(train[cont.var.names], 2, function(x){length(unique(x))} )

# Summary: Categorical Variables
cat.var.names <- c(paste("v", c(3, 22, 24, 30, 31, 47, 52, 56, 66, 71, 74, 75, 79, 91, 107, 110, 112, 113, 125), sep=""))
summary(train[cat.var.names])

# number of unique levels
apply(train[cat.var.names], 2, function(x){length(unique(x))} )

# checking correspondence among categorical variables
table(train$v91, train$v107)
apply(train[cat.var.names], 2, function(x){length(unique(x))} )

# visualizing categorical variable according to target variable value
par(mfrow=c(1,2))
barplot(table(train$target, train$v3), main="v3")
plot(prop.table(table(train$v3, train$target), 1), main="v3")

barplot(table(train$target, train$v24), main="v24")
plot(prop.table(table(train$v24, train$target), 1), main="v24")

barplot(table(train$target, train$v30), main="v30")
plot(prop.table(table(train$v30, train$target), 1), main="v30")

barplot(table(train$target, train$v31), main="v31")
plot(prop.table(table(train$v31, train$target), 1), main="v31")

barplot(table(train$target, train$v47), main="v47")
plot(prop.table(table(train$v47, train$target), 1), main="v47")

barplot(table(train$target, train$v52), main="v52")
plot(prop.table(table(train$v52, train$target), 1), main="v52")

barplot(table(train$target, train$v66), main="v66")
plot(prop.table(table(train$v66, train$target), 1), main="v66")

barplot(table(train$target, train$v71), main="v71")
plot(prop.table(table(train$v71, train$target), 1), main="v71")

barplot(table(train$target, train$v74), main="v74")
plot(prop.table(table(train$v74, train$target), 1), main="v74")

barplot(table(train$target, train$v75), main="v75")
plot(prop.table(table(train$v75, train$target), 1), main="v75")

barplot(table(train$target, train$v91), main="v91")
plot(prop.table(table(train$v91, train$target), 1), main="v91")

barplot(table(train$target, train$v107), main="v107")
plot(prop.table(table(train$v107, train$target), 1), main="v107")

barplot(table(train$target, train$v110), main="v110")
plot(prop.table(table(train$v110, train$target), 1), main="v110")






