## KAGGLE CONTEST: BNPP DATASET
## MATH 289 FINAL REPORT
## AUTHORS: MITESH GADGIL & sAURABH KULKARNI
## PREDICTIVE MODELLING USING LOGISTIC REGRESSION

library(mice)
library(VIM)
library(boot)
library(glmnet)

# Inputting Data
test <- read.csv("test.csv")
train <- read.csv("train.csv")

# Pre-processing Data
train[train ==""] <- NA
test[test ==""] <- NA
dim(na.omit(train)) 
dim(na.omit(test)) 

train.nona <- na.omit(train)
test.nona <- na.omit(test)
feature_NA.train <- apply(train.nona,1,function(x)sum(is.na(x)))
feature_NA.test <- apply(test.nona,1,function(x)sum(is.na(x)))
levels(train$v3)[1] <- NA
levels(train$v30)[1] <- NA
levels(train$v31)[1] <- NA
levels(train$v91)[1] <- NA
levels(test$v3)[1] <- NA
levels(test$v30)[1] <- NA
levels(test$v31)[1] <- NA
levels(test$v91)[1] <- NA

# Choosing features to be used
cont.var.names <- c(paste("v",50,10,20,12,119,sep=""))
cat.var.names <- c(paste("v", c( 71,66,3,30), sep=""))

# Function to normalize feature vectors 
normalise <- function(x){
     for (i in 1:ncol(x))
     {
      norm = sqrt(sum((x[,i]**2)))
      x[,i] <- x[,i]/norm
     }
    return (x)
   }
  
train.cont_norm.nona<- normalise(train.nona[cont.var.names])
test.cont_norm.nona<- normalise(test.nona[cont.var.names])

# One-hot encoding of categorical variables 
train.cat.nona <- train.nona[cat.var.names]
test.cat.nona <- test.nona[cat.var.names]
df <- rbind(train.cat.nona,test.cat.nona)
encoded <- model.matrix(~ .-1, data=df,contrasts.arg = lapply(df, contrasts, contrasts=FALSE))

# Final training and test data frames combining encoded-categorical and continuous variables
final.train <- cbind.data.frame(train.cont_norm.nona,encoded[1:nrow(train.nona),],"target"=as.factor(train.nona$target))
final.test <- cbind.data.frame(test.cont_norm.nona,encoded[(nrow(train.nona)+1):nrow(encoded),])

# Logistic Model 
glm.nona <- glm(target~.,data=final.train,family = binomial)

# Cost function defined as Logloss for cross-validation
cost <- function(o,p){
  err=0
  for(i in 1:length(o)){
  err = err + (o[i]*log(p[i]) + (1-o[i])*log(1-p[i]))
  }
  return(-err/length(o))
}

# Cross-validation using 10-folds
glm.nona.cv.err <- cv.glm(final.train, glm.nona, cost, K=10)$delta

# Prediction on test set
PredictedProb <-predict(glm.nona, final.test , type="response")

# Displays ID and predicted probability values
head(PredictedProb)

