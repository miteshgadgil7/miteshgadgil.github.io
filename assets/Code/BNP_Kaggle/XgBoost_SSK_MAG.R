## KAGGLE CONTEST: BNPP DATASET
## MATH 289 FINAL REPORT
## AUTHORS: MITESH GADGIL & sAURABH KULKARNI
## PREDICTIVE MODELLING USING XGBOOST

library(xgboost)

# Inputting data
test <- read.csv("test.csv")
train <- read.csv("train.csv")

# Replacing missing data with NA
train[train ==""] <- NA
test[test ==""] <- NA

# Storing "target" as a different vector and equalizing test and train data columns
y <- train[, 'target']
train <- train[, -2]

# the number of NA's in a data point are stored to be used as a feature 
feature_NA.train <- apply(train,1,function(x)sum(is.na(x)))
feature_NA.test <- apply(test,1,function(x)sum(is.na(x)))

train[is.na(train)] <- -1
test[is.na(test)] <- -1

# Find factor variables and translate to numeric
# store column numbers of factor attributes in f and f.t
f <- c()
for(i in 1:ncol(train)) {
  if (is.factor(train[, i])) f <- c(f, i)
}

f.t <- c()
for(i in 1:ncol(test)) {
  if (is.factor(test[, i])) f.t <- c(f.t, i)
}

# COnverting the test and training sets into numeric variables
ttrain <- rbind(train, test)
for (i in f) {
  ttrain[, i] <- as.numeric(ttrain[, i]) 
}
train <- ttrain[1:nrow(train), ]
train <- cbind.data.frame(train,"feature"=feature_NA.train)

test <- ttrain[(nrow(train)+1):nrow(ttrain), ]
test <- cbind.data.frame(test,"feature"=feature_NA.test)

# the function that returns probability vector for test set after training according to 
#parameters in param0 for number of iterations given in 'iter'

doPrediction <- function(y, train, test, param0, iter) {
  n<- nrow(train)
  xgtrain <- xgb.DMatrix(as.matrix(train), label = y,missing = NaN)
  xgval = xgb.DMatrix(as.matrix(test),missing = NaN)
  watchlist <- list('train' = xgtrain)
  model = xgb.train(
    nrounds = iter
    , params = param0
    , data = xgtrain
    , watchlist = watchlist
    , print.every.n = 100
    , nthread = 8 
  )
  p <- predict(model, xgval)
  rm(model)
  gc()
  p
}

# Set parameters of the algorithm: eta and max_depth can control overfitting
# logloss is the evaluation metric used for evaluation
param0 <- list(
  # general , non specific params - just guessing
  "objective"  = "binary:logistic"
  , "eval_metric" = "logloss"
  , "eta" = 0.01
  , "subsample" = 0.8
  , "colsample_bytree" = 0.8
  , "min_child_weight" = 1
  , "max_depth" = 10
)

# Preparing submission file
submission <- read.table("sample_submission.csv", header=TRUE, sep=',')
ensemble <- rep(0, nrow(test))

# change to 1:n to get an ensemble of 'n' trained models 
for (i in 1:2) {
  p <- doPrediction(y, train, test, param0, 1200) 
  ensemble <- ensemble + p
}
submission$PredictedProb <- ensemble/i

# writing the submission file
write.csv(submission, "submission.csv", row.names=F, quote=F)
