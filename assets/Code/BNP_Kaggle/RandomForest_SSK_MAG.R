# KAGGLE Contest: BNP Dataset
# MATH289C: Final
# Authors: Saurabh Kulkarni, Mitesh Gadgil
# Classification Techniques: Random Forests

### RANDOM FOREST
# Training Dataset Preprocessing
trainData <- read.csv("train.csv")
# To eliminate points with missing variables
mytrain <- trainData[complete.cases(trainData),]
# To delete categorical variables
del = array()
i = 1
for (j in 1:ncol(mytrain)){
  if (class(mytrain[,j]) == "factor"){
    del[i] = j
    i = i + 1
  }
}
RFX <- mytrain[,-c(1,2,del)]
RFy <- as.factor(mytrain[,2]) # make response variable a factor

# Test Data Preprocessing
testData <- read.csv("test.csv")
#mytest <- testData[complete.cases(testData),]
test.full <- testData
test.full[is.na(test.full)]<- -1
del = array()
i = 1
for (j in 1:ncol(test.full)){
  if (class(test.full[,j]) == "factor"){
    del[i] = j
    i = i + 1
  }
}
testX <- test.full[,-c(1,del)]


# Training the Classifier
index = 1:nrow(RFX)
samplesize = 10000
Sample = sample(index, size = samplesize)
RFX.sub <- RFX#[Sample[1:(samplesize/2)],]
RFy.sub <- RFy#[Sample[1:(samplesize/2)]]
testX.test <- testX#[Sample[(samplesize/2+1):samplesize],]

#install.packages("randomForest")
require("randomForest")

ntree=100
begTime <- Sys.time()
RF <- randomForest(RFX.sub, RFy.sub, importance = TRUE, ntree=ntree, mtry=10,
                   na.action=na.roughfix, #can also use na.action = na.omit
                   replace=FALSE)
runTime <- Sys.time()-begTime
runTime

plot(RF)
print(RF)
# check importance
round(importance(RF), 2) 
# significance of variable
varImpPlot(RF,main = "Random Forest: Significant Variables", scale=T, bg = par("bg"),
           color = par("fg"), gcolor = par("fg"), lcolor = "gray",cex = par("cex"), pch = 21, gpch = 21, )


# Prediction on the test dataset
RF.pred.test <- as.vector(as.numeric(predict(RF, newdata = testX,type = "prob")))

# We generated a submission file to upload on kaggle.com
submission <- read.table("sample_submission.csv",header = T, sep=',')
submission$PredictedProb <- RF.pred.test[1:114393]
write.csv(submission,"RF_submission.csv",row.names = F,quote = F)




