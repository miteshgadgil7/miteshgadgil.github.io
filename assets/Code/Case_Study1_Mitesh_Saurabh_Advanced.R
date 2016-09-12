# Case Study 1 - Advanced Analysis
# Using Data babies..txt and babies23.txt


#cleaning small dataset and checking missing values
dat11 = read.table("babies..txt", header=TRUE,na.strings = c("9","99","999")) 

# global NA handling small

image(is.na(dat11), main = "Visualization of Missing Values", xlab = "Observation", ylab = "Variable", xaxt = "n", yaxt = "n", bty = "n")
axis(1, seq(0, 1, length.out = nrow(dat11)), 1:nrow(dat11), col = "white")
axis(2, (0:(length(names(dat11))-1))/(length(names(dat11))-1), names(dat11), col = "white", las = 2)

dat11c = na.omit(dat11)
plot(dat11c$gestation,dat11c$bwt,col=ifelse(dat11c$smoke==0,"blue","red"),pch=20)
abline(v=280,h=88, col="green")

# cleaning large dataset and checking missing values
dat2 = read.table("babies23.txt",header = TRUE)
dat2$smoke<- dat1$smoke
dat2$gestation[which(dat2$gestation==999)] <- NA
dat2$wt[which(dat2$wt==999)] <- NA
dat2$parity[which(dat2$parity==99)] <- NA
dat2$race[which(dat2$race==99)] <- NA
dat2$age[which(dat2$age==99)] <- NA
dat2$ed[which(dat2$ed==9)] <- NA
dat2$ht[which(dat2$ht==99)] <- NA
dat2$wt[which(dat2$wt==999)] <- NA
dat2$drace[which(dat2$drace==99)] <- NA
dat2$dage[which(dat2$dage==99)] <- NA
dat2$ded[which(dat2$ded==9)] <- NA
dat2$dht[which(dat2$dht==99)] <- NA
dat2$dwt[which(dat2$dwt==999)] <- NA
dat2$inc[which(dat2$inc==98 | dat2$inc==99)] <- NA
dat2$number[which(dat2$number==99 |dat2$number==98 |dat2$number==9  )] <- NA
drops <- c("time","id","pluralty","outcome","date","dwt","dht")
dat2_t <- dat2[,!(names(dat2) %in% drops)]

# global NA handling large
image(is.na(dat2), main = "Visualization for Missing Values", xlab = "Observation", ylab = "Variable", xaxt = "n", yaxt = "n", bty = "n")
axis(1, seq(0, 1, length.out = nrow(dat2_t)), 1:nrow(dat2_t), col = "white")
axis(2, (0:(length(names(dat2_t))-1))/(length(names(dat2_t))-1), names(dat2_t), col = "white", las = 2)

dat2c = na.omit(dat2_t)

#separating into smoking and non-smoking
dat11c_s = dat11c[which(dat11c$smoke==1),]
dat11c_ns = dat11c[which(dat11c$smoke==0),]
dat2c_s = dat2c[which(dat2c$smoke==1),]
dat2c_ns = dat2c[which(dat2c$smoke==0),]


# t-tests for non-categorical variables: if p-value<0.05 then different grps else same
t.test(dat11c_s$weight,dat1c_ns$weight)
t.test(dat11c_s$height,dat1c_ns$height)
t.test(dat11c_s$age,dat1c_ns$age)

# for categorical variables looking at % population in each group: nrows()


#looking for similarities in attributes of lbwt babies in smoking and non smoking grps
lbwt_ns<- dat2c$wt[which(dat2c$wt<88 & dat2c$smoke==0)]
lbwt_s <- dat2c$wt[which(dat2c$wt<88 & dat2c$smoke==1)]
nbwt_ns <- dat2c$wt[which(dat2c$wt>=88 & dat2c$smoke==0)]
nbwt_s <- dat2c$wt[which(dat2c$wt>=88 & dat2c$smoke==1)]
e<- cbind(v1=c(length(lbwt_s),length(nbwt_s)),v2=c(length(lbwt_ns),length(nbwt_ns)))

lg_ns<- dat2c$gestation[which(dat2c$gestation<280 & dat2c$smoke==0)]
lg_s <- dat2c$gestation[which(dat2c$gestation<280 & dat2c$smoke==1)]
ng_ns <- dat2c$gestation[which(dat2c$gestation>=280 & dat2c$smoke==0)]
ng_s <- dat2c$gestation[which(dat2c$gestation>=280 & dat2c$smoke==1)]
f<- cbind(v1=c(length(lg_s),length(ng_s)),v2=c(length(lg_ns),length(ng_ns)))

#kurtosis
s_bwt<-dat11c_s$bwt
ns_bwt<-dat11c_ns$bwt
k_s<- mean(((s_bwt-mean(s_bwt))/sd(s_bwt))^4)
k_ns<- mean(((ns_bwt-mean(ns_bwt))/sd(ns_bwt))^4)

#skew
s_skew<-mean(((s_bwt-mean(s_bwt))/sd(s_bwt))^3)
ns_skew<-mean(((ns_bwt-mean(ns_bwt))/sd(ns_bwt))^3)

#numerical summaries
mean.s_bwt<- mean(s_bwt)
mean.ns_bwt<- mean(ns_bwt)
sd.s_bwt <- sd(s_bwt)
sd.ns_bwt <- sd(ns_bwt)


# ggplot 
ggplot(dat11c, aes(x=gestation, y=bwt,color=factor(smoke))) +scale_color_manual(values=c("blue", "red"))+ geom_point()+ geom_vline(xintercept = 280)+geom_hline(yintercept = 88)+labs(x="Gestation Period (weeks)",y="Birth Weight (ounces)",title="Scatterplot: Birthweight vs Gestation (Smoking)") # scatterplot
ggplot(dat2c, aes(x=gestation, y=wt,color=factor(ed))) +scale_color_manual(values=rainbow(8))+ geom_point()+ geom_vline(xintercept = 280)+geom_hline(yintercept = 88)+labs(xlab("Gestation Period (weeks)"))+labs(ylab("Birth Weight (ounces)"))+ggtitle("Scatterplot: Birthweight vs Gestation(Mother's Education)") # scatterplot
ggplot(dat11c, aes(x=weight, y=bwt)) + geom_point()+geom_hline(yintercept = 88)+labs(x="Mother's Weight",y="Birth Weight (ounces)",title="Scatterplot: Birthweight vs Mother's Weight ") # scatterplot
ggplot(dat2c, aes(x=age, y=wt,color=factor(smoke))) + geom_point()+geom_hline(yintercept = 88)+scale_color_manual(values=c("blue", "red"))+labs(x="Mother's Age",y="Birth Weight (ounces)",title="Scatterplot: Birthweight vs Mother's Age ") # scatterplot

ggplot(dat2c_s, aes(factor(inc))) + labs(x="Income groups",y="Count",title="Bar plot: Income distribution in smoking mothers")+geom_bar(fill="royalblue3") # bar plot
ggplot(dat2c_ns, aes(factor(inc))) + labs(x="Income groups",y="Count",title="Bar plot: Income distribution in non-smoking mothers")+geom_bar(fill="royalblue3") # bar plot

barplot(e, main="Group Barplots: Birthweight comparison", ylab= "Count",names.arg=c("Smoking","Non-smoking"),beside=TRUE, col=c("red","darkblue"))
legend("top", fill= c("red","darkblue"), legend=c("Low Birthweight", "Normal Birthweight")  )

barplot(f, main="Group Barplots: Gestation Period comparison", ylab= "Count",names.arg=c("Smoking","Non-smoking"),beside=TRUE, col=c("red","darkblue"))
legend("topleft", fill= c("red","darkblue"), legend=c("Low Gestation", "Normal Gestation")  )

