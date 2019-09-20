#input libraries
library(readxl)
library(reshape2)
library(dummies)
library(qcc)

#input data
set.seed(123)
data<-read_excel("./Desktop/eBayAuctions.xls")
trainIndex = sample(1:nrow(data), size = round(0.6*nrow(data)), replace=FALSE)
train = data[trainIndex,]
train$Duration<-as.character(train$Duration)
test = data[-trainIndex,]
test$Duration<-as.character(test$Duration)


# Generate pivot_table for mean of (competitive?) as a function of categorical variables(Category,currency,endDay,Duration) 
mdata = melt(train, id.vars = c("Category","currency","endDay","Duration"), measure.vars = "Competitive?")
category_mean = dcast(mdata, Category ~ variable, fun.aggregate = mean)
category_mean<-category_mean[order((category_mean[2])),]

currency_mean = dcast(mdata, currency ~ variable, fun.aggregate = mean)
currency_mean<-currency_mean[order(currency_mean[2]),]

duration_mean = dcast(mdata, Duration ~ variable, fun.aggregate = mean)
duration_mean<-duration_mean[order(duration_mean[2]),]

endday_mean = dcast(mdata, endDay ~ variable, fun.aggregate = mean)
endday_mean<-endday_mean[order(endday_mean[2]),]

# Reduce Categories
for (i in 1:(nrow(category_mean)-1)){
    if ((category_mean[i+1,2]-category_mean[i,2])<=0.03){
      test[test['Category']==category_mean[i,1],'Category']<-category_mean[i+1,1]
      train[train['Category']==category_mean[i,1],'Category']<-category_mean[i+1,1]
    }
}
for (i in 1:(nrow(currency_mean)-1)){
  if ((currency_mean[i+1,2]-currency_mean[i,2])<=0.03){
    test[test['currency']==currency_mean[i,1],'currency']<-currency_mean[i+1,1]
    train[train['currency']==currency_mean[i,1],'currency']<-currency_mean[i+1,1]
  }
}
for (i in 1:(nrow(duration_mean)-1)){
  if ((duration_mean[i+1,2]-duration_mean[i,2])<=0.03){
    test[test['Duration']==duration_mean[i,1],'Duration']<-duration_mean[i+1,1]
    train[train['Duration']==duration_mean[i,1],'Duration']<-duration_mean[i+1,1]
  }
}
for (i in 1:(nrow(endday_mean)-1)){
  if ((endday_mean[i+1,2]-endday_mean[i,2])<=0.03){
    test[test['endDay']==endday_mean[i,1],'endDay']<-endday_mean[i+1,1]
    train[train['endDay']==endday_mean[i,1],'endDay']<-endday_mean[i+1,1]
  }
}

#Generate dummies variables
train_dummy<-dummy.data.frame(as.data.frame(train))
names(train_dummy)
#Models
fit.all<-glm(`Competitive?` ~.,family=binomial(link='logit'),data=train_dummy)
summary(fit.all)

#find highest estimation
coeffs<-abs(fit.all$coefficients)
coeffs<-coeffs[!is.na(coeffs)]
h_pred=names(coeffs[match(max(coeffs),coeffs)])
single_subset = c(h_pred,"Competitive?")
fit.single = glm(`Competitive?` ~. , family=binomial(link='logit'), data=train_dummy[single_subset])
summary(fit.single)

#find 4 highest estimatied predictors
s<-summary(fit.all)
names(tail(sort(coeffs),5))

#Reduced logistic regression model
reduced_subset<-c("CategoryCollectibles","CategoryElectronics","currencyGBP","sellerRating","endDayMon","endDaySun","ClosePrice","OpenPrice","Competitive?")
fit.reduced = glm(`Competitive?` ~. , family=binomial(link='logit'), data=train_dummy[reduced_subset])

#Comparing models
anova(fit.reduced, fit.all, test='Chisq')


