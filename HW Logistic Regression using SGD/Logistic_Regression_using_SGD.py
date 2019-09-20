#!/usr/bin/env python
# coding: utf-8

import numpy as np
from sklearn.datasets import load_iris
from sklearn import linear_model
import random
random.seed(123)


# Using iris data as training dataset
def load_test_data():
    iris = load_iris()
    X = iris.data  
    y = iris.target
    delete_index = []
    # Remove the data label = 2. In other words, only keep class = 0,1 for simplicity.
    for index in range(len(y)):
        if y[index] == 2:
            delete_index.append(index)
    X = np.delete(X,delete_index,0)
    y = np.delete(y,delete_index)
    return X,y


def sgd_class (X,y):
    # Use for unit test
    sgd = linear_model.SGDClassifier(loss= 'log',max_iter=1500,tol=0.0001)
    sgd.fit(X, y)
    return sgd


# Logistic Regression using SGD 
def logistic_regression(ws):
    return 1 / (1 + np.exp(-ws))

def sgd(X, y):
    #Run randomly
    w = np.ones(4)
    r,c = np.shape(X)
    # define maximum iteration
    max_iteration = 1000
    # define learning rate
    learning_rate = 0.001
    for i in range(max_iteration):
        index = random.randint(0, (r - 1))
        random_x = X[index]
        random_y = y[index]
        w = w - learning_rate * (logistic_regression(np.dot(w,random_x)) - random_y) * random_x
    return w


# Driver program
X,y = load_test_data()
w = sgd(X,y)
predicted_y=[]
for x in X:
    predicted_y.append(logistic_regression(np.dot(w,x)))
for i in range(len(predicted_y)):
    if predicted_y[i] > 0.5:
        predicted_y[i] = 1
    else:
        predicted_y[i] = 0

print ("Score of Logistic Regression using SGD: ",np.sum( y == predicted_y)/len(y))
print (predicted_y)

# Result from SGDclassfier, the sklearn function
sgdlg = sgd_class(X,y)
result = sgdlg.predict(X)
print ("Score of Logistic Regression using SGDclassifier",sgdlg.score(X,y))
print (result)

# Unittest
print("Unittest result:", np.alltrue(result == predicted_y))

