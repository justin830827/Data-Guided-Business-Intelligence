# Load the libraries
library(vars)
library(urca)
library(pcalg)
require(Rgraphviz)
# To install pcalg library you may first need to execute the following commands:
#source("https://bioconductor.org/biocLite.R")
#biocLite("graph")
#biocLite("RBGL")


# Read the input data 
data <- read.csv("data.csv")

# Build a VAR model 
# Select the lag order using the Schwarz Information Criterion with a maximum lag of 10
# see ?VARSelect to find the optimal number of lags and use it as input to VAR()
optimal_lags <- VARselect(data, type="const")
varModel <- VAR(data, p = optimal_lags$selection[3])

# Extract the residuals from the VAR model 
# see ?residuals
residuals <- residuals(varModel)

# Check for stationarity using the Augmented Dickey-Fuller test 
# see ?ur.df
ad_test <- apply(residuals, 2, ur.df)
lapply(ad_test, summary)

# Check whether the variables follow a Gaussian distribution  
# see ?ks.test
ks_test <- apply(residuals, 2, ks.test, y = "pnorm")

# Write the residuals to a csv file to build causal graphs using Tetrad software
write.csv(x = residuals,file = "residuals.csv",row.names = FALSE)

# OR Run the PC and LiNGAM algorithm in R as follows,
# see ?pc and ?LINGAM 

# PC Algorithm
suffStat <- list(C = cor(residuals), n = nrow(residuals))
pc_fit <-pc(suffStat = suffStat, indepTest = gaussCItest,alpha = 0.1,labels = colnames(residuals),verbose = TRUE)

# LiNGAM Algorithm
lingam_fit <- lingam(residuals,verbose = TRUE)
