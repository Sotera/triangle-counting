#! /usr/bin/env Rscript

library(igraph)
library(plyr)

args <- commandArgs(trailingOnly = TRUE)
filename <- args[1]
edges <- read.table(filename, header=F, sep=",")
g <- graph.data.frame(edges, directed=F)
g <- simplify(g)

triangles <- adjacent.triangles(g, vids=V(g))
ccoef <- transitivity(g, type='local')
deg <- degree(g, v=V(g), mode='total')
data <- data.frame(triangles, ccoef, deg)

# Let's bin the degrees to make our numbers directly comparable
expBin <- function(deg){
  omega <- 2
  tau <- 1000
  if (deg < tau){
    return(deg)
  }
  else{
    return(floor(log(1+(omega-1)*(deg-tau)/log(omega))) + tau)
  }
}
data$bin <- apply(data['deg'], 1, expBin)


binned_data <-ddply(data,
                    "bin",
                    function(df)c(mean(df$ccoef), sum(df$triangles))
)
colnames(binned_data) <- c('deg', 'avg(ccoef)', '# triangles') # note its number of triangles with at least one vertex of degree 'deg' (triangles appear more than once in this list!)
print(binned_data)
print(paste('Global clustering coefficient = ', transitivity(g, type='global')))
print(paste('Total # of triangles = ', sum(binned_data["# triangles"]/3)))





write.table(binned_data[c("deg", "avg(ccoef)")], file="exact.csv", sep=",", row.names=FALSE)
