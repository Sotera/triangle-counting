#! /usr/bin/env Rscript
#
# Reads in exact and estimated clustering coefficient data
# and produces a nice comparison plot of avg(coeff) over 
# vertex degree

library(ggplot2)
library(ggthemes)

args <- commandArgs(trailingOnly = TRUE)
filename_exact <- args[1]
filename_est <- args[2]

exact <- read.table(filename_exact, sep=",", header=TRUE)
colnames(exact) <- c("degree", "coeff")
exact$error <- exact$degree * 0.0
exact$kind <- rep("exact", times=nrow(exact))
sampled <- read.table(filename_est, sep=",", header=TRUE)
colnames(sampled) <- c("degree", "coeff", "error")
sampled$kind <- rep("estimate", time=nrow(sampled))

df <- rbind(sampled, exact)

ggplot() + 
  geom_point(data=df, aes(x=degree, y=coeff, color=kind), alpha=I(0.5), size=5) + 
  geom_errorbar(data=subset(df, kind=="estimate"), aes(x=degree, y=coeff, ymin = coeff-error, ymax=coeff+error, color=kind), alpha=I(0.5)) +
  scale_color_brewer(palette="Set1") + 
  labs(x="Degree bin", y="Average clustering coefficient") + 
  geom_rangeframe() + 
  theme_tufte()


ggsave(file="error_figure.png")
