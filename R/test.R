# 大豆杂交组合配置与分析脚本

# 加载必要的包
if (!requireNamespace("devtools", quietly = TRUE)) {
  install.packages("devtools")
}
if (!requireNamespace("soyplant", quietly = TRUE)) {
  devtools::install_github("zhaoqingsonga/soyplant")
}
if (!requireNamespace("openxlsx", quietly = TRUE)) {
  install.packages("openxlsx")
}
if (!requireNamespace("rstudioapi", quietly = TRUE)) {
  install.packages("rstudioapi")
}

library(soyplant)
library(openxlsx)
library(rstudioapi)

#