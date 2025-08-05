devtools::load_all("E:/FangCloudSync/R_WD360/Project/soyplant")

library(openxlsx)
library(dplyr)
source("R/mainfunction.R")

# 批次统计
summarize_cross_batches()

# 批次筛选
#mycross <- filter_cross_by_batches(c("N2561种腐病","N2561种腐病反交"))
mycross <- filter_cross_by_batches(c("一"))




# 批次删除（可选）
#clear_cross_batches(c("25test"), preview = FALSE)

# 生成账本字段定义
fields <- c("fieldid", "code", "place", "stageid", "name", "rows", "line_number", "rp")

# 组合前缀与文件路径
PRE <- "G2571"
myfilename <- "output/N/G_转基因-杂交组合-可做-1111.xlsx"

# 构建组合数据
mydata <- data.frame(
  ma = mycross$ma_名称,
  pa = mycross$pa_名称,
  memo = paste(mycross$ma_特征特性, mycross$pa_特征特性, sep = "+")
)

# 一：生成组合编码
my_combi <- get_combination(
  mydata,
  prefix = PRE,
  startN = 1,
  only = TRUE,
  order = FALSE
)

# 添加年份
my_combi$year <- 2025

# 二：生成种植计划
planted <- my_combi |>
  planting(
    interval = 999,
    s_prefix = PRE,
    place = "宿州",
    rp = 1,
    digits = 3,
    ck = NULL,
    rows = 2,
    startN=1
  )

# 三：保存 Excel 工作簿
savewb(
  origin = my_combi,
  planting = planted,
  myview = planted[, c(fields, "ma", "pa")],
  combi_matrix = combination_matrix(my_combi),
  filename = myfilename,
  overwrite = FALSE
)
