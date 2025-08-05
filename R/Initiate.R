#devtools::install_github("zhaoqingsonga/soyplant")
devtools::load_all("E:/FangCloudSync/R_WD360/Project/soyplant")
#setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
library(openxlsx)
source("R/mainfunction.R")
####
# 以下内容为重置内容
#以个两条是重置parent_table.rds数据
# parent<-read.xlsx("data/亲本.xlsx","杂交")

#initialize_parent_table(parent)

#重新生成重组表，加亲本或对亲本信息改动后才用生成
#生成前要将cross_table.rd中content中内容生成cross_matrix.rds
#combi<-combine_records()

# #将以前的二维结果导入至，cross_table.rds的content字段
# df<-read.xlsx("data/170杂交组合配制.xlsx","杂交",startRow = 8,cols = 9:200)
# rownames(df)<-colnames(df)
# fill_content_from_matrix(as.matrix(df))
# 
#以有内容为初始内容，只在初始化时运行，以后不再运行。

