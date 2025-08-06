#加载必要的包
library(openxlsx)
source("R/mainfunction.R")
library(dplyr)
library(stringr)

#初始化部分，将数据重置
#---------------------------------------
#读入原始亲本数据
parent<-read.xlsx("data/亲本test.xlsx","杂交")
print(parent)
#初始化亲本表，生成parent_table.rds
initialize_parent_table(parent)
#查看亲本表
readRDS("data/parent_table.rds")
#初始化组合表，生成cross_table.rds
combi<-combine_records()
#查看组合表
readRDS("data/cross_table.rds")
#--------------------------------------

#增加亲本并更新组合
add_parent_and_append_cross(data.frame(名称=c("test1","test2")))
#删除亲本并更新组合
remove_last_parents(2)
#导出excel格式编辑，注意不能增加或删除记录。
edit_parent_table_via_excel("export")
#导入编辑结果
edit_parent_table_via_excel("import")
#亲本筛选用select_parent()
select_parent(特点=="高油")

#配制杂交组合,可根据条件进行筛选亲本
mycross<-run_cross_plan(
  n=3,
  mother_names=select_parent(特点=="高油"),
  father_names=select_parent(特点=="高产"),
  content_value="2025_test01"
)

#或直接写亲本表中有的亲本,没有的亲本会自动过滤
mycross<-run_cross_plan(
  n=2,
  mother_names=c("郑1307","中黄301","中黄13","平豆2号","sjfksdjfs"),
  father_names=c("中黄13","齐黄34","金豆99","平豆2号"),
  content_value="A"
)

#查看杂交批次
summarize_cross_batches()
#选择杂交批次
filter_cross_by_batches(c("2025_test01"))
#删除杂交批次
clear_cross_batches(c("2025_test01"))

#按批次提取杂交组合
filter_cross_by_batches(c("2025_test01"))

#查看组合矩阵
export_cross_matrix()
readRDS("data/cross_matrix.rds")
#包含反交记录
read.xlsx("data/cross_matrix_reverse.xlsx")

#查看亲本作用情况

merge_mother_father_stats()

