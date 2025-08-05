devtools::load_all("E:/FangCloudSync/R_WD360/Project/soyplant")
library(openxlsx)
source("R/mainfunction.R")
source("R/edit_parent_table_app.R")
library(shiny)
library(rhandsontable)


#二维矩阵内容至content字段
fill_content_from_matrix()

#增加亲本并追加组合
add_parent_and_append_cross(data.frame(名称=c("mgod03","mgod08"),特点="优良株"))
#add_parent_and_append_cross(data.frame(名称=read.table("clipboard")[,1]))




#可以删除最后的记录并删除相应的组合
#remove_last_parents(2)


#对内容进行修改
edit_parent_table()
edit_parent_table_app() 

#用excel进行修改
## 先导出
edit_parent_table_via_excel("export")

## 再导入
edit_parent_table_via_excel("import")


