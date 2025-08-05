#devtools::load_all("E:/FangCloudSync/R_WD360/Project/soyplant")
devtools::install_github("zhaoqingsonga/soyplant")
library(openxlsx)
source("R/mainfunction.R")
library(dplyr)
library(stringr)
#输出二维矩阵
mycross<-export_cross_matrix()

#配置杂交组合，
#第一步亲本筛选
##母本筛选
parent_table<-readRDS("data/parent_table.rds")
mother<-parent_table|>filter(转基因=="否",
                     str_detect(审定编号, "2023")|str_detect(审定编号, "2024"),
                     str_detect(适宜区域, "南片")|
                       str_detect(适宜区域, "淮北")|
                       str_detect(适宜区域, "河南")|
                       str_detect(审定编号, "皖审")
                     )

mother_names<-mother$名称
mother_names<-c(mother_names,"圣豆101","圣豆3","中豆57","南农66")
##转基因父本筛选
G2_parent<-readRDS("data/G2_parent.rds")
G_father<-parent_table|>filter(名称%in%G2_parent|str_detect(名称,"GLHJD"))
G_father_names<-G_father$名称                         

#常规父本选择
Nfather_names<-c("冀农科018","油6019","南农47","冀农科022","冀农科091",
                 "中黄340","华豆17","徐豆31","徐豆32","GM25H056","GM25H057")




#第二步-配置转基因杂交组合
set.seed(2356)
mycross<-run_cross_plan(n = 10,
                        mother_names,
                        G_father_names,
                        content_value = "test"
)

#转基因可做组合：
mother_names<-read.table("clipboard")[,1]
G_father_names<-read.table("clipboard")[,1]
#配置转基因杂交组合
set.seed(2356)
mycross<-run_cross_plan(n = 363,
                        mother_names,
                        G_father_names,
                        content_value = "25可做转基因组合"
)

mycross<-run_cross_plan(n = 3,
                        c("冀豆12","冀豆17","徐豆18"),
                        c("中联豆5077","中联豆6024"),
                        content_value = "25test"
)



#批次统计
summarize_cross_batches()

#批次筛选
mycross<-filter_cross_by_batches(c("test"))

#批次删除
#clear_cross_batches(c("25可做转基因组合"),preview = FALSE)


#统计做杂交情况
stac_parents<-merge_mother_father_stats()


