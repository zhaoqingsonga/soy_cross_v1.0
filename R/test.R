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

# 设置工作目录（如果在RStudio外部运行，会使用当前工作目录）
tryCatch({
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
  message("工作目录已设置为当前脚本所在目录")
} catch (e) {
  message("无法获取当前脚本路径，使用默认工作目录: ", getwd())
})

# 数据导入函数
load_hybrid_data <- function(parent_file = "../data/杂交亲本.xlsx", 
                             matrix_file = "../data/170杂交组合配制.xlsx") {
  # 导入杂交亲本数据
  parent_data <- tryCatch({
    read.xlsx(parent_file, "杂交")
  }, error = function(e) {
    stop(paste("读取杂交亲本数据失败:", e$message))
  })
  
  # 导入杂交矩阵数据
  matrix_data <- tryCatch({
    read.xlsx(matrix_file, "杂交", rows = 8:200, cols = 9:200)
  }, error = function(e) {
    stop(paste("读取杂交矩阵数据失败:", e$message))
  })
  
  list(parent = parent_data, matrix = matrix_data)
}

# 主处理流程
process_hybridization <- function(data, max_crosses = 5, reference_variety = "冀豆12") {
  # 组合记录
  combined_records <- combine_records(data$parent)
  message("已组合杂交亲本记录")
  
  # 转换为矩阵
  matrix_data <- as.matrix(data$matrix)
  
  # 添加矩阵内容，包括对称组合
  combined_with_content <- add_matrix_content(
    combined_records, 
    matrix_data,
    add_symmetric = TRUE
  )
  message("已添加杂交组合内容，包括对称组合")
  
  # 生成杂交计划
  cross_result <- tryCatch({
    generate_cross_plan(
      combined_with_content,
      max_crosses = max_crosses,
      NULL,
      reference_variety = reference_variety
    )
  }, error = function(e) {
    stop(paste("生成杂交计划失败:", e$message))
  })
  
  list(
    plan = cross_result$plan,
    updated_data = cross_result$updated_df
  )
}

# 分析与报告函数
generate_reports <- function(hybrid_data) {
  # 杂交亲本统计
  parent_stats <- merge_mother_father_stats(hybrid_data)
  message("已完成杂交亲本统计分析")
  
  # 批次统计
  batch_stats <- summarize_cross_batches(hybrid_data)
  message("已完成杂交批次统计分析")
  
  list(
    parent_stats = parent_stats,
    batch_stats = batch_stats
  )
}

# 主函数
main <- function() {
  # 记录开始时间
  start_time <- Sys.time()
  message("开始杂交组合配置分析...")
  
  # 加载数据
  data <- load_hybrid_data()
  
  # 处理杂交数据
  hybridization_result <- process_hybridization(
    data,
    max_crosses = 5,
    reference_variety = "冀豆12"
  )
  
  # 生成报告
  reports <- generate_reports(hybridization_result$updated_data)
  
  # 输出特定日期的反交组合
  specific_date_crosses <- subset(
    hybridization_result$updated_data,
    content == "2025-06-06反交"
  )
  
  # 记录结束时间
  end_time <- Sys.time()
  message("杂交组合配置分析完成，用时:", difftime(end_time, start_time, units = "secs"), "秒")
  
  # 返回结果
  list(
    cross_plan = hybridization_result$plan,
    updated_data = hybridization_result$updated_data,
    reports = reports,
    specific_date_crosses = specific_date_crosses
  )
}

# 执行主函数
if (sys.nframe() == 0) {
  result <- main()
  
  # 打印简要结果
  message("\n===== 杂交组合配置结果摘要 =====")
  message("生成的杂交计划包含 ", nrow(result$cross_plan), " 个组合")
  message("特定日期(2025-06-06)的反交组合数量: ", nrow(result$specific_date_crosses))
  message("=================================\n")
  
  # 保存结果到文件（可选）
  # write.xlsx(result$cross_plan, "杂交计划_结果.xlsx")
  # write.xlsx(result$updated_data, "更新的杂交数据.xlsx")
}    