#' 初始化组合杂交记录表（含 content，写出 RDS 文件）
#'
#' 
#' @description
#' 从亲本主表生成两两杂交组合，字段加上 ma_ / pa_ 前缀，输出为 RDS 文件。
#'
#' @param df 可选。输入数据框，默认从 "data/parent_table.rds" 加载。
#' @param file 输出文件路径，默认为 "data/cross_table.rds"
#'
#' @return 返回组合数据框 cross_table，并保存为 RDS 文件
#'
#' @export
combine_records <- function(df = NULL, file = "data/cross_table.rds") {
  # 默认读取 parent_table.rds
  if (is.null(df)) {
    parent_file <- "data/parent_table.rds"
    if (!file.exists(parent_file)) {
      stop("❌ 默认 parent_table.rds 文件不存在：", parent_file)
    }
    df <- readRDS(parent_file)
    message("📂 已从 '", parent_file, "' 读取亲本主表，共 ", nrow(df), " 条记录。")
  }
  
  n <- nrow(df)
  if (n < 2) {
    stop("❌ 数据框必须至少包含两条记录。")
  }
  
  base_pairs <- utils::combn(n, 2)
  
  all_pairs <- do.call(rbind, lapply(seq_len(ncol(base_pairs)), function(k) {
    i <- base_pairs[1, k]
    j <- base_pairs[2, k]
    data.frame(
      i = c(i, j),
      j = c(j, i),
      pair_id = rep(k, 2),
      stringsAsFactors = FALSE
    )
  }))
  
  combined <- purrr::pmap_dfr(
    all_pairs,
    function(i, j, pair_id) {
      ma <- df[i, , drop = FALSE]
      pa <- df[j, , drop = FALSE]
      
      names(ma) <- paste0("ma_", names(ma))
      names(pa) <- paste0("pa_", names(pa))
      
      result <- dplyr::bind_cols(
        ma,
        pa,
        ma_row = i,
        pa_row = j,
        pair_id = pair_id
      )
      
      result$content <- NA_character_
      
      result <- result |>
        dplyr::relocate(pair_id, .before = dplyr::everything()) |>
        dplyr::relocate(ma_row, pa_row, .after = pair_id) |>
        dplyr::relocate(content, .after = pa_row)
      
      all_names <- names(result)
      ma_vars <- all_names[grepl("^ma_", all_names)]
      pa_vars <- all_names[grepl("^pa_", all_names)]
      
      base_names <- intersect(sub("^ma_", "", ma_vars), sub("^pa_", "", pa_vars)) |>
        sort()
      
      reordered <- unlist(purrr::map(base_names, \(name) {
        c(paste0("ma_", name), paste0("pa_", name))
      }))
      
      final_order <- c("pair_id", "ma_row", "pa_row", "content", reordered)
      dplyr::select(result, dplyr::all_of(final_order))
    }
  )
  
  cross_table <- dplyr::arrange(combined, pair_id)
  
  # 自动创建输出目录
  dir_path <- dirname(file)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
  
  # 保存为 .rds 文件
  saveRDS(cross_table, file)
  
  message(glue::glue("✅ 已生成 {length(unique(cross_table$pair_id))} 个组合，结果已保存为：'{file}'"))
  invisible(cross_table)
}


#' 从矩阵内容填充组合表（直接操作默认文件，支持预览与保存）
#'
#' @description
#' 自动将交叉矩阵内容填入默认组合表（data/cross_table.rds）中的 content 字段，
#' 支持添加反交记录、覆盖已有内容，以及预览修改结果。
#'
#' @param matrix 矩阵对象，若为空则从 matrix_file 文件加载
#' @param matrix_file 矩阵文件路径，默认 "data/cross_matrix.rds"
#' @param add_symmetric 是否添加反交记录，默认 TRUE
#' @param overwrite 是否覆盖已有 content 值，默认 TRUE
#' @param preview 是否仅预览修改，不保存，默认 FALSE
#'
#' @return 修改或新增的组合内容数据框（仅包含被修改的记录）
#' @export
fill_content_from_matrix <- function(
    matrix = NULL,
    matrix_file = "data/cross_matrix.rds",
    add_symmetric = TRUE,
    overwrite = TRUE,
    preview = FALSE
) {
  # === 1. 读取组合表 ===
  if (!file.exists("data/cross_table.rds")) {
    stop("❌ 无法找到默认组合数据：data/cross_table.rds")
  }
  combined_df <- readRDS("data/cross_table.rds")
  
  # === 2. 读取矩阵 ===
  if (!is.null(matrix)) {
    mat <- matrix
  } else {
    if (!file.exists(matrix_file)) {
      stop("❌ 无法找到矩阵文件：", matrix_file)
    }
    mat <- readRDS(matrix_file)
  }
  if (!is.matrix(mat)) stop("❌ matrix 参数或 matrix_file 中的数据不是矩阵")
  if (nrow(mat) != ncol(mat)) stop("❌ 矩阵必须是正方形")
  
  # === 3. 从矩阵生成 content_df（含反交） ===
  n <- nrow(mat)
  entries <- list()
  for (i in seq_len(n)) {
    for (j in seq_len(n)) {
      val <- mat[i, j]
      if (!is.na(val) && nzchar(val)) {
        entries[[length(entries) + 1]] <- list(ma_row = i, pa_row = j, content = val)
        if (add_symmetric && i != j) {
          entries[[length(entries) + 1]] <- list(ma_row = j, pa_row = i, content = paste0(val, "反交"))
        }
      }
    }
  }
  content_df <- dplyr::bind_rows(entries)
  
  # === 4. 合并到组合表 ===
  merged_df <- dplyr::left_join(combined_df, content_df, by = c("ma_row", "pa_row"), suffix = c("", ".new"))
  
  # === 5. 判断哪些需要更新 ===
  merged_df <- merged_df %>%
    dplyr::mutate(
      updated = !is.na(content.new) & (overwrite | is.na(content) | !nzchar(content)),
      content = dplyr::if_else(updated, content.new, content)
    )
  
  # === 6. 提取变更记录 ===
  modified_entries <- merged_df %>%
    dplyr::filter(updated) %>%
    dplyr::select(-updated, -content.new)
  
  # === 7. 最终结果清理 ===
  final_df <- merged_df %>%
    dplyr::select(-updated, -content.new)
  
  # === 8. 提示信息 ===
  n_total <- nrow(modified_entries)
  n_symmetric <- sum(grepl("反交$", modified_entries$content))
  cat("✅ 即将填入 ", n_total, " 条组合内容，其中 ", n_symmetric, " 条为反交\n")
  
  if (preview) {
    cat("🔍 当前为预览模式，组合数据未被修改\n")
    flush.console()
    return(modified_entries)
  }
  
  # === 9. 保存数据 ===
  saveRDS(final_df, "data/cross_table.rds")
  cat("💾 组合数据已保存到 data/cross_table.rds\n")
  flush.console()
  
  return(modified_entries)
}



#' 按 content 内容（批次）统计正交组合数量
#'
#' @description
#' 从 cross_table（默认为 RDS 文件）读取数据，对 content（通常为日期）分组统计。
#' 仅统计正交组合（过滤掉“反交”记录）。
#'
#' @param df 可选，含 content 字段的组合数据框；若为 NULL，则从 file 读取
#' @param file 默认读取路径为 "data/cross_table.rds"
#'
#' @return 一个数据框，每行代表一个批次（content 值），包含该批次的组合数量。
#'
#' @export
summarize_cross_batches <- function(df = NULL, file = "data/cross_table.rds") {
  if (is.null(df)) {
    if (!file.exists(file)) {
      stop("❌ 找不到默认组合表文件：", file)
    }
    df <- readRDS(file)
  }
  
  if (!"content" %in% names(df)) {
    stop("❌ 数据框中缺少 content 字段")
  }
  
  df |>
    dplyr::filter(
      !is.na(content),
      nzchar(content),
      !grepl("反交", content)
    ) |>
    dplyr::group_by(content) |>
    dplyr::summarise(
      n_crosses = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::arrange(desc(content))
}



#' 母本使用统计（可选是否包含空内容）
#'
#' @description
#' 对组合数据框按母本名称（`ma_名称`）进行统计，计算每个亲本作为母本参与的组合次数，
#' 并列出对应使用过的父本（`pa_名称`）列表，同时返回该母本对应的其它字段信息（如 ID、蛋白、株高等）。
#'
#' 默认情况下仅统计 `content` 不为空、且不包含“反交”的正交组合。可通过参数控制是否包括空内容。
#'
#' @param df 包含 `ma_名称`, `pa_名称`, `content` 和母本相关信息字段的数据框。
#' @param include_empty_content 合理布尔值，是否包括 `content` 为空或缺失的记录，默认值为 `FALSE`。
#'
#' @return 数据框，每行为一个母本，包含其组合次数、所用父本列表及母本自身信息。
#'
#' @export

summarize_by_mother_name <- function(df, include_empty_content = FALSE) {
  if (!all(c("ma_名称", "pa_名称", "content") %in% names(df))) {
    stop("数据框必须包含 ma_名称、pa_名称 和 content 字段")
  }
  
  valid <- df
  
  if (!include_empty_content) {
    valid <- valid |>
      dplyr::filter(
        !is.na(content),
        nzchar(content),
        !grepl("反交", content)
      )
  }
  
  summary_tbl <- valid |>
    dplyr::group_by(ma_名称) |>
    dplyr::summarise(
      n_combinations = dplyr::n(),
      used_fathers = paste(unique(pa_名称), collapse = ", ")
    )
  
  mother_info <- valid |>
    dplyr::select(dplyr::starts_with("ma_")) |>
    dplyr::distinct(ma_名称, .keep_all = TRUE)
  
  dplyr::left_join(summary_tbl, mother_info, by = "ma_名称") |>
    dplyr::arrange(desc(n_combinations))
}




#' 父本使用统计（可选是否包含空内容）
#'
#' @description
#' 对组合数据框按父本名称（`pa_名称`）进行统计，计算每个亲本作为父本参与的组合次数，
#' 并列出对应使用过的母本（`ma_名称`）列表，同时返回该父本对应的其它字段信息（如 ID、蛋白、株高等）。
#'
#' 默认情况下仅统计 `content` 不为空、且不包含“反交”的正交组合。可通过参数控制是否包括空内容。
#'
#' @param df 包含 `pa_名称`, `ma_名称`, `content` 和父本相关信息字段的数据框。
#' @param include_empty_content 合理布尔值，是否包括 `content` 为空或缺失的记录，默认值为 `FALSE`。
#'
#' @return 数据框，每行为一个父本，包含其组合次数、所用母本列表及父本自身信息。
#'
#' @export

summarize_by_father_name <- function(df, include_empty_content = FALSE) {
  if (!all(c("ma_名称", "pa_名称", "content") %in% names(df))) {
    stop("数据框必须包含 ma_名称、pa_名称 和 content 字段")
  }
  
  valid <- df
  
  if (!include_empty_content) {
    valid <- valid |>
      dplyr::filter(
        !is.na(content),
        nzchar(content),
        !grepl("反交", content)
      )
  }
  
  summary_tbl <- valid |>
    dplyr::group_by(pa_名称) |>
    dplyr::summarise(
      n_combinations = dplyr::n(),
      used_mothers = paste(unique(ma_名称), collapse = ", ")
    )
  
  father_info <- valid |>
    dplyr::select(dplyr::starts_with("pa_")) |>
    dplyr::distinct(pa_名称, .keep_all = TRUE)
  
  dplyr::left_join(summary_tbl, father_info, by = "pa_名称") |>
    dplyr::arrange(desc(n_combinations))
}




#' 合并母本与父本使用统计（默认从 cross_table.rds 读取）
#'
#' @description
#' 合并母本与父本的使用统计，生成亲本综合使用表。默认统计正交组合。
#'
#' @param df 可选，组合数据框；为空时默认读取 "data/cross_table.rds"
#' @param include_empty_content 是否包含 content 为空的组合（默认 FALSE）
#'
#' @return 数据框：每个亲本在母本和父本方向的使用频次、搭配列表，以及本身字段
#'
#' @export
merge_mother_father_stats <- function(df = NULL, include_empty_content = FALSE) {
  # 自动加载数据
  if (is.null(df)) {
    file_path <- "data/cross_table.rds"
    if (!file.exists(file_path)) {
      stop("❌ 未找到组合数据文件：", file_path)
    }
    df <- readRDS(file_path)
    message("📂 已加载组合数据：", file_path)
  }
  
  # 调用依赖函数
  mother_stats <- summarize_by_mother_name(df, include_empty_content) |>
    dplyr::rename(
      parent_name = ma_名称,
      n_as_mother = n_combinations,
      used_fathers = used_fathers
    )
  
  father_stats <- summarize_by_father_name(df, include_empty_content) |>
    dplyr::rename(
      parent_name = pa_名称,
      n_as_father = n_combinations,
      used_mothers = used_mothers
    )
  
  # 合并母本和父本信息
  merged <- dplyr::full_join(mother_stats, father_stats, by = "parent_name")
  
  info_cols <- grep("^ma_|^pa_", names(merged), value = TRUE)
  ma_only <- grep("^ma_", info_cols, value = TRUE)
  pa_only <- grep("^pa_", info_cols, value = TRUE)
  
  # 合并重复信息（优先使用母本字段）
  for (col in sub("^ma_", "", ma_only)) {
    ma_col <- paste0("ma_", col)
    pa_col <- paste0("pa_", col)
    merged[[col]] <- dplyr::coalesce(merged[[ma_col]], merged[[pa_col]])
  }
  
  merged |>
    dplyr::select(-all_of(c(ma_only, pa_only))) |>
    dplyr::relocate(parent_name, n_as_mother, used_fathers, n_as_father, used_mothers)
}


#' 配置杂交组合（支持筛选、自定义 content，避免自交）
#'
#' @param df 组合数据框，包含 pair_id、ma_row、pa_row、ma_名称、pa_名称、content 等字段
#' @param n 欲配置的组合数量
#' @param mother_names 可选，母本名称列表
#' @param father_names 可选，父本名称列表
#' @param mother_filter 可选，母本筛选表达式（如 quote(ma_蛋白 > 12)）
#' @param father_filter 可选，父本筛选表达式（如 quote(pa_株高 < 150)）
#' @param content_value 可选，填写到 content 字段的值（默认使用当前日期）
#'
#' @return 一个列表，包括：
#'   \item{plan}{配置成功的正交组合}
#'   \item{updated_df}{更新后的完整组合表，已修改 content 字段}
#'
#' @export
generate_cross_plan <- function(df,
                                n,
                                mother_names = NULL,
                                father_names = NULL,
                                mother_filter = NULL,
                                father_filter = NULL,
                                content_value = NULL) {
  stopifnot(all(c("pair_id", "ma_名称", "pa_名称", "content") %in% names(df)))
  
  # 强制 content 为字符型，避免 bind_rows 类型冲突
  df$content <- as.character(df$content)
  
  available <- df |>
    dplyr::filter(is.na(content) | !nzchar(content)) |>
    dplyr::filter(ma_名称 != pa_名称)
  
  if (!is.null(mother_names)) {
    available <- available |>
      dplyr::filter(ma_名称 %in% mother_names)
  }
  
  if (!is.null(father_names)) {
    available <- available |>
      dplyr::filter(pa_名称 %in% father_names)
  }
  
  if (!is.null(mother_filter)) {
    available <- available |>
      dplyr::filter(!!mother_filter)
  }
  
  if (!is.null(father_filter)) {
    available <- available |>
      dplyr::filter(!!father_filter)
  }
  
  available <- available |>
    dplyr::distinct(pair_id, .keep_all = TRUE)
  
  if (nrow(available) < n) {
    stop("筛选后的组合数量不足，最多可配置 ", nrow(available), " 条")
  }
  
  selected <- dplyr::slice_sample(available, n = n)
  
  content_str <- if (is.null(content_value)) format(Sys.Date(), "%Y-%m-%d") else content_value
  
  direct <- df |>
    dplyr::inner_join(selected |> dplyr::select(pair_id, ma_row, pa_row),
                      by = c("pair_id", "ma_row", "pa_row")) |>
    dplyr::mutate(content = as.character(content_str))
  
  inverse <- df |>
    dplyr::filter(pair_id %in% selected$pair_id) |>
    dplyr::anti_join(direct |> dplyr::select(pair_id, ma_row, pa_row),
                     by = c("pair_id", "ma_row", "pa_row")) |>
    dplyr::mutate(content = as.character(paste0(content_str, "反交")))
  
  untouched <- df |>
    dplyr::filter(!(pair_id %in% selected$pair_id)) |>
    dplyr::mutate(content = as.character(content))
  
  updated_df <- dplyr::bind_rows(direct, inverse, untouched) |>
    dplyr::arrange(pair_id)
  
  list(
    plan = direct,
    updated_df = updated_df
  )
}


#' 执行杂交计划（读取并写回 RDS 文件）
#'
#' @param file RDS 文件路径，默认为 "data/cross_table.rds"
#' @param n 欲生成的组合数量
#' @param mother_names 可选，母本名称向量
#' @param father_names 可选，父本名称向量
#' @param mother_filter 可选，母本条件表达式（如 quote(ma_蛋白 > 12)）
#' @param father_filter 可选，父本条件表达式（如 quote(pa_株高 < 150)）
#' @param content_value 可选，写入 content 字段的值（默认当前日期）
#'
#' @return 一个列表：plan（正交组合），updated_df（已更新内容的表）
#'
#' @export
run_cross_plan <- function(n,
                           mother_names = NULL,
                           father_names = NULL,
                           mother_filter = NULL,
                           father_filter = NULL,
                           content_value = NULL,
                           file = "data/cross_table.rds") {
  
  if (!file.exists(file)) {
    stop("❌ 文件不存在：", file)
  }
  
  # Step 1: 读取 .rds 组合表
  df <- readRDS(file)
  
  result <- generate_cross_plan(
    df = df,
    n = n,
    mother_names = mother_names,
    father_names = father_names,
    mother_filter = mother_filter,
    father_filter = father_filter,
    content_value = content_value
  )
  
  # Step 3: 保存更新后的表
  saveRDS(result$updated_df, file)
  
  # Step 4: 控制台提示
  message(glue::glue("✅ 已成功生成 {n} 条组合，结果已存入：'{file}'"))
  
  #Step 5:写入日志
  # 生成日志记录
  log_entry <- data.frame(
    timestamp     = Sys.time(),
    n             = n,
    mother_count  = length(mother_names),
    father_count  = length(father_names),
    mother_names  = paste(mother_names, collapse = "/"),
    father_names  = paste(father_names, collapse = "/"),
    content_value = content_value,
    stringsAsFactors = FALSE
  )
  
  # 写入日志 CSV
  log_dir <- "logs"
  log_file <- file.path(log_dir, "cross_plan_log.csv")
  
  if (!dir.exists(log_dir)) {
    dir.create(log_dir)
  }
  
  if (!file.exists(log_file)) {
    readr::write_csv(log_entry, log_file)
  } else {
    readr::write_csv(log_entry, log_file, append = TRUE)
  }
  message(glue::glue("✅ 所用亲本已存入：'{log_file}'"))
 
  return(result$plan)
}





#' 提取指定批次的杂交组合记录
#'
#' @description
#' 根据指定 content 值（如批次名称、日期等），提取组合数据中对应的杂交组合记录。
#' 若未提供数据框，则默认读取 "data/cross_table.rds"。
#'
#' @param batches 字符向量，指定一个或多个 content 值（如 "2025第二批"）。
#' @param df 可选，组合数据框，需包含 content 字段。
#' @param file 字符串，默认路径为 "data/cross_table.rds"。
#'
#' @return 一个数据框，仅包含属于指定批次的组合记录。
#'
#' @export
filter_cross_by_batches <- function(
    batches,
    df = NULL,
    file = "data/cross_table.rds"
) {
  if (is.null(df)) {
    if (!file.exists(file)) {
      stop("❌ 文件不存在：", file)
    }
    df <- readRDS(file)
  }
  
  if (!"content" %in% names(df)) {
    stop("❌ 数据框中缺少 content 字段")
  }
  
  df |>
    dplyr::filter(
      !is.na(content),
      nzchar(content),
      content %in% batches
    ) |>
    dplyr::arrange(content, pair_id)
}



#' 清除指定批次的 content 内容（含反交，维护 cross_table.rds）
#'
#' @description
#' 删除指定 content 批次的正交记录，并同步清除对应反交组合的 content 字段。
#' 支持预览模式和详细日志输出，默认操作 data/cross_table.rds。
#'
#' @param batches 字符向量，指定要清除的 content 批次名（如 "2025-06-06"）
#' @param df 可选，手动传入组合数据框，默认从 file 加载
#' @param file RDS 文件路径，默认 "data/cross_table.rds"
#' @param preview 逻辑值，仅查看将清除的内容，默认 FALSE
#' @param verbose 逻辑值，是否输出详细日志，默认 FALSE
#'
#' @return
#' - 如果 preview = TRUE：返回将被清除的记录（正交 + 反交）
#' - 如果 preview = FALSE：返回已更新的数据框，并写回 RDS 文件
#'
#' @export
clear_cross_batches <- function(batches,
                                df = NULL,
                                file = "data/cross_table.rds",
                                preview = FALSE,
                                verbose = FALSE) {
  required_cols <- c("pair_id", "ma_row", "pa_row", "content")
  
  # 加载数据
  if (is.null(df)) {
    if (!file.exists(file)) stop("❌ 文件不存在：", file)
    df <- readRDS(file)
  }
  
  if (!all(required_cols %in% names(df))) {
    stop("❌ 缺少字段：", paste(setdiff(required_cols, names(df)), collapse = ", "))
  }
  
  df <- df |>
    dplyr::mutate(
      ma_row = as.integer(ma_row),
      pa_row = as.integer(pa_row)
    )
  
  to_clear <- df |>
    dplyr::filter(content %in% batches & !grepl("反交", content)) |>
    dplyr::select(pair_id, ma_row, pa_row)
  
  if (nrow(to_clear) == 0) {
    message("⚠️ 未找到匹配的正交记录：", paste(batches, collapse = ", "))
    return(dplyr::tibble())
  }
  
  flipped <- to_clear |>
    dplyr::rename(pa_row = ma_row, ma_row = pa_row)
  
  full_keys <- dplyr::bind_rows(to_clear, flipped)
  
  if (preview) {
    message("👁 预览模式：将清除以下记录内容：")
    return(
      df |> dplyr::semi_join(full_keys, by = c("pair_id", "ma_row", "pa_row"))
    )
  }
  
  n_forward <- nrow(to_clear)
  n_total <- nrow(full_keys)
  
  cat("⚠️ 即将清除 content 字段：\n")
  cat("📦 批次名：", paste(batches, collapse = ", "), "\n")
  cat("🧬 正交记录数：", n_forward, "\n")
  cat("🔁 包含反交记录数：", n_total - n_forward, "\n")
  cat("🧹 总记录数：", n_total, "\n")
  
  ans <- readline("🔐 是否确认清除以上记录？(y/n): ")
  if (tolower(ans) != "y") {
    message("❌ 操作已取消，未做任何更改。")
    return("未操作!")
  }
  
  # ✅ 正确标记 + 安全替换
  df <- df |>
    dplyr::left_join(
      full_keys |> dplyr::mutate(flag_to_clear = TRUE),
      by = c("pair_id", "ma_row", "pa_row")
    ) |>
    dplyr::mutate(
      content = ifelse(flag_to_clear %in% TRUE, NA_character_, content)
    ) |>
    dplyr::select(-flag_to_clear)
  
  saveRDS(df, file)
  
  if (verbose) {
    message("🧹 已清除批次：", paste(batches, collapse = ", "))
    message("✔️ 正交记录：", n_forward)
    message("✔️ 总清除数（含反交）：", n_total)
    message("💾 数据已写入：", file)
  }
  
  return("成功删除!")
}



#' 从组合数据生成杂交矩阵，输出为 RDS（正交）和 Excel（含反交）
#'
#' @description
#' 从组合表（默认 data/cross_table.rds）中提取杂交信息，生成 n×n 矩阵。
#' 输出两个文件：一个为 .rds（仅正交），一个为 .xlsx（含正交和反交）。
#'
#' @param df 可选，组合数据框，若为 NULL 则从 file_rds 加载
#' @param file_rds RDS 输入文件路径（默认 "data/cross_table.rds"）
#' @param file_rds_out RDS 输出文件路径（正交内容，默认 "output/cross_matrix.rds"）
#' @param file_excel_out Excel 输出文件路径（含反交内容，默认 "output/cross_matrix.xlsx"）
#' @param n 可选，矩阵维度，默认自动推断
#'
#' @return 一个列表，含两个矩阵：rds_matrix 和 excel_matrix
#'
#' @export
export_cross_matrix <- function(df = NULL,
                                file_rds = "data/cross_table.rds",
                                file_rds_out = "data/cross_matrix.rds",
                                file_excel_out = "data/cross_matrix_reverse.xlsx",
                                n = NULL) {
  
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("请先安装 openxlsx 包：install.packages('openxlsx')")
  }
  
  required_cols <- c("ma_row", "pa_row", "content", "ma_名称", "pa_名称")
  
  # 加载数据
  if (is.null(df)) {
    if (!file.exists(file_rds)) {
      stop("❌ 找不到组合数据文件：", file_rds)
    }
    df <- readRDS(file_rds)
  }
  
  # 检查字段
  if (!all(required_cols %in% names(df))) {
    stop("❌ 缺少必要字段：", paste(required_cols, collapse = ", "))
  }
  
  # 过滤出正交记录（用于 RDS）
  valid <- df |>
    dplyr::filter(!is.na(content), nzchar(content), !grepl("反交", content))
  
  if (nrow(valid) == 0) {
    stop("⚠️ 没有正交记录可用于生成矩阵")
  }
  
  if (is.null(n)) {
    n <- max(c(df$ma_row, df$pa_row), na.rm = TRUE)
  }
  
  # 提取行列名
  row_names <- df |>
    dplyr::distinct(ma_row, ma_名称) |>
    dplyr::arrange(ma_row) |>
    dplyr::pull(ma_名称)
  
  col_names <- df |>
    dplyr::distinct(pa_row, pa_名称) |>
    dplyr::arrange(pa_row) |>
    dplyr::pull(pa_名称)
  
  # 初始化两个矩阵
  mat_rds <- matrix(NA_character_, nrow = n, ncol = n)
  mat_excel <- matrix(NA_character_, nrow = n, ncol = n)
  
  # 填入内容
  for (i in seq_len(nrow(valid))) {
    r <- valid$ma_row[i]
    c <- valid$pa_row[i]
    val <- valid$content[i]
    mat_rds[r, c] <- val
    mat_excel[r, c] <- val
    mat_excel[c, r] <- paste0(val, "反交")
  }
  
  # 设置维度名
  rownames(mat_rds) <- row_names
  colnames(mat_rds) <- col_names
  
  rownames(mat_excel) <- row_names
  colnames(mat_excel) <- col_names
  
  # 创建目录
  dir.create(dirname(file_rds_out), showWarnings = FALSE, recursive = TRUE)
  dir.create(dirname(file_excel_out), showWarnings = FALSE, recursive = TRUE)
  
  # 写出 RDS（正交内容）
  saveRDS(mat_rds, file_rds_out)
  
  # 写出 Excel（正 + 反交）
  openxlsx::write.xlsx(
    list(cross_matrix = as.data.frame(mat_excel)),
    file = file_excel_out,
    rowNames = TRUE
  )
  
  # 信息输出
  message("✅ 正交矩阵已保存为 RDS：", file_rds_out)
  message("✅ 全矩阵（含反交）已保存为 Excel：", file_excel_out)
  
  invisible(list(
    rds_matrix = mat_rds,
    excel_matrix = mat_excel
  ))
}



#' 初始化规范化亲本主表（自动生成 id，保存为 RDS 文件）
#'
#' @description
#' 将原始亲本数据标准化，添加唯一 ID 字段，保存为 R 数据文件（.rds）。
#'
#' @param df 原始亲本信息数据框，至少包含“名称”字段
#' @param file_path 输出路径，默认 "data/parent_table.rds"
#'
#' @return 返回标准化的数据框，并保存为 RDS 文件
#' @export
initialize_parent_table <- function(df,
                                    file_path = "data/parent_table.rds") {
  if (!"名称" %in% names(df)) {
    stop("❌ 原始数据必须包含字段：'名称'")
  }
  
  # 添加 ID 字段（自动编号）
  df_clean <- df |>
    dplyr::mutate(
      id = sprintf("P%04d", dplyr::row_number())
    ) |>
    dplyr::relocate(id, .before = 1)
  
  # 创建目录（如不存在）
  dir_path <- dirname(file_path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
  
  # 保存为 RDS
  saveRDS(df_clean, file = file_path)
  
  message("✅ 亲本主表已初始化并保存为 RDS：", file_path)
  return(df_clean)
}


#' 添加亲本记录（自动编号，仅保留 id，去除 parent_id、created_at 等）
#'
#' @param new_data 数据框，至少包含“名称”字段
#' @param file_path Excel 文件路径（默认 "data/parent_table.xlsx"）
#'
#' @return 更新后的数据框（含新增记录）
#' @export
# add_parent_record <- function(new_data,
#                               file_path = "data/parent_table.xlsx") {
#   if (!requireNamespace("openxlsx", quietly = TRUE)) {
#     stop("请先安装 openxlsx：install.packages('openxlsx')")
#   }
#   
#   if (!"名称" %in% names(new_data)) {
#     stop("❌ 数据必须包含“名称”字段")
#   }
#   
#   # 加载或初始化原数据
#   if (file.exists(file_path)) {
#     old_df <- openxlsx::read.xlsx(file_path)
#   } else {
#     dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
#     old_df <- data.frame(id = character(), 名称 = character(), stringsAsFactors = FALSE)
#   }
#   
#   if (!"id" %in% names(old_df)) {
#     old_df$id <- NA_character_
#   }
#   
#   # 去除已存在的名称
#   existed_names <- old_df$名称
#   to_add <- new_data |>
#     dplyr::filter(!(名称 %in% existed_names)) |>
#     dplyr::distinct(名称, .keep_all = TRUE)
#   
#   skipped <- new_data$名称[new_data$名称 %in% existed_names]
#   
#   if (nrow(to_add) == 0) {
#     message("❌ 所有名称已存在，未添加任何记录。")
#     return(invisible(old_df))
#   }
#   
#   # 自动编号
#   last_id <- suppressWarnings(max(as.integer(sub("^P", "", old_df$id)), na.rm = TRUE))
#   if (!is.finite(last_id)) last_id <- 0
#   new_ids <- sprintf("P%04d", (last_id + 1):(last_id + nrow(to_add)))
#   
#   # 添加 id 并整理列顺序
#   new_clean <- to_add |>
#     dplyr::mutate(id = new_ids) |>
#     dplyr::relocate(id, .before = 1)
#   
#   updated <- dplyr::bind_rows(old_df, new_clean)
#   
#   # 写入 Excel
#   wb <- openxlsx::createWorkbook()
#   openxlsx::addWorksheet(wb, "patent_table")
#   openxlsx::writeData(wb, "patent_table", updated)
#   openxlsx::setColWidths(wb, "patent_table", cols = 1:ncol(updated), widths = "auto")
#   openxlsx::saveWorkbook(wb, file_path, overwrite = TRUE)
#   
#   # 提示信息
#   message("✅ 成功添加 ", nrow(new_clean), " 条记录：", paste(new_clean$名称, collapse = ", "))
#   if (length(skipped) > 0) {
#     message("⚠️ 跳过已存在：", paste(skipped, collapse = ", "))
#   }
#   
#   invisible(updated)
# }


#' 添加亲本并增量更新组合杂交记录
#'
#' @param new_data 包含“名称”字段的数据框。
#' @param parent_file 亲本主表 RDS 路径（默认："data/parent_table.rds"）。
#' @param cross_file 杂交组合表 RDS 路径（默认："data/cross_table.rds"）。
#'
#' @return 返回更新后的组合数据框。
#' @export
add_parent_and_append_cross <- function(new_data,
                                        parent_file = "data/parent_table.rds",
                                        cross_file = "data/cross_table.rds") {
  if (!"名称" %in% names(new_data)) {
    stop("❌ new_data 必须包含“名称”字段。")
  }
  
  # 原始亲本数据
  if (file.exists(parent_file)) {
    old_parents <- readRDS(parent_file)
  } else {
    old_parents <- data.frame(id = character(), 名称 = character(), stringsAsFactors = FALSE)
  }
  
  # 找出需要添加的新亲本
  to_add <- new_data |>
    dplyr::filter(!(名称 %in% old_parents$名称)) |>
    dplyr::distinct(名称, .keep_all = TRUE)
  
  if (nrow(to_add) == 0) {
    message("⚠️ 没有新的亲本需要添加。")
    return(invisible(NULL))
  }
  
  # 自动编号
  last_id <- suppressWarnings(max(as.integer(sub("^P", "", old_parents$id)), na.rm = TRUE))
  if (!is.finite(last_id)) last_id <- 0
  new_ids <- sprintf("P%04d", (last_id + 1):(last_id + nrow(to_add)))
  
  to_add <- to_add |>
    dplyr::mutate(id = new_ids) |>
    dplyr::relocate(id, .before = 1)
  
  # 合并并保存 parent_table.rds
  updated_parents <- dplyr::bind_rows(old_parents, to_add)
  dir.create(dirname(parent_file), recursive = TRUE, showWarnings = FALSE)
  saveRDS(updated_parents, parent_file)
  
  message("✅ 已添加亲本：", paste(to_add$名称, collapse = ", "))
  message("📦 parent_table.rds 已更新。")
  
  # 加载现有组合表
  if (file.exists(cross_file)) {
    old_cross <- readRDS(cross_file)
    old_cross <- old_cross |> dplyr::mutate(pair_id = as.character(pair_id))
    existing_pair_ids <- old_cross$pair_id
  } else {
    old_cross <- NULL
    existing_pair_ids <- character()
  }
  
  # 计算新组合（不含自交）
  new_indexes <- which(updated_parents$id %in% to_add$id)
  all_indexes <- seq_len(nrow(updated_parents))
  
  new_pairs <- tidyr::expand_grid(ma_row = new_indexes, pa_row = all_indexes) |>
    dplyr::filter(ma_row != pa_row)
  
  # 添加反交组合
  full_pairs <- dplyr::bind_rows(
    new_pairs,
    new_pairs |> dplyr::rename(ma_row = pa_row, pa_row = ma_row)
  ) |>
    dplyr::mutate(pair_id = paste0(
      pmin(updated_parents$id[ma_row], updated_parents$id[pa_row]),
      "_",
      pmax(updated_parents$id[ma_row], updated_parents$id[pa_row])
    ))
  
  # 去掉已存在的组合
  new_pairs_filtered <- full_pairs |>
    dplyr::filter(!(pair_id %in% existing_pair_ids))
  
  if (nrow(new_pairs_filtered) == 0) {
    message("⚠️ 没有需要添加的新组合。")
    return(invisible(old_cross))
  }
  
  # 构建新组合记录
  new_cross <- purrr::pmap_dfr(
    new_pairs_filtered,
    function(ma_row, pa_row, pair_id) {
      ma <- updated_parents[ma_row, , drop = FALSE]
      pa <- updated_parents[pa_row, , drop = FALSE]
      
      names(ma) <- paste0("ma_", names(ma))
      names(pa) <- paste0("pa_", names(pa))
      
      dplyr::bind_cols(
        pair_id = pair_id,
        ma_row = ma_row,
        pa_row = pa_row,
        content = NA_character_,
        ma,
        pa
      )
    }
  )
  
  # 合并并保存组合记录
  updated_cross <- dplyr::bind_rows(old_cross, new_cross) |>
    dplyr::arrange(pair_id)
  
  dir.create(dirname(cross_file), recursive = TRUE, showWarnings = FALSE)
  saveRDS(updated_cross, cross_file)
  
  message("🔗 新增组合 ", nrow(new_cross), " 条，cross_table.rds 已更新。")
  
  invisible(updated_cross)
}


#' 删除最后 N 个亲本，并同步更新组合表
#'
#' @param n 删除的亲本个数（只能从尾部删除）。
#' @param parent_file RDS 亲本主表路径（默认："data/parent_table.rds"）。
#' @param cross_file RDS 组合记录路径（默认："data/cross_table.rds"）。
#'
#' @return 更新后的 parent_table。
#' @export
remove_last_parents <- function(n,
                                parent_file = "data/parent_table.rds",
                                cross_file = "data/cross_table.rds") {
  if (!file.exists(parent_file)) {
    stop("❌ 未找到 parent_table.rds 文件。")
  }
  
  parent_df <- readRDS(parent_file)
  
  if (n <= 0 || n > nrow(parent_df)) {
    stop("❌ 删除个数无效，必须在 1 和现有记录数之间。")
  }
  
  # 找出要删除的亲本（按 id 排序）
  parent_df <- parent_df |>
    dplyr::arrange(as.integer(sub("^P", "", id)))
  
  to_remove <- tail(parent_df, n)
  
  cat("⚠️ 即将删除以下亲本记录：\n")
  print(to_remove[, c("id", "名称")])
  answer <- readline(prompt = "❓ 确认删除？请输入 yes 继续：")
  
  if (tolower(answer) != "yes") {
    message("❌ 已取消删除操作。")
    return(invisible(parent_df))
  }
  
  # 保留其余亲本
  remaining_df <- head(parent_df, -n)
  
  # 保存更新后的 parent_table
  saveRDS(remaining_df, parent_file)
  message("✅ 已删除 ", n, " 条亲本记录，并更新 parent_table.rds")
  
  # 同步更新组合表
  if (file.exists(cross_file)) {
    cross_df <- readRDS(cross_file)
    
    # 强制 id 类型一致
    removed_ids <- to_remove$id
    
    updated_cross <- cross_df |>
      dplyr::filter(
        !(ma_id %in% removed_ids | pa_id %in% removed_ids)
      )
    
    saveRDS(updated_cross, cross_file)
    
    message("🔗 已从 cross_table.rds 中删除相关组合：",
            nrow(cross_df) - nrow(updated_cross), " 条记录")
  } else {
    message("📂 未找到组合表 cross_table.rds，无需同步更新。")
  }
  
  invisible(remaining_df)
}



#' 将自然语言解析为 run_cross_plan 参数并执行
#'
#' @param command 中文指令（如“配置10个杂交组合，母本为郑1307，父本蛋白大于13，不包含转基因”）
#'
#' @return run_cross_plan 返回值
#'
#' @export
#' 
#' 中文句式	映射参数
# “配置10个杂交组合”	n = 10
# “母本为A、B、C”	mother_names = c(...)
# “父本为...”	father_names = ...
# “排除亲本为...”	exclude_names = ...
# “母本蛋白大于13”	mother_filter = ...
# “父本株高小于150”	father_filter = ...
# “母本来源为河南”	mother_filter = ...
# “不包含转基因”	*_filter != '是'
#' 将自然语言解析为 run_cross_plan 的参数并调用
#'
#' 支持组合数、母本/父本/排除列表、蛋白、株高、来源、转基因条件
#'
#' @param command 中文自然语言指令
#'
#' @return run_cross_plan 返回结果
#'
#' @export
parse_cross_command <- function(command) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("请安装 stringr 包")
  if (!requireNamespace("rlang", quietly = TRUE)) stop("请安装 rlang 包")
  
  args <- list()
  
  # 提取组合数量
  n_match <- stringr::str_match(command, "配置(\\d+)个?杂交组合")
  if (!is.na(n_match[1, 2])) {
    args$n <- as.integer(n_match[1, 2])
  } else {
    stop("❌ 无法识别组合数量，请使用“配置10个杂交组合”格式")
  }
  
  # 抽取列表字段
  extract_names <- function(pattern) {
    name_str <- stringr::str_match(command, pattern)[, 2]
    if (!is.na(name_str)) {
      stringr::str_split(name_str, "[、,，\\s]+")[[1]] |> stringr::str_trim()
    } else {
      NULL
    }
  }
  
  args$mother_names <- extract_names("母本为([\\p{Han}A-Za-z0-9、,，\\s]+)")
  args$father_names <- extract_names("父本为([\\p{Han}A-Za-z0-9、,，\\s]+)")
  args$exclude_names <- extract_names("排除亲本为([\\p{Han}A-Za-z0-9、,，\\s]+)")
  
  # 构造逻辑表达式
  build_filter <- function(target, field, logic, value) {
    rlang::parse_expr(paste0(target, "_", field, " ", logic, " ", value))
  }
  
  combine_filters <- function(existing, new_expr) {
    if (is.null(existing)) return(new_expr)
    rlang::parse_expr(paste0("(", rlang::expr_text(existing), ") & (", rlang::expr_text(new_expr), ")"))
  }
  
  # === 母本条件 ===
  mother_exprs <- list()
  
  if (grepl("母本蛋白大于(\\d+\\.*\\d*)", command)) {
    val <- as.numeric(stringr::str_match(command, "母本蛋白大于(\\d+\\.*\\d*)")[, 2])
    mother_exprs <- c(mother_exprs, build_filter("ma", "蛋白", ">", val))
  }
  
  if (grepl("母本株高小于(\\d+)", command)) {
    val <- as.numeric(stringr::str_match(command, "母本株高小于(\\d+)")[, 2])
    mother_exprs <- c(mother_exprs, build_filter("ma", "株高", "<", val))
  }
  
  if (grepl("母本来源为([\\p{Han}A-Za-z0-9]+)", command)) {
    region <- stringr::str_match(command, "母本来源为([\\p{Han}A-Za-z0-9]+)")[, 2]
    mother_exprs <- c(mother_exprs, rlang::parse_expr(paste0("ma_来源 == '", region, "'")))
  }
  
  # ✅ 母本转基因条件
  if (grepl("母本.*不包含转基因", command)) {
    mother_exprs <- c(mother_exprs, rlang::parse_expr("ma_转基因 != '是'"))
  } else if (grepl("母本.*包含转基因", command)) {
    mother_exprs <- c(mother_exprs, rlang::parse_expr("ma_转基因 == '是'"))
  } else if (grepl("不包含转基因", command)) {
    mother_exprs <- c(mother_exprs, rlang::parse_expr("ma_转基因 != '是'"))
  }
  
  if (length(mother_exprs) > 0) {
    args$mother_filter <- Reduce(combine_filters, mother_exprs)
  }
  
  # === 父本条件 ===
  father_exprs <- list()
  
  if (grepl("父本蛋白大于(\\d+\\.*\\d*)", command)) {
    val <- as.numeric(stringr::str_match(command, "父本蛋白大于(\\d+\\.*\\d*)")[, 2])
    father_exprs <- c(father_exprs, build_filter("pa", "蛋白", ">", val))
  }
  
  if (grepl("父本株高小于(\\d+)", command)) {
    val <- as.numeric(stringr::str_match(command, "父本株高小于(\\d+)")[, 2])
    father_exprs <- c(father_exprs, build_filter("pa", "株高", "<", val))
  }
  
  if (grepl("父本来源为([\\p{Han}A-Za-z0-9]+)", command)) {
    region <- stringr::str_match(command, "父本来源为([\\p{Han}A-Za-z0-9]+)")[, 2]
    father_exprs <- c(father_exprs, rlang::parse_expr(paste0("pa_来源 == '", region, "'")))
  }
  
  # ✅ 父本转基因条件
  if (grepl("父本.*不包含转基因", command)) {
    father_exprs <- c(father_exprs, rlang::parse_expr("pa_转基因 != '是'"))
  } else if (grepl("父本.*包含转基因", command)) {
    father_exprs <- c(father_exprs, rlang::parse_expr("pa_转基因 == '是'"))
  } else if (grepl("不包含转基因", command)) {
    father_exprs <- c(father_exprs, rlang::parse_expr("pa_转基因 != '是'"))
  }
  
  if (length(father_exprs) > 0) {
    args$father_filter <- Reduce(combine_filters, father_exprs)
  }
  
  # ✅ 输出解析结果
  cat("📋 解析参数如下：\n")
  print(args)
  
  # ✅ 调用主函数
  result <- do.call(run_cross_plan, args)
  return(result)
}



# Step 2: 调用生成函数



#' 编辑并维护 parent_table.rds（含备份与日志）
#'
#' @param file 路径，默认 "data/parent_table.rds"
#' @param backup_dir 自动备份目录，默认 "backup"
#' @param log_file 可选，变更日志 CSV 文件路径，默认 "logs/parent_edit_log.csv"
#'
#' @return 修改后的数据框（invisible 返回）
#' @importFrom digest digest
#' @importFrom dplyr bind_rows
#' @export
edit_parent_table <- function(file = "data/parent_table.rds",
                              backup_dir = "backup",
                              log_file = "logs/parent_edit_log.csv") {
  # 检查依赖包
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("请先安装 digest 包: install.packages('digest')")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("请先安装 dplyr 包: install.packages('dplyr')")
  }
  
  if (!file.exists(file)) {
    stop("❌ 未找到文件：", file)
  }
  
  # 创建备份目录
  if (!dir.exists(backup_dir)) {
    dir.create(backup_dir, recursive = TRUE)
  }
  
  # 创建日志目录
  log_dir <- dirname(log_file)
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }
  
  old_df <- readRDS(file)
  old_df$content_hash <- digest::digest(old_df)  # 用于日志校验
  
  # 自动备份
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_path <- file.path(backup_dir, paste0("parent_table_", timestamp, ".rds"))
  saveRDS(old_df, backup_path)
  message("📦 已备份原始文件至：", backup_path)
  
  # 打开编辑界面
  message("✏️ 请编辑数据表，完成后关闭窗口")
  new_df <- utils::edit(old_df[, setdiff(names(old_df), "content_hash")])
  
  if (identical(old_df[, names(new_df)], new_df)) {
    message("✅ 没有修改，原文件保持不变。")
    return(invisible(new_df))
  }
  
  # 字段类型检查（可根据需求扩展）
  numeric_fields <- c("蛋白", "脂肪", "株高")
  for (field in numeric_fields) {
    ma_col <- paste0("ma_", field)
    if (ma_col %in% names(new_df) && !is.numeric(new_df[[ma_col]])) {
      warning("⚠️ 字段 ", ma_col, " 应为数值型，但检测到类型为：", class(new_df[[ma_col]]))
    }
  }
  
  # 保存文件
  saveRDS(new_df, file)
  message("✅ 修改已保存：", file)
  
  # 变更汇总
  n_old <- nrow(old_df)
  n_new <- nrow(new_df)
  # 这里用 dplyr::bind_rows 来安全合并记录，避免重复计数错误
  combined <- dplyr::bind_rows(old_df, new_df)
  n_changed <- nrow(unique(combined)) - n_old
  
  message("📌 记录数量：", n_old, " → ", n_new)
  message("📌 改动记录数：", n_changed)
  
  # 写入日志
  log_entry <- data.frame(
    time = Sys.time(),
    old_n = n_old,
    new_n = n_new,
    changed_n = n_changed,
    file = normalizePath(file),
    backup = normalizePath(backup_path),
    stringsAsFactors = FALSE
  )
  
  if (file.exists(log_file)) {
    logs <- read.csv(log_file, stringsAsFactors = FALSE)
    logs <- dplyr::bind_rows(logs, log_entry)
  } else {
    logs <- log_entry
  }
  write.csv(logs, log_file, row.names = FALSE)
  message("📝 修改日志已记录到：", log_file)
  
  invisible(new_df)
}


#' 使用 Excel 方式编辑并更新 parent_table（导出 + 导入）
#'
#' @param mode 模式："export"（默认）或 "import"
#' @param rds_path RDS 文件路径，默认 "data/parent_table.rds"
#' @param xlsx_path Excel 文件路径，默认 "parent_table_edit.xlsx"
#' @param backup 是否备份旧版本，默认 TRUE（import 时有效）
#'
#' @export
edit_parent_table_via_excel <- function(mode = c("export", "import"),
                                        rds_path = "data/parent_table.rds",
                                        xlsx_path = "data/parent_table_edit.xlsx",
                                        backup = TRUE) {
  mode <- match.arg(mode)

  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("请先安装 openxlsx：install.packages('openxlsx')")
  }

  if (mode == "export") {
    # ---- 导出 Excel ----
    if (!file.exists(rds_path)) {
      stop("❌ 找不到亲本表文件：", rds_path)
    }

    df <- readRDS(rds_path)
    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "parent_table")
    openxlsx::writeData(wb, "parent_table", df)
    openxlsx::setColWidths(wb, "parent_table", cols = 1:ncol(df), widths = "auto")
    openxlsx::addStyle(wb, "parent_table",
                       openxlsx::createStyle(textDecoration = "bold", fgFill = "#DDEBF7"),
                       rows = 1, cols = 1:ncol(df), gridExpand = TRUE)
    openxlsx::saveWorkbook(wb, xlsx_path, overwrite = TRUE)
    message("✅ 已导出 Excel：", xlsx_path)
    return(invisible(xlsx_path))

  } else {
    # ---- 导入 Excel 并保存 ----
    if (!file.exists(xlsx_path)) {
      stop("❌ 找不到 Excel 文件：", xlsx_path)
    }

    new_df <- openxlsx::read.xlsx(xlsx_path)

    # 字段类型检查（可扩展）
    numeric_fields <- c("ma_蛋白", "ma_脂肪", "ma_株高")
    for (col in numeric_fields) {
      if (col %in% names(new_df) && !is.numeric(new_df[[col]])) {
        warning("⚠️ 字段 ", col, " 应为数值型，但当前为 ", class(new_df[[col]]))
      }
    }

    # 自动备份
    if (file.exists(rds_path) && backup) {
      ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
      backup_file <- sub("\\.rds$", paste0("_backup_", ts, ".rds"), rds_path)
      file.copy(rds_path, backup_file)
      message("📦 已备份原文件至：", backup_file)
    }

    saveRDS(new_df, rds_path)
    message("✅ 修改已导入并保存至：", rds_path)
    return(invisible(new_df))
  }
}


#' 按条件筛选亲本，返回名称向量
#'
#' @param ... 直接写 filter 的条件表达式，如 特点 == "高蛋白" & 蛋白 > 45
#' @param file 亲本主表RDS路径，默认"data/parent_table.rds"
#' @return 满足条件的亲本名称向量
#' @export
select_parent <- function(..., file = "data/parent_table.rds") {
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("请先安装dplyr包")
  if (!file.exists(file)) stop("❌ 未找到文件：", file)
  df <- readRDS(file)
  if (!"名称" %in% names(df)) stop("❌ 数据中缺少'名称'字段")
  res <- dplyr::filter(df, ...)
  return(res$名称)
}
