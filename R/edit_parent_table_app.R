#' 启动亲本表 Excel 风格编辑器（基于 rhandsontable）
#'
#' @param file RDS 文件路径，默认 "data/parent_table.rds"
#' @export
edit_parent_table_app <- function(file = "data/parent_table.rds") {
  if (!file.exists(file)) {
    stop("❌ 找不到文件：", file)
  }
  
  library(shiny)
  library(rhandsontable)
  library(shinyjs)
  
  df <- readRDS(file)
  
  ui <- fluidPage(
    useShinyjs(),  # 启用 shinyjs
    titlePanel("📋 亲本表编辑器"),
    tags$style(HTML("
      body { 
        font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; 
        background-color: #f8f9fa;
      }
      .btn-success { background-color: #28a745; color: white; }
      .btn-danger { background-color: #dc3545; color: white; }
      .btn-secondary { background-color: #6c757d; color: white; }
      .well { background-color: #e9ecef; }
    ")),
    
    rHandsontableOutput("parent_table", height = "300px"),
    br(),
    
    fluidRow(
      column(12, 
             actionButton("save", "💾 保存修改", class = "btn-success"),
             actionButton("cancel", "❌ 退出不保存", class = "btn-danger")
      )
    ),
    br(),
    verbatimTextOutput("status")
  )
  
  server <- function(input, output, session) {
    values <- reactiveValues(data = df, dirty = FALSE)
    
    # 渲染可编辑表格
    output$parent_table <- renderRHandsontable({
      rhandsontable(
        values$data,
        useTypes = TRUE,
        stretchH = "all",
        rowHeaders = TRUE,
        colHeaders = colnames(values$data),
        contextMenu = TRUE
      ) %>%
        hot_table(highlightCol = TRUE, highlightRow = TRUE) %>%
        hot_cols(manualColumnResize = TRUE) %>%
        hot_rows(rowHeights = 30)
    })
    
    # 监听表格变化
    observeEvent(input$parent_table, {
      values$data <- hot_to_r(input$parent_table)
      values$dirty <- TRUE
    })
    
    # 保存数据
    observeEvent(input$save, {
      tryCatch({
        # 创建备份目录（如果不存在）
        backup_dir <- "data/backups/"
        if (!dir.exists(backup_dir)) dir.create(backup_dir, recursive = TRUE)
        
        # 创建带时间戳的备份
        timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
        backup_file <- file.path(backup_dir, paste0("parent_table_backup_", timestamp, ".rds"))
        saveRDS(readRDS(file), backup_file)  # 备份原始数据
        
        # 保存当前数据
        saveRDS(values$data, file)
        
        values$dirty <- FALSE
        output$status <- renderText({
          paste0("✅ 修改已保存：", file, "\n📦 备份文件：", backup_file)
        })
        
        # 显示成功消息5秒
        showNotification("数据保存成功！", type = "message", duration = 3)
        
      }, error = function(e) {
        output$status <- renderText({
          paste0("❌ 保存失败：", e$message)
        })
      })
    })
    
    # 退出应用
    observeEvent(input$cancel, {
      # 使用 isolate() 安全访问反应式值
      if (isolate(values$dirty)) {
        showModal(modalDialog(
          title = "未保存的修改",
          "您有未保存的修改，确定要退出吗？",
          footer = tagList(
            modalButton("取消"),
            actionButton("confirm_cancel", "退出不保存", class = "btn-danger")
          )
        ))
      } else {
        stopApp()
      }
    })
    
    # 确认退出
    observeEvent(input$confirm_cancel, {
      removeModal()
      stopApp()
    })
    
    # 退出时提醒保存 - 使用 isolate() 安全访问
    session$onSessionEnded(function() {
      if (isolate(values$dirty)) {
        showNotification("警告：有未保存的修改！", type = "warning")
      }
    })
  }
  
  runGadget(shinyApp(ui, server), viewer = dialogViewer("亲本表编辑器", width = 1000, height = 800))
}


