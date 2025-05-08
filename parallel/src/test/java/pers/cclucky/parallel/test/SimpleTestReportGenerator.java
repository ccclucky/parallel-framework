package pers.cclucky.parallel.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 简化版测试报告生成器
 * 生成Markdown格式的测试报告
 */
public class SimpleTestReportGenerator {
    
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private final String reportPath;
    private final StringBuilder reportContent = new StringBuilder();
    private final List<String[]> currentTable = new ArrayList<>();
    private String[] currentTableHeader;
    
    /**
     * 构造报告生成器
     * 
     * @param reportPath 报告文件路径
     */
    public SimpleTestReportGenerator(String reportPath) {
        this.reportPath = reportPath;
        
        // 确保目录存在
        File reportFile = new File(reportPath);
        File parentDir = reportFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
    }
    
    /**
     * 开始一个新报告
     * 
     * @param title 报告标题
     * @return 报告生成器
     */
    public SimpleTestReportGenerator beginReport(String title) {
        reportContent.append("# ").append(title).append("\n\n");
        reportContent.append("生成时间: ").append(DATE_FORMAT.format(new Date())).append("\n\n");
        return this;
    }
    
    /**
     * 添加一个段落
     * 
     * @param text 段落文本
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addParagraph(String text) {
        reportContent.append(text).append("\n\n");
        return this;
    }
    
    /**
     * 添加一个章节标题
     * 
     * @param sectionName 章节名称
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addSection(String sectionName) {
        reportContent.append("## ").append(sectionName).append("\n\n");
        return this;
    }
    
    /**
     * 添加一个子章节标题
     * 
     * @param subSectionName 子章节名称
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addSubSection(String subSectionName) {
        reportContent.append("### ").append(subSectionName).append("\n\n");
        return this;
    }
    
    /**
     * 添加一个键值对
     * 
     * @param key 键
     * @param value 值
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addKeyValue(String key, String value) {
        reportContent.append("**").append(key).append("**: ").append(value).append("\n\n");
        return this;
    }
    
    /**
     * 添加一个表格头
     * 
     * @param headers 表头列名
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addTableHeader(String... headers) {
        // 清空当前表格
        currentTable.clear();
        currentTableHeader = headers;
        return this;
    }
    
    /**
     * 添加一行表格数据
     * 
     * @param values 单元格值
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addTableRow(String... values) {
        currentTable.add(values);
        return this;
    }
    
    /**
     * 渲染当前表格到报告中
     * 
     * @return 报告生成器
     */
    public SimpleTestReportGenerator renderTable() {
        if (currentTableHeader == null || currentTableHeader.length == 0) {
            return this;
        }
        
        // 添加表头
        reportContent.append("| ");
        for (String header : currentTableHeader) {
            reportContent.append(header).append(" | ");
        }
        reportContent.append("\n");
        
        // 添加分隔行
        reportContent.append("| ");
        for (int i = 0; i < currentTableHeader.length; i++) {
            reportContent.append("--- | ");
        }
        reportContent.append("\n");
        
        // 添加数据行
        for (String[] row : currentTable) {
            reportContent.append("| ");
            for (String cell : row) {
                reportContent.append(cell).append(" | ");
            }
            reportContent.append("\n");
        }
        
        reportContent.append("\n");
        
        // 清空当前表格
        currentTable.clear();
        currentTableHeader = null;
        
        return this;
    }
    
    /**
     * 添加一条水平线
     * 
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addHorizontalLine() {
        reportContent.append("---\n\n");
        return this;
    }
    
    /**
     * 添加一条代码块
     * 
     * @param language 代码语言
     * @param code 代码内容
     * @return 报告生成器
     */
    public SimpleTestReportGenerator addCodeBlock(String language, String code) {
        reportContent.append("```").append(language).append("\n");
        reportContent.append(code).append("\n");
        reportContent.append("```\n\n");
        return this;
    }
    
    /**
     * 结束当前报告并写入文件
     * 
     * @return 是否成功
     */
    public boolean finishReport() {
        try {
            // 如果还有未渲染的表格，先渲染
            if (currentTableHeader != null && !currentTable.isEmpty()) {
                renderTable();
            }
            
            // 添加页脚
            reportContent.append("\n---\n");
            reportContent.append("*报告生成时间: ").append(DATE_FORMAT.format(new Date())).append("*\n");
            
            // 写入文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportPath))) {
                writer.write(reportContent.toString());
            }
            
            return true;
        } catch (IOException e) {
            System.err.println("写入报告文件失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 获取报告文件路径
     * 
     * @return 报告文件路径
     */
    public String getReportPath() {
        return reportPath;
    }
} 