package pers.cclucky.parallel.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * 测试数据生成器
 */
public class DataGenerator {
    
    private static final String[] OPERATIONS = {"ADD", "UPDATE", "DELETE", "QUERY", "PROCESS", "CALCULATE", "ANALYZE", "TRANSFORM"};
    private static final String[] DATA_TYPES = {"USER", "ORDER", "PRODUCT", "TRANSACTION", "LOG", "METRIC", "EVENT", "NOTIFICATION"};
    private static final Random random = new Random();
    private static final AtomicInteger idCounter = new AtomicInteger(0);
    
    /**
     * 生成测试数据文件
     * 
     * @param outputPath 输出路径
     * @param numFiles 文件数量
     * @param recordsPerFile 每个文件的记录数量
     * @param minSizeKb 每条记录最小大小(KB)
     * @param maxSizeKb 每条记录最大大小(KB)
     * @param compress 是否压缩文件
     * @throws IOException 如果文件操作失败
     */
    public static void generateTestData(String outputPath, int numFiles, int recordsPerFile, 
                                      int minSizeKb, int maxSizeKb, boolean compress) throws IOException {
        
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        
        long totalSize = 0;
        
        for (int fileIndex = 0; fileIndex < numFiles; fileIndex++) {
            String fileName = String.format("data_file_%04d.%s", fileIndex, compress ? "gz" : "json");
            File outputFile = new File(outputDir, fileName);
            
            try (FileOutputStream fos = new FileOutputStream(outputFile);
                 GZIPOutputStream gzos = compress ? new GZIPOutputStream(fos) : null) {
                
                for (int recordIndex = 0; recordIndex < recordsPerFile; recordIndex++) {
                    int recordSize = random.nextInt(maxSizeKb - minSizeKb + 1) + minSizeKb;
                    String record = generateRecord(recordSize * 1024); // 转换为字节
                    byte[] recordBytes = record.getBytes(StandardCharsets.UTF_8);
                    
                    if (compress) {
                        gzos.write(recordBytes);
                        if (recordIndex < recordsPerFile - 1) {
                            gzos.write('\n');
                        }
                    } else {
                        fos.write(recordBytes);
                        if (recordIndex < recordsPerFile - 1) {
                            fos.write('\n');
                        }
                    }
                    
                    totalSize += recordBytes.length;
                }
                
                if (compress) {
                    gzos.finish();
                }
            }
            
            System.out.printf("已生成文件 %s (%d KB)%n", 
                    fileName, outputFile.length() / 1024);
        }
        
        System.out.printf("共生成 %d 个文件，总大小约 %.2f MB%n", 
                numFiles, totalSize / (1024.0 * 1024.0));
    }
    
    /**
     * 生成单条测试记录
     * 
     * @param sizeInBytes 记录大小（字节）
     * @return 生成的JSON记录
     */
    private static String generateRecord(int sizeInBytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        
        // 基本字段
        int id = idCounter.incrementAndGet();
        String operation = OPERATIONS[random.nextInt(OPERATIONS.length)];
        String dataType = DATA_TYPES[random.nextInt(DATA_TYPES.length)];
        long timestamp = System.currentTimeMillis() - random.nextInt(86400000); // 过去24小时内的时间戳
        
        sb.append("\"id\":").append(id).append(",");
        sb.append("\"operation\":\"").append(operation).append("\",");
        sb.append("\"dataType\":\"").append(dataType).append("\",");
        sb.append("\"timestamp\":").append(timestamp).append(",");
        
        // 添加随机属性直到达到所需大小
        int propertiesCount = 1;
        while (sb.length() < sizeInBytes - 100) { // 保留一些空间用于结尾
            sb.append("\"property").append(propertiesCount).append("\":\"");
            
            // 添加随机字符串
            int propertyLength = random.nextInt(100) + 50;
            for (int i = 0; i < propertyLength; i++) {
                sb.append((char)(random.nextInt(26) + 'a'));
            }
            
            sb.append("\",");
            propertiesCount++;
        }
        
        // 添加结尾
        sb.append("\"priority\":").append(random.nextInt(10)).append(",");
        sb.append("\"status\":\"").append(random.nextBoolean() ? "SUCCESS" : "PENDING").append("\"");
        
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * 获取测试数据总记录数
     * 
     * @param dataDir 数据目录
     * @return 记录总数
     */
    public static long countTotalRecords(String dataDir) throws IOException {
        File dir = new File(dataDir);
        if (!dir.exists() || !dir.isDirectory()) {
            return 0;
        }
        
        long totalRecords = 0;
        File[] files = dir.listFiles((d, name) -> name.startsWith("data_file_"));
        
        for (File file : files) {
            if (file.getName().endsWith(".gz") || file.getName().endsWith(".json")) {
                // 每个文件的记录数可以从文件名或文件内容中解析
                // 这里简单估算
                totalRecords += file.length() / 5120; // 假设平均每条记录5KB
            }
        }
        
        return totalRecords;
    }
} 