package pers.cclucky.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 字符串测试数据生成器
 * 用于生成大量随机字符串用于性能测试
 */
public class StringDataGenerator {
    private static final Random random = new Random();
    
    // 随机字符池
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+[]{}|;:,.<>?";
    
    /**
     * 生成指定数量和长度的随机字符串
     *
     * @param count 字符串数量
     * @param minLength 最小长度
     * @param maxLength 最大长度
     * @return 随机字符串列表
     */
    public static List<String> generateRandomStrings(int count, int minLength, int maxLength) {
        List<String> strings = new ArrayList<>(count);
        
        for (int i = 0; i < count; i++) {
            int length = random.nextInt(maxLength - minLength + 1) + minLength;
            strings.add(generateRandomString(length));
        }
        
        return strings;
    }
    
    /**
     * 生成单个随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }
    
    /**
     * 生成随机字符串并保存到文件
     *
     * @param outputDir 输出目录
     * @param fileCount 文件数量
     * @param stringsPerFile 每个文件的字符串数量
     * @param minLength 最小字符串长度
     * @param maxLength 最大字符串长度
     * @throws IOException 如果文件操作失败
     */
    public static void generateStringDataFiles(String outputDir, int fileCount, int stringsPerFile, 
                                               int minLength, int maxLength) throws IOException {
        File dir = new File(outputDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        int totalStrings = 0;
        long totalLength = 0;
        
        for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
            String fileName = String.format("strings_%03d.txt", fileIndex);
            File outputFile = new File(dir, fileName);
            
            try (FileWriter writer = new FileWriter(outputFile)) {
                for (int i = 0; i < stringsPerFile; i++) {
                    int length = random.nextInt(maxLength - minLength + 1) + minLength;
                    String randomString = generateRandomString(length);
                    
                    writer.write(randomString);
                    writer.write('\n');
                    
                    totalStrings++;
                    totalLength += randomString.length();
                }
            }
            
            System.out.printf("已生成文件 %s (%d KB)%n", 
                    fileName, outputFile.length() / 1024);
        }
        
        double avgLength = totalLength / (double) totalStrings;
        System.out.printf("共生成 %d 个字符串，平均长度 %.2f 字符，总大小约 %.2f MB%n", 
                totalStrings, avgLength, totalLength / (1024.0 * 1024.0));
    }
    
    /**
     * 从测试数据目录加载字符串
     *
     * @param dataDir 数据目录
     * @param maxStrings 最大加载字符串数量，<=0表示加载全部
     * @return 字符串列表
     * @throws IOException 如果文件操作失败
     */
    public static List<String> loadTestStrings(String dataDir, int maxStrings) throws IOException {
        File dir = new File(dataDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IOException("测试数据目录不存在: " + dataDir);
        }
        
        File[] files = dir.listFiles((d, name) -> name.startsWith("strings_") && name.endsWith(".txt"));
        if (files == null || files.length == 0) {
            throw new IOException("数据目录中没有找到字符串数据文件: " + dataDir);
        }
        
        List<String> strings = new ArrayList<>();
        
        for (File file : files) {
            try (java.util.Scanner scanner = new java.util.Scanner(file)) {
                while (scanner.hasNextLine()) {
                    strings.add(scanner.nextLine());
                    
                    if (maxStrings > 0 && strings.size() >= maxStrings) {
                        return strings;
                    }
                }
            }
        }
        
        return strings;
    }
} 