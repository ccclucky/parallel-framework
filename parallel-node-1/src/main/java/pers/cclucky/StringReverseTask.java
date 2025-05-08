package pers.cclucky;

import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskContext;
import pers.cclucky.parallel.api.TaskSlice;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StringReverseTask implements Task<String, String> {
    private String inputString;
    private int sliceSize = 5; // 每个分片处理的字符数
    private String taskId;

    // 添加无参构造函数
    public StringReverseTask() {
        this.taskId = UUID.randomUUID().toString();
    }

    // 保留带参构造函数用于本地测试
    public StringReverseTask(String inputString) {
        this();
        this.inputString = inputString;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public List<TaskSlice<String>> slice(TaskContext context) {
        // 从上下文中获取输入字符串（如果构造时未提供）
        if (inputString == null || inputString.isEmpty()) {
            inputString = context.getParameter("inputString", "");
        }
        
        List<TaskSlice<String>> slices = new ArrayList<>();

        // 将输入字符串分成多个小段
        for (int i = 0; i < inputString.length(); i += sliceSize) {
            int end = Math.min(i + sliceSize, inputString.length());
            String sliceData = inputString.substring(i, end);

            // 创建分片
            TaskSlice<String> slice = new TaskSlice<>(
                    UUID.randomUUID().toString(),
                    sliceData,
                    i / sliceSize,
                    sliceSize
            );

            slices.add(slice);
        }

        return slices;
    }

    @Override
    public String process(TaskSlice<String> slice, TaskContext context) {
        // 处理单个分片 - 反转字符串片段
        String data = slice.getData();
        StringBuilder reversed = new StringBuilder(data).reverse();
        return reversed.toString();
    }

    @Override
    public String reduce(List<String> results, TaskContext context) {
        // 合并所有分片的结果
        StringBuilder finalResult = new StringBuilder();

        // 注意：因为我们是按顺序分片的，需要按相反顺序合并
        for (int i = results.size() - 1; i >= 0; i--) {
            finalResult.append(results.get(i));
        }

        return finalResult.toString();
    }
}
