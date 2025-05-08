package pers.cclucky.parallel.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.core.schedule.TaskScheduler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultTaskService implements TaskService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultTaskService.class);
    private final TaskScheduler taskScheduler;
    
    public DefaultTaskService(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }
    
    @Override
    public <T, R> String submitTask(Task<T, R> task) {
        return taskScheduler.submitTask(task);
    }
    
    @Override
    public <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task) {
        return taskScheduler.submitTaskAsync(task);
    }
    
    @Override
    public String getTaskStatus(String taskId) {
        return taskScheduler.getTaskStatus(taskId);
    }
    
    @Override
    public <R> TaskResult<R> getTaskResult(String taskId) {
        return taskScheduler.getTaskResult(taskId);
    }
    
    @Override
    public <R> CompletableFuture<TaskResult<R>> getTaskResultAsync(String taskId) {
        return taskScheduler.getTaskResultAsync(taskId);
    }
    
    @Override
    public boolean cancelTask(String taskId) {
        return taskScheduler.cancelTask(taskId);
    }
    
    @Override
    public int getTaskProgress(String taskId) {
        return taskScheduler.getTaskProgress(taskId);
    }
    
    @Override
    public List<String> batchGetTaskStatus(List<String> taskIds) {
        // 简单实现，实际可能需要优化
        return taskIds.stream()
            .map(this::getTaskStatus)
            .collect(java.util.stream.Collectors.toList());
    }
}
