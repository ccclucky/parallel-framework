package pers.cclucky.parallel.api;

import java.io.Serializable;
import java.util.List;

/**
 * 并行计算框架的任务接口
 * 用户需要实现此接口来定义自己的任务
 */
public interface Task<T, R> extends Serializable {
    
    /**
     * 获取任务ID
     * @return 任务的唯一标识符
     */
    String getTaskId();
    
    /**
     * 任务分片逻辑
     * @param context 任务上下文
     * @return 分片后的子任务列表
     */
    List<TaskSlice<T>> slice(TaskContext context);
    
    /**
     * 处理单个任务分片
     * @param slice 任务分片
     * @param context 任务上下文
     * @return 处理结果
     */
    R process(TaskSlice<T> slice, TaskContext context);
    
    /**
     * 合并所有分片的处理结果
     * @param results 所有分片的处理结果
     * @param context 任务上下文
     * @return 最终的合并结果
     */
    R reduce(List<R> results, TaskContext context);
    
    /**
     * 任务进度回调
     * @param progress 进度百分比(0-100)
     * @param message 进度信息
     */
    default void onProgress(int progress, String message) {
        // 默认空实现，用户可根据需要覆盖
    }
    
    /**
     * 任务完成回调
     * @param result 任务结果
     */
    default void onComplete(R result) {
        // 默认空实现，用户可根据需要覆盖
    }
    
    /**
     * 任务失败回调
     * @param e 异常信息
     */
    default void onError(Throwable e) {
        // 默认空实现，用户可根据需要覆盖
    }
} 