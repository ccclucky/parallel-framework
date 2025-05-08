package pers.cclucky.parallel.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务结果类
 * 用于封装任务执行的结果信息
 * @param <R> 结果数据类型
 */
public class TaskResult<R> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 任务状态枚举
     */
    public enum Status {
        /** 成功 */
        SUCCESS,
        /** 失败 */
        FAILED,
        /** 取消 */
        CANCELLED,
        /** 超时 */
        TIMEOUT
    }
    
    private final String taskId;
    private final String sliceId;
    private final Status status;
    private final R data;
    private final Throwable error;
    private final long executionTime;
    private final Map<String, Object> metadata;
    private final int sliceIndex;
    
    private TaskResult(Builder<R> builder) {
        this.taskId = builder.taskId;
        this.sliceId = builder.sliceId;
        this.status = builder.status;
        this.data = builder.data;
        this.error = builder.error;
        this.executionTime = builder.executionTime;
        this.metadata = builder.metadata;
        this.sliceIndex = builder.sliceIndex;
    }

    public int getSliceIndex() {
        return sliceIndex;
    }
    
    /**
     * 获取任务ID
     * @return 任务ID
     */
    public String getTaskId() {
        return taskId;
    }
    
    /**
     * 获取分片ID
     * @return 分片ID
     */
    public String getSliceId() {
        return sliceId;
    }
    
    /**
     * 获取任务状态
     * @return 任务状态
     */
    public Status getStatus() {
        return status;
    }
    
    /**
     * 获取结果数据
     * @return 结果数据
     */
    public R getData() {
        return data;
    }
    
    /**
     * 获取错误信息
     * @return 错误信息
     */
    public Throwable getError() {
        return error;
    }
    
    /**
     * 获取执行时间（毫秒）
     * @return 执行时间
     */
    public long getExecutionTime() {
        return executionTime;
    }
    
    /**
     * 获取元数据
     * @return 元数据Map
     */
    public Map<String, Object> getMetadata() {
        return new HashMap<>(metadata);
    }
    
    /**
     * 获取指定元数据
     * @param key 元数据键
     * @param <T> 元数据类型
     * @return 元数据值
     */
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key) {
        return (T) metadata.get(key);
    }
    
    /**
     * 是否成功
     * @return 是否成功
     */
    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
    
    /**
     * 是否已完成（不论成功或失败）
     * @return 是否已完成
     */
    public boolean isCompleted() {
        return status == Status.SUCCESS || status == Status.FAILED || 
               status == Status.CANCELLED || status == Status.TIMEOUT;
    }
    
    /**
     * 获取错误信息（字符串）
     * @return 错误信息
     */
    public String getErrorString() {
        if (error != null) {
            return error.getMessage();
        } else if (status != Status.SUCCESS) {
            return status.name();
        }
        return null;
    }
    
    /**
     * 获取结果数据
     * @return 结果数据
     */
    public R getResult() {
        return data;
    }
    
    /**
     * 创建成功结果
     * @param taskId 任务ID
     * @param data 结果数据
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> success(String taskId, R data) {
        return new Builder<R>()
                .taskId(taskId)
                .status(Status.SUCCESS)
                .data(data)
                .build();
    }
    
    /**
     * 创建成功结果（带分片ID）
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param data 结果数据
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> success(String taskId, String sliceId, R data) {
        return new Builder<R>()
                .taskId(taskId)
                .sliceId(sliceId)
                .status(Status.SUCCESS)
                .data(data)
                .build();
    }
    
    /**
     * 创建失败结果
     * @param taskId 任务ID
     * @param error 错误信息
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> failed(String taskId, Throwable error) {
        return new Builder<R>()
                .taskId(taskId)
                .status(Status.FAILED)
                .error(error)
                .build();
    }
    
    /**
     * 创建失败结果（带分片ID）
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param error 错误信息
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> failed(String taskId, String sliceId, Throwable error) {
        return new Builder<R>()
                .taskId(taskId)
                .sliceId(sliceId)
                .status(Status.FAILED)
                .error(error)
                .build();
    }
    
    /**
     * 创建取消结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> cancelled(String taskId) {
        return new Builder<R>()
                .taskId(taskId)
                .status(Status.CANCELLED)
                .build();
    }
    
    /**
     * 创建超时结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public static <R> TaskResult<R> timeout(String taskId) {
        return new Builder<R>()
                .taskId(taskId)
                .status(Status.TIMEOUT)
                .build();
    }
    
    /**
     * 构建器类
     * @param <R> 结果数据类型
     */
    public static class Builder<R> {
        private String taskId;
        private String sliceId;
        private Status status;
        private R data;
        private Throwable error;
        private long executionTime;
        private Map<String, Object> metadata = new HashMap<>();
        private int sliceIndex;
        
        public Builder<R> taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }
        
        public Builder<R> sliceId(String sliceId) {
            this.sliceId = sliceId;
            return this;
        }
        
        public Builder<R> status(Status status) {
            this.status = status;
            return this;
        }
        
        public Builder<R> data(R data) {
            this.data = data;
            return this;
        }
        
        public Builder<R> error(Throwable error) {
            this.error = error;
            return this;
        }
        
        public Builder<R> executionTime(long executionTime) {
            this.executionTime = executionTime;
            return this;
        }
        
        public Builder<R> sliceIndex(int sliceIndex) {
            this.sliceIndex = sliceIndex;
            return this;
        }
        
        public Builder<R> addMetadata(String key, Object value) {
            this.metadata.put(key, value);
            return this;
        }
        
        public Builder<R> metadata(Map<String, Object> metadata) {
            this.metadata.putAll(metadata);
            return this;
        }
        
        public TaskResult<R> build() {
            return new TaskResult<>(this);
        }
    }
} 