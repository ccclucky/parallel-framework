package pers.cclucky.parallel.api;

import java.io.Serializable;

/**
 * 任务分片类
 * 包含任务分片的基本信息和数据
 * @param <T> 分片数据类型
 */
public class TaskSlice<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String sliceId;
    private final T data;
    private final int index;
    private final int total;
    
    /**
     * 创建任务分片
     * @param sliceId 分片ID
     * @param data 分片数据
     * @param index 分片索引
     * @param total 分片总数
     */
    public TaskSlice(String sliceId, T data, int index, int total) {
        this.sliceId = sliceId;
        this.data = data;
        this.index = index;
        this.total = total;
    }
    
    /**
     * 获取分片ID
     * @return 分片ID
     */
    public String getSliceId() {
        return sliceId;
    }
    
    /**
     * 获取分片数据
     * @return 分片数据
     */
    public T getData() {
        return data;
    }
    
    /**
     * 获取分片索引
     * @return 分片索引
     */
    public int getIndex() {
        return index;
    }
    
    /**
     * 获取分片总数
     * @return 分片总数
     */
    public int getTotal() {
        return total;
    }
    
    @Override
    public String toString() {
        return "TaskSlice{" +
                "sliceId='" + sliceId + '\'' +
                ", index=" + index +
                ", total=" + total +
                '}';
    }
} 