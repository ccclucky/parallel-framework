package pers.cclucky.parallel.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务上下文
 * 提供任务执行过程中的上下文信息和共享数据
 */
public class TaskContext implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String taskId;
    private final Map<String, Object> parameters;
    private final Map<String, Object> attributes;
    
    /**
     * 创建任务上下文
     * @param taskId 任务ID
     */
    public TaskContext(String taskId) {
        this.taskId = taskId;
        this.parameters = new HashMap<>();
        this.attributes = new ConcurrentHashMap<>();
    }
    
    /**
     * 获取任务ID
     * @return 任务ID
     */
    public String getTaskId() {
        return taskId;
    }
    
    /**
     * 设置任务参数
     * @param key 参数键
     * @param value 参数值
     * @return 当前上下文实例
     */
    public TaskContext setParameter(String key, Object value) {
        parameters.put(key, value);
        return this;
    }
    
    /**
     * 获取任务参数
     * @param key 参数键
     * @param <T> 参数类型
     * @return 参数值
     */
    @SuppressWarnings("unchecked")
    public <T> T getParameter(String key) {
        return (T) parameters.get(key);
    }
    
    /**
     * 获取任务参数，如果不存在则返回默认值
     * @param key 参数键
     * @param defaultValue 默认值
     * @param <T> 参数类型
     * @return 参数值或默认值
     */
    @SuppressWarnings("unchecked")
    public <T> T getParameter(String key, T defaultValue) {
        Object value = parameters.get(key);
        return value != null ? (T) value : defaultValue;
    }
    
    /**
     * 设置任务属性（线程安全）
     * @param key 属性键
     * @param value 属性值
     * @return 当前上下文实例
     */
    public TaskContext setAttribute(String key, Object value) {
        attributes.put(key, value);
        return this;
    }
    
    /**
     * 获取任务属性
     * @param key 属性键
     * @param <T> 属性类型
     * @return 属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }
    
    /**
     * 获取任务属性，如果不存在则返回默认值
     * @param key 属性键
     * @param defaultValue 默认值
     * @param <T> 属性类型
     * @return 属性值或默认值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, T defaultValue) {
        Object value = attributes.get(key);
        return value != null ? (T) value : defaultValue;
    }
    
    /**
     * 判断是否存在指定任务属性
     * @param key 属性键
     * @return 是否存在
     */
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }
    
    /**
     * 移除任务属性
     * @param key 属性键
     * @return 被移除的属性值
     */
    @SuppressWarnings("unchecked")
    public <T> T removeAttribute(String key) {
        return (T) attributes.remove(key);
    }
} 