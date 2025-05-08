package pers.cclucky.parallel.api.service;

import java.util.List;
import java.util.Map;

/**
 * 监控服务接口
 * 定义系统监控相关的RPC方法
 */
public interface MonitorService {
    
    /**
     * 获取集群状态
     * @return 集群状态信息
     */
    Map<String, Object> getClusterStatus();
    
    /**
     * 获取所有Worker节点信息
     * @return Worker节点列表
     */
    List<Map<String, Object>> getAllWorkers();
    
    /**
     * 获取指定Worker节点信息
     * @param workerId 节点ID
     * @return 节点信息
     */
    Map<String, Object> getWorkerInfo(String workerId);
    
    /**
     * 获取正在执行的任务列表
     * @return 任务列表
     */
    List<Map<String, Object>> getRunningTasks();
    
    /**
     * 获取任务的详细执行情况
     * @param taskId 任务ID
     * @return 任务执行详情
     */
    Map<String, Object> getTaskDetail(String taskId);
    
    /**
     * 获取系统性能指标
     * @return 性能指标数据
     */
    Map<String, Object> getPerformanceMetrics();
    
    /**
     * 设置警报阈值
     * @param metricName 指标名称
     * @param threshold 阈值
     * @return 是否设置成功
     */
    boolean setAlertThreshold(String metricName, double threshold);
    
    /**
     * 获取系统警报
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 警报列表
     */
    List<Map<String, Object>> getAlerts(long startTime, long endTime);
} 