package pers.cclucky;

import pers.cclucky.parallel.ParallelFramework;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            // 初始化并行框架
            ParallelFramework framework = new ParallelFramework("application.properties");
            framework.init();
            framework.start();
            
            logger.info("工作节点已启动，ID: {}", framework.getNodeId());
            
            // 保持程序运行
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("正在关闭工作节点...");
                framework.stop();
            }));
            
            // 阻塞主线程，保持程序运行
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("节点启动失败", e);
        }
    }
}