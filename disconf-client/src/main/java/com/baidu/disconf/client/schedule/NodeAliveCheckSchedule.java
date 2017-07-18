package com.baidu.disconf.client.schedule;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.disconf.client.config.DisClientConfig;

/**
 * zookeeper节点存活检测
 * @author chenglilong
 *
 */
public class NodeAliveCheckSchedule {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeAliveCheckSchedule.class);
	
	//初始化延时10s
	private final static int INITAL_DELAY = 10000;
	
	//每十分钟检查一次
	private final static int PERIOD = 10 * 60 * 1000;
	
	private static ScheduledExecutorService executeService = null;
	
	/**
	 * 开始zookeeper节点存活检测
	 */
	public static void start(){
		
		if (DisClientConfig.getInstance().ENABLE_NODE_EXISTS_CHECK) {
			
			LOGGER.info("start the NodeAliveCheckSchedule! Check zookeeper node alive in every ten minutes!");
			
			executeService = Executors.newSingleThreadScheduledExecutor();
			executeService.scheduleAtFixedRate(new NodeAliveCheckTask(), INITAL_DELAY, PERIOD, TimeUnit.MILLISECONDS);
		}
	}
	
	public static void close(){
		if(executeService != null){
			executeService.shutdown();
		}
	}
	
}
