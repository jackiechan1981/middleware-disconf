package com.baidu.disconf.client.schedule;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.disconf.client.DisconfMgr;
import com.baidu.disconf.client.core.DisconfCoreMgr;
import com.baidu.disconf.client.store.inner.DisconfCenterStore;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import com.baidu.disconf.core.common.zookeeper.ZookeeperMgr;

/**
 * zk节点存活检测
 * @author: chenglilong
 * @since: 2017年7月6日
 *
 */
public class NodeAliveCheckTask implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(NodeAliveCheckTask.class);

	@Override
	public void run() {

		Map<String, String> needRebuildConfs = new HashMap<String, String>();

		Map<String, String> tempChildMap = DisconfCenterStore.getInstance().getTempChildPathMap();
		try {
			for (String key : tempChildMap.keySet()) {
				String path = tempChildMap.get(key);
				boolean nodeExists = ZookeeperMgr.getInstance().exists(path);
				if (!nodeExists) {
					needRebuildConfs.put(key, path);
				}
			}

			executeTask(needRebuildConfs);

		} catch (Exception e) {
			LOGGER.error("NodeAliveCheckTask execute failed! \t" + e.toString());
		}
	}

	private void executeTask(Map<String, String> needRebuildConfs) throws Exception {

		//创建线程池，核心线程数为2，最大线程数为4，线程空闲时间为3s，深度为10的有界队列
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 4, 3, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(10));
		final CountDownLatch count = new CountDownLatch(needRebuildConfs.size());

		for (final String key : needRebuildConfs.keySet()) {

			final String confName = StringUtils.split(key, ":")[2]; //key格式：hostname:FILE:simple.properties

			threadPool.execute(new Runnable() {

				@Override
				public void run() {
					LOGGER.info("zookeeper node is rebuilding, key: " + key);
					DisconfCoreMgr disconfCoreMgr = DisconfMgr.getInstance().getDisconfCoreMgr();
					try {
						if (StringUtils.contains(key, DisConfigTypeEnum.FILE.toString())) {
							disconfCoreMgr.processOneConfAndCallback(confName,
									DisConfigTypeEnum.FILE);
						} else {
							disconfCoreMgr.processOneConfAndCallback(confName,
									DisConfigTypeEnum.ITEM);
						}
					} catch (Exception e) {
						LOGGER.error(key + ", rebuild failed! \t" + e.toString());
					}
					count.countDown();
					LOGGER.info("zookeeper node has been rebuilded sucessful, key: " + key);
				}
			});
		}

		count.await();
		LOGGER.info("a round of NodeAliveCheckTaskSchedule has been executed successful! "
				+ needRebuildConfs.size() + " node has been rebuilded!");
	}

}
