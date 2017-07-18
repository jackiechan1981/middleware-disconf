package com.baidu.disconf.client.core.processor.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.disconf.client.DisconfMgr;
import com.baidu.disconf.client.common.update.IDisconfUpdate;
import com.baidu.disconf.client.config.inner.DisClientComConfig;
import com.baidu.disconf.client.core.DisconfCoreMgr;
import com.baidu.disconf.client.store.DisconfStoreProcessor;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import com.baidu.disconf.core.common.zookeeper.ZookeeperMgr;

/**
 * @author liaoqiqi
 * @version 2014-8-4
 */
public class DisconfCoreProcessUtils {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DisconfCoreProcessUtils.class);

    /**
     * 调用此配置影响的回调函数
     */
    public static void callOneConf(DisconfStoreProcessor disconfStoreProcessor,
                                   String key) throws Exception {

        List<IDisconfUpdate> iDisconfUpdates = disconfStoreProcessor.getUpdateCallbackList(key);

        //
        // 获取回调函数列表
        //

        // CALL
        for (IDisconfUpdate iDisconfUpdate : iDisconfUpdates) {

            if (iDisconfUpdate != null) {

                LOGGER.info("start to call " + iDisconfUpdate.getClass());

                // set defined
                try {

                    iDisconfUpdate.reload();

                } catch (Exception e) {

                    LOGGER.error(e.toString(), e);
                }
            }
        }
    }
    
    /**
     * 当会话过期，并且NodeWatch事件丢失时，重建此节点
     * @param disconfStoreProcessor 仓库算子
     * @param key 配置文件或配置项 名称
     */
	public static void rebuildExpiredNodeIfNonWatchExecuted(DisconfStoreProcessor disconfStoreProcessor, String key,
			DisConfigTypeEnum disConfigTypeEnum) {
		String hostName = DisClientComConfig.getInstance().getLocalHostName();
		String pathKey = hostName + ":" + disConfigTypeEnum + ":" + key; 
		String tempChildPath = disconfStoreProcessor.getTempChildPathMap().get(pathKey);
		boolean nodeExists = true;
		
		//对当前重建的节点加锁，防止多个线程并发时同时检测到节点不存在，而重复创建
		synchronized (tempChildPath) {
			try {
				nodeExists = ZookeeperMgr.getInstance().exists(tempChildPath);
			} catch (Exception e) {
				LOGGER.info(pathKey + ", zkPath: " + tempChildPath + ", check exists failed! \t" + e.toString());
				nodeExists = false;
			}
			if (!nodeExists) {
				LOGGER.info(pathKey + ", zkPath: " + tempChildPath + ", is rebuilding!");
				try {
					DisconfCoreMgr disconfCoreMgr = DisconfMgr.getInstance().getDisconfCoreMgr();
					disconfCoreMgr.processOneConfAndCallback(key, disConfigTypeEnum);

				} catch (Exception e) {
					LOGGER.error(pathKey + ", zkPath: " + tempChildPath + ", rebuild failed! \t" + e.toString());
				}

				LOGGER.info(pathKey + ", zkPath: " + tempChildPath + ", rebuild successful!");
			}
		}
	}

}
