package com.baidu.disconf.client.store.aspect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.disconf.client.DisconfMgr;
import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import com.baidu.disconf.client.common.annotations.DisconfItem;
import com.baidu.disconf.client.config.DisClientConfig;
import com.baidu.disconf.client.config.DisClientSysConfig;
import com.baidu.disconf.client.config.inner.DisClientComConfig;
import com.baidu.disconf.client.core.processor.DisconfCoreProcessor;
import com.baidu.disconf.client.core.processor.DisconfCoreProcessorFactory;
import com.baidu.disconf.client.fetcher.FetcherFactory;
import com.baidu.disconf.client.fetcher.FetcherMgr;
import com.baidu.disconf.client.store.DisconfStoreProcessor;
import com.baidu.disconf.client.store.DisconfStoreProcessorFactory;
import com.baidu.disconf.client.support.registry.Registry;
import com.baidu.disconf.client.support.registry.RegistryFactory;
import com.baidu.disconf.client.support.utils.MethodUtils;
import com.baidu.disconf.client.watch.WatchMgr;
import com.baidu.disconf.client.watch.impl.WatchMgrImpl;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import com.baidu.disconf.core.common.path.DisconfWebPathMgr;
import com.baidu.disconf.core.common.zookeeper.ZookeeperMgr;

/**
 * 配置拦截
 *
 * @author liaoqiqi
 * @version 2014-6-11
 */
@Aspect
public class DisconfAspectJ {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DisconfAspectJ.class);

    @Pointcut(value = "execution(public * *(..))")
    public void anyPublicMethod() {
    }

    /**
     * 获取配置文件数据, 只有开启disconf远程才会进行切面
     *
     * @throws Throwable
     */
    @Around("anyPublicMethod() && @annotation(disconfFileItem)")
    public Object decideAccess(ProceedingJoinPoint pjp, DisconfFileItem disconfFileItem) throws Throwable {

        if (DisClientConfig.getInstance().ENABLE_DISCONF) {

            MethodSignature ms = (MethodSignature) pjp.getSignature();
            Method method = ms.getMethod();

            //
            // 文件名
            //
            Class<?> cls = method.getDeclaringClass();
            DisconfFile disconfFile = cls.getAnnotation(DisconfFile.class);

            //
            // Field名
            //
            Field field = MethodUtils.getFieldFromMethod(method, cls.getDeclaredFields(), DisConfigTypeEnum.FILE);
            if (field != null) {

                //
                // 请求仓库配置数据
                //
                DisconfStoreProcessor disconfStoreProcessor =
                        DisconfStoreProcessorFactory.getDisconfStoreFileProcessor();
                
                //解决disconf和应用断连问题：重建过期节点
                rebuildExpiredNodeIfNonWatchExecuted(disconfStoreProcessor, disconfFile.filename(), DisConfigTypeEnum.FILE);
                
                Object ret = disconfStoreProcessor.getConfig(disconfFile.filename(), disconfFileItem.name());
                if (ret != null) {
                    LOGGER.debug("using disconf store value: " + disconfFile.filename() + " ("
                            + disconfFileItem.name() +
                            " , " + ret + ")");
                    return ret;
                }
            }
        }

        Object rtnOb;

        try {
            // 返回原值
            rtnOb = pjp.proceed();
        } catch (Throwable t) {
            LOGGER.info(t.getMessage());
            throw t;
        }

        return rtnOb;
    }

    /**
     * 获取配置项数据, 只有开启disconf远程才会进行切面
     *
     * @throws Throwable
     */
    @Around("anyPublicMethod() && @annotation(disconfItem)")
    public Object decideAccess(ProceedingJoinPoint pjp, DisconfItem disconfItem) throws Throwable {

        if (DisClientConfig.getInstance().ENABLE_DISCONF) {
            //
            // 请求仓库配置数据
            //
            DisconfStoreProcessor disconfStoreProcessor = DisconfStoreProcessorFactory.getDisconfStoreItemProcessor();
            
            //解决disconf和应用断连问题：重建过期节点
            rebuildExpiredNodeIfNonWatchExecuted(disconfStoreProcessor, disconfItem.key(), DisConfigTypeEnum.ITEM);
            
            Object ret = disconfStoreProcessor.getConfig(null, disconfItem.key());
            if (ret != null) {
                LOGGER.debug("using disconf store value: (" + disconfItem.key() + " , " + ret + ")");
                return ret;
            }
        }

        Object rtnOb;

        try {
            // 返回原值
            rtnOb = pjp.proceed();
        } catch (Throwable t) {
            LOGGER.info(t.getMessage());
            throw t;
        }

        return rtnOb;
    }
    
    /**
     * 解决disconf和应用断连问题：当会话过期，并且NodeWatch事件丢失时，重建此节点
     * @param disconfStoreProcessor 仓库算子
     * @param key 配置文件或配置项 名称
     * @throws Exception
     */
	private void rebuildExpiredNodeIfNonWatchExecuted(DisconfStoreProcessor disconfStoreProcessor, String key,
			DisConfigTypeEnum disConfigTypeEnum) throws Exception {
		String hostName = DisClientComConfig.getInstance().getLocalHostName();
		String tempChildPath = disconfStoreProcessor.getTempChildPathMap().get(hostName + ":" + key);
		boolean nodeExists = true;
		try {
			nodeExists = ZookeeperMgr.getInstance().exists(tempChildPath);
		} catch (Exception e) {
			LOGGER.error(hostName + "-" + key + ", zkPath: " + tempChildPath + ", check exists failed! \t" + e.toString());
			return;
		}
		if (!nodeExists) {
			LOGGER.info(hostName + "-" + key + ", zkPath: " + tempChildPath + ", is rebuilding!");
			FetcherMgr fetcherMgr = FetcherFactory.getFetcherMgr();
			WatchMgr watchMgr = null;
			try {
				String hosts = fetcherMgr.getValueFromServer(
						DisconfWebPathMgr.getZooHostsUrl(DisClientSysConfig.getInstance().CONF_SERVER_ZOO_ACTION));
				String zooPrefix = fetcherMgr.getValueFromServer(
						DisconfWebPathMgr.getZooPrefixUrl(DisClientSysConfig.getInstance().CONF_SERVER_ZOO_ACTION));
				watchMgr = new WatchMgrImpl();
				watchMgr.init(hosts, zooPrefix, DisClientConfig.getInstance().DEBUG);

				Registry registry = RegistryFactory.getSpringRegistry(DisconfMgr.getInstance().getApplicationContext());
				DisconfCoreProcessor processor = DisconfCoreProcessorFactory.getDisconfCoreProcessorByType(watchMgr,
						fetcherMgr, registry, disConfigTypeEnum);
				processor.updateOneConfAndCallback(key);

			} catch (Exception e) {
				LOGGER.error(hostName + "-" + key + ", zkPath: " + tempChildPath + ", rebuild failed! \t" + e.toString());
			}

			LOGGER.info(hostName + "-" + key + ", zkPath: " + tempChildPath + ", rebuild successful!");
		}
	}
    
}
