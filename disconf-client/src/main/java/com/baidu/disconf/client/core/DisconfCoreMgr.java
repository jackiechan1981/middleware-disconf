package com.baidu.disconf.client.core;

import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;

/**
 * 核心处理模块：包括第一次扫描时配置入库; 第二次扫描时配置注入;
 *
 * @author liaoqiqi
 * @version 2014-6-10
 */
public interface DisconfCoreMgr {

    /**
     * (第一次扫描时使用)<br/>
     * 1. 获取远程的所有配置数据<br/>
     * 2. 注入到仓库中<br/>
     * 3. Watch 配置
     */
    void process();

    /**
     * (第一次、或额外 扫描时使用)<br/>
     * 1. 获取远程的所有配置数据<br/>
     * 2. 注入到仓库中<br/>
     * 3. Watch 配置
     */
    void processFile(String fileName);
    
    /**
     * (重建节点时使用)<br/>
     * 1. 获取远程的所有配置数据<br/>
     * 2. 注入到仓库中<br/>
     * 3. Watch 配置
     * 4. 调用配置的回调函数
     */
    void processOneConfAndCallback(String key, DisConfigTypeEnum disConfigTypeEnum) throws Exception;

    /**
     * 特殊的，将数据注入到 配置项、配置文件 的实体中(第二次扫描时使用)
     */
    void inject2DisconfInstance();

    /**
     * 释放一些资源
     */
    void release();
}
