/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.io.IOException;
import java.util.Map;

import org.apache.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * ConfigManager 提供了从磁盘加载配置文件到内存，以及将内存数据持久化到磁盘的基础能力。
 */
public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected RocksDBConfigManager rocksDBConfigManager;

    /**
     * ConfigManager 提供了 load 方法来从磁盘读取配置文件，如果配置文件没有内容，则加载 .bak 的备份文件，然后将内容解码到内存中。当修改了内存数据后，就会调用 persist 来将数据持久化到磁盘文件中，
     * 持久化的时候会自动创建一个 .bak 的备份文件。
     *
     * @return
     */
    public boolean load() {
        String fileName = null;
        try {
            // 加载配置文件，转成 json 字符串
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            // 配置文件为空时，加载备份文件
            if (null == jsonString || jsonString.length() == 0) {
                // 加载 .bak 备份文件
                return this.loadBak();
            } else {
                // 解码
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            // 加载备份文件
            return this.loadBak();
        }
    }

    // 加载 .bak 配置文件
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized <T> void persist(String topicName, T t) {
        // stub for future
        this.persist();
    }

    public synchronized <T> void persist(Map<String, T> m) {
        // stub for future
        this.persist();
    }

    // 持久化
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }


    protected void decode0(final byte[] key, final byte[] body) {

    }

    public boolean stop() {
        return true;
    }

    // 配置文件的路径
    public abstract String configFilePath();

    // 将内存数据编码成 json 字符串
    public abstract String encode();

    // 编码成 json 字符串
    public abstract String encode(final boolean prettyFormat);

    // 将 json 字符串解码成内存数据
    public abstract void decode(final String jsonString);
}
