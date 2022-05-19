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
package org.apache.dubbo.metadata.report;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_DIRECTORY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.metadata.report.support.Constants.METADATA_REPORT_KEY;

/**
 * 2019-08-09
 */
public class MetadataReportInstance {

    private AtomicBoolean init = new AtomicBoolean(false);

    private final Map<String, MetadataReport> metadataReports = new HashMap<>();

    public void init(MetadataReportConfig config) {
        if (!init.compareAndSet(false, true)) {
            return;
        }
        ApplicationModel applicationModel = config.getApplicationModel();

        // 在这里通过SPI机制的adaptive自适应，生成一个代理类，底层会通过自适应的机制，根据url里的参数真实的去拿到对应的实现类，来调用他的方法
        // 他应为我们指定的是用zk作为一个元数据中心，拿到的应该是一个ZooKeeperMetadataReportFactory
        MetadataReportFactory metadataReportFactory = applicationModel.getExtensionLoader(MetadataReportFactory.class).getAdaptiveExtension();
        URL url = config.toUrl();
        if (METADATA_REPORT_KEY.equals(url.getProtocol())) {
            String protocol = url.getParameter(METADATA_REPORT_KEY, DEFAULT_DIRECTORY);
            url = URLBuilder.from(url)
                    .setProtocol(protocol)
                    .setScopeModel(config.getScopeModel())
                    .removeParameter(METADATA_REPORT_KEY)
                    .build();
        }
        url = url.addParameterIfAbsent(APPLICATION_KEY, applicationModel.getCurrentConfig().getName());
        String relatedRegistryId = config.getRegistry() == null ? DEFAULT_KEY : config.getRegistry();
//        RegistryConfig registryConfig = applicationModel.getConfigManager().getRegistry(relatedRegistryId)
//                .orElseThrow(() -> new IllegalStateException("Registry id " + relatedRegistryId + " does not exist."));

        // 这种url一般来说针对zk的url地址
        MetadataReport metadataReport = metadataReportFactory.getMetadataReport(url);
        if (metadataReport != null) {
            metadataReports.put(relatedRegistryId, metadataReport);
        }
    }

    public Map<String, MetadataReport> getMetadataReports(boolean checked) {
        if (checked) {
            checkInit();
        }
        return metadataReports;
    }

    // 可以通过他拿到具体的一个MetadataReport
    public MetadataReport getMetadataReport(String registryKey) {
        checkInit();
        MetadataReport metadataReport = metadataReports.get(registryKey);
        if (metadataReport == null) {
            metadataReport = metadataReports.values().iterator().next();
        }
        return metadataReport;
    }


    private void checkInit() {
        if (!init.get()) {
            throw new IllegalStateException("the metadata report was not initialized.");
        }
    }

}
