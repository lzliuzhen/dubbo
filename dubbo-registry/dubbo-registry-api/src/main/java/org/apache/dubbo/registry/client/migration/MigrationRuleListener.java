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
package org.apache.dubbo.registry.client.migration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.client.migration.model.MigrationRule;
import org.apache.dubbo.registry.integration.RegistryProtocol;
import org.apache.dubbo.registry.integration.RegistryProtocolListener;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.RegistryConstants.INIT;

/**
 * Listens to {@MigrationRule} from Config Center.
 *
 * 监听配置中心里的migration rule，迁移规则的变化
 *
 * - Migration rule is of consumer application scope. 迁移规则是consumer应用级别的范围都有效的
 * - Listener is shared among all invokers (interfaces), it keeps the relation between interface and handler.
 * 所以说这个migration rule listener是在所有的invoker里都有效的，他是用来维系接口和handler之间的关系
 *
 * There are two execution points:
 * - Refer, invoker behaviour is determined with default rule.
 * 这个东西什么时候会执行，RegistryProtocol进行refer的时候，针对migration invoker进行回调处理，此时会基于默认的迁移规则来决定invoker后续的行为
 * migration invoker后续的一些行为，都是会由默认的迁移规则来指定
 * - Rule change, invoker behaviour is changed according to the newly received rule.
 * 如果说你的配置中心里有一个迁移规则的变化，此时invoker后续的一些行为会由新的规则来进行决定和改变
 *
 *
 * 单单从设计者为什么要在这里加一个监听器，通过监听器来回调处理我们的源头的migration invoker
 * 他是希望通过这个监听器里的规则，来决定migration invoker后续的一些行为，也就是决定了后续的一些invoker的组装逻辑和行为
 * 也就直接对rpc调用过程能的执行有一定的影响力
 * 包括就是说，对这个规则还做到了可以去监听规则的变化，一旦规则变化之后，就会改变invoker链条的行为
 *
 */
@Activate
public class MigrationRuleListener implements RegistryProtocolListener, ConfigurationListener {
    private static final Logger logger = LoggerFactory.getLogger(MigrationRuleListener.class);
    private static final String DUBBO_SERVICEDISCOVERY_MIGRATION = "DUBBO_SERVICEDISCOVERY_MIGRATION";
    private static final String MIGRATION_DELAY_KEY = "dubbo.application.migration.delay";
    private static final int MIGRATION_DEFAULT_DELAY_TIME = 60000;
    private String ruleKey;

    // 一个migration invoker就对应了一个migration rule handler
    protected final Map<MigrationInvoker, MigrationRuleHandler> handlers = new ConcurrentHashMap<>();
    protected final LinkedBlockingQueue<String> ruleQueue = new LinkedBlockingQueue<>();

    private final AtomicBoolean executorSubmit = new AtomicBoolean(false);
    // 线程池，池子是他自己直接创建的
    // 吐槽一丢丢，按照我们之前所说的，为什么要设计repository组件，executor repository
    // 很明显犯了一个错误，不知道是谁在这里来写代码，他的线程池没有走executor repository，还是应该先通过SPI机制
    // 获取到ExecutorRepository，就可以来有对应的线程池获取出来
    private final ExecutorService ruleManageExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("Dubbo-Migration-Listener"));

    protected ScheduledFuture<?> localRuleMigrationFuture;
    protected Future<?> ruleMigrationFuture;

    private DynamicConfiguration configuration;

    private volatile String rawRule;
    private volatile MigrationRule rule; // 默认的迁移规则
    private ModuleModel moduleModel;

    // 刚开始你的这个listener是通过SPI机制获取出来的实例，构建对象的时候，必然会有一个init初始化逻辑
    public MigrationRuleListener(ModuleModel moduleModel) {
        this.moduleModel = moduleModel;
        init();
    }

    private void init() {
        // 在这里我们需要去研究一下这个所谓的刚开始的迁移规则到底是怎么搞出来的

        this.ruleKey = moduleModel.getApplicationModel().getApplicationName() + ".migration";
        this.configuration = moduleModel.getModelEnvironment().getDynamicConfiguration().orElse(null);

        if (this.configuration != null) {
            logger.info("Listening for migration rules on dataId " + ruleKey + ", group " + DUBBO_SERVICEDISCOVERY_MIGRATION);
            // 如果有动态配置中心，nacos、zk作为一个动态配置中心，给这个动态配置中心加一个监听器，监听器一旦施加之后
            // 就是指明是对哪个配置项变动施加监听，而且监听就是他自己
            // 经典，挺优秀的，dubbo如果要支持外部的动态配置中心（nacos、zk、apollo），一旦DynamicConfiguration，跟外部配置中心能够连接上
            // 同时可以做到说，外部配置中心一旦有配置项变化，就会反向推送更新给你的DynamicConfigurtion，此时他就会感知到你有一些配置项是有一些变化的
            // DynamicConfiguration就会去判断和发现你施加监听的是谁，你对哪个配置项加了监听，此时就会把最新的配置项的值回调给你的监听器
            configuration.addListener(ruleKey, DUBBO_SERVICEDISCOVERY_MIGRATION, this);

            // 从你的动态配置中心里，尝试直接获取到你的原始rule规则，字符串
            String rawRule = configuration.getConfig(ruleKey, DUBBO_SERVICEDISCOVERY_MIGRATION);
            if (StringUtils.isEmpty(rawRule)) {
                rawRule = INIT; // 如果你获取出来的是空，此时就给他默认值就是一个INIT
            }
            // 就可以去设置你的一个原始规则
            setRawRule(rawRule);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("Using default configuration rule because config center is not configured!");
            }
            setRawRule(INIT);
        }

        String localRawRule = moduleModel.getModelEnvironment().getLocalMigrationRule();
        if (!StringUtils.isEmpty(localRawRule)) {
            localRuleMigrationFuture = moduleModel.getApplicationModel().getApplicationExecutorRepository().getSharedScheduledExecutor()
                .schedule(() -> {
                    if (this.rawRule.equals(INIT)) {
                        this.process(new ConfigChangedEvent(null, null, localRawRule));
                    }
                }, getDelay(), TimeUnit.MILLISECONDS);
        }
    }

    private int getDelay() {
        int delay = MIGRATION_DEFAULT_DELAY_TIME;
        String delayStr = ConfigurationUtils.getProperty(moduleModel, MIGRATION_DELAY_KEY);
        if (StringUtils.isEmpty(delayStr)) {
            return delay;
        }

        try {
            delay = Integer.parseInt(delayStr);
        } catch (Exception e) {
            logger.warn("Invalid migration delay param " + delayStr);
        }
        return delay;
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        // 就有一个config变更的事件
        String rawRule = event.getContent();
        if (StringUtils.isEmpty(rawRule)) {
            // fail back to startup status
            rawRule = INIT;
            //logger.warn("Received empty migration rule, will ignore.");
        }
        try {
            ruleQueue.put(rawRule); // 他是有一个基于队列的异步化的处理，需要处理的原始规则放入了一个queue里去
            // 代码级的精巧的设计，如果说在短时间内对一个配置项频繁变更，此时可能会并发的收到多次事件的通知
            // 串行排队等待，多线程通过cas解决并发问题，来有一个有序处理多个变更的机制
        } catch (InterruptedException e) {
            logger.error("Put rawRule to rule management queue failed. rawRule: " + rawRule, e);
        }

        // 这里这样做有一个好处，如果说同时噼里啪啦接收到了很多个规则的变化，此时通过这个cas的控制
        // 同一时间只能有一个变化的规则进入处理

        // 精巧的设计，很漂亮的，invoker构建逻辑含糊不清，单单是说迁移规则的反向推送变更处理
        // 这块代码，写的非常有水平的

        if (executorSubmit.compareAndSet(false, true)) {
            // 唯一的一个线程进来到这里之后如何处理规则的变更，不光是一个规则变更，queue里可能会积压很多变更
            // 有一个单线程的线程池，属于就一个线程进来，此时就提交一个任务给一个单线程的线程池，来进行异步化的处理
            // 线程池提交任务之后，他需要一段时间来进行处理，可以拿到一个future，后续通过future就可以拿到线程异步处理的结果
            ruleMigrationFuture = ruleManageExecutor.submit(() -> {
                // 走了一个while true无限循环
                // 这个线程只会提交一次，
                while (true) {
                    String rule = "";
                    try {
                        // 他每次循环，就会从queue里获取一个rule
                        rule = ruleQueue.take();
                        if (StringUtils.isEmpty(rule)) {
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException e) {
                        logger.error("Poll Rule from config center failed.", e);
                    }
                    if (StringUtils.isEmpty(rule)) {
                        continue;
                    }
                    if (Objects.equals(this.rawRule, rule)) {
                        logger.info("Ignore duplicated rule");
                        continue;
                    }
                    try {
                        logger.info("Using the following migration rule to migrate:");
                        logger.info(rule);

                        // 解析这个rule，设置这个rule
                        setRawRule(rule);

                        if (CollectionUtils.isNotEmptyMap(handlers)) {
                            // 每次rule有变更之后，都会去回调你的MigrationRuleHandler
                            // 他又写死了一个线程池
                            ExecutorService executorService = Executors.newFixedThreadPool(100, new NamedThreadFactory("Dubbo-Invoker-Migrate"));
                            List<Future<?>> migrationFutures = new ArrayList<>(handlers.size());
                            handlers.forEach((_key, handler) -> {
                                // 对每个migration handler都会去提交 一个任务进去，在这个里面就会去执行一个迁移操作
                               Future<?> future = executorService.submit(() -> {
                                    handler.doMigrate(this.rule); // 一旦这个迁移操作执行，传入进去了新的规则，可能直接就会影响migration invoker后续的行为逻辑
                                });
                               migrationFutures.add(future);
                            });

                            Throwable migrationException = null;
                            for (Future<?> future : migrationFutures) {
                                try {
                                    future.get(); // 等待每个handler都要同步执行完毕之后，才能算结束
                                } catch (InterruptedException ie) {
                                    logger.warn("Interrupted while waiting for migration async task to finish.");
                                } catch (ExecutionException ee) {
                                    migrationException = ee.getCause();
                                }
                            }
                            if (migrationException != null) {
                                logger.error("Migration async task failed.", migrationException);
                            }
                            executorService.shutdown();
                        }
                    } catch (Throwable t) {
                        logger.error("Error occurred when migration.", t);
                    }
                }
            });
        }

    }

    public void setRawRule(String rawRule) {
        this.rawRule = rawRule; // 对你的规则会去执行一下parse，把字符串的规则解析为你的规则对象
        this.rule = parseRule(this.rawRule);
    }

    private MigrationRule parseRule(String rawRule) {
        MigrationRule tmpRule = rule == null ? MigrationRule.INIT : rule;
        if (INIT.equals(rawRule)) {
            tmpRule = MigrationRule.INIT;
        } else {
            try {
                tmpRule = MigrationRule.parse(rawRule);
            } catch (Exception e) {
                logger.error("Failed to parse migration rule...", e);
            }
        }
        return tmpRule;
    }

    @Override
    public void onExport(RegistryProtocol registryProtocol, Exporter<?> exporter) {

    }

    @Override
    public void onRefer(RegistryProtocol registryProtocol, ClusterInvoker<?> invoker, URL consumerUrl, URL registryURL) {
        MigrationRuleHandler<?> migrationRuleHandler = handlers.computeIfAbsent((MigrationInvoker<?>) invoker, _key -> {
            // 给这个migration invoker会设置一个rule listener，就是当前的这个listener设置进去就可以了
            // 另外由构建了一个migration rule handler
            ((MigrationInvoker<?>) invoker).setMigrationRuleListener(this);
            return new MigrationRuleHandler<>((MigrationInvoker<?>) invoker, consumerUrl);
        });

        // 其实就是拿默认构建好的一个迁移规则处理器，来执行一下对应的迁移逻辑
        // INIT规则
        migrationRuleHandler.doMigrate(rule);

        // 如果说这个逻辑是我们自己实现的，此时我们就会拿到这个migration invoker
        // 我们自己就可以去影响和控制这个里面的invoker调用链条的组装关系了，甚至我们可以在这个里面加入自己的invoker
        // 这个口子一旦留了，只要我们愿意，随时可以定制rpc调用链路的过程，在里面删减掉一些东西，也可以加入一些东西
    }

    @Override
    public void onDestroy() {
        if (configuration != null) {
            configuration.removeListener(ruleKey, this);
        }
        if (ruleMigrationFuture != null) {
            ruleMigrationFuture.cancel(true);
        }
        if (localRuleMigrationFuture != null) {
            localRuleMigrationFuture.cancel(true);
        }
        if (ruleManageExecutor != null) {
            ruleManageExecutor.shutdown();
        }
        ruleQueue.clear();
    }

    public Map<MigrationInvoker, MigrationRuleHandler> getHandlers() {
        return handlers;
    }

    protected void removeMigrationInvoker(MigrationInvoker<?> migrationInvoker) {
        handlers.remove(migrationInvoker);
    }

    public MigrationRule getRule() {
        return rule;
    }
}
