/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.EventThreadPoolExecutor;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;

public class ElasticSearchTransportClient implements ElasticSearchClient {

    public static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchTransportClient.class);

    private InetSocketTransportAddress[] serverAddresses;
    private ElasticSearchEventSerializer serializer;
    private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
    private BulkRequestBuilder bulkRequestBuilder;
    private Client client;

    private BulkRequestBuilder[] bulkRequestBuilders = null;
    private LinkedBlockingQueue<EventInfo> eventQueues = new LinkedBlockingQueue();

    private EventProcessThread[] threads = null;
    private IndexNameBuilder indexNameBuilder = null;

    private AtomicInteger processNum = new AtomicInteger(0);
    private boolean allAlive = true;
    private boolean noException = true;

    private EventThreadPoolExecutor threadPoolExecutor = null;
    private ExecutorService executorService = null;

    private int threadNum = 1;
    private static final int BULK_RETRY_TIMES = 3;
    private static final int BULK_RETRY_WAIT_TIME = 5 * 1000;
    private String indexName = null;

    private Event event = null;

    @VisibleForTesting
    InetSocketTransportAddress[] getServerAddresses() {
        return serverAddresses;
    }

    @VisibleForTesting
    void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
        this.bulkRequestBuilder = bulkRequestBuilder;
    }

    /**
     * Transport client for external cluster
     *
     * @param hostNames
     * @param clusterName
     * @param serializer
     */
    public ElasticSearchTransportClient(String[] hostNames, String clusterName,
                                        ElasticSearchEventSerializer serializer) {
        configureHostnames(hostNames);
        this.serializer = serializer;
        openClient(clusterName);
    }

    public ElasticSearchTransportClient(String[] hostNames, String clusterName,
                                        ElasticSearchEventSerializer serializer, int threadNum) {
        configureHostnames(hostNames);
        this.serializer = serializer;
        openClient(clusterName);
        logger.info("[self]threadNum " + threadNum);

        if (threadNum > 1) {
            this.threadNum = threadNum;
            bulkRequestBuilders = new BulkRequestBuilder[threadNum];
            resetBulkRequestBuilder();
            init();
            executorService = Executors.newFixedThreadPool(threadNum);
            threadPoolExecutor = new EventThreadPoolExecutor(threadNum, threadNum, 3, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.DiscardPolicy());

        }
    }

    private void init() {
        threads = new EventProcessThread[threadNum];

        for (int i = 0; i < threadNum; i++) {
            threads[i] = new EventProcessThread(i);
            threads[i].start();
        }
    }

    private void resetBulkRequestBuilder() {
        for (int i = 0; i < bulkRequestBuilders.length; i++) {
            bulkRequestBuilders[i] = client.prepareBulk();
        }

    }

    public ElasticSearchTransportClient(String[] hostNames, String clusterName,
                                        ElasticSearchIndexRequestBuilderFactory indexBuilder) {
        configureHostnames(hostNames);
        this.indexRequestBuilderFactory = indexBuilder;
        openClient(clusterName);

        logger.info("[self]  create index...clusterName:" + clusterName);
    }

    /**
     * Local transport client only for testing
     *
     * @param indexBuilderFactory
     */
    public ElasticSearchTransportClient(ElasticSearchIndexRequestBuilderFactory indexBuilderFactory) {
        this.indexRequestBuilderFactory = indexBuilderFactory;
        openLocalDiscoveryClient();
    }

    /**
     * Local transport client only for testing
     *
     * @param serializer
     */
    public ElasticSearchTransportClient(ElasticSearchEventSerializer serializer) {
        this.serializer = serializer;
        openLocalDiscoveryClient();
    }

    /**
     * Used for testing
     *
     * @param client     ElasticSearch Client
     * @param serializer Event Serializer
     */
    public ElasticSearchTransportClient(Client client,
                                        ElasticSearchEventSerializer serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    /**
     * Used for testing
     *
     * @param client ElasticSearch Client
     */
    public ElasticSearchTransportClient(Client client,
                                        ElasticSearchIndexRequestBuilderFactory requestBuilderFactory) throws IOException {
        this.client = client;
        requestBuilderFactory.createIndexRequest(client, null, null, null);
    }

    private void configureHostnames(String[] hostNames) {
        logger.warn(Arrays.toString(hostNames));
        serverAddresses = new InetSocketTransportAddress[hostNames.length];

        try {
            for (int i = 0; i < hostNames.length; i++) {
                String[] hostPort = hostNames[i].trim().split(":");
                String host = hostPort[0].trim();
                int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                        : DEFAULT_PORT;
                serverAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(host), port);
            }
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
        client = null;
    }

    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
                         String indexType, long ttlMs) throws Exception {
        if (bulkRequestBuilder == null) {
            bulkRequestBuilder = client.prepareBulk();
        }
        this.event = event;
        IndexRequestBuilder indexRequestBuilder = null;
        if (indexRequestBuilderFactory == null) {
            indexRequestBuilder = client
                    .prepareIndex(indexNameBuilder.getIndexName(event), indexType)
                    .setSource(serializer.getContentBuilder(event));// .bytes()

        } else {
            logger.info("[self]create index..." + new String(event.getBody(), "UTF-8"));
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                logger.info("[self]index event header key:" + entry.getKey() + " value:" + entry.getValue());
            }
            indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
                    client, indexNameBuilder.getIndexPrefix(event), indexType, event);
        }

//        if (ttlMs > 0) {
//            indexRequestBuilder.setTTL(ttlMs);
//        }
        bulkRequestBuilder.add(indexRequestBuilder);
    }

    @Override
    public void execute() throws Exception {
        try {

            BulkResponse bulkResponse = null;
            for (int i = 0; i < BULK_RETRY_TIMES; i++) {
                bulkResponse = bulkRequestBuilder.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    String error = bulkResponse.buildFailureMessage();
                    if (error.contains("IllegalArgumentException") || error.contains("MapperParsingException")) {  //格式错误，即便bulk多次也无法解决；
                        if (this.event != null) {
                            logger.error("[self: " + indexName + "] [Message]:" + new String(this.event.getBody(), "UTF-8") + error);
                        }
                        logger.error("[self: " + indexName + "] skip this bulk opration...");
                        bulkRequestBuilder = client.prepareBulk();
                        return;
                    }
                    Thread.sleep(BULK_RETRY_WAIT_TIME);
                    continue;
                } else {
                    break;
                }
            }
            if (bulkResponse.hasFailures()) {
                String error = bulkResponse.buildFailureMessage();

                if (this.event != null) {
                    logger.error("[self: " + indexName + "] [Message]:" + new String(this.event.getBody(), "UTF-8") + error);

                }
                if (error.contains("IllegalArgumentException") || error.contains("MapperParsingException")) {  //格式错误，即便bulk多次也无法解决；
                    logger.error("[self: " + indexName + "] skip this bulk opration...");
                    return;
                }
                logger.info("[self:" + indexName + "] thread [ ] bulk has failures wait...");
                throw new EventDeliveryException(bulkResponse.buildFailureMessage());
            }

        } finally {
            bulkRequestBuilder = client.prepareBulk();
        }
    }


    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType, long ttlMs, int count) throws Exception {
        if (threadNum == 1) {
            addEvent(event, indexNameBuilder, indexType, ttlMs);
            return;
        }
        if (this.indexNameBuilder == null) {
            this.indexNameBuilder = indexNameBuilder;
        }

        int index = count % threadNum;

        EventInfo eventInfo = new EventInfo(event, indexType, ttlMs, index);
        boolean flag = eventQueues.offer(eventInfo);
        if (!flag) {
            logger.error("[self" + indexName + "] put event into queue error");
        }
    }

    @Override
    public void execute(int count) throws Exception {

        if (threadNum == 1) {
            execute();
            return;
        }

        try {
            /**
             * 等待处理完count条消息；
             */
            long end = 0, start = System.currentTimeMillis();
            while (processNum.get() < count) {
                Thread.sleep(10);
                end = System.currentTimeMillis();
                if (end - start > 30 * 60 * 1000) {  //强制退出；
                    logger.error("[self:" + indexName + "] add event cost too much time...break! active count:" + " finishNum:" + processNum + " count:" + count);
                    noException = false;
                    break;
                }
            }

            /**
             * 若某线程退出，需重新启动；
             */
            if (!allAlive) {
                logger.info("[self:" + indexName + "] thread is not all alive, need restart ...");
                for (int i = 0; i < threads.length; i++) {
                    if (threads[i] == null || !threads[i].isAlive()) {
                        threads[i] = new EventProcessThread(i);
                        threads[i].start();
                        logger.info("[self:" + indexName + "] restart thread [" + i + "]...");
                    }
                }
                allAlive = true;
            }

            /**
             * 已出现异常，需抛出，并rollback；
             */
            if (!noException) {
                noException = true;
                throw new Exception("[self:" + indexName + "] event process error! see log...");
            }
            /**
             * 批量提交ES
             */
            threadPoolExecutor.reset();
            for (int i = 0; i < threadNum; i++) {
                threadPoolExecutor.execute(new BulkRequestTask(i));
            }
            //threadPoolExecutor.isEndTask();
            /**
             * 等待处理完count条消息；
             */
            end = start = System.currentTimeMillis();
            while (threadPoolExecutor.getFinishNum() < threadNum) {
                Thread.sleep(10);
                end = System.currentTimeMillis();
                if (end - start > 30 * 60 * 1000) {  //15分钟 后 强制退出；
                    logger.error("[self:" + indexName + "] bulk to es cost too much time...break! active count:" + " finishNum:" + threadPoolExecutor.getFinishNum());
                    break;
                }
            }

            if (threadPoolExecutor.getActiveCount() > 0) {
                logger.error("[self:" + indexName + "] bulk thread activeCount:" + threadPoolExecutor.getActiveCount());
            }


            /**
             * 已出现异常，需抛出，并rollback；
             */
            if (!noException) {
                noException = true;
                throw new Exception("[self:" + indexName + "] bulk commit error! see log...");
            }
        } catch (Exception e) {
            throw e;
        } finally {
            threadPoolExecutor.reset();
            processNum.set(0);
            eventQueues.clear();
            resetBulkRequestBuilder();
        }
    }


    /**
     * Open client to elaticsearch cluster
     *
     * @param clusterName
     */
//    private void openClient(String clusterName) {
//        logger.info("Using ElasticSearch hostnames: {} ",
//                Arrays.toString(serverAddresses));
//        Settings settings = ImmutableSettings.settingsBuilder()
//                .put("cluster.name", clusterName).build();
//
//        TransportClient transportClient = new TransportClient(settings);
//        for (InetSocketTransportAddress host : serverAddresses) {
//            transportClient.addTransportAddress(host);
//        }
//        if (client != null) {
//            client.close();
//        }
//        client = transportClient;
//    }
    private void openClient(String clusterName) {
        logger.info("Using ElasticSearch hostnames: {} ",
                Arrays.toString(serverAddresses));
        Settings settings = Settings.builder().put("cluster.name", clusterName)// 指定集群名称
                //.put("client.transport.sniff", true)// 探测集群中机器状态
                .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);
        transportClient.addTransportAddresses(serverAddresses);


        if (client != null) {
            client.close();
        }

        client = transportClient;
    }


    /*
     * FOR TESTING ONLY...
     *
     * Opens a local discovery node for talking to an elasticsearch server running
     * in the same JVM
     */
    private void openLocalDiscoveryClient() {
        logger.info("Using ElasticSearch AutoDiscovery mode");

//        Node node = NodeBuilder.nodeBuilder().client(true).local(true).node();
//        if (client != null) {
//            client.close();
//        }
//        client = node.client();
    }

    @Override
    public void configure(Context context) {
        this.indexName = context.getString(INDEX_NAME);
    }


    /**
     * 每个线程，将一个BulkRequestBuilder提交ES，批量建索引；
     */
    class BulkRequestTask implements Runnable {
        private int threadId;

        public BulkRequestTask(int i) {
            this.threadId = i;
        }

        @Override
        public void run() {
            try {
                BulkRequestBuilder brb = bulkRequestBuilders[threadId];
                if (brb.numberOfActions() == 0) {
                    logger.info(" [self:" + indexName + "] no requests added in bulk [" + threadId + "]");
                    return;
                }
                BulkResponse bulkResponse = null;
                int tryNum = 0;
                for (; tryNum < BULK_RETRY_TIMES; tryNum++) {
                    bulkResponse = brb.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        String error = bulkResponse.buildFailureMessage();
                        if (error.contains("IllegalArgumentException") || error.contains("MapperParsingException")) {  //格式错误，即便bulk多次也无法解决；
                            logger.error("[self:" + indexName + "] thread [" + threadId + "] bulk Exception skip... " + error);
                            break;
                        }
                        logger.info("[self:" + indexName + "] thread [" + threadId + "] bulk has failures wait...");
                        Thread.sleep(BULK_RETRY_WAIT_TIME);
                        continue;
                    } else {
                        break;
                    }
                }
                if (tryNum >= BULK_RETRY_TIMES && bulkResponse.hasFailures()) {
                    throw new EventDeliveryException(bulkResponse.buildFailureMessage());
                }

            } catch (EventDeliveryException e) {
                logger.error("[self:" + indexName + "] thread [" + threadId + "] EventDeliveryException:" + e.getMessage());
                noException = false;
                e.printStackTrace();

            } catch (Exception e) {
                noException = false;
                logger.error("[self:" + indexName + "] thread [" + threadId + "] Execute Exception:" + e.getMessage());
                e.printStackTrace();
            } finally {
                bulkRequestBuilders[threadId] = client.prepareBulk();
                threadPoolExecutor.runOver();
            }

        }
    }

    private synchronized void addIndexRequestBuilder(IndexRequestBuilder indexRequestBuilder, int id) {
        bulkRequestBuilders[id].add(indexRequestBuilder);
    }


    /**
     * 每个线程，不停从对应的eventQueues取event，解析后将生成的indexRequestBuilder，根据id，放入相应的bulkRequestBuilder中；
     */
    private class EventProcessThread extends Thread {
        private int threadId;

        public EventProcessThread(int i) {
            this.threadId = i;
        }

        @Override
        public void run() {
            while (true) {
                EventInfo eventInfo = null;
                try {
                    //eventQueues.poll();
                    eventInfo = eventQueues.poll(20, TimeUnit.SECONDS);
                    if (eventInfo == null) {
                        //sleep(5 * 1000);
                        logger.info("[slef] thread [" + threadId + "] eventQueues is empty size:" + eventQueues.size() + " indexName:" + indexName);
                        continue;
                    }
                    Event event = eventInfo.event;
                    String indexType = eventInfo.indexType;
                    long ttlMs = eventInfo.ttlMs;

                    IndexRequestBuilder indexRequestBuilder = null;
                    if (indexRequestBuilderFactory == null) {
                        indexRequestBuilder = client
                                .prepareIndex(indexNameBuilder.getIndexName(event), indexType)
                                .setSource(serializer.getContentBuilder(event)) ;
                    } else {
                        logger.info("[self:" + indexName + "] thread [" + threadId + "] create index..." + new String(event.getBody(), "UTF-8"));
                        for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                            logger.info("[self:" + indexName + "] thread [" + threadId + "] index event header key:" + entry.getKey() + " value:" + entry.getValue());
                        }
                        indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
                                client, indexNameBuilder.getIndexPrefix(event), indexType, event);
                    }

                    if (ttlMs > 0) {
                        indexRequestBuilder.setTTL(ttlMs);
                    }

                    addIndexRequestBuilder(indexRequestBuilder, eventInfo.id);
                    processNum.incrementAndGet();
                } catch (Exception e) {
                    allAlive = false;
                    noException = false;
                    e.printStackTrace();
                    try {
                        logger.error("[self:" + indexName + "] Process Event error from thread [" + threadId + "] " + e.getMessage() + " log:" + new String(eventInfo.event.getBody(), "UTF-8"));
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                    return;
                } finally {
                }
            }
        }


    }

    private class EventInfo {
        private Event event;
        private String indexType;
        private long ttlMs;
        private int id;

        public EventInfo(Event event, String indexType, long ttlMs, int index) {
            this.event = event;
            this.indexType = indexType;
            this.ttlMs = ttlMs;
            this.id = index;
        }

        public EventInfo(EventInfo eventInfo) {
            this.event = eventInfo.event;
            this.indexType = eventInfo.indexType;
            this.ttlMs = eventInfo.ttlMs;
            this.id = eventInfo.id;
        }
    }
}
