## Flume-elasticsearch-sink
---
以多线程的方式往ES导数，并实现正则解析

## 适应 ElasticSearch 5.5版本的flume sink组件

## 配置
---
| 配置项   |      默认值      |  含义 |
|----------|:-------------:|------:|
| threadNum   |  1 | 往ES中导数的线程数 |
| batchSize |    200   |   批量写的总数据条数，每个线程为batchSize/threadNum |
| categories | defaultCategory |    可配置多个category，空格分隔 |
| defaultCategory.regex | null |    正则解析串 |
| defaultCategory.fields | null |    解析出的对应字段名 |
| defaultCategory.timeField | null |    解析出的时间字段名 |
| defaultCategory.timeField | null |    解析出的时间字段格式 |
| defaultCategory.converFields | null |    需要转换格式的字段 |
| defaultCategory.converTypes | null |    转换后的格式 |
| defaultCategory.storeOrgLog | false |    是否需要保存解析前的原始日志数据 |
## 例子
---
DatabusToEs.sinks.sinkdemo.type=org.apache.flume.sink.elasticsearch.ElasticSearchSink 
DatabusToEs.sinks.sinkdemo.hostNames=localhost:9300
DatabusToEs.sinks.sinkdemo.indexName=nginx_access_log
DatabusToEs.sinks.sinkdemo.threadNum=6
DatabusToEs.sinks.sinkdemo.indexType=logs
DatabusToEs.sinks.sinkdemo.clusterName=elasticsearch
DatabusToEs.sinks.sinkdemo.batchSize=2000
DatabusToEs.sinks.sinkdemo.serializer=org.apache.flume.sink.elasticsearch.ElasticSearchLogStashRegexEventSerializer
DatabusToEs.sinks.sinkdemo.channel=esAdNginxchannel
DatabusToEs.sinks.sinkdemo.serializer.categories=defaultCategory

DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.regex=^sip=(.*)&file=(.*)&ext=.*&data=([^\\s]*) ([^\\s]*) ([^\\s]*)
 \\[(.*)\\] "([^\\s]*) ([^\\s]*) ([^\\s]*)" ([^\\s]*) ([^\\s]*) ([^\\s]*) "(.*)" "([^"]*)" (.*) ([\\d]*)$
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.fields=log_ip,log_file,remote_addr,host,server_addr,time_local,meth
od,url,http_type,request_time,status,body_bytes_sent,http_referer,http_user_agent,upstream_addr,bytes_sent
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.timeField=time_local
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.timeFormat=dd/MMM/yyyy:HH:mm:ss Z
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.converFields=request_time,status,body_bytes_sent,bytes_sent
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.converTypes=float,int,int,int
DatabusToEs.sinks.sinkdemo.serializer.defaultCategory.storeOrgLog=false

