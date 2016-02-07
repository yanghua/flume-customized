# flume-customized

> 基于Apache Flume NG v1.6.0版本定制

## source

- targetPattern: 支持按天的滚动日志收集
- targetYoungestFileName:支持老版本的log4j按天滚动时，当前文件不包含日期格式

## eventDeserializer

### MultiLineDeserializer
多行日志读取&合并

- newLineStartPrefix：日志行分隔前缀
- wrappedByDocker: 是否被docker格式包裹

```
此类不可直接使用,必须放入flume源码的:org.apache.flume.serialization包路径下

然后在类:org.apache.flume.serialization.EventDeserializerType 增加枚举 MULTILINE

重新编译 module :flume-ng-core,将新生成的jar 替换原来的.
```

## sink

### MessagebusSink

增加了对[基于RabbitMQ的消息总线](https://github.com/yanghua/banyan)的Sink支持

- batchSize: 日志批量发送的阈值
- zkConnectionStr: zookeeper的连接字符串(ip:port;ip:port)
- sourceSecret: 消息源secret
- sinkName: 消息槽name
- streamToken: 消息流token

> 消息总线的客户端，可通过编译[消息总线](https://github.com/yanghua/banyan)得到