# analyzeEngine
Apache storm Car Data Analyze Engine
# Created by hanpeng on 2017/5/8.
 1、库分离，如UAC保存车辆实时信息到MongoDB A，分析引擎需要使用MongoB进行分离，单独部署;
 2、通过kafka消息队列实现消息实时读取，上层系统作为生产者往队列中生产消息，本程序作为消费者来处理数据；
 3、通过spout实现读取消息队列，作为数据入口;
 4、通过DispatchBolt接收到DataSpout推送的消息，进行首次处理，可以处理一些所有车辆共有数据，然后再根据解析出来不同车辆类型走不同Bolt;
 5、Chill&TankerBolt根据车辆类型不同分析其不同数据;
 6、需要结合smessageGate进行一些灵活配置及支持简单动态脚本语言;
 7、消息安全未处理，当节点处理失败及 需要使用taskid 及 streamid 重新发送;
 8、消息过滤考虑幂等设计或去重方式;
