package org.hp.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.hp.storm.bolt.ChillBolt;
import org.hp.storm.bolt.DispatchBolt;
import org.hp.storm.bolt.TankerBolt;
import org.hp.storm.spout.DataSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hanpeng on 2017/5/6.
 * 1、库分离，如UAC保存车辆实时信息到MongoDB A，分析引擎需要使用MongoB进行分离，单独部署;
 * 2、通过kafka消息队列实现消息实时读取，上层系统作为生产者往队列中生产消息，本程序作为消费者来处理数据；
 * 3、通过spout实现读取消息队列，作为数据入口;
 * 4、通过DispatchBolt接收到DataSpout推送的消息，进行首次处理，可以处理一些所有车辆共有数据，然后再根据解析出来不同车辆类型走不同Bolt;
 * 5、Chill&TankerBolt根据车辆类型不同分析其不同数据;
 * 6、需要结合smessageGate进行一些灵活配置及支持简单动态脚本语言;
 * 7、消息安全未处理，当节点处理失败及 需要使用taskid 及 streamid 重新发送;
 * 8、消息过滤考虑幂等设计或去重方式;
 */
public class TopologyApplication {

    public static void main(String[] args) {
        System.out.println("aaa");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new DataSpout(), 1);
        builder.setBolt("dispatchBolt", new DispatchBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("tanker", new TankerBolt(), 1).shuffleGrouping("dispatchBolt", "tanker");
        builder.setBolt("chill", new ChillBolt(), 1).shuffleGrouping("dispatchBolt", "chill");


        Config conf = new Config();
        conf.setDebug(false);
        conf.put("filepath", "src/main/resources/data.txt");
        Map<String, String> carTypeBolt = new HashMap<String, String>();
        carTypeBolt.put("1", "tanker");
        carTypeBolt.put("2", "chill");
        conf.put("carTypeBolt", carTypeBolt);
        conf.setMaxSpoutPending(5);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("topologytest", conf, builder.createTopology());
    }
}
