package org.hp.storm.bolt;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.tuple.Values;
import org.hp.storm.utils.JsonUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hanpeng on 2017/5/7.
 */
public class DispatchBolt extends BaseRichBolt {

    OutputCollector collector;
    Map conf;

    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     * <p>
     * This includes the:
     *
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context   This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.conf = stormConf;
    }

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. The IBolt does not have to process the Tuple
     * immediately. It is perfectly fine to hang onto a tuple and process it later
     * (for instance, to do an aggregation or join).
     * <p>
     * Tuples should be emitted using the OutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some point using the OutputCollector.
     * Otherwise, Storm will be unable to determine when tuples coming off the spouts
     * have been completed.
     * <p>
     * For the common case of acking an input tuple at the end of the execute method,
     * see IBasicBolt which automates this.
     *
     * @param input The input tuple to be processed.
     */
    public void execute(Tuple input) {
        String str = input.getString(0);
//        System.out.println("DisPatchBolt接收到了:" + str);

        Map<String, Object> map = null;
        try {
            map = JsonUtil.json2Map(str);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        System.out.println("转换后的map:"+map);

        String carType = (String) map.get("carType");
        Map<String, String> carTypeBolt = (Map) conf.get("carTypeBolt");
        String bolt = carTypeBolt.get(carType);
//        String bolt = (String) conf.get("boltName");
//        System.out.println("分发bolt:"+bolt);
        collector.emit(bolt, new Values(map));


    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //这里无法获取到稍后通过配置来读取配置文件  通过配置动态declareoutputStream
//        System.out.println("filePath测试:"+conf.get("filepath"));
//        Map<String, String> carTypeBolt = (Map) conf.get("carTypeBolt");
//        for(String bolt:carTypeBolt.keySet()){
//            declarer.declareStream(bolt, new Fields("map"));
//        }
        declarer.declareStream("tanker", new Fields("map"));
        //注意这里的分发的streamid tanker必须在topology里面注册bolt时候声明不然报错
        //builder.setBolt("tanker",new TankerBolt(),1).shuffleGrouping("dispatchBolt","tanker");
        //前面setbolt也要改成配置文件读取方式
        declarer.declareStream("chill", new Fields("map"));
    }
}
