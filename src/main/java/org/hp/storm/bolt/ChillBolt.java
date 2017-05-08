package org.hp.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by hanpeng on 2017/5/8.
 */
public class ChillBolt extends BaseRichBolt {
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
        Map map = (Map) input.getValue(0);
        boolean warning = false;
        String carState = (String) map.get("carState");
        switch (carState) {
            case "1":
                carState = "奔跑中";
                break;
            case "2":
                carState = "休息中";
                break;
        }
        int temp = (int) map.get("temp");
        if (temp > -15) {
            warning = true;
        }

        System.out.println("ChillBolt 冷藏运输车:" + "车牌号码:" + map.get("carNum") +
                " 时速:" + map.get("carSpeed") + " 当前状态:" + carState + " 冷库温度:" + map.get("temp") + "  车辆报警:" + warning);

    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
