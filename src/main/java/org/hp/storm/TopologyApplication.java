package org.hp.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.hp.storm.bolt.TestBolt;
import org.hp.storm.spout.TestSpout;

/**
 * Created by hanpeng on 2017/5/6.
 */
public class TopologyApplication {

    public static void main(String[] args) {
        System.out.println("aaa");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new TestSpout(),1);
        builder.setBolt("bolt",new TestBolt(),1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put("filepath","src/main/resources/data.txt");
        conf.setMaxSpoutPending(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("topologytest",conf,builder.createTopology());
    }
}
