package com.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		//Topology Definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("document-reader", new DocumentReader());
		builder.setBolt("word-normalizer", new WordNoramlizer()).shuffleGrouping("document-reader");
		builder.setBolt("word-counter", new WordCounter()).shuffleGrouping("word-normalizer");
//		builder.setBolt("word-counter", new WordCounter(),2)
//		.fieldsGrouping("word-normalizer", new Fields("word"));

		//Configuration(This is merged wiht the cluster configuration at run time)
		Config config = new Config();
		config.put("wordsFile", args[0]);
		config.setDebug(false);
		
		//Topology Run
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting started topology", config, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
