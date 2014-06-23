package com.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		TopologyMain main = new TopologyMain();
		// main.directGrouping(args);
		main.shuffleGrouping(args);
		main.fieldGrouping(args);
		main.allGrouping(args);
	}

	public void shuffleGrouping(String[] args) throws InterruptedException {
		// Topology Definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("document-reader", new DocumentReader());
		builder.setBolt("word-normalizer", new WordNoramlizer())
				.shuffleGrouping("document-reader");
		// builder.setBolt("word-counter", new
		// WordCounter()).shuffleGrouping("word-normalizer");

		// add parallelism
		builder.setBolt("word-counter", new WordCounter(), 2).shuffleGrouping(
				"word-normalizer");

		// Configuration(This is merged with the cluster configuration at run
		// time)
		Config config = new Config();
		config.put("wordsFile", args[0]);
		config.setDebug(false);

		// Topology Run
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting started topology", config,
				builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}

	/**
	 * parallelism based on field grouping to make same word goes to same word
	 * counter instance
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public void fieldGrouping(String[] args) throws InterruptedException {
		// Topology Definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("document-reader", new DocumentReader());
		builder.setBolt("word-normalizer", new WordNoramlizer())
				.shuffleGrouping("document-reader");

		builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping(
				"word-normalizer", new Fields("word"));

		// Configuration(This is merged with the cluster configuration at run
		// time)
		Config config = new Config();
		config.put("wordsFile", args[0]);
		config.setDebug(false);

		// Topology Run
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting started topology", config,
				builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}

	/**
	 * sends a single copy of each tuple to all instances
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public void allGrouping(String[] args) throws InterruptedException {
		// Topology Definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("document-reader", new DocumentReader());
		builder.setBolt("word-normalizer", new WordNoramlizer())
				.shuffleGrouping("document-reader");
		builder.setBolt("word-counter", new WordCounter(), 2).allGrouping(
				"word-normalizer");

		// Configuration(This is merged with the cluster configuration at run
		// time)
		Config config = new Config();
		config.put("wordsFile", args[0]);
		config.setDebug(false);

		// Topology Run
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting started topology", config,
				builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}

	// /**
	// * sends a single copy of each tuple to all instances
	// * @param args
	// * @throws InterruptedException
	// */
	// public void directGrouping(String[] args) throws InterruptedException{
	// //Topology Definition
	// TopologyBuilder builder = new TopologyBuilder();
	// builder.setSpout("document-reader", new DocumentReader());
	// builder.setBolt("word-normalizer", new
	// WordNormalizerDirectGrouping()).shuffleGrouping("document-reader");
	// builder.setBolt("word-counter", new
	// WordCounter(),2).directGrouping("word-normalizer");
	//
	//
	// //Configuration(This is merged with the cluster configuration at run
	// time)
	// Config config = new Config();
	// config.put("wordsFile", args[0]);
	// config.setDebug(false);
	//
	// //Topology Run
	// config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	// LocalCluster cluster = new LocalCluster();
	// cluster.submitTopology("Getting started topology", config,
	// builder.createTopology());
	// Thread.sleep(10000);
	// cluster.shutdown();
	// }
}
