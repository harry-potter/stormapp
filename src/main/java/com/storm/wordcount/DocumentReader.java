package com.storm.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


/**
 * Reads the document and provides each line to the bolt.
 */
public class DocumentReader implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/** The context. */
	private TopologyContext context;

	/** The collector. */
	private SpoutOutputCollector collector;

	/** The file reader. */
	private FileReader fileReader;

	private boolean completed = false;

	/**
	 * Creates the file and gets the collector and context.First method that is
	 * called in a spout.
	 * 
	 * @param TopologyContext
	 *            contains all the topology data
	 * @param conf
	 *            object which is created in the topology definition
	 * @param SpoutOutputCollector
	 *            emits data
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context = context;
		try {
			this.fileReader = new FileReader(new File(conf.get("wordsFile")
					.toString()));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["
					+ conf.get("wordsFile") + "]");

		}
		this.collector = collector;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	/**
	 * Reads the file and emits a value per line. This method is called
	 * periodically and must release the thread when there is no work.
	 */
	@Override
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str));
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);

	}

	
	@Override
	public void fail(Object msgId) {
		System.out.println("Fail:" + msgId);

	}

	/**
	 * Declare the output filed.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));

	}

	

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
