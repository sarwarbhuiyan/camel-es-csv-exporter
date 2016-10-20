package com.sarwarbhuiyan.tools.elasticsearch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ShutdownRoute;
import org.apache.camel.ShutdownRunningTask;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.camel.main.Main;
import org.apache.camel.model.dataformat.JsonLibrary;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;

public class CSVExporterRouteBuilder extends RouteBuilder {

	private int bulkSize = 500;
	private int outputWorkers = 1;
	private boolean preserveIDs = false;
	private String scrollPeriod = "1m";
	private String sourceHost = "localhost";
	private String sourcePort = "9200";
	private String sourceIndex = null;
	private Object scrollSize;
	private String scanQuery;

	private Main main;
	private String columns;
	private String outputFilePath = "report.csv";

	public int getBulkSize() {
		return bulkSize;
	}

	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}

	public int gtetOutputWorkers() {
		return outputWorkers;
	}

	public void setOutputWorkers(int outputWorkers) {
		this.outputWorkers = outputWorkers;
	}

	public boolean isPreserveIDs() {
		return preserveIDs;
	}

	public void setPreserveIDs(boolean preserveIDs) {
		this.preserveIDs = preserveIDs;
	}

	public String getScrollPeriod() {
		return scrollPeriod;
	}

	public void setScrollPeriod(String scrollPeriod) {
		this.scrollPeriod = scrollPeriod;
	}

	public String getSourceHost() {
		return sourceHost;
	}

	public void setSourceHost(String sourceHost) {
		this.sourceHost = sourceHost;
	}

	public String getSourcePort() {
		return sourcePort;
	}

	public void setSourcePort(String sourcePort) {
		this.sourcePort = sourcePort;
	}

	public String getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(String sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	public void setMain(Main main) {
		this.main = main;
	}

	public CSVExporterRouteBuilder(String sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	@Override
	public void configure() throws Exception {
		StringBuilder sourceRouteOptionsBuilder = new StringBuilder();
		sourceRouteOptionsBuilder.append("eshttp://elasticsearch?")
				.append("ip=").append(this.sourceHost).append("&")
				.append("port=").append(this.sourcePort).append("&")
				.append("operation=SCAN_SCROLL").append("&")
				.append("indexName=").append(this.sourceIndex)
				.append("&").append("scrollSize=").append(this.bulkSize)
				.append("&").append("scrollPeriod=").append(this.scrollPeriod);
		if (this.scanQuery != null)
			sourceRouteOptionsBuilder.append("&scanQuery=").append(this.scanQuery);

		//create output file or delete and re-create file
		File file = new File(this.outputFilePath);
		if (!file.exists())
			file.createNewFile();
		else {
			file.delete();
			file.createNewFile();
		}
		
		PrintWriter fileOutputWriter = new PrintWriter(new FileOutputStream(file));
		fileOutputWriter.write(this.columns + System.getProperty("line.separator"));
		fileOutputWriter.flush();
		fileOutputWriter.close();
		
		CsvDataFormat newCSV = new CsvDataFormat();
		newCSV.setSkipHeaderRecord(false)
		.setHeader(this.columns.split(","))
		.setSkipHeaderRecord(true);

		from(sourceRouteOptionsBuilder.toString())
		.startupOrder(2)
		.shutdownRunningTask(ShutdownRunningTask.CompleteAllTasks)		
		.to("seda:bulkRequests");
		
		from("seda:bulkRequests?concurrentConsumers="+this.outputWorkers)
				.startupOrder(1)
				.shutdownRunningTask(ShutdownRunningTask.CompleteAllTasks)
				.unmarshal().json(JsonLibrary.Jackson, true)
				.process(new Processor() {

					private JavaPropsMapper mapper = new JavaPropsMapper();
					public void process(Exchange exchange) throws Exception {
						HashMap hashMap = (HashMap)exchange.getIn().getBody();
						exchange.getIn().setBody(mapper.writeValueAsProperties(hashMap));
					}
				})
				.marshal(newCSV)
				.to("file:.?fileName="+this.outputFilePath+"&fileExist=Append&forceWrites=true");
	}

	public String getScanQuery(String scanQuery) {
		return this.scanQuery;
	}

	public void setScanQuery(String scanQuery) {
		this.scanQuery = scanQuery;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}

}
