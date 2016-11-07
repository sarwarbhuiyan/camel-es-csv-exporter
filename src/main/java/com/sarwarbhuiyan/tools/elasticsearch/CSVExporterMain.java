package com.sarwarbhuiyan.tools.elasticsearch;

import org.apache.camel.CamelContext;
import org.apache.camel.main.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class CSVExporterMain extends Main {
	
	@Override
	protected CamelContext createContext() {
		CamelContext camelContext = super.createContext();
		return camelContext;
	}

	public static void main(String[] args) {
		
		CommandLineParser parser = new DefaultParser();
		
		Options options = new Options();
		options.addOption(org.apache.commons.cli.Option.builder().argName("sourceHost").hasArg().desc("Source Elasticsearch Host").longOpt("sourceHost").build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("sourcePort").hasArg().desc("Source Elasticsearch Port").longOpt("sourcePort").build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("sourceIndex").hasArg().desc("Source Index").longOpt("sourceIndex").required().build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("scrollPeriod").hasArg().desc("Scroll Period (default: 1m)").longOpt("scrollPeriod").build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("query").hasArg().desc("Query (default: { \"query\": \"{\"match_all\": {}}}").longOpt("query").build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("columns").hasArg().desc("Header column keys (e.g. department,name.first,name.last").longOpt("columns").required().build());
		options.addOption(org.apache.commons.cli.Option.builder().argName("outputFile").hasArg().desc("Header column keys (e.g. department,name.first,name.last").longOpt("outputFile").required().build());
		
		try {
			if(args.length < 1) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( "es-csv-exporter", options );
			}
			CommandLine line = parser.parse(options, args);
			String sourceIndex = line.getOptionValue("sourceIndex");
			CSVExporterRouteBuilder routeBuilder = new CSVExporterRouteBuilder(sourceIndex);
			if(line.hasOption("sourceHost"))
				routeBuilder.setSourceHost(line.getOptionValue("sourceHost"));
			if(line.hasOption("sourcePort"))
				routeBuilder.setSourcePort(line.getOptionValue("sourcePort"));
		
			
			if(line.hasOption("scrollPeriod"))
				routeBuilder.setScrollPeriod(line.getOptionValue("scrollPeriod"));
			if(line.hasOption("query"))
				routeBuilder.setScanQuery(line.getOptionValue("query"));
			if(line.hasOption("columns"))
				routeBuilder.setColumns(line.getOptionValue("columns"));
			if(line.hasOption("outputFile"))
				routeBuilder.setOutputFilePath(line.getOptionValue("outputFile"));
			
			final CSVExporterMain main = new CSVExporterMain();
			routeBuilder.setMain(main);
			main.addRouteBuilder(routeBuilder);
			main.setDuration(-1);
			main.run();
			
		} catch (org.apache.commons.cli.ParseException e) {
			LOG.error("Parse exception: " + e.getMessage());
		} catch (NumberFormatException e) {
			LOG.error("Number expected, found non-numeric string instead");
		} catch (Exception e) {
			LOG.error("Unknown error", e);
		}
	}
}
