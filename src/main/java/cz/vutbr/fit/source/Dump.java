package cz.vutbr.fit.source;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.lucene.document.DateTools;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import eu.juniper.sa.monitoring.agent.MonitoringAgentForSqlFile;
import eu.juniper.sa.monitoring.agent.MonitoringAgentInterface;
import eu.juniper.sa.monitoring.sensor.DataConnectionSensorInterface;
import eu.juniper.sa.monitoring.sensor.ProgramInstanceSensorInterface;

/**
* A spout for emitting tweets read from dumps
* Emits: List of tweets
* @author ikouril
*/
public class Dump{

	private int blockSize;
	BufferedReader reader;
	private int dumpId;
	
	
	/**
	 * Creates a new DumpSpout.
	 * @param blockSize granularity
	 * @param dumpId id of dump to read from
	 * @throws IOException 
	 */
	public Dump(int blockSize,int dumpId) throws IOException{
		this.blockSize=blockSize;
		this.dumpId=dumpId;
		try {
			reader=new BufferedReader(new InputStreamReader(new URL("http://athena3.fit.vutbr.cz/twitterstorm/dump.json"+dumpId+"test").openStream()));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Gets next data to be processed
	 * @return Output - type (resolves next processing step), data (list), id (of actual Output)
	 */
	public Output nextData() {
		
		
		ArrayList<Tweet> blockToSend=new ArrayList<Tweet>();
		while (blockToSend.size()<blockSize){
			String line = null;
			try {
				line = reader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (line!=null){
				JSONObject json = null;
				json = (JSONObject) JSONValue.parse(line);
				String tweetDate=(String) json.get("date");
				Date date = null;
				try {
					date = tweetDate==null?null:DateTools.stringToDate(tweetDate);
				} catch (java.text.ParseException e) {
					date=null;
				}
				Tweet tweet=new Tweet((String)json.get("text"), (String)json.get("name"), date);
				blockToSend.add(tweet);
			}
			else{
				break;
			}
		}
		
		if (blockToSend.size()>0){
			String id=UUID.randomUUID().toString();
			Output out=new Output("dump",blockToSend,id);
			return out;
		}
		else
			return null;
	}


}