package cz.vutbr.fit.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import com.google.common.collect.Multiset;
import cz.vutbr.fit.util.InfoHolder;
import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;


public class Index {
	IndexWriterConfig conf=null;
    Directory directory=null;
    IndexWriter iw=null;
    IndexReader ir=null;
    Analyzer analyzer=null;
    Map<String,ArrayList<InfoHolder>> infos=new HashMap<String,ArrayList<InfoHolder>>();
    
    public Index(){
    	if (analyzer==null)
			analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		if (conf==null)
			conf = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
		if (directory==null){
				directory = new RAMDirectory();
		}
		if (iw==null){
	        try {
				iw = new IndexWriter(directory, conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			iw.commit();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }
    
    public void execute(Output in){
    	
    	String streamID=in.getType();
    	
    	if (streamID.equals("filter")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
			String id=(String) in.getId();
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setTweet(tweets.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setTweet(tweets.get(i));
				}
				checkHolders(holders,id);
			}
		}
		else if (streamID.equals("gender")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
			String id=(String) in.getId();
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setGender(tweets.get(i).getGender());
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setGender(tweets.get(i).getGender());
				}
				checkHolders(holders,id);
			}
		}
		else if (streamID.equals("lemma")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
			String id=(String) in.getId();
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setKeywords(tweets.get(i).getKeywords());
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setKeywords(tweets.get(i).getKeywords());
				}
				checkHolders(holders,id);
			}
		}
		else if (streamID.equals("ner")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
			String id=(String) in.getId();
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setPersons(tweets.get(i).getPerson());
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setPersons(tweets.get(i).getPerson());
				}
				checkHolders(holders,id);
			}
		}
		else if (streamID.equals("sentiment")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
			String id=(String) in.getId();
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setSentiment(tweets.get(i).getSentiment());
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setSentiment(tweets.get(i).getSentiment());
				}
				checkHolders(holders,id);
			}
		}
    	
    }
    
    private void checkHolders(ArrayList<InfoHolder> holders,String id) {
		if (holdersComplete(holders)){
			for (int i=0;i<holders.size();i++){
				Tweet tweet=holders.get(i).getResult();
				String game=tweet.getGame();
				String author=tweet.getAuthor();
				String text=tweet.getText();
				Date date=tweet.getDate();
				String datadisc=tweet.getDatadisc();
				String part=tweet.getPart();
				String variant=tweet.getVariant();
				String dateString=date==null?"null":DateTools.dateToString(date, DateTools.Resolution.SECOND);
				String person=tweet.getPerson();
				String sentiment=tweet.getSentiment();
				String gender=tweet.getGender();
				Multiset<String> originalKeywords=tweet.getKeywords();
				String keywords="";
				for (String keyword:originalKeywords.elementSet()){
					if (keywords.isEmpty())
						keywords=keyword;
					else
						keywords+=", "+keyword;
				}

				Document document=new Document();
				document.add(new Field("game",game, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("datadisc",datadisc, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("part",part, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("variant",variant, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("author",author, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("text",text, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("date",dateString, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("person",person, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("keywords",keywords, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("sentiment",sentiment, Field.Store.YES, Field.Index.ANALYZED));
				document.add(new Field("gender",gender, Field.Store.YES, Field.Index.ANALYZED));
				try {
					iw.addDocument(document);
					iw.commit();
				} catch (IOException e) {
					e.printStackTrace();
				}
				

			}
			infos.remove(id);
		}
		
	}
    
    private boolean holdersComplete(ArrayList<InfoHolder> holders){
		for (int i=0;i<holders.size();i++){
			if (!holders.get(i).isComplete())
				return false;
		}
		return true;
	}

}
