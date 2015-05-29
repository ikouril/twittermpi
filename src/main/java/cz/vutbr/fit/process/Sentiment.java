package cz.vutbr.fit.process;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

public class Sentiment {
	private Annotator sentimentClassifier;
	
	public Sentiment(Annotator sentimentClassifier){
		this.sentimentClassifier=sentimentClassifier;
	}

	
	public Output execute(Output in){
		if (in==null)
			return null;
		
		ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
		for (int i=0;i<tweets.size();i++){
			sentimentClassifier.annotate(tweets.get(i).getTextAnnot());
			
			List<CoreMap> sentences = tweets.get(i).getTextAnnot().get(CoreAnnotations.SentencesAnnotation.class);
			int positive=0;
			int negative=0;
			String sentiment="Neutral";
			for (CoreMap sentence : sentences) {
				String result = sentence.get(SentimentCoreAnnotations.ClassName.class);
				if (result.equals("Positive"))
					positive++;
				else if (result.equals("Negative"))
					negative++;
			}
			
			if (positive>negative)
				sentiment="Positive";
			else if (negative>positive)
				sentiment="Negative";
			tweets.get(i).setSentiment(sentiment);
		}
		Output out=new Output("parse",tweets,in.getId());
		return out;
	}
}
