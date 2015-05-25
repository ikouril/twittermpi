package cz.vutbr.fit.process;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;

public class Lemma {
	private Annotator lemmatizer;
	
	public Lemma(Annotator lemmatizer){
		this.lemmatizer=lemmatizer;
	}

	public Output execute(Output in){
		ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
		ArrayList<Multiset<String>> allKeywords=new ArrayList<Multiset<String>>();
		for (int i=0;i<tweets.size();i++){
			lemmatizer.annotate(tweets.get(i).getTextAnnot());
			Multiset<String> keywords=HashMultiset.create();
			List<CoreMap> sentences = tweets.get(i).getTextAnnot().get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  String tag=token.get(PartOfSpeechAnnotation.class);
				  if (tag.equals("FW") || tag.startsWith("VB") || tag.startsWith("NN")){
					  String lemma=token.get(LemmaAnnotation.class);
					  if (lemma.length()>2)
						  keywords.add(lemma);
				  }	
	            }
			}
			allKeywords.add(keywords);
			tweets.get(i).setKeywords(keywords);
		}
		Output out=new Output("lemma",tweets,in.getId());
		return out;
	}
}
