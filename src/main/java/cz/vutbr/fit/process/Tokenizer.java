package cz.vutbr.fit.process;

import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.pipeline.Annotator;

public class Tokenizer {
	private Annotator tokenizer;
	
	public Tokenizer(Annotator tokenizer){
		this.tokenizer=tokenizer;
	}
	
	public Output execute(Output in){
		List<Tweet> data=(List<Tweet>) in.getData();
		for (int i=0;i<data.size();i++){
			tokenizer.annotate(data.get(i).getAuthorAnnot());
			tokenizer.annotate(data.get(i).getTextAnnot());
		}
		Output out=new Output("tokenize",data,in.getId());
		return out;
	}

}
