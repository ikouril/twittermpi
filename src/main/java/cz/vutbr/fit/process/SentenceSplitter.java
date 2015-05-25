package cz.vutbr.fit.process;

import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.pipeline.Annotator;

public class SentenceSplitter {
	private Annotator sentenceSplitter;
	
	public SentenceSplitter(Annotator sentenceSplitter){
		this.sentenceSplitter=sentenceSplitter;
	}
	
	public Output execute(Output in){
		List<Tweet> data=(List<Tweet>) in.getData();
		for (int i=0;i<data.size();i++){
			sentenceSplitter.annotate(data.get(i).getAuthorAnnot());
			sentenceSplitter.annotate(data.get(i).getTextAnnot());
		}
		Output out=new Output("segment",data,in.getId());
		return out;
	}

}
