package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Fields lineField = new Fields("line");
		Fields wordField = new Fields( "word" );
		Fields countField = new Fields("count");
		Pipe assembly = new Pipe("assembly");
		
		Function toLowerCase = new ToLowerCaseFunction(lineField);
		assembly = new Each(assembly, lineField, toLowerCase, Fields.REPLACE);
		RegexSplitGenerator splitter = new RegexSplitGenerator(wordField, "[\\s\\(\\)\\[\\],.'//]");
		assembly = new Each(assembly, lineField, splitter, Fields.RESULTS);
		
		Filter badStringFilter = new BadStringFilter();
		assembly = new Each(assembly, wordField, badStringFilter);
		
		Pipe wcPipe = new Pipe("wc", assembly);
		wcPipe = new GroupBy(wcPipe, wordField);
		wcPipe = new Every(wcPipe, wordField, new Count(), Fields.ALL);
		
		return FlowDef.flowDef().addSource(assembly, source)//
				.addTailSink(wcPipe, sink);
	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s07.html
	 * 
	 * {@link First} or {@link Max} can be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		return null;
	}
	
}
