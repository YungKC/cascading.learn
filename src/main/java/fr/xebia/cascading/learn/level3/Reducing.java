package fr.xebia.cascading.learn.level3;

import cascading.flow.FlowDef;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Once each input has been individually curated, it can be needed to aggregate information.
 */
public class Reducing {
	
	/**
	 * {@link GroupBy} "word" and then apply {@link Count}. It should be noted
	 * that once grouped, the semantic is different. You will need to use a
	 * {@link Every} instead of a {@link Each}. And {@link Count} is an
	 * {@link Aggregator} instead of a {@link Function}.
	 * 
	 * source field(s) : "word"
	 * sink field(s) : "word","count"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef aggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
	    Fields word = new Fields( "word" );

		Pipe pipe = new Pipe("wc");
		pipe = new GroupBy( pipe, word);

	    pipe = new Every(pipe, word, new Count(), Fields.ALL);

	    
		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);
	}
	
	/**
	 * Aggregation should be done as soon as possible and Cascading does have a technique almost similar to map/reduce 'combiner'.
	 * Use {@link CountBy} in order to do the same thing as above. It is shorter to write and more efficient.
	 * 
	 * source field(s) : "word"
	 * sink field(s) : "word","count"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s08.html
	 */
	public static FlowDef efficientlyAggregate(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
	    Fields word = new Fields( "word" );
	    Fields count = new Fields("count");
	    
		Pipe pipe = new Pipe("wc");
		pipe = new CountBy( pipe, word, count);

	    
		return FlowDef.flowDef()//
				.addSource(pipe, source) //
				.addTail(pipe)//
				.addSink(pipe, sink);
	}
}
