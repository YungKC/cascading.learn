package fr.xebia.cascading.learn.level4;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch03s03.html
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
		
		Fields common = new Fields( "year" );
		Fields declared = new Fields(
		  "yearPres", "president", "yearParty", "party"
		);
		
		Pipe presidentPipe = new Pipe("presidentPipe");
		Pipe partiesPipe = new Pipe("partiesPipe");
		Pipe joinpipe =
		  new CoGroup( presidentPipe, common, partiesPipe, common, declared, new InnerJoin() );
		
        joinpipe = new Discard( joinpipe, new Fields( "yearPres", "yearParty" ) );

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put("presidentPipe", presidentsSource);
        sources.put("partiesPipe", partiesSource);
        
		return FlowDef.flowDef().addSources(sources)//
				.addTail(joinpipe)//
				.addSink(joinpipe, sink);	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.5/userguide/html/ch08s07.html
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {

		Pipe pipe = new Pipe( "head" );
		Fields party = new Fields("party");
		
		Pipe gaullistPipe = new Pipe("gaullistPipe", pipe);
		Pipe republicanPipe = new Pipe("republicanPipe", pipe);
		Pipe socialistPipe = new Pipe("socialistPipe", pipe);
		gaullistPipe = new Each(gaullistPipe, party, new ExpressionFilter("!\"Gaullist\".equals(party)", String.class));
		republicanPipe = new Each(republicanPipe, party, new ExpressionFilter("!\"Republican\".equals(party)", String.class));
		socialistPipe = new Each(socialistPipe, party, new ExpressionFilter("!\"Socialist\".equals(party)", String.class));


		return FlowDef.flowDef().addSource(pipe, source)//
				.addTailSink(gaullistPipe, gaullistSink)//
				.addTailSink(republicanPipe, republicanSink)//
				.addTailSink(socialistPipe, socialistSink);

	}
	
}