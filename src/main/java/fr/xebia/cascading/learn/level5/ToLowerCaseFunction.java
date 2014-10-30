package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ToLowerCaseFunction extends BaseOperation implements Function {
	public ToLowerCaseFunction(Fields fieldDeclaration) {
		super(1, fieldDeclaration);
	}
	
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
	    TupleEntry argument = functionCall.getArguments();
	    String token = toLowerCase( argument.getString( 0 ) );

	    if( token.length() > 0 )
	      {
	      Tuple result = new Tuple();
	      result.add( token );
	      functionCall.getOutputCollector().add( result );
	      }
	}

	private String toLowerCase(String text) {
		return text.trim().toLowerCase();
	}
}
