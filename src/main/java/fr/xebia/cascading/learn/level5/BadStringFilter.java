package fr.xebia.cascading.learn.level5;

import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class BadStringFilter extends BaseOperation implements Filter {
	private final Pattern pattern = Pattern.compile("[0-9]");
	
	public BadStringFilter() {
		super(1);
	}

	public boolean isRemove(FlowProcess flowProcess, FilterCall call) {
		// get the arguments TupleEntry
		TupleEntry arguments = call.getArguments();

		// filter out the current Tuple if the first argument length is greater
		// than the second argument integer value
		String word = arguments.getString(0);
		return word.length() == 0 || pattern.matcher(word).find();
	}
}
