package com.dc.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * UDF to generate all dates given [baseDate, count, dateformat]
 * dateFormat is expected to be as per Joda specs.
 */
public final class DateGenerator extends GenericUDF
{

	Converter[] converters = null;
	StringObjectInspector inputOI = null;
	List<Object> dates = new ArrayList<>();

	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
		if(objectInspectors.length != 3) {
			throw new UDFArgumentException("All params are required: baseDate, count and dateFormat");
		}

		if (!(objectInspectors[0] instanceof StringObjectInspector)
				|| !(objectInspectors[1] instanceof StringObjectInspector)
				|| !(objectInspectors[2] instanceof StringObjectInspector)) {
			throw new UDFArgumentException("All input should be of type STRING");
		}

		this.inputOI = (StringObjectInspector) objectInspectors[0];
		this.converters = new Converter[objectInspectors.length];
		converters[0] = ObjectInspectorConverters.getConverter(inputOI, PrimitiveObjectInspectorFactory.writableStringObjectInspector);

		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		String start = inputOI.getPrimitiveJavaObject(deferredObjects[0].get());
		String counter = inputOI.getPrimitiveJavaObject(deferredObjects[1].get());
		String dateFormat = inputOI.getPrimitiveJavaObject(deferredObjects[2].get());

		if (start == null || counter == null || dateFormat == null) {
			return null;
		}

		DateTimeFormatter formatter = DateTimeFormat.forPattern(dateFormat);
		DateTime baseDate = null;
		Integer count = null;
		DateTime endDate = null;
		try {
			baseDate = formatter.parseDateTime(start);
			count = Integer.valueOf(counter);
		} catch (IllegalArgumentException e) {
			System.err.println("ERROR: DateRangeToDateArray - can't parse baseDate or count: " + baseDate + " " + count);
			baseDate = null;
		}

		if ( baseDate == null || count == null ) {
			return null;
		}
		if(count<0){
			endDate = baseDate;
			baseDate = baseDate.minusDays(-1*count);
		}else{
			endDate = baseDate.plusDays(count);
		}
		return getDatesArrayForRange(baseDate, endDate, formatter);
	}

	private List<Object> getDatesArrayForRange(DateTime start,
			DateTime end, DateTimeFormatter formatter) {
		
		dates.clear();
		while (start.isBefore(end) || start.isEqual(end)) {
			Text dateAsText = new Text(start.toString(formatter));
			dates.add(dateAsText);
			start = start.plusDays(1);
		}
		return dates;
	}
	
	@Override
	public String getDisplayString(String[] strings) {
		return "Generate all dates given [baseDate, count, dateformat]";
	}
}
