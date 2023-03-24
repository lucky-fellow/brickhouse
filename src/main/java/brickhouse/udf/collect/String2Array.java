package brickhouse.udf.collect;

//import com.alibaba.fastjson.JSONArray;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;

/**
 * @author 周德鹏
 * @version 1.0
 * @date 2023/3/22 17:12
 * @describe
 */
@Description(name = "string2array", value = "_FUNC_(value) - Returns LIST ", extended = "Example:\n  > SELECT _FUNC_(\"[]\") FROM src LIMIT 1;\n  []")
public class String2Array extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private static final Log LOG = LogFactory.getLog(String2Array.class.getName());


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("缺失数组参数");
        }
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableStringObjectInspector);
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Text s = (Text) converters[0].convert(arguments[0].get());
        ArrayList<Text> result = new ArrayList<Text>();

        JSONArray jsonArray = null;
        try {
            jsonArray = JSONArray.parseArray(s.toString());
        } catch (Exception e) {
            LOG.warn("exception data row : " + s, e);
            return result;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            result.add(new Text(jsonArray.getString(i)));
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "array_contains(" + children[0] + ")";
    }
}
