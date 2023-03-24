package brickhouse.udf.collect;



import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 周德鹏
 * @version 1.0
 * @date 2023/3/22 17:12
 * @describe
 */
@Description(name = "string2map", value = "_FUNC_(value) - Returns MAP ", extended = "Example:\n  > SELECT _FUNC_(\"{}\") FROM src LIMIT 1;\n  {}")
public class String2Map extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private static final Log LOG = LogFactory.getLog(String2Map.class.getName());


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
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Text s = (Text) converters[0].convert(arguments[0].get());
        Map<String, Object> result = new HashMap<>();

        try {
            result = JSONObject.parseObject(s.toString()).getInnerMap();
        } catch (Exception e) {
            LOG.warn("exception data row : " + s, e);
            return result;
        }
        for (String key : result.keySet()) {
            result.put(key, result.getOrDefault(key, "").toString());
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "string2map";
    }
}
