package brickhouse.udf.service;

import brickhouse.udf.collect.String2Array;
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

/**
 * @author 周德鹏
 * @version 1.0
 * @date 2024/8/20 11:54
 * @describe
 */
@Description(name = "url2home_page", value = "_FUNC_(value) - Returns string ", extended = "Example:\n  > SELECT _FUNC_(\"pkg:maven/aa/bb\") FROM src LIMIT 1;\n  []")
public class Purl2HomePage extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private static final Log LOG = LogFactory.getLog(String2Array.class.getName());

    //    入参校验
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("缺失Purl参数");
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

    //    执行转换逻辑
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Text input = (Text) converters[0].convert(arguments[0].get());
        try {
            return purl2url(input.toString());

        } catch (Exception e) {
            return null;
        }
    }

    // 显示函数说明
    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "array_contains(" + children[0] + ") \n version :v1.0.20240820";
    }

    private String purl2url(String url) {
        //        https://github.com/package-url/purl-spec
        String urlLow = url.replaceAll("/$", "") + "/";
        if (urlLow.startsWith("pkg:github/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String vendor = vendor_name[1];
            String name = vendor_name[2];
            return "https://github.com/" + vendor + "/" + name;
        }
        if (urlLow.contains("pkg:maven/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String vendor = vendor_name[1];
            String name = vendor_name[2];
            return "https://mvnrepository.com/artifact/" + vendor + "/" + name;
        }
        if (urlLow.startsWith("pkg:pypi/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String name = vendor_name[1];
            return "https://pypi.org/project/" + name;
        }
        if (urlLow.startsWith("pkg:npm/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String name = vendor_name[1];
            //            如果 name 首字母为@ 依据规范替换为40%
            name = name.replaceAll("^@", "40%");
            return "https://www.npmjs.com/package/" + name;
        }
        //        以上为主流的四个


        if (urlLow.startsWith("pkg:gitlab/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String vendor = vendor_name[1];
            String name = vendor_name[2];
            return "https://gitlab.com/" + vendor + "/" + name;
        }
        if (urlLow.startsWith("pkg:gitee/")) {
            //          大小写不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String vendor = vendor_name[1];
            String name = vendor_name[2];
            return "https://gitee.com/" + vendor + "/" + name;
        }

//          when url like 'https://www.nuget.org/packages/%' then concat('pkg:nuget/',split(purl_1,'/')[3])
//            when url like 'https://rubygems.org/gems/%' then concat('pkg:gem/',split(purl_1,'/')[3])
        if (urlLow.startsWith("pkg:nuget/")) {
//          大小写 不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String name = vendor_name[1];
            return "https://www.nuget.org/packages/" + name;
        }
        if (urlLow.startsWith("pkg:gem/")) {
            //          大小写 不敏感
            String[] vendor_name = urlLow.toLowerCase().split("/");
            String name = vendor_name[1];
            return "https://rubygems.org/gems/" + name;
        }
        return null;
    }

    //    Test
//    public static void main(String[] args) throws HiveException {
//        Purl2Url purl2Url = new Purl2Url();
//        String testUrl = "pkg:gem/css_splitter";
//        String url = purl2Url.purl2url(testUrl);
//        System.out.println(url);
//    }
}
