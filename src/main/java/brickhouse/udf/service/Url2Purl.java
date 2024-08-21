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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 周德鹏
 * @version 1.0
 * @date 2024/8/20 11:54
 * @describe
 */
@Description(name = "url2purl", value = "_FUNC_(value) - Returns string ", extended = "Example:\n  > SELECT _FUNC_(\"https://rubygems.org/gems/css_splitter\") FROM src LIMIT 1;\n  []")
public class Url2Purl extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private static final Log LOG = LogFactory.getLog(String2Array.class.getName());
    private String type_regex = "(github\\.com/)" +
            "|(mvnrepository\\.com/artifact/)" +
            "|(apache\\.org/repos/asf/)" +
            "|(pypi\\.org/)" +
            "|(npmjs\\.com/package/)" +
            "|(gitlab\\.com/)" +
            "|(gitee\\.com/)" +
            "|(nuget\\.org/packages/)" +
            "|(rubygems\\.org/gems/)" +
            "|(proxy\\.golang.org/)" +
            "|(pkg\\.go\\.dev/)" +
            "";
    private Pattern type_pattern = Pattern.compile(type_regex);

    private Pattern url_end_pattern = Pattern.compile("(.*?(@|\\?|#|$))");

    private String url2purl(String url) {
        //        https://github.com/package-url/purl-spec
        String urlLow = url.replaceAll("/$", "").trim();
//         正则提取第一个出现的关键字
        Matcher matcher = type_pattern.matcher(urlLow);
        String first_type;
        if (matcher.find()) {
            first_type = matcher.group();
        } else {
            return null;
        }

        if (first_type.equals("github.com/")) {
//          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("github.com/")[1].replace(".git", "/");
            String vendor = vendor_name.split("/")[0];
            String name = vendor_name.split("/")[1];
            return "pkg:github/" + vendor + "/" + name;
        }

        if (first_type.equals("mvnrepository.com/artifact/")) {
            //          大小写 敏感
            String vendor_name = urlLow.split("mvnrepository.com/artifact/")[1];
            String vendor = vendor_name.split("/")[0];
            String name = vendor_name.split("/")[1];
            return "pkg:maven/" + vendor + "/" + name;
        }

        if (first_type.equals("apache.org/repos/asf")) {
            //          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().replace("?p=", "/").split("apache.org/repos/asf/")[1].replace(".git", "/");
            String vendor = vendor_name.split("/")[0];
            return "pkg:apache/" + vendor;
        }

        if (first_type.equals("pypi.org/")) {
//          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("pypi.org/")[1];
            String name = vendor_name.split("/")[0];
            return "pkg:pypi/" + name;
        }

        if (first_type.equals("npmjs.com/package/")) {
            /**
             *                  npm test case
             * https://www.npmjs.com/package/@antv/s2?activeTab=versions
             * https://www.npmjs.com/package/40%antv/s2?activeTab=versions
             * https://www.npmjs.com/package/40%antv/s2
             * https://www.npmjs.com/package/vue?activeTab=versions
             * @describe
             */
            String vendor_name = urlLow.toLowerCase().split("npmjs.com/package/")[1];
            //            如果 name 首字母为@ 依据规范替换为40%
            vendor_name = vendor_name.replaceAll("^@", "40%").replaceAll("/$", "");
            Matcher matcher_npm = url_end_pattern.matcher(vendor_name);
            if (matcher_npm.find()) {
                return "pkg:npm/" + matcher_npm.group().replaceAll("[@|\\?]$", "");
            } else {
                return null;
            }
        }

        if (first_type.equals("gitlab.com/")) {
            //          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("gitlab.com/")[1].replace(".git", "/");
            String vendor = vendor_name.split("/")[0];
            String name = vendor_name.split("/")[1];
            return "pkg:gitlab/" + vendor + "/" + name;
        }

        if (first_type.equals("gitee.com/")) {
            //          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("gitee.com/")[1].replace(".git", "/");
            String vendor = vendor_name.split("/")[0];
            String name = vendor_name.split("/")[1];
            return "pkg:gitee/" + vendor + "/" + name;
        }

        if (first_type.equals("nuget.org/packages/")) {
            //          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("nuget.org/packages/")[1];
            String name = vendor_name.split("/")[0];
            return "pkg:nuget/" + name;
        }

        if (first_type.equals("rubygems.org/gems/")) {
            //          大小写 不敏感
            String vendor_name = urlLow.toLowerCase().split("rubygems.org/gems/")[1];
            String name = vendor_name.split("/")[0];
            return "pkg:gem/" + name;
        }

//        golang
        if (first_type.equals("proxy.golang.org/")) {
//            https://proxy.golang.org/github.com/alex-d/trumbowyg/@v/v2.13.0+incompatible.zip
            String vendor_name = urlLow.toLowerCase().split("proxy.golang.org/")[1];
            String name = vendor_name.split("/@v")[0];
            return "pkg:golang/" + name;
        }
        if (first_type.equals("pkg.go.dev/")) {
            /**
             *
             * https://pkg.go.dev/bitbucket.org/innius/go-sns
             * https://pkg.go.dev/bitbucket.org/innius/go-sns@v1.0.14
             * https://pkg.go.dev/github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tds@v1.0.987?tab=versions
             * https://pkg.go.dev/github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tds@v1.0.987
             * https://pkg.go.dev/k8s.io/component-helpers?tab=licenses
             */
            String vendor_name = urlLow.toLowerCase().split("pkg.go.dev/")[1];
            Matcher matcher_golang = url_end_pattern.matcher(vendor_name);
            if (matcher_golang.find()) {
                return "pkg:golang/" + matcher_golang.group().replaceAll("[@|\\?|#]$", "");
            } else {
                return null;
            }
        }
        return null;
    }

    //    入参校验
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("缺失url参数");
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
            return url2purl(input.toString());

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


    //    Test
    public static void main(String[] args) throws HiveException {
        Url2Purl url2Purl = new Url2Purl();
//        ***********github test***************
//        String testUrl = "http://github.com/pillarjs/path-to-regexp";
//        String testUrl = "http://github.com/pillarjs/path-to-regexp.git";
//        String testUrl = "http://github.com/pillarjs/git";
//        String testUrl = "http://github.com/pillarjs/git/aa//bb";


//        ***********maven test***************
//        https://repo1.maven.org/maven2/org/apache/rocketmq/rocketmq-broker/4.9.7/rocketmq-broker-4.9.7.jar
//        String testUrl = "https://mvnrepository.com/artifact/com.kpouer/k-mapview/1.1.0";


//        ***********npm test***************
//        String testUrl = "https://www.npmjs.com/package/vue-router";
//        String testUrl = "https://rubygems.org/gems/css_splitter";
        String testUrl = "https://www.npmjs.com/package/@antv/s2";
//        https://www.npmjs.com/package/@antv/s2?activeTab=versions


//        ***********go test***************
//            https://pkg.go.dev/github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tds@v1.0.987?tab=versions
//            https://pkg.go.dev/github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tds@v1.0.987
//
//        String testUrl = "https://proxy.golang.org/github.com/alex-d/trumbowyg";
//        String testUrl = "https://proxy.golang.org/github.com/alex-d/trumbowyg/@v/v2.13.0+incompatible.zip";
//        String testUrl = "https://pkg.go.dev/bitbucket.org/innius/go-sns";
//        String testUrl = "https://pkg.go.dev/bitbucket.org/innius/go-sns@v1.0.14";
//        String testUrl = "https://pkg.go.dev/github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tds@v1.0.987?tab=versions";
//        String testUrl = "https://pkg.go.dev/k8s.io/component-helpers?tab=licenses";
//
        Object evaluate = url2Purl.url2purl(testUrl);
        System.out.println(evaluate);


    }
}
