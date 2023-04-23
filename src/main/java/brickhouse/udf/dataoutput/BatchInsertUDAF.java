package brickhouse.udf.dataoutput;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Description(name = "batch_insert",
        value = "_FUNC_(x) - Returns an array of all the elements in the aggregation group "
)
public class BatchInsertUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = LoggerFactory.getLogger(BatchInsertUDAF.class.getName());
//    java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazy.LazyString cannot be cast to org.apache.hadoop.io.Text

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) {
//        todo 参数校验
        return new GenericUDAFBatchInsertEvaluator();
    }

    public static class GenericUDAFBatchInsertEvaluator extends GenericUDAFEvaluator {
        StandardListObjectInspector internalMergeOI;
        StandardStructObjectInspector listElementObjectInspector;
        ObjectInspector[] inputOIs;
        ObjectInspector[] outputOIs;

        static class ArrayAggBuffer implements GenericUDAFEvaluator.AggregationBuffer {
            //            合并中间结果集
            List<Object[]> result = new ArrayList<>();
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                LOG.debug("jdbc udaf :init-1" + "\t" + mode + "\t" + parameters[0].getClass());
                int length = parameters.length;
                inputOIs = new ObjectInspector[length];
//                第一阶段init  传入数据是 parameters 数组 元素为可变参数
//                返回为list<Union>
                List<String> fieldName = new ArrayList<>(length);
                outputOIs = new ObjectInspector[length];
                for (int i = 0; i < length; i++) {
                    fieldName.add("col" + i);
                    inputOIs[i] = parameters[i];
                    outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
                }
                List<ObjectInspector> fieldOIs = Arrays.asList(inputOIs);
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs));

            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    LOG.debug("jdbc udaf :init-2.1" + "\t" + mode + "\t" + parameters[0].getClass());
                    int length = parameters.length;
                    inputOIs = new ObjectInspector[length];
                    //no map aggregation. 没有数据
                    List<String> fieldName = new ArrayList<>(length);
                    outputOIs = new ObjectInspector[length];
                    for (int i = 0; i < parameters.length; i++) {
                        ObjectInspector standardObjectInspector = ObjectInspectorUtils.getStandardObjectInspector(parameters[i]);
                        inputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(standardObjectInspector);
                        outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
                        fieldName.add("col" + i);
                    }
                    List<ObjectInspector> fieldOIs = Arrays.asList(inputOIs);
                    if (mode == Mode.FINAL) {
                        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
                    }
//                    这里返回的不是单条的类型 而是聚合后的 list
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                            ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs));
                } else {
                    LOG.debug("jdbc udaf :init-2.2" + "\t" + mode + "\t" + parameters[0].getClass());
                    //                第二阶段init  传入数据是 parameters 数组 元素为一个，第一个为聚合数据list<list<Struct>>
//用于merge 的数据解释器
//                   parameters==> list<list<Struct<name,value>>>
//                   internalMergeOI==> list<Struct<name,value>>
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
//                    仍然是一个list
//                    listElementObjectInspector ==>Struct<name,value>
//                    inputOIs ==><name,value> StandardStructObjectInspector not to StandardListObjectInspector
                    listElementObjectInspector = (StandardStructObjectInspector) internalMergeOI.getListElementObjectInspector();
                    List<? extends StructField> allStructFieldRefs = listElementObjectInspector.getAllStructFieldRefs();
                    int length = allStructFieldRefs.size();
                    inputOIs = new ObjectInspector[length];
                    outputOIs = new ObjectInspector[length];
                    for (int i = 0; i < allStructFieldRefs.size(); i++) {
                        inputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(allStructFieldRefs.get(i).getFieldObjectInspector());
                        outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
                    }
                    StandardListObjectInspector loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    if (mode == Mode.FINAL) {
                        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
                    }
                    return loi;
                }
            }
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            LOG.debug("jdbc udaf :iterate begin" + "\t");
            LOG.debug("jdbc udaf :iterate begin parameters length:" + "\t" + parameters.length);
            LOG.debug("jdbc udaf :iterate begin inputOIs length:" + "\t" + inputOIs.length);
            Object[] singleData = new Object[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                LOG.debug("jdbc udaf :iterate begin inputOIs i" + i + "class:" + "\t" + inputOIs[i].getClass());
//                org.apache.hadoop.hive.serde2.lazy.LazyString cannot be cast to org.apache.hadoop.io.Text
                Object lazy = ObjectInspectorUtils.copyToStandardObject(parameters[i], inputOIs[i]);
                LOG.debug("jdbc udaf :iterate begin lazy class:" + "\t" + lazy.getClass());
//                singleData[i] = lazy;
                singleData[i] = parameters[i];
            }
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
//            单条记录
            myagg.result.add(singleData);
            LOG.debug("jdbc udaf :iterate end myagg.result:" + "\t" + myagg.result.size());
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            LOG.debug("jdbc udaf :terminatePartial" + "\t");
//            // return value goes here
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            List<Object> res = new ArrayList<>(myagg.result.size());
            for (Object object : myagg.result) {
                Object[] singleData = (Object[]) object;
                res.add(singleData);
            }
            return res;
        }


        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            LOG.debug("jdbc udaf :merge" + "\t");
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            List<Object> sub;
            if (partial instanceof List) {
                sub = (List) ObjectInspectorUtils.copyToStandardObject(partial, internalMergeOI);
            } else if (partial instanceof LazyBinaryArray) {
                sub = ((LazyBinaryArray) partial).getList();
            } else {
                LOG.error("zzzzzzzzzz:merge error" + "\t");
                throw new HiveException("Invalid >> type: " + partial.getClass().getName());
            }
            LOG.debug("jdbc udaf :merge: myagg.result.size() begin" + "\t" + myagg.result.size());
            putIntoSet(sub, myagg);
            LOG.debug("jdbc udaf :merge: myagg.result.size() end" + "\t" + myagg.result.size());
        }

        //        最终阶段
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LOG.debug("jdbc udaf :terminate" + "\t");
            // final return value goes here
            ArrayAggBuffer myagg = (ArrayAggBuffer) agg;
            ArrayList<Object> res = new ArrayList<Object>(myagg.result.size());
            res.addAll(myagg.result);

            LOG.debug("jdbc udaf : length" + myagg.result.size() + "\t" + res.size());
            Object o1 = ObjectInspectorUtils.copyToStandardJavaObject(myagg.result.get(0)[0], inputOIs[0]);
            Object o2 = ObjectInspectorUtils.copyToStandardJavaObject(myagg.result.get(0)[1], inputOIs[1]);
            LOG.debug("jdbc udaf : peek" + o1 + "\to2: " + o2);
            return res.size();
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LOG.debug("jdbc udaf :AggregationBuffer begin" + "\t");
            AggregationBuffer buff = new ArrayAggBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            LOG.debug("jdbc udaf :reset begin" + "\t");
            ArrayAggBuffer arrayBuff = (ArrayAggBuffer) agg;
            arrayBuff.result = new ArrayList();
        }


        //        N 多条merge
        private void putIntoSet(List<Object> inputObjects, ArrayAggBuffer myagg) {
            LOG.debug("jdbc udaf :putIntoSet begin" + "\t");
            for (int i = 0; i < inputObjects.size(); i++) {
                Object singledata = inputObjects.get(i);
                assert singledata instanceof LazyBinaryStruct;
                ArrayList<Object> fieldsAsList = ((LazyBinaryStruct) singledata).getFieldsAsList();

                Object[] res = new Object[fieldsAsList.size()];
                for (int j = 0; j < fieldsAsList.size(); j++) {
                    Object field = fieldsAsList.get(j);
                    res[j] = ObjectInspectorUtils.copyToStandardObject(field, inputOIs[j]);
                }
                myagg.result.add(res);
            }
        }


    }
}
