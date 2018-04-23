/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.example.spring.namespace.mybatis.algorithm;

import com.google.common.collect.BoundType;
import io.shardingjdbc.core.api.algorithm.sharding.ListShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.PreciseShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.RangeShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.ShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.complex.ComplexKeysShardingAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 多列分表算法，按月分表
 * chao.chen
 */
public final class PreciseTableComplexShardingAlgorithm implements ComplexKeysShardingAlgorithm  {

    Logger logger = LoggerFactory.getLogger(PreciseModuloTableShardingByDateAlgorithm.class);
    static final DateFormat dateFormat = new SimpleDateFormat("yyyyMM");

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, Collection<ShardingValue> shardingValues) {
        Collection<String> arr = new HashSet<>();

        if(null==shardingValues || shardingValues.size()==0){
            throw new UnsupportedOperationException("shardingValues is empty");
        }

        for(ShardingValue shardingValue:shardingValues){
            if(shardingValue instanceof PreciseShardingValue){
                arr.add(this.doSharding(availableTargetNames,(PreciseShardingValue)shardingValue));
            }else if(shardingValue instanceof ListShardingValue){
                arr.addAll(this.doSharding(availableTargetNames,(ListShardingValue)shardingValue));
            }else if(shardingValue instanceof  RangeShardingValue){
                arr.addAll(this.doSharding(availableTargetNames,(RangeShardingValue)shardingValue));
            }else{
                throw  new UnsupportedOperationException("this shardingValue instance has not been surpported ... "+ shardingValue);
            }
        }

        return arr;
    }


    private Collection<String> doSharding(Collection<String> availableTargetNames, ListShardingValue shardingValue) {
        Collection<String> arr = new HashSet<String>();
        Collection collection = shardingValue.getValues();
        for(Object obj : collection){
            arr.add(buildShardingTableName(shardingValue.getLogicTableName(),shardingValue.getColumnName(),obj));
        }
        return arr;
    }

    /**
     *
     * @param availableTargetNames
     * @param shardingValue
     * @return
     */
    public String doSharding(final Collection<String> availableTargetNames, PreciseShardingValue shardingValue) {
        return buildShardingTableName(shardingValue.getLogicTableName(), shardingValue.getColumnName(), shardingValue.getValue());// shardingValue.getLogicTableName()+"_"+dateFormat.format(shardingValue.getValue());
    }

    /**
     * 只有countDate会有该类型操作
     * @param collection
     * @param rangeShardingValue
     * @return
     */
    public Collection<String> doSharding(Collection<String> collection, RangeShardingValue<Date> rangeShardingValue) {
        Collection<String> shardingTableCollection = new ArrayList<String>();

        if(!rangeShardingValue.getValueRange().hasLowerBound()||!rangeShardingValue.getValueRange().hasUpperBound()){
            throw new UnsupportedOperationException("sharding value must contain lowerBound and upperBound");
        }

        //1. trunc端点时间
        Calendar calendarLower = Calendar.getInstance();
        Calendar calendarUpper = Calendar.getInstance();
        calendarLower.setTime(rangeShardingValue.getValueRange().lowerEndpoint());
        calendarUpper.setTime(rangeShardingValue.getValueRange().upperEndpoint());
        calendarLower.set(calendarLower.get(Calendar.YEAR),calendarLower.get(Calendar.MONTH),1,0,0);
        calendarUpper.set(calendarUpper.get(Calendar.YEAR),calendarUpper.get(Calendar.MONTH),1,0,0);

        //2. 端点定位
        Calendar calendarShardingBegin = Calendar.getInstance();
        Calendar calendarShardingEnd = Calendar.getInstance();

        if(BoundType.CLOSED==rangeShardingValue.getValueRange().lowerBoundType()) {//lower compare
            calendarShardingBegin.setTime(calendarLower.getTime());
        }else{
            throw new UnsupportedOperationException("sharding value lower endpoint must be closed");
        }

        if(BoundType.CLOSED==rangeShardingValue.getValueRange().upperBoundType()) {//upper compare
            calendarShardingEnd.setTime(calendarUpper.getTime());
        }else{
            if(rangeShardingValue.getValueRange().upperEndpoint().compareTo(calendarUpper.getTime())==0){
                calendarShardingEnd.setTime(calendarUpper.getTime());
            }else {
                calendarShardingEnd.setTime(calendarUpper.getTime());
                calendarShardingEnd.add(Calendar.MONTH,1);
            }
        }
        //3. 拼实体表列表
        Calendar calendarTemp = Calendar.getInstance();
        calendarTemp.setTime(calendarShardingBegin.getTime());
        while(calendarTemp.compareTo(calendarLower)>=0 && calendarTemp.compareTo(calendarUpper)<=0){
            shardingTableCollection.add(buildShardingTableName(rangeShardingValue.getLogicTableName(),rangeShardingValue.getColumnName(),calendarTemp.getTime()));
            calendarTemp.add(Calendar.MONTH,1);
        }

        logger.info("doSharding..."+shardingTableCollection.toString());//TODO for test
        return shardingTableCollection;
    }
    private String buildShardingTableName(String logicTableName, String columnName, Object value) {
        if("COUNT_DATE".equals(columnName.toUpperCase())){
            return logicTableName+"_"+dateFormat.format(value);
        }else if("ID".equals(columnName.toUpperCase())){
            return logicTableName+"_"+value.toString().substring(0,6);
        }else if("SYSTEM_FLOW_ID".equals(columnName.toUpperCase())){//TODO 改成真实的字段名
            return logicTableName+"_"+value.toString().substring(0,6);
        }else {
            throw new UnsupportedOperationException("this column is not unsupported to sharding");
        }
    }
}
