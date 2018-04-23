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
import io.shardingjdbc.core.api.algorithm.sharding.PreciseShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.RangeShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import io.shardingjdbc.core.api.algorithm.sharding.standard.RangeShardingAlgorithm;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 单列分表算法，按月分表
 * chao.chen
 */
public final class PreciseModuloTableShardingByDateAlgorithm implements PreciseShardingAlgorithm<Date>,RangeShardingAlgorithm<Date> {


    Logger logger = LoggerFactory.getLogger(PreciseModuloTableShardingByDateAlgorithm.class);
    static final DateFormat dateFormat = new SimpleDateFormat("yyyy_MM");
    
    @Override
    public String doSharding(final Collection<String> availableTargetNames, final PreciseShardingValue<Date> shardingValue) {
        return buildShardingTableName(shardingValue.getLogicTableName(),shardingValue.getValue());// shardingValue.getLogicTableName()+"_"+dateFormat.format(shardingValue.getValue());
    }

    @Override
    public Collection<String> doSharding(Collection<String> collection, RangeShardingValue<Date> rangeShardingValue) {
        Collection<String> shardingTableCollection = new ArrayList<String>();

        if(!rangeShardingValue.getValueRange().hasLowerBound()||!rangeShardingValue.getValueRange().hasUpperBound()){
            throw new UnsupportedOperationException("sharding value must contain lowerBound and upperBound");
        }

        //1. trunc端点时间
        Calendar calendarLower = Calendar.getInstance();
        Calendar calendarUpper = Calendar.getInstance();
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
            shardingTableCollection.add(buildShardingTableName(rangeShardingValue.getLogicTableName(),calendarTemp.getTime()));
            calendarTemp.add(Calendar.MONTH,1);
        }

        logger.info("doSharding..."+shardingTableCollection.toString());//TODO for test
        return shardingTableCollection;
    }


    private String buildShardingTableName(String logicTableName, Date value) {
        return logicTableName+"_"+dateFormat.format(value);
    }
}
