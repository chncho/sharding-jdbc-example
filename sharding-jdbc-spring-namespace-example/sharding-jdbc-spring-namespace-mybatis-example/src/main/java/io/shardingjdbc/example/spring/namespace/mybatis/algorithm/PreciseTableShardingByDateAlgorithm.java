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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

/**
 * 单列分表算法，按月分表
 * chao.chen
 */
public final class PreciseTableShardingByDateAlgorithm implements PreciseShardingAlgorithm<Date>{


    Logger logger = LoggerFactory.getLogger(PreciseTableShardingByDateAlgorithm.class);
    static final DateFormat dateFormat = new SimpleDateFormat("yyyy_MM");
    
    @Override
    public String doSharding(final Collection<String> availableTargetNames, final PreciseShardingValue<Date> shardingValue) {
        return buildShardingTableName(shardingValue.getLogicTableName(),shardingValue.getValue());// shardingValue.getLogicTableName()+"_"+dateFormat.format(shardingValue.getValue());
    }


    private String buildShardingTableName(String logicTableName, Date value) {
        return logicTableName+"_"+dateFormat.format(value);
    }
}
