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

package io.shardingjdbc.example.spring.namespace.mybatis.repository;

import io.shardingjdbc.example.spring.namespace.mybatis.entity.Order;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

public interface OrderRepository {
    
    void createIfNotExistsTable();
    
    void truncateTable();

    Long insert(Order model);

    List<Order> find(@Param("from") Date from,@Param("to") Date to);
    List<Order> find2(@Param("eqDate") Date eqDate);

    List<Order> find3(@Param("from") Date from,@Param("to") Date to);


    void insertSelect(@Param("from") Date from,@Param("to") Date to);


    void delete(Long orderId);
    
    void dropTable();
}
