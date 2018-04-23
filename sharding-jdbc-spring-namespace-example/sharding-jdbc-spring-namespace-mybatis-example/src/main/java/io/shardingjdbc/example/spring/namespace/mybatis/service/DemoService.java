package io.shardingjdbc.example.spring.namespace.mybatis.service;

import io.shardingjdbc.example.spring.namespace.mybatis.entity.Order;
import io.shardingjdbc.example.spring.namespace.mybatis.repository.OrderRepository;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Service
public class DemoService {
    
    @Resource
    private OrderRepository orderRepository;
    /*
    @Resource
    private OrderItemRepository orderItemRepository;*/
    
    public void demo() {
//        orderRepository.createIfNotExistsTable();
//        orderItemRepository.createIfNotExistsTable();
//        orderRepository.truncateTable();
//        orderItemRepository.truncateTable();
        List<Long> orderIds = new ArrayList<>(10);
        System.out.println("1.Insert--------------");
        Calendar calendar = Calendar.getInstance();
        for (int i = 0; i < 3; i++) {


            Order order = new Order();
            order.setUserId(51);
            order.setStatus("INSERT_TEST");
            order.setCountDate(calendar.getTime());
            orderRepository.insert(order);
            long orderId = order.getOrderId();
            orderIds.add(orderId);
            System.out.println("... ... ... ... "+orderId);

            calendar.add(Calendar.MONTH,1);
//            if(true) continue;
//
//            OrderItem item = new OrderItem();
//            item.setOrderId(orderId);
//            item.setUserId(51);
//            item.setStatus("INSERT_TEST");
//            orderItemRepository.insert(item);
        }
//        System.out.println(orderItemRepository.selectAll());

//        if(true) return;
//
//        System.out.println("2.Delete--------------");
//        for (Long each : orderIds) {
//            orderRepository.delete(each);
//            orderItemRepository.delete(each);
//        }
//        System.out.println(orderItemRepository.selectAll());
//        orderItemRepository.dropTable();
//        orderRepository.dropTable();
    }

    public void testFind (Date from,Date to){
        System.out.println("testFind...begin");
        List<Order> list = orderRepository.find3(from,to);
        System.out.println("testFind..."+list.toString());
    }
    public void testFind2 (Date from){
        System.out.println("testFind...begin");
        List<Order> list = orderRepository.find2(from);
        System.out.println("testFind..."+list.toString());
    }
    public void testInsertSelect (Date from,Date to){
        System.out.println("testInsertSelect...begin");
        orderRepository.insertSelect(from,to);
        System.out.println("testInsertSelect...");
    }
}
