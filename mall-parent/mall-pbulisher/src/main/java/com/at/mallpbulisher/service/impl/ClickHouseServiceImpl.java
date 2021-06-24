package com.at.mallpbulisher.service.impl;

import com.at.mallpbulisher.mapper.OrderWideMapper;
import com.at.mallpbulisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zero
 * @create 2021-04-09 17:04
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {


    @Autowired
    OrderWideMapper OrderWideMapper;


    @Override
    public BigDecimal getOrderAmount(String date) {
        return OrderWideMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //进行转换
//        List<Map>     [{"hr":10,"order_amount":1000.00} ,{"hr":11,"order_amount":2000.00}...... ]
//        Map    {"10":1000.00,"11":2000.00,...... }
        List<Map> mapList = OrderWideMapper.selectOrderAmountHour(date);
        Map<String,BigDecimal> hourMap=new HashMap<>();
        for (Map map : mapList) {
            hourMap.put(  String.valueOf(map.get("hr"))  , (BigDecimal) map.get("order_amount"));
        }
        return hourMap;
    }
}
