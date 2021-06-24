package com.at.mallpbulisher.controller;

import com.alibaba.fastjson.JSON;
import com.at.mallpbulisher.service.ClickHouseService;
import com.at.mallpbulisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author zero
 * @create 2021-04-05 21:04
 */
@RestController
public class PublishController {



    @Autowired
    EsService esService;


    @Autowired
    ClickHouseService clickHouseService;


    @GetMapping("/realtime-total")
    public List<Map<String,Object>> realtimeTotal(@RequestParam("date") String date){

        List<Map<String,Object>> rsList=new ArrayList<>();

        Map<String,Object> dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");

        try {
            Long dauTotal = esService.getDauTotal(date);
            dauMap.put("value",dauTotal);

        } catch (Exception e) {
            e.printStackTrace();
        }

        rsList.add(dauMap);

        Map<String,Object> newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",223);

        rsList.add(newMidMap);

        Map<String,Object> orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        BigDecimal orderAmount = clickHouseService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        rsList.add(orderAmountMap);


        return rsList;

    }

    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id") String id ,@RequestParam("date") String date){
        if(id.equals("dau")){
            Map dayDau = esService.getDauHour(date);

            String yDate = yDate(date);

            Map yDayDau = esService.getDauHour(yDate);

            Map<String,Map<String,Object>> map = new HashMap<>();

            map.put("today",dayDau);
            map.put("yesterday",yDayDau);

            return JSON.toJSONString(map);
        }else if(id.equals("order_amount")){
            Map orderAmountHourMapTD = clickHouseService.getOrderAmountHour(date);
            String yd = yDate(date);
            Map orderAmountHourMapYD = clickHouseService.getOrderAmountHour(yd);

            Map<String,Map<String,BigDecimal>> rsMap=new HashMap<>();
            rsMap.put("yesterday",orderAmountHourMapYD);
            rsMap.put("today",orderAmountHourMapTD);
            return  JSON.toJSONString(rsMap);
        }

        return  null;


    }


    public String yDate(String date){

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date parse = dateFormat.parse(date);

            Date yDate = DateUtils.addDays(parse, -1);

            String format = dateFormat.format(yDate);

            return format;

        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式错误");
        }
    }


}


