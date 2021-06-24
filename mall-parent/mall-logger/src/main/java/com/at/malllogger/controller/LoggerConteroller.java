package com.at.malllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zero
 * @create 2021-04-03 21:11
 */
@Slf4j
@RestController
public class LoggerConteroller {

    @Autowired
    KafkaTemplate KafkaTemplate;

    @RequestMapping("/applog")
    public String log(@RequestBody String logString){
//        System.out.println(log);

        log.info(logString);

        JSONObject jsonObject = JSON.parseObject(logString);

        //kafka 分流
        if(jsonObject.get("start") != null && jsonObject.get("start").toString().length() > 0){

            //启动日志
            KafkaTemplate.send("GMALL_START",logString);

        }else{

            //事件日志
            KafkaTemplate.send("GMALL_EVENT",logString);

        }


        return logString;
    }

}
