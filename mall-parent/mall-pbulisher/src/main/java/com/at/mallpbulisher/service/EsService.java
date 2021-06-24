package com.at.mallpbulisher.service;

import java.util.Map;

/**
 * @author zero
 * @create 2021-04-05 21:09
 */
public interface EsService {

    //日活的总数查询
    public   Long getDauTotal(String date);
    //日活的分时查询
    public Map getDauHour(String date);

}
