package com.at.mallpbulisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author zero
 * @create 2021-04-09 17:03
 */
public interface ClickHouseService {

    public BigDecimal getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
