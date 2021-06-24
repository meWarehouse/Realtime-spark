package com.at.mallpbulisher.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author zero
 * @create 2021-04-09 17:03
 */
@Mapper
public interface OrderWideMapper {

    //查询当日总额
    public BigDecimal selectOrderAmount(String date);

    //查询当日分时交易额
    public List<Map> selectOrderAmountHour(String date);
}
