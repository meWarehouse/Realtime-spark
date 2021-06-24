package com.at.mallpbulisher.service.impl;

import com.at.mallpbulisher.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zero
 * @create 2021-04-05 21:09
 */
@Service
public class EsServiceImpl implements EsService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {

        String indexName = "mall2021_dau_info" + date + "-query";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());

        Search search = new Search.Builder("").addIndex(indexName).addType("_doc").build();

        Long res = 0L;

        try {
            SearchResult searchResult = jestClient.execute(search);

            res = searchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }

        return res;
    }

    @Override
    public Map getDauHour(String date) {

        String indexName = "mall2021_dau_info" + date + "-query";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("groupby_hr").field("hr").size(24));

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        Map aggsMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);

            TermsAggregation groupby_hr = searchResult.getAggregations().getTermsAggregation("groupby_hr");
            if (groupby_hr != null) {
                if (groupby_hr.getBuckets() != null && groupby_hr.getBuckets().size() > 0) {
                    for (TermsAggregation.Entry bucket : groupby_hr.getBuckets()) {
                        aggsMap.put(bucket.getKey(), bucket.getCount());
                    }
                }
            }


        } catch (IOException e) {
            throw new RuntimeException("ES查询异常");
        }


        return aggsMap;
    }
}
