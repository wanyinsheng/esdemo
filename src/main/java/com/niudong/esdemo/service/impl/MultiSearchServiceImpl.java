package com.niudong.esdemo.service.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.MultiSearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述批量搜索相关API使用和测试
 *
 */
@Service
public class MultiSearchServiceImpl implements MultiSearchService {
  private static Log log = LogFactory.getLog(MultiSearchServiceImpl.class);

  private RestHighLevelClient restClient;

  // 初始化连接
  @PostConstruct
  public void initEs() {
    restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

    log.info("ElasticSearch init in service.");
  }

  // 关闭连接
  public void closeEs() {
    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 构建MultiSearchRequest实例
  public void buildMultiSearchRequest() {
    // 构建MultiSearchRequest实例
    MultiSearchRequest request = new MultiSearchRequest();

    // 构建搜索请求对象1
    SearchRequest firstSearchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("user", "niudong1"));
    firstSearchRequest.source(searchSourceBuilder);
    // 将搜索请求对象1 添加到 MultiSearchRequest实例 中
    request.add(firstSearchRequest);

    // 构建搜索请求对象2
    SearchRequest secondSearchRequest = new SearchRequest();
    searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("user", "niudong2"));
    secondSearchRequest.source(searchSourceBuilder);
    // 将搜索请求对象2 添加到 MultiSearchRequest实例 中
    request.add(secondSearchRequest);

  }


  // 构建MultiSearchRequest实例
  public MultiSearchRequest buildMultiSearchRequest(String field, String[] keywords) {
    // 构建MultiSearchRequest实例
    MultiSearchRequest request = new MultiSearchRequest();

    // 构建搜索请求对象1
    SearchRequest firstSearchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery(field, keywords[0]));
    firstSearchRequest.source(searchSourceBuilder);
    // 将搜索请求对象1 添加到 MultiSearchRequest实例 中
    request.add(firstSearchRequest);

    // 构建搜索请求对象2
    SearchRequest secondSearchRequest = new SearchRequest();
    searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery(field, keywords[1]));
    secondSearchRequest.source(searchSourceBuilder);
    // 将搜索请求对象2 添加到 MultiSearchRequest实例 中
    request.add(secondSearchRequest);

    return request;
  }

  // 同步执行MultiSearchRequest
  public void executeMultiSearchRequest(String field, String[] keywords) {
    // 构建MultiSearchRequest实例
    MultiSearchRequest request = buildMultiSearchRequest(field, keywords);

    try {
      MultiSearchResponse response = restClient.msearch(request, RequestOptions.DEFAULT);

      // 解析返回结果MultiSearchResponse
      processMultiSearchResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析返回结果MultiSearchResponse
  private void processMultiSearchResponse(MultiSearchResponse response) {
    // 获取返回结果集合
    Item[] items = response.getResponses();

    // 判断返回结果集合是否为空
    if (items == null || items.length <= 0) {
      log.info("items is null.");
      return;
    }

    for (Item item : items) {
      Exception exception = item.getFailure();
      if (exception != null) {
        log.info("eception is " + exception.toString());
      }

      SearchResponse searchResponse = item.getResponse();
      SearchHits hits = searchResponse.getHits();
      if (hits.getTotalHits().value <= 0) {
        log.info("hits.getTotalHits().value is 0.");
        return;
      }

      SearchHit[] hitArray = hits.getHits();
      for (int i = 0; i < hitArray.length; i++) {
        SearchHit hit = hitArray[i];
        log.info("id is " + hit.getId() + ";index is " + hit.getIndex() + ";source is "
            + hit.getSourceAsString());
      }
    }
  }

  // 异步执行MultiSearchRequest
  public void executeMultiSearchRequestAsync(String field, String[] keywords) {
    // 构建MultiSearchRequest实例
    MultiSearchRequest request = buildMultiSearchRequest(field, keywords);

    // 构建监听器
    ActionListener<MultiSearchResponse> listener = new ActionListener<MultiSearchResponse>() {
      @Override
      public void onResponse(MultiSearchResponse response) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.msearchAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
