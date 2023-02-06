package com.niudong.esdemo.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.ScrollSearchService;

import ch.qos.logback.classic.Logger;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述滚动搜索相关API使用和测试
 *
 */
@Service
public class ScrollSearchServiceImpl implements ScrollSearchService {
  private static Log log = LogFactory.getLog(ScrollSearchServiceImpl.class);

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

  // 构建SearchRequest:带有滚动搜索参数
  public void buildAndExecuteScrollSearchRequest(String indexName, int size) {
    // 索引名称为
    SearchRequest searchRequest = new SearchRequest(indexName);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("content", "美联储"));

    // 创建SearchRequest及其相应的SearchSourceBuilder。还可以选择设置大小以控制一次检索多少结果。
    searchSourceBuilder.size(size);
    searchRequest.source(searchSourceBuilder);

    // 设置滚动间隔
    searchRequest.scroll(TimeValue.timeValueMinutes(1L));

    try {
      SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);

      // 读取返回的滚动ID，该ID指向保持活动状态的搜索上下文，并将在后续搜索滚动调用中需要。
      String scrollId = searchResponse.getScrollId();
      // 检索第一批搜索结果
      SearchHits hits = searchResponse.getHits();

      while (hits != null && hits.getHits().length != 0) {
        // 设置滚动标识符
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(30));
        SearchResponse searchScrollResponse =
            restClient.scroll(scrollRequest, RequestOptions.DEFAULT);

        // 读取新的滚动ID，该ID指向保持活动状态的搜索上下文，并将在以下搜索滚动调用中需要。
        scrollId = searchScrollResponse.getScrollId();
        // 检索另一批搜索结果
        hits = searchScrollResponse.getHits();

        log.info("scrollId is " + scrollId);
        log.info(
            "total hits is " + hits.getTotalHits().value + ";now hits is " + hits.getHits().length);
      }
      
      //清除滚动搜索的上下文信息
      executeClearScrollRequest(scrollId);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }

  }

  // 构建SearchRequest:带有滚动搜索参数,异步执行
  public void buildAndExecuteScrollSearchRequestAsync(String indexName, int size) {
    // 索引名称为
    SearchRequest searchRequest = new SearchRequest(indexName);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("title", "Elasticsearch"));

    // 创建SearchRequest及其相应的SearchSourceBuilder。还可以选择设置大小以控制一次检索多少结果。
    searchSourceBuilder.size(size);
    searchRequest.source(searchSourceBuilder);

    // 设置滚动间隔
    searchRequest.scroll(TimeValue.timeValueMinutes(1L));

    try {
      SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);

      // 读取返回的滚动ID，该ID指向保持活动状态的搜索上下文，并将在后续搜索滚动调用中需要。
      String scrollId = searchResponse.getScrollId();
      // 检索第一批搜索结果
      SearchHits hits = searchResponse.getHits();

      // 配置监听器
      ActionListener<SearchResponse> scrollListener = new ActionListener<SearchResponse>() {
        @Override
        public void onResponse(SearchResponse searchResponse) {
          // 读取新的滚动ID，该ID指向保持活动状态的搜索上下文，并将在以下搜索滚动调用中需要。
          String scrollId = searchResponse.getScrollId();
          // 检索另一批搜索结果
          SearchHits hits = searchResponse.getHits();

          log.info("scrollId is " + scrollId);
          log.info("total hits is " + hits.getTotalHits().value + ";now hits is "
              + hits.getHits().length);
        }

        @Override
        public void onFailure(Exception e) {

        }
      };

      while (hits != null && hits.getHits().length != 0) {
        // 设置滚动标识符
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(30));

        // 异步执行
        restClient.scrollAsync(scrollRequest, RequestOptions.DEFAULT, scrollListener);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }

  }

  // 构建ClearScrollRequest
  public void buildClearScrollRequest(String scrollId) {
    ClearScrollRequest request = new ClearScrollRequest();
    // 添加单个滚动标识符
    request.addScrollId(scrollId);

    // 添加多个滚动标识符
    List<String> scrollIds = new ArrayList<>();
    scrollIds.add(scrollId);
    request.setScrollIds(scrollIds);

  }

  // 同步执行清除滚动搜索上下文的请求
  public void executeClearScrollRequest(String scrollId) {
    ClearScrollRequest request = new ClearScrollRequest();
    // 添加单个滚动标识符
    request.addScrollId(scrollId);

    try {
      ClearScrollResponse response = restClient.clearScroll(request, RequestOptions.DEFAULT);

      // 如果请求成功，则会返回true的结果
      boolean success = response.isSucceeded();
      // 返回已释放的搜索上下文数
      int released = response.getNumFreed();

      log.info("success is " + success + ";released is  " + released);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 异步执行清除滚动搜索上下文的请求
  public void executeClearScrollRequestAsync(String scrollId) {
    ClearScrollRequest request = new ClearScrollRequest();
    // 添加单个滚动标识符
    request.addScrollId(scrollId);

    // 添加监听器
    ActionListener<ClearScrollResponse> listener = new ActionListener<ClearScrollResponse>() {
      @Override
      public void onResponse(ClearScrollResponse clearScrollResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.clearScrollAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
