package com.niudong.esdemo.service.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.CountService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索统计相关API使用和测试
 *
 */
@Service
public class CountServiceImpl implements CountService {
  private static Log log = LogFactory.getLog(CountServiceImpl.class);

  private RestHighLevelClient restClient;

  // 初始化连接
  @PostConstruct
  public void initEs() {
    restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.56.111", 9200, "http"),
        new HttpHost("192.168.56.111", 9201, "http")));

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

  // 构建CountRequest
  public CountRequest buildCountRequest() {
    // 创建CountRequest。如果没有参数，这将针对所有索引运行。
    CountRequest countRequest = new CountRequest();

    // 大多数搜索参数都添加到SearchSourceBuilder中。
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // 向SearchSourceBuilder添加“全部匹配”查询。
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());

    // 添加 SearchSourceBuilder 到 CountRequest.
    countRequest.source(searchSourceBuilder);

    return countRequest;
  }

  // 构建CountRequest
  public CountRequest buildCountRequest(String indexName, String routeName) {
    // 将请求限制为特定名称的索引
    CountRequest countRequest = new CountRequest(indexName).
    // 设置路由参数
        routing(routeName)
        // 设置IndiceOptions控制如何解析不可用索引以及如何展开通配符表达式
        .indicesOptions(IndicesOptions.lenientExpandOpen())
        // 使用首选参数，例如执行搜索以首选本地分片。默认值是在分片之间随机选择。
        .preference("_local");

    return countRequest;
  }

  // 构建CountRequest
  public CountRequest buildCountRequest(String indexName, String routeName, String field,
      String content) {
    // 将请求限制为特定名称的索引
    CountRequest countRequest = new CountRequest(indexName).
    // 设置路由参数
        routing(routeName)
        // 设置IndiceOptions控制如何解析不可用索引以及如何展开通配符表达式
        .indicesOptions(IndicesOptions.lenientExpandOpen())
        // 使用首选参数，例如执行搜索以首选本地分片。默认值是在分片之间随机选择。
        .preference("_local");

    // 使用默认选项创建SearchSourceBuilder。
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    // 设置查询。可以是任何类型的QueryBuilder
    sourceBuilder.query(QueryBuilders.termQuery(field, content));

    // 将SearchSourceBuilder添加到CountRequest
    countRequest.source(sourceBuilder);

    return countRequest;
  }

  // 同步执行CountRequest
  public void executeCountRequest() {
    CountRequest countRequest = buildCountRequest();

    try {
      CountResponse countResponse = restClient.count(countRequest, RequestOptions.DEFAULT);

      // 解析CountResponse
      processCountResponse(countResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }

  }

  // 解析CountResponse
  private void processCountResponse(CountResponse countResponse) {
    // 搜索请求对应的结果命中总数
    long count = countResponse.getCount();

    // http状态代码
    RestStatus status = countResponse.status();

    // 请求是否提前终止
    Boolean terminatedEarly = countResponse.isTerminatedEarly();
    log.info("count is " + count + ";status is " + status.getStatus() + ";terminatedEarly is "
        + terminatedEarly);

    // 与搜索请求对应的分片总数
    int totalShards = countResponse.getTotalShards();
    // 执行搜索请求跳过的分片数量
    int skippedShards = countResponse.getSkippedShards();
    // 执行搜索请求成功的分片数量
    int successfulShards = countResponse.getSuccessfulShards();
    // 执行搜索请求失败的分片数量
    int failedShards = countResponse.getFailedShards();
    log.info("totalShards is " + totalShards + ";skippedShards is " + skippedShards
        + ";successfulShards is " + successfulShards + ";failedShards is " + failedShards);

    // 通过遍历ShardSearchFailures数组来处理可能的失败信息
    if (countResponse.getShardFailures() == null) {
      return;
    }
    for (ShardSearchFailure failure : countResponse.getShardFailures()) {
      log.info("fail index is " + failure.index());
    }


  }

  // 异步执行CountRequest
  public void executeCountRequestAsync() {
    // 构建CountRequest
    CountRequest countRequest = buildCountRequest();

    // 构建监听器
    ActionListener<CountResponse> listener = new ActionListener<CountResponse>() {

      @Override
      public void onResponse(CountResponse countResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.countAsync(countRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }

  }
}
