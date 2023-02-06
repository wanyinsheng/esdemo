package com.niudong.esdemo.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.EvalQueryQuality;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.MetricDetail;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RatedSearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.RankEvalService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索结果评估相关API使用和测试
 *
 */
@Service
public class RankEvalServiceImpl implements RankEvalService {
  private static Log log = LogFactory.getLog(RankEvalServiceImpl.class);

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


  // 构建RankEvalRequest
  public RankEvalRequest buildRankEvalRequest(String index, String documentId, String field,
      String content) {
    EvaluationMetric metric = new PrecisionAtK();
    List<RatedDocument> ratedDocs = new ArrayList<>();
    // 添加按索引名称、ID和分级指定的分级文档
    ratedDocs.add(new RatedDocument(index, documentId, 1));
    SearchSourceBuilder searchQuery = new SearchSourceBuilder();
    // 创建要评估的搜索查询
    searchQuery.query(QueryBuilders.matchQuery(field, content));
    // 将前三个部分合并为RatedRequest
    RatedRequest ratedRequest = new RatedRequest("content_query", ratedDocs, searchQuery);
    List<RatedRequest> ratedRequests = Arrays.asList(ratedRequest);
    // 创建排名评估规范
    RankEvalSpec specification = new RankEvalSpec(ratedRequests, metric);
    // 创建排名评估请求
    RankEvalRequest request = new RankEvalRequest(specification, new String[] {index});

    return request;
  }

  // 同步执行RankEvalRequest
  public void executeRankEvalRequest(String index, String documentId, String field,
      String content) {
    // 构建RankEvalRequest
    RankEvalRequest request = buildRankEvalRequest(index, documentId, field, content);

    try {
      RankEvalResponse response = restClient.rankEval(request, RequestOptions.DEFAULT);

      // 处理RankEvalResponse
      processRankEvalResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 处理RankEvalResponse
  private void processRankEvalResponse(RankEvalResponse response) {
    // 总体评价结果
    double evaluationResult = response.getMetricScore();
    log.info("evaluationResult is " + evaluationResult);

    Map<String, EvalQueryQuality> partialResults = response.getPartialResults();
    // 由其查询ID键控的部分结果
    EvalQueryQuality evalQuality = partialResults.get("content_query");
    log.info("content_query id is " + evalQuality.getId());

    // 每个部分结果的度量分数
    double qualityLevel = evalQuality.metricScore();
    log.info("qualityLevel is " + qualityLevel);

    List<RatedSearchHit> hitsAndRatings = evalQuality.getHitsAndRatings();
    RatedSearchHit ratedSearchHit = hitsAndRatings.get(2);
    // 分级搜索命中包含完全成熟的搜索命中SearchHit
    log.info("SearchHit id is " + ratedSearchHit.getSearchHit().getId());
    // 分级搜索命中还包含一个可选的<integer>分级 Optional<Integer> ，如果文档在请求中未获得分级，则该分级不存在。
    log.info("rate's isPresent is " + ratedSearchHit.getRating().isPresent());

    MetricDetail metricDetails = evalQuality.getMetricDetails();
    String metricName = metricDetails.getMetricName();
    // 度量详细信息以请求中使用的度量命名
    log.info("metricName is " + metricName);


    PrecisionAtK.Detail detail = (PrecisionAtK.Detail) metricDetails;
    // 在转换到请求中使用的度量之后，度量详细信息提供了对度量计算部分的深入了解。
    log.info("detail's relevantRetrieved is " + detail.getRelevantRetrieved());
    log.info("detail's retrieved is " + detail.getRetrieved());

  }

  // 异步执行RankEvalRequest
  public void executeRankEvalRequestAsync(String index, String documentId, String field,
      String content) {
    // 构建RankEvalRequest
    RankEvalRequest request = buildRankEvalRequest(index, documentId, field, content);

    // 构建监听器
    ActionListener<RankEvalResponse> listener = new ActionListener<RankEvalResponse>() {
      @Override
      public void onResponse(RankEvalResponse response) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.rankEvalAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
