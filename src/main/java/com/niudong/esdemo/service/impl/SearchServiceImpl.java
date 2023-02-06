package com.niudong.esdemo.service.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.SearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索相关API使用和测试
 *
 */
@Service
public class SearchServiceImpl implements SearchService {
  private static Log log = LogFactory.getLog(SearchServiceImpl.class);

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

  // 构建SearchRequest
  public void buildSearchRequest() {
    SearchRequest searchRequest = new SearchRequest();

    // 大多数搜索参数都添加到SearchSourceBuilder中。它为进入搜索请求主体的所有内容提供setter。
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // 向SearchSourceBuilder添加“全部匹配”查询。
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());

    // 将SearchSourceBuilder添加到seachRequest。
    searchRequest.source(searchSourceBuilder);

    /*
     * 可选参数配置
     */

    // 在索引上限制请求
    searchRequest = new SearchRequest("posts");

    // 设置路由参数
    searchRequest.routing("routing");

    // 设置IndiceOptions控制方法： 如何解析不可用索引以及如何扩展通配符表达式
    searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

    // 使用首选参数，例如执行搜索以首选本地分片。默认值是在分片之间随机化。
    searchRequest.preference("_local");

    /*
     * 使用SearchSourceBuilder
     */

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    // 设置查询条件
    sourceBuilder.query(QueryBuilders.termQuery("content", "货币"));
    // 设置搜索结果索引的起始地址，默认为0。
    sourceBuilder.from(0);
    // 设置要返回的搜索命中数的大小。默认值为10。
    sourceBuilder.size(5);
    // 设置一个可选的超时，控制允许搜索的时间。
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

    // 将SearchSourceBuilder添加到SearchRequest中：
    searchRequest.source(sourceBuilder);

    /*
     * 搜索查询MatchQueryBuilder的使用
     */
    // 方法1
    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("content", "货币");
    // 创建QueryBuilder对象，提供配置搜索查询选项的方法如下:
    // 对匹配查询启用模糊匹配
    matchQueryBuilder.fuzziness(Fuzziness.AUTO);

    // 在匹配查询上设置前缀长度
    matchQueryBuilder.prefixLength(3);

    // 设置最大扩展选项以控制查询的模糊过程
    matchQueryBuilder.maxExpansions(10);

    // 方法2 流式编程
    matchQueryBuilder = QueryBuilders.matchQuery("content", "货币").fuzziness(Fuzziness.AUTO)
        .prefixLength(3).maxExpansions(10);

    // 添加matchQueryBuilder到searchSourceBuilder
    searchSourceBuilder.query(matchQueryBuilder);

    /*
     * 指定排序的方法使用
     */
    // 按分数降序排序（默认）
    sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    // 按ID字段升序排序
    sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));

    /*
     * 源筛选的方法使用
     */
    // 默认情况下，搜索请求返回文档源的内容，但与RESTAPI一样，我们可以覆盖此行为。例如，可以完全关闭源检索：
    sourceBuilder.fetchSource(false);

    // 该方法还接受一个或多个通配符模式的数组，以更细粒度的方式控制哪些字段被包括或排除
    String[] includeFields = new String[] {"title", "innerObject.*"};
    String[] excludeFields = new String[] {"user"};
    sourceBuilder.fetchSource(includeFields, excludeFields);

    /*
     * 配置请求高亮显示
     */

    HighlightBuilder highlightBuilder = new HighlightBuilder();

    // 为title字段创建字段高亮
    HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");

    // 设置字段高亮类型
    highlightTitle.highlighterType("unified");

    // 将highlightTitle添加到highlightBuilder
    highlightBuilder.field(highlightTitle);

    // 添加第二个高亮显示字段
    HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
    highlightBuilder.field(highlightUser);

    searchSourceBuilder.highlighter(highlightBuilder);

    /*
     * 聚合请求的使用
     */
    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms("by_company").field("company.keyword");
    aggregation.subAggregation(AggregationBuilders.avg("average_age").field("age"));
    searchSourceBuilder.aggregation(aggregation);

    /*
     * Suggestions 建议请求的使用
     */

    // TermSuggestionBuilder 中为content字段添加货币的Suggestions
    SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("content").text("货币");
    SuggestBuilder suggestBuilder = new SuggestBuilder();

    // 添加Suggestions建议生成器 并命名
    suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);

    // 添加suggestBuilder到searchSourceBuilder
    searchSourceBuilder.suggest(suggestBuilder);

    /*
     * 分析查询和聚合API的使用
     */
    searchSourceBuilder.profile(true);

  }

  // 参数化构建SearchRequest
  public SearchRequest buildSearchRequest(String filed, String text) {
    SearchRequest searchRequest = new SearchRequest();

    // 大多数搜索参数都添加到SearchSourceBuilder中。它为进入搜索请求主体的所有内容提供setter。
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // 向SearchSourceBuilder添加“全部匹配”查询。
    //searchSourceBuilder.query(QueryBuilders.matchAllQuery());

    // 将SearchSourceBuilder添加到seachRequest。
    searchRequest.source(searchSourceBuilder);

    /*
     * 使用SearchSourceBuilder
     */

    // 设置查询条件
    searchSourceBuilder.query(QueryBuilders.termQuery(filed, text));
    // 设置搜索结果索引的起始地址，默认为0。
    searchSourceBuilder.from(0);
    // 设置要返回的搜索命中数的大小。默认值为10。
    searchSourceBuilder.size(5);
    // 设置一个可选的超时，控制允许搜索的时间。
    searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

    // 将SearchSourceBuilder添加到SearchRequest中：
    searchRequest.source(searchSourceBuilder);

    /*
     * 配置请求高亮显示
     */

    HighlightBuilder highlightBuilder = new HighlightBuilder();

    // 为title字段创建字段高亮
    HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field(filed);

    // 设置字段高亮类型
    highlightTitle.highlighterType("unified");

    // 将highlightTitle添加到highlightBuilder
    highlightBuilder.field(highlightTitle);

    searchSourceBuilder.highlighter(highlightBuilder);

    /*
     * Suggestions 建议请求的使用
     */

    // TermSuggestionBuilder 中为content字段添加货币的Suggestions
    SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion(filed).text(text);
    SuggestBuilder suggestBuilder = new SuggestBuilder();

    // 添加Suggestions建议生成器 并命名
    suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);

    // 添加suggestBuilder到searchSourceBuilder
    searchSourceBuilder.suggest(suggestBuilder);

    return searchRequest;
  }

  // 同步方式执行SearchRequest
  public void executeSearchRequest() {
    SearchRequest searchRequest = buildSearchRequest("content", "货币");

    // 运行
    try {
      SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
      log.info(searchResponse.toString());

      // 解析SearchResponse
      processSearchResponse(searchResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析SearchResponse
  private void processSearchResponse(SearchResponse searchResponse) {
    if (searchResponse == null) {
      return;
    }

    // 获取HTTP状态代码
    RestStatus status = searchResponse.status();
    // 获取请求执行时间
    TimeValue took = searchResponse.getTook();
    // 获取请求是否提前终止
    Boolean terminatedEarly = searchResponse.isTerminatedEarly();
    // 获取请求是否超时
    boolean timedOut = searchResponse.isTimedOut();
    log.info("status is " + status + ";took is " + took + ";terminatedEarly is " + terminatedEarly
        + ";timedOut is " + timedOut);

    // 查看搜索影响的分片总数
    int totalShards = searchResponse.getTotalShards();
    // 执行搜索成功的分片的统计信息
    int successfulShards = searchResponse.getSuccessfulShards();
    // 执行搜索失败的分片的统计信息
    int failedShards = searchResponse.getFailedShards();
    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards);
    for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
      log.info("fail is " + failure.toString());
    }

    // 获取响应中包含的搜索结果
    SearchHits hits = searchResponse.getHits();
    // SearchHits提供有关所有结果的全局信息，如点击总数或最高分数：
    TotalHits totalHits = hits.getTotalHits();
    // 点击总数
    long numHits = totalHits.value;
    // 最高分数
    float maxScore = hits.getMaxScore();
    log.info("numHits is " + numHits + ";maxScore is " + maxScore);

    // 嵌套在searchhits中的是可以迭代的单个搜索结果
    SearchHit[] searchHits = hits.getHits();
    for (SearchHit hit : searchHits) {
      // SearchHit提供对基本信息的访问，如索引、文档ID和每次搜索的得分：
      String index = hit.getIndex();
      String id = hit.getId();
      float score = hit.getScore();

      log.info("docId is " + id + ";docIndex is " + index + ";docScore is " + score);

      // 以JSON字符串形式返回文档源
      String sourceAsString = hit.getSourceAsString();

      // 以键/值对的映射形式返回文档源
      Map<String, Object> sourceAsMap = hit.getSourceAsMap();
      String documentTitle = (String) sourceAsMap.get("title");
      List<Object> users = (List<Object>) sourceAsMap.get("user");
      Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
      log.info(
          "sourceAsString is " + sourceAsString + ";sourceAsMap size is " + sourceAsMap.size());

      // 高亮显示
      Map<String, HighlightField> highlightFields = hit.getHighlightFields();
      HighlightField highlight = highlightFields.get("content");
      // 获取包含高亮显示的字段内容的一个或多个片段
      Text[] fragments = highlight.fragments();
      String fragmentString = fragments[0].string();
      log.info("fragmentString is " + fragmentString);

    }

    // 聚合搜索
    Aggregations aggregations = searchResponse.getAggregations();
    if (aggregations == null) {
      return;
    }
    // 按content聚合
    Terms byCompanyAggregation = aggregations.get("by_content");
    // 获取Elastic为关键词的buckets
    Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
    // 获取平均年龄的子聚合
    Avg averageAge = elasticBucket.getAggregations().get("average_age");
    double avg = averageAge.getValue();
    log.info("avg is " + avg);

    // Suggest搜索
    Suggest suggest = searchResponse.getSuggest();
    if (suggest == null) {
      return;
    }
    // 按content搜索Suggest
    TermSuggestion termSuggestion = suggest.getSuggestion("content");
    for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
      for (TermSuggestion.Entry.Option option : entry) {
        String suggestText = option.getText().string();
        log.info("suggestText is " + suggestText);
      }
    }

    // 搜索时分析结果
    Map<String, ProfileShardResult> profilingResults = searchResponse.getProfileResults();
    if (profilingResults == null) {
      return;
    }
    for (Map.Entry<String, ProfileShardResult> profilingResult : profilingResults.entrySet()) {
      String key = profilingResult.getKey();
      ProfileShardResult profileShardResult = profilingResult.getValue();
      log.info("key is " + key + ";profileShardResult is " + profileShardResult.toString());
    }
  }

  // 异步方式执行SearchRequest
  public void executeSearchRequestAsync() {
    SearchRequest searchRequest = buildSearchRequest("content", "货币");

    // 构建监听器
    ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
      @Override
      public void onResponse(SearchResponse searchResponse) {
        log.info("response is " + searchResponse.toString());
      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 运行
    try {
      restClient.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
