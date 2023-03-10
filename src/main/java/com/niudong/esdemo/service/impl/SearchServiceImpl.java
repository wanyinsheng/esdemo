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
 * @author ??????
 * @desc:??????????????????????????????API???????????????
 *
 */
@Service
public class SearchServiceImpl implements SearchService {
  private static Log log = LogFactory.getLog(SearchServiceImpl.class);

  private RestHighLevelClient restClient;

  // ???????????????
  @PostConstruct
  public void initEs() {
    restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

    log.info("ElasticSearch init in service.");
  }

  // ????????????
  public void closeEs() {
    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // ??????SearchRequest
  public void buildSearchRequest() {
    SearchRequest searchRequest = new SearchRequest();

    // ?????????????????????????????????SearchSourceBuilder?????????????????????????????????????????????????????????setter???
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // ???SearchSourceBuilder?????????????????????????????????
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());

    // ???SearchSourceBuilder?????????seachRequest???
    searchRequest.source(searchSourceBuilder);

    /*
     * ??????????????????
     */

    // ????????????????????????
    searchRequest = new SearchRequest("posts");

    // ??????????????????
    searchRequest.routing("routing");

    // ??????IndiceOptions??????????????? ???????????????????????????????????????????????????????????????
    searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

    // ??????????????????????????????????????????????????????????????????????????????????????????????????????
    searchRequest.preference("_local");

    /*
     * ??????SearchSourceBuilder
     */

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    // ??????????????????
    sourceBuilder.query(QueryBuilders.termQuery("content", "??????"));
    // ???????????????????????????????????????????????????0???
    sourceBuilder.from(0);
    // ?????????????????????????????????????????????????????????10???
    sourceBuilder.size(5);
    // ????????????????????????????????????????????????????????????
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

    // ???SearchSourceBuilder?????????SearchRequest??????
    searchRequest.source(sourceBuilder);

    /*
     * ????????????MatchQueryBuilder?????????
     */
    // ??????1
    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("content", "??????");
    // ??????QueryBuilder??????????????????????????????????????????????????????:
    // ?????????????????????????????????
    matchQueryBuilder.fuzziness(Fuzziness.AUTO);

    // ????????????????????????????????????
    matchQueryBuilder.prefixLength(3);

    // ??????????????????????????????????????????????????????
    matchQueryBuilder.maxExpansions(10);

    // ??????2 ????????????
    matchQueryBuilder = QueryBuilders.matchQuery("content", "??????").fuzziness(Fuzziness.AUTO)
        .prefixLength(3).maxExpansions(10);

    // ??????matchQueryBuilder???searchSourceBuilder
    searchSourceBuilder.query(matchQueryBuilder);

    /*
     * ???????????????????????????
     */
    // ?????????????????????????????????
    sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
    // ???ID??????????????????
    sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));

    /*
     * ????????????????????????
     */
    // ???????????????????????????????????????????????????????????????RESTAPI??????????????????????????????????????????????????????????????????????????????
    sourceBuilder.fetchSource(false);

    // ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    String[] includeFields = new String[] {"title", "innerObject.*"};
    String[] excludeFields = new String[] {"user"};
    sourceBuilder.fetchSource(includeFields, excludeFields);

    /*
     * ????????????????????????
     */

    HighlightBuilder highlightBuilder = new HighlightBuilder();

    // ???title????????????????????????
    HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");

    // ????????????????????????
    highlightTitle.highlighterType("unified");

    // ???highlightTitle?????????highlightBuilder
    highlightBuilder.field(highlightTitle);

    // ?????????????????????????????????
    HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
    highlightBuilder.field(highlightUser);

    searchSourceBuilder.highlighter(highlightBuilder);

    /*
     * ?????????????????????
     */
    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms("by_company").field("company.keyword");
    aggregation.subAggregation(AggregationBuilders.avg("average_age").field("age"));
    searchSourceBuilder.aggregation(aggregation);

    /*
     * Suggestions ?????????????????????
     */

    // TermSuggestionBuilder ??????content?????????????????????Suggestions
    SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("content").text("??????");
    SuggestBuilder suggestBuilder = new SuggestBuilder();

    // ??????Suggestions??????????????? ?????????
    suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);

    // ??????suggestBuilder???searchSourceBuilder
    searchSourceBuilder.suggest(suggestBuilder);

    /*
     * ?????????????????????API?????????
     */
    searchSourceBuilder.profile(true);

  }

  // ???????????????SearchRequest
  public SearchRequest buildSearchRequest(String filed, String text) {
    SearchRequest searchRequest = new SearchRequest();

    // ?????????????????????????????????SearchSourceBuilder?????????????????????????????????????????????????????????setter???
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    // ???SearchSourceBuilder?????????????????????????????????
    //searchSourceBuilder.query(QueryBuilders.matchAllQuery());

    // ???SearchSourceBuilder?????????seachRequest???
    searchRequest.source(searchSourceBuilder);

    /*
     * ??????SearchSourceBuilder
     */

    // ??????????????????
    searchSourceBuilder.query(QueryBuilders.termQuery(filed, text));
    // ???????????????????????????????????????????????????0???
    searchSourceBuilder.from(0);
    // ?????????????????????????????????????????????????????????10???
    searchSourceBuilder.size(5);
    // ????????????????????????????????????????????????????????????
    searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

    // ???SearchSourceBuilder?????????SearchRequest??????
    searchRequest.source(searchSourceBuilder);

    /*
     * ????????????????????????
     */

    HighlightBuilder highlightBuilder = new HighlightBuilder();

    // ???title????????????????????????
    HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field(filed);

    // ????????????????????????
    highlightTitle.highlighterType("unified");

    // ???highlightTitle?????????highlightBuilder
    highlightBuilder.field(highlightTitle);

    searchSourceBuilder.highlighter(highlightBuilder);

    /*
     * Suggestions ?????????????????????
     */

    // TermSuggestionBuilder ??????content?????????????????????Suggestions
    SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion(filed).text(text);
    SuggestBuilder suggestBuilder = new SuggestBuilder();

    // ??????Suggestions??????????????? ?????????
    suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);

    // ??????suggestBuilder???searchSourceBuilder
    searchSourceBuilder.suggest(suggestBuilder);

    return searchRequest;
  }

  // ??????????????????SearchRequest
  public void executeSearchRequest() {
    SearchRequest searchRequest = buildSearchRequest("content", "??????");

    // ??????
    try {
      SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
      log.info(searchResponse.toString());

      // ??????SearchResponse
      processSearchResponse(searchResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // ??????ES?????????
      closeEs();
    }
  }

  // ??????SearchResponse
  private void processSearchResponse(SearchResponse searchResponse) {
    if (searchResponse == null) {
      return;
    }

    // ??????HTTP????????????
    RestStatus status = searchResponse.status();
    // ????????????????????????
    TimeValue took = searchResponse.getTook();
    // ??????????????????????????????
    Boolean terminatedEarly = searchResponse.isTerminatedEarly();
    // ????????????????????????
    boolean timedOut = searchResponse.isTimedOut();
    log.info("status is " + status + ";took is " + took + ";terminatedEarly is " + terminatedEarly
        + ";timedOut is " + timedOut);

    // ?????????????????????????????????
    int totalShards = searchResponse.getTotalShards();
    // ??????????????????????????????????????????
    int successfulShards = searchResponse.getSuccessfulShards();
    // ??????????????????????????????????????????
    int failedShards = searchResponse.getFailedShards();
    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards);
    for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
      log.info("fail is " + failure.toString());
    }

    // ????????????????????????????????????
    SearchHits hits = searchResponse.getHits();
    // SearchHits???????????????????????????????????????????????????????????????????????????
    TotalHits totalHits = hits.getTotalHits();
    // ????????????
    long numHits = totalHits.value;
    // ????????????
    float maxScore = hits.getMaxScore();
    log.info("numHits is " + numHits + ";maxScore is " + maxScore);

    // ?????????searchhits??????????????????????????????????????????
    SearchHit[] searchHits = hits.getHits();
    for (SearchHit hit : searchHits) {
      // SearchHit???????????????????????????????????????????????????ID???????????????????????????
      String index = hit.getIndex();
      String id = hit.getId();
      float score = hit.getScore();

      log.info("docId is " + id + ";docIndex is " + index + ";docScore is " + score);

      // ???JSON??????????????????????????????
      String sourceAsString = hit.getSourceAsString();

      // ??????/????????????????????????????????????
      Map<String, Object> sourceAsMap = hit.getSourceAsMap();
      String documentTitle = (String) sourceAsMap.get("title");
      List<Object> users = (List<Object>) sourceAsMap.get("user");
      Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
      log.info(
          "sourceAsString is " + sourceAsString + ";sourceAsMap size is " + sourceAsMap.size());

      // ????????????
      Map<String, HighlightField> highlightFields = hit.getHighlightFields();
      HighlightField highlight = highlightFields.get("content");
      // ???????????????????????????????????????????????????????????????
      Text[] fragments = highlight.fragments();
      String fragmentString = fragments[0].string();
      log.info("fragmentString is " + fragmentString);

    }

    // ????????????
    Aggregations aggregations = searchResponse.getAggregations();
    if (aggregations == null) {
      return;
    }
    // ???content??????
    Terms byCompanyAggregation = aggregations.get("by_content");
    // ??????Elastic???????????????buckets
    Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
    // ??????????????????????????????
    Avg averageAge = elasticBucket.getAggregations().get("average_age");
    double avg = averageAge.getValue();
    log.info("avg is " + avg);

    // Suggest??????
    Suggest suggest = searchResponse.getSuggest();
    if (suggest == null) {
      return;
    }
    // ???content??????Suggest
    TermSuggestion termSuggestion = suggest.getSuggestion("content");
    for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
      for (TermSuggestion.Entry.Option option : entry) {
        String suggestText = option.getText().string();
        log.info("suggestText is " + suggestText);
      }
    }

    // ?????????????????????
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

  // ??????????????????SearchRequest
  public void executeSearchRequestAsync() {
    SearchRequest searchRequest = buildSearchRequest("content", "??????");

    // ???????????????
    ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
      @Override
      public void onResponse(SearchResponse searchResponse) {
        log.info("response is " + searchResponse.toString());
      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // ??????
    try {
      restClient.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // ??????ES?????????
      closeEs();
    }
  }
}
