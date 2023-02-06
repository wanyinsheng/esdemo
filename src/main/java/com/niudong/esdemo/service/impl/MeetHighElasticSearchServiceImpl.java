package com.niudong.esdemo.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsResponse.TermVector;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;

import com.niudong.esdemo.service.MeetHighElasticSearchService;

/**
 * 
 * @author 牛冬
 * @desc:ES高级客户端初始化相关操作
 *
 */
@Service
public class MeetHighElasticSearchServiceImpl implements MeetHighElasticSearchService {
  private static Log log = LogFactory.getLog(MeetHighElasticSearchServiceImpl.class);

  private RestHighLevelClient restClient;

  /**
   * 本部分用于介绍如何与ElasticSearch构建连接和关闭连接
   */

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

  /**
   * 本部分用于介绍ElasticSearch 索引API的使用
   */
  // 基于String构建IndexRequest
  public void buildIndexRequestWithString(String indexName, String document) {
    // 索引名称
    IndexRequest request = new IndexRequest(indexName);
    // 文档ID
    request.id(document);
    // String类型的文档
    String jsonString = "{" + "\"user\":\"niudong\"," + "\"postDate\":\"2019-07-30\","
        + "\"message\":\"Hello Elasticsearch\"" + "}";
    request.source(jsonString, XContentType.JSON);
  }

  // 基于Map构建IndexRequest
  public void buildIndexRequestWithMap(String indexName, String document) {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("user", "niudong");
    jsonMap.put("postDate", new Date());
    jsonMap.put("message", "Hello Elasticsearch");
    // 作为自动转换为JSON格式的MAP提供的文档源
    IndexRequest indexRequest = new IndexRequest(indexName).id(document).source(jsonMap);
  }

  // 基于XContentBuilder构建IndexRequest
  public void buildIndexRequestWithXContentBuilder(String indexName, String document) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.field("user", "niudong");
        builder.timeField("postDate", new Date());
        builder.field("message", "Hello Elasticsearch");
      }
      builder.endObject();
      // 作为XContentBuilder对象提供的文档源，ElasticSearch内置的帮助器用于生成JSON内容
      IndexRequest indexRequest = new IndexRequest(indexName).id(document).source(builder);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 基于Key-value 构建IndexRequest
  public void buildIndexRequestWithKV(String indexName, String document) {
    // 作为Key-value对提供的文档源，转换为JSON格式
    IndexRequest indexRequest = new IndexRequest(indexName).id(document).source("user", "niudong",
        "postDate", new Date(), "message", "Hello Elasticsearch");
  }

  // 构建IndexRequest的其他参数配置
  public void buildIndexRequestWithParam(String indexName, String document) {
    // 作为Key-value对提供的文档源，转换为JSON格式
    IndexRequest request = new IndexRequest(indexName).id(document).source("user", "niudong",
        "postDate", new Date(), "message", "Hello Elasticsearch");

    request.routing("routing");// 路由值

    // 设置超时时间
    request.timeout(TimeValue.timeValueSeconds(1));
    request.timeout("1s");

    // 设置超时策略
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    request.setRefreshPolicy("wait_for");

    // 设置版本
    request.version(2);

    // 设置版本类型
    request.versionType(VersionType.EXTERNAL);

    // 设置操作类型
    request.opType(DocWriteRequest.OpType.CREATE);
    request.opType("create");

    // 索引文档之前要执行的接收管道的名称
    request.setPipeline("pipeline");
  }

  // 索引文档
  public void indexDocuments(String indexName, String document) {
    // 作为Key-value对提供的文档源，转换为JSON格式
    IndexRequest indexRequest = new IndexRequest(indexName).id(document).source("user", "niudong",
        "postDate", new Date(), "message",
        "Hello Elasticsearch!北京时间8月1日凌晨2点，美联储公布7月议息会议结果。一如市场预期，美联储本次降息25个基点，将联邦基金利率的目标范围调至2.00%-2.25%。此次是2007-2008年间美国为应对金融危机启动降息周期后，美联储十年多以来首次降息。美联储公布利率决议后，美股下跌，美元上涨，人民币汇率下跌");

    try {
      IndexResponse indexResponse = restClient.index(indexRequest, RequestOptions.DEFAULT);

      // 解析索引结果
      processIndexResponse(indexResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 解析索引结果
  private void processIndexResponse(IndexResponse indexResponse) {
    String index = indexResponse.getIndex();
    String id = indexResponse.getId();
    log.info("index is " + index + ", id is " + id);

    if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
      // 文档创建时
      log.info("Document is created!");
    } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
      // 文档更新时
      log.info("Document has updated!");
    }
    ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
    if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
      // 处理成功shards 小于总shards 的情况
      log.info("Successed shards are not enough!");
    }
    if (shardInfo.getFailed() > 0) {
      for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
        String reason = failure.reason();
        log.info("Fail reason is " + reason);
      }
    }
  }

  // 异步索引文档
  public void indexDocumentsAsync(String indexName, String document) {
    // 作为Key-value对提供的文档源，转换为JSON格式
    IndexRequest indexRequest = new IndexRequest(indexName).id(document).source("user", "niudong",
        "postDate", new Date(), "message", "Hello Elasticsearch");

    ActionListener listener = new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse indexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * 获取索引文档数据相关API的使用
   */
  // 构建GetRequest
  public void buildGetRequest(String indexName, String document) {
    GetRequest getRequest = new GetRequest(indexName, document);

    // 可选配置参数
    // 禁用源检索，默认情况下启用
    getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

    // 为特定字段配置源包含
    String[] includes = new String[] {"message", "*Date"};
    String[] excludes = Strings.EMPTY_ARRAY;
    FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
    getRequest.fetchSourceContext(fetchSourceContext);

    // 为特定字段配置源排除
    includes = Strings.EMPTY_ARRAY;
    excludes = new String[] {"message"};
    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
    getRequest.fetchSourceContext(fetchSourceContext);

    getRequest.storedFields("message"); // 为特定存储字段配置检索（要求在映射中单独存储字段）
    try {
      GetResponse getResponse = restClient.get(getRequest, RequestOptions.DEFAULT);
      String message = getResponse.getField("message").getValue();// 检索消息存储字段（要求该字段单独存储在映射中）
      log.info("message is " + message);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 路由值
    getRequest.routing("routing");

    // 偏好值
    getRequest.preference("preference");

    // 将实时标志设置为假（默认为真）
    getRequest.realtime(false);
    // 在检索文档之前执行刷新（默认为false）
    getRequest.refresh(true);
    // 配置版本号
    getRequest.version(2);
    // 配置版本类型
    getRequest.versionType(VersionType.EXTERNAL);
  }

  // 同步方式获取索引文档
  public void getIndexDocuments(String indexName, String document) {
    GetRequest getRequest = new GetRequest(indexName, document);
    try {
      GetResponse getResponse = restClient.get(getRequest, RequestOptions.DEFAULT);

      // 处理GetResponse
      processGetResponse(getResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  // 处理GetResponse
  private void processGetResponse(GetResponse getResponse) {
    String index = getResponse.getIndex();
    String id = getResponse.getId();
    log.info("id is " + id + ", index is " + index);

    if (getResponse.isExists()) {
      long version = getResponse.getVersion();
      String sourceAsString = getResponse.getSourceAsString(); // 以字符串形式检索文档
      Map<String, Object> sourceAsMap = getResponse.getSourceAsMap(); // 以Map<String, Object>形式检索文档
      byte[] sourceAsBytes = getResponse.getSourceAsBytes(); // 以byte[]形式检索文档

      log.info("version is " + version + ", sourceAsString is " + sourceAsString);
    } else {
      // 找不到文档时在此处处理。请注意，尽管返回的响应具有404状态代码，但返回的是有效的getResponse，而不是引发异常。这样的响应不包含任何源文档，并且其isexists方法返回false。

    }
  }

  // 异步方式获取索引文档
  public void getIndexDocumentsAsync(String indexName, String document) {
    GetRequest getRequest = new GetRequest(indexName, document);

    ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
      @Override
      public void onResponse(GetResponse getResponse) {
        String id = getResponse.getId();
        String index = getResponse.getIndex();
        log.info("id is " + id + ", index is " + index);
      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.getAsync(getRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  // 同步方式校验索引文档是否存在
  public void checkExistIndexDocuments(String indexName, String document) {
    GetRequest getRequest = new GetRequest(indexName, document);
    // 禁用提取源
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    // 禁用提取存储字段
    getRequest.storedFields("_none_");

    try {
      boolean exists = restClient.exists(getRequest, RequestOptions.DEFAULT);
      log.info("索引" + indexName + "下的" + document + "文档的存在性是" + exists);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();

  }

  // 异步方式校验索引文档是否存在
  public void checkExistIndexDocumentsAsync(String indexName, String document) {
    GetRequest getRequest = new GetRequest(indexName, document);
    // 禁用提取源
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    // 禁用提取存储字段
    getRequest.storedFields("_none_");

    // 定义监听器
    ActionListener<Boolean> listener = new ActionListener<Boolean>() {
      @Override
      public void onResponse(Boolean exists) {
        log.info("索引" + indexName + "下的" + document + "文档的存在性是" + exists);
      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.existsAsync(getRequest, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();

  }

  /*
   * 以下是删除文档索引的API使用
   */
  // 构建DeleteRequest
  public void buildDeleteRequestIndexDocuments(String indexName, String document) {
    DeleteRequest request = new DeleteRequest(indexName, document);

    // 设置路由
    request.routing("routing");
    // 设置超时
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");
    // 设置刷新策略
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    request.setRefreshPolicy("wait_for");
    // 设置版本
    request.version(2);
    // 设置版本类型
    request.versionType(VersionType.EXTERNAL);
  }

  // 同步方式删除索引文档
  public void deleteIndexDocuments(String indexName, String document) {
    DeleteRequest request = new DeleteRequest(indexName, document);
    try {
      DeleteResponse deleteResponse = restClient.delete(request, RequestOptions.DEFAULT);

      // 处理DeleteResponse
      processDeleteRequest(deleteResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    //closeEs();
  }

  // 处理DeleteResponse
  private void processDeleteRequest(DeleteResponse deleteResponse) {
    String index = deleteResponse.getIndex();
    String id = deleteResponse.getId();
    long version = deleteResponse.getVersion();
    log.info("delete id is " + id + ", index is " + index + ",version is " + version);

    ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
    if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
      log.info("Success shards are not enough");
    }
    if (shardInfo.getFailed() > 0) {
      for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
        String reason = failure.reason();
        log.info("Fail reason is " + reason);
      }
    }
  }

  // 异步方式删除索引文档
  public void deleteIndexDocumentsAsync(String indexName, String document) {
    DeleteRequest request = new DeleteRequest(indexName, document);

    ActionListener listener = new ActionListener<DeleteResponse>() {
      @Override
      public void onResponse(DeleteResponse deleteResponse) {
        String id = deleteResponse.getId();
        String index = deleteResponse.getIndex();
        long version = deleteResponse.getVersion();
        log.info("delete id is " + id + ", index is " + index + ",version is " + version);
      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.deleteAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  /*
   * 以下是更新文档索引的API使用
   */
  // 构建UpdateRequest
  public void buildUpdateRequestIndexDocuments(String indexName, String document) {
    UpdateRequest request = new UpdateRequest(indexName, document);

    /*
     * 可选参数配置
     */
    // 设置路由
    request.routing("routing");

    // 设置超时
    request.timeout(TimeValue.timeValueSeconds(1));
    request.timeout("1s");

    // 设置刷新策略
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    request.setRefreshPolicy("wait_for");

    // 设置：当更新的文档在更新时被另一个操作更改，则重试更新操作的次数
    request.retryOnConflict(3);

    // 启用源检索，默认情况下禁用
    request.fetchSource(true);

    // 为特定字段配置 源包含 关系
    String[] includes = new String[] {"updated", "r*"};
    String[] excludes = Strings.EMPTY_ARRAY;
    request.fetchSource(new FetchSourceContext(true, includes, excludes));

    // 为特定字段配置 源排除 关系
    includes = Strings.EMPTY_ARRAY;
    excludes = new String[] {"updated"};
    request.fetchSource(new FetchSourceContext(true, includes, excludes));

    /*
     * 其他方式构建UpdateRequest
     */
    // 方式2：以JSON格式的字符串形式 构建文档
    request = new UpdateRequest(indexName, document);
    String jsonString = "{" + "\"updated\":\"2019-12-31\"," + "\"reason\":\"Year update！\"" + "}";
    request.doc(jsonString, XContentType.JSON);

    // 方式3：作为MAP形式文档源自动转换为JSON格式来构建文档
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("updated", new Date());
    jsonMap.put("reason", "Year update!");
    request = new UpdateRequest(indexName, document).doc(jsonMap);

    // 方式4：作为XContentBuilder对象提供文档源，ElasticSearch内置帮助器用于生成JSON内容
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.timeField("updated", new Date());
        builder.field("reason", "Year update!");
      }
      builder.endObject();
      request = new UpdateRequest(indexName, document).doc(builder);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 方式5：以key-value提供文档源，将转换为JSON格式
    request =
        new UpdateRequest(indexName, document).doc("updated", new Date(), "reason", "Year update！");


    /*
     * 以下展示Upserts的使用
     */
    jsonString = "{\"created\":\"2019-12-31\"}";
    request.upsert(jsonString, XContentType.JSON);
  }

  // 同步方式更新索引文档
  public void updateIndexDocuments(String indexName, String document) {
    UpdateRequest request = new UpdateRequest(indexName, document);
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("message", new Date());
    jsonMap.put("reason", "Year update!");
    jsonMap.put("content1", null);
    jsonMap.put("content",
        "2015年12月，美联储开启新一轮加息周期，至2018年12月，美联储累计加息9次，每次加息25个基点，其中2018年共加息四次，将联邦基金利率的目标范围调至2.25%-2.50%区间。今年以来，美联储未有一次加息动作，联邦基金利率的目标范围维持不变。\r\n");
    request = new UpdateRequest(indexName, document).doc(jsonMap);

    try {
      UpdateResponse updateResponse = restClient.update(request, RequestOptions.DEFAULT);

      // 处理UpdateResponse
      processUpdateRequest(updateResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  // 处理UpdateResponse
  private void processUpdateRequest(UpdateResponse updateResponse) {
    String index = updateResponse.getIndex();
    String id = updateResponse.getId();
    long version = updateResponse.getVersion();
    log.info("update id is " + id + ", index is " + index + ",version is " + version);

    if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
      // 创建文档成功时

    } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
      // 更新文档成功时
      // 查看更新的数据
      log.info(updateResponse.getResult().toString());

    } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
      // 删除文档成功时

    } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
      // 无文档操作时

    }
  }

  // 异步方式更新索引文档
  public void updateIndexDocumentsAsync(String indexName, String document) {
    UpdateRequest request = new UpdateRequest(indexName, document);

    ActionListener listener = new ActionListener<UpdateResponse>() {
      @Override
      public void onResponse(UpdateResponse updateResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.updateAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  /*
   * 以下是词向量相关API的使用介绍
   */

  // 构建TermVectorsRequest
  public void buildTermVectorsRequest(String indexName, String document, String field) {
    // 方式1：索引中存在的文档
    TermVectorsRequest request = new TermVectorsRequest(indexName, document);
    request.setFields(field);

    // 方式2：索引中不存在的文档, 也可以人工为文档生成词向量
    try {
      XContentBuilder docBuilder = XContentFactory.jsonBuilder();
      docBuilder.startObject().field("user", "niudong").endObject();
      request = new TermVectorsRequest(indexName, docBuilder);
    } catch (Exception e) {
      e.printStackTrace();
    }

    /*
     * 可选参数
     */

    // 将FieldStatistics设置为false（默认为true）可忽略文档计数、文档频率总和、总术语频率总和。
    request.setFieldStatistics(false);

    // 将termstatistics设置为true（默认值为false），以显示术语总频率和文档频率。
    request.setTermStatistics(true);

    // 将“位置”设置为“假”（默认为“真”）以忽略位置的输出。
    request.setPositions(false);

    // 将“偏移”设置为“假”（默认为“真”）以忽略偏移的输出。
    request.setOffsets(false);

    // 将“有效载荷”设置为“假”（默认为“真”）以忽略有效载荷的输出。
    request.setPayloads(false);

    Map<String, Integer> filterSettings = new HashMap<>();
    filterSettings.put("max_num_terms", 3);
    filterSettings.put("min_term_freq", 1);
    filterSettings.put("max_term_freq", 10);
    filterSettings.put("min_doc_freq", 1);
    filterSettings.put("max_doc_freq", 100);
    filterSettings.put("min_word_length", 1);
    filterSettings.put("max_word_length", 10);
    // 设置filtersettings，根据tf-idff分数筛选可返回的词条。
    request.setFilterSettings(filterSettings);

    Map<String, String> perFieldAnalyzer = new HashMap<>();
    perFieldAnalyzer.put("user", "keyword");
    // 设置PerFieldAnalyzer,指定与字段具有的分析器不同的分析器。
    request.setPerFieldAnalyzer(perFieldAnalyzer);

    // 将realtime设置为false（默认值为true）以在realtime附近检索术语向量。
    request.setRealtime(false);

    // 设置路由
    request.setRouting("routing");
  }

  // 同步执行TermVectorsRequest请求
  public void exucateTermVectorsRequest(String indexName, String document, String field) {
    TermVectorsRequest request = new TermVectorsRequest(indexName, document);
    request.setFields(field);

    try {
      TermVectorsResponse response = restClient.termvectors(request, RequestOptions.DEFAULT);

      // 处理TermVectorsResponse
      processTermVectorsResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    //closeEs();
  }

  // 处理TermVectorsResponse
  private void processTermVectorsResponse(TermVectorsResponse response) {
    String index = response.getIndex();
    String type = response.getType();
    String id = response.getId();
    // 指示是否找到文档。
    boolean found = response.getFound();
    log.info("index is " + index + ",id is " + id + ", type is " + type + ", found is " + found);

    List<TermVector> list = response.getTermVectorsList();
    log.info("list is " + list.size());
    for (TermVector tv : list) {
      processTermVector(tv);
    }

  }

  // 处理TermVector
  private void processTermVector(TermVector tv) {
    String fieldname = tv.getFieldName();
    int docCount = tv.getFieldStatistics().getDocCount();
    long sumTotalTermFreq = tv.getFieldStatistics().getSumTotalTermFreq();
    long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
    log.info("fieldname is " + fieldname + "; docCount is " + docCount + "; sumTotalTermFreq is "
        + sumTotalTermFreq + ";sumDocFreq is " + sumDocFreq);

    if (tv.getTerms() == null) {
      return;
    }

    List<TermVectorsResponse.TermVector.Term> terms = tv.getTerms();
    for (TermVectorsResponse.TermVector.Term term : terms) {
      String termStr = term.getTerm();
      int termFreq = term.getTermFreq();
      int docFreq = term.getDocFreq() == null ? 0 : term.getDocFreq();
      long totalTermFreq = term.getTotalTermFreq() == null ? 0 : term.getTotalTermFreq();
      float score = term.getScore() == null ? 0 : term.getScore();
      log.info("termStr is " + termStr + "; termFreq is " + termFreq + "; docFreq is " + docFreq
          + ";totalTermFreq is " + totalTermFreq + ";score is " + score);

      if (term.getTokens() != null) {
        List<TermVectorsResponse.TermVector.Token> tokens = term.getTokens();
        for (TermVectorsResponse.TermVector.Token token : tokens) {
          int position = token.getPosition() == null ? 0 : token.getPosition();
          int startOffset = token.getStartOffset() == null ? 0 : token.getStartOffset();
          int endOffset = token.getEndOffset() == null ? 0 : token.getEndOffset();
          String payload = token.getPayload();

          log.info("position is " + position + "; startOffset is " + startOffset + "; endOffset is "
              + endOffset + ";payload is " + payload);
        }
      }
    }


  }

  // 异步执行TermVectorsRequest请求
  public void exucateTermVectorsRequestAsync(String indexName, String document, String field) {
    TermVectorsRequest request = new TermVectorsRequest(indexName, document);
    request.setFields(field);

    ActionListener listener = new ActionListener<TermVectorsResponse>() {
      @Override
      public void onResponse(TermVectorsResponse termVectorsResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.termvectorsAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();
  }

  /*
   * 以下展示批量请求的API使用方法
   */

  // 构建BulkRequest
  public void buildBulkRequest(String indexName, String field) {
    /*
     * 方式1：添加同型请求
     */
    BulkRequest request = new BulkRequest();
    // 添加第一个IndexRequest
    request.add(new IndexRequest(indexName).id("1").source(XContentType.JSON, field,
        "事实上，自今年年初开始，美联储就已传递出货币政策或将转向的迹象"));
    // 添加第二个IndexRequest
    request.add(new IndexRequest(indexName).id("2").source(XContentType.JSON, field,
        "自6月起，市场对于美联储降息的预期愈发强烈"));
    // 添加第三个IndexRequest
    request.add(new IndexRequest(indexName).id("3").source(XContentType.JSON, field,
        "从此前美联储降息历程来看，美联储降息将打开全球各国央行的降息窗口"));
    /*
     * 方式2：添加异型请求
     */
    // 添加一个 DeleteRequest
    request.add(new DeleteRequest(indexName, "3"));
    // 添加一个 UpdateRequest
    request.add(new UpdateRequest(indexName, "2").doc(XContentType.JSON, field,
        "自今年初美联储暂停加息以来，全球范围内的降息大幕就已拉开，不仅包括新兴经济体，发达经济体也加入降息阵营，仅7月份一个月内，就有6国央行降息"));
    // 添加一个IndexRequest
    request.add(new IndexRequest(indexName).id("4").source(XContentType.JSON, field,
        "在此次美联储降息后，央行或不会立即跟进降息z"));

    /*
     * 以下是可选参数的配置
     */
    // 设置超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 设置数据刷新策略
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    request.setRefreshPolicy("wait_for");

    // 设置在继续执行索引/更新/删除操作之前必须处于活动状态的碎片副本数。
    request.waitForActiveShards(2);
    request.waitForActiveShards(ActiveShardCount.ALL);

    // 用于所有子请求的全局pipelineid
    request.pipeline("pipelineId");

    // 用于所有子请求的全局路由ID
    request.routing("routingId");
  }

  // 同步方式执行BulkRequest
  public void executeBulkRequest(String indexName, String field) {
    BulkRequest request = new BulkRequest();
    // 添加第一个IndexRequest
    request.add(new IndexRequest(indexName).id("1").source(XContentType.JSON, field,
        "事实上，自今年年初开始，美联储就已传递出货币政策或将转向的迹象"));
    // 添加第二个IndexRequest
    request.add(new IndexRequest(indexName).id("2").source(XContentType.JSON, field,
        "自6月起，市场对于美联储降息的预期愈发强烈"));
    // 添加第三个IndexRequest
    request.add(new IndexRequest(indexName).id("3").source(XContentType.JSON, field,
        "从此前美联储降息历程来看，美联储降息将打开全球各国央行的降息窗口"));

    try {
      BulkResponse bulkResponse = restClient.bulk(request, RequestOptions.DEFAULT);

      // 解析BulkResponse
      processBulkResponse(bulkResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();

  }

  // 解析BulkResponse
  private void processBulkResponse(BulkResponse bulkResponse) {
    if (bulkResponse == null) {
      return;
    }

    for (BulkItemResponse bulkItemResponse : bulkResponse) {
      DocWriteResponse itemResponse = bulkItemResponse.getResponse();

      switch (bulkItemResponse.getOpType()) {
        // 索引状态
        case INDEX:
          // 索引生成
        case CREATE:
          IndexResponse indexResponse = (IndexResponse) itemResponse;
          String index = indexResponse.getIndex();
          String id = indexResponse.getId();
          long version = indexResponse.getVersion();
          log.info("create id is " + id + ", index is " + index + ",version is " + version);

          break;
        // 索引更新
        case UPDATE:
          UpdateResponse updateResponse = (UpdateResponse) itemResponse;
          break;
        // 索引删除
        case DELETE:
          DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
      }
    }
  }

  // 异步方式执行BulkRequest
  public void executeBulkRequestAsync(String indexName, String field) {
    BulkRequest request = new BulkRequest();
    // 添加第一个IndexRequest
    request.add(new IndexRequest(indexName).id("1").source(XContentType.JSON, field,
        "事实上，自今年年初开始，美联储就已传递出货币政策或将转向的迹象"));
    // 添加第二个IndexRequest
    request.add(new IndexRequest(indexName).id("2").source(XContentType.JSON, field,
        "自6月起，市场对于美联储降息的预期愈发强烈"));
    // 添加第三个IndexRequest
    request.add(new IndexRequest(indexName).id("3").source(XContentType.JSON, field,
        "从此前美联储降息历程来看，美联储降息将打开全球各国央行的降息窗口"));

    // 构建监听器
    ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
      @Override
      public void onResponse(BulkResponse bulkResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.bulkAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 关闭ES连接
    closeEs();

  }

  // 构建BulkProcessor
  public void buildBulkRequestWithBulkProcessor(String indexName, String field) {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        // 批量处理前的动作
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        // 批量处理后的动作
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        // 批量处理后的动作
      }
    };

    BulkProcessor bulkProcessor = BulkProcessor.builder((request, bulkListener) -> restClient
        .bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener).build();

    /*
     * BulkProcessor的配置
     */
    BulkProcessor.Builder builder = BulkProcessor.builder((request, bulkListener) -> restClient
        .bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);

    // 根据当前添加的操作数设置刷新批量请求的时间（默认值为1000，使用-1表示禁用）
    builder.setBulkActions(500);

    // 根据当前添加的操作大小设置刷新批量请求的时间（默认为5MB，使用-1表示禁用）
    builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));

    // 设置允许执行的并发请求数（默认为1，使用0时表示仅允许执行单个请求）
    builder.setConcurrentRequests(0);

    // 设置刷新间隔刷（默认为未设置）
    builder.setFlushInterval(TimeValue.timeValueSeconds(10L));

    // 设置一个恒定的后退策略，该策略最初等待1秒，最多重试3次。
    builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));

    /**
     * 添加索引请求
     */
    IndexRequest one = new IndexRequest(indexName).id("6").source(XContentType.JSON, "title",
        "8月1日，中国空军发布强军宣传片《初心伴我去战斗》，通过歼-20、轰-6K等新型战机练兵备战的震撼场景，展现新时代空军发展的新气象，彰显中国空军维护国家主权、保卫国家安全、保障和平发展的意志和能力。");
    IndexRequest two = new IndexRequest(indexName).id("7").source(XContentType.JSON, "title",
        "在2分钟的宣传片中，中国空军现役先进战机悉数亮相，包括歼-20、歼-16、歼-11、歼-10B/C、苏-35、苏-27、轰-6K等机型");
    IndexRequest three = new IndexRequest(indexName).id("8").source(XContentType.JSON, "title",
        "宣传片发布正逢八一建军节，而今年是新中国成立70周年，也是人民空军成立70周年。70年来，中国空军在各领域取得全面发展，战略打击、战略预警、空天防御和战略投送等能力得到显著进步。");

    bulkProcessor.add(one);
    bulkProcessor.add(two);
    bulkProcessor.add(three);
  }

  // 用BulkProcessor在同步方式下 执行BulkRequest
  public void executeBulkRequestWithBulkProcessor(String indexName, String field) {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        // 批量处理前的动作
        int numberOfActions = request.numberOfActions();
        log.info("Executing bulk " + executionId + " with " + numberOfActions + " requests");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        // 批量处理后的动作
        if (response.hasFailures()) {
          log.info("Bulk " + executionId + " executed with failures");
        } else {
          log.info("Bulk " + executionId + " completed in " + response.getTook().getMillis()
              + " milliseconds");
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        // 批量处理后的动作
        log.error("Failed to execute bulk", failure);
      }
    };

    BulkProcessor bulkProcessor = BulkProcessor.builder((request, bulkListener) -> restClient
        .bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener).build();

    /**
     * 添加索引请求
     */
    IndexRequest one = new IndexRequest(indexName).id("6").source(XContentType.JSON, "title",
        "8月1日，中国空军发布强军宣传片《初心伴我去战斗》，通过歼-20、轰-6K等新型战机练兵备战的震撼场景，展现新时代空军发展的新气象，彰显中国空军维护国家主权、保卫国家安全、保障和平发展的意志和能力。");
    IndexRequest two = new IndexRequest(indexName).id("7").source(XContentType.JSON, "title",
        "在2分钟的宣传片中，中国空军现役先进战机悉数亮相，包括歼-20、歼-16、歼-11、歼-10B/C、苏-35、苏-27、轰-6K等机型");
    IndexRequest three = new IndexRequest(indexName).id("8").source(XContentType.JSON, "title",
        "宣传片发布正逢八一建军节，而今年是新中国成立70周年，也是人民空军成立70周年。70年来，中国空军在各领域取得全面发展，战略打击、战略预警、空天防御和战略投送等能力得到显著进步。");

    bulkProcessor.add(one);
    bulkProcessor.add(two);
    bulkProcessor.add(three);
  }

  /*
   * 以下介绍Multi - Get API相关的使用
   */
  // 构建MultiGetRequest
  public void buildMultiGetRequest(String indexName, String[] documentIds) {
    if (documentIds == null || documentIds.length <= 0) {
      return;
    }

    MultiGetRequest request = new MultiGetRequest();
    for (String documentId : documentIds) {
      // 添加请求
      request.add(new MultiGetRequest.Item(indexName, documentId));
    }

    /*
     * 可选参数使用介绍
     */
    // 禁用源检索， 默认情况下启用
    request.add(new MultiGetRequest.Item(indexName, documentIds[0])
        .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));

    // 为特定字段配置 源包含关系
    String[] excludes = Strings.EMPTY_ARRAY;
    String[] includes = {"title", "content"};
    FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
    request.add(
        new MultiGetRequest.Item(indexName, documentIds[0]).fetchSourceContext(fetchSourceContext));

    // 为特定字段配置 源排除关系
    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
    request.add(
        new MultiGetRequest.Item(indexName, documentIds[0]).fetchSourceContext(fetchSourceContext));

    // 为特定存储字段配置检索（ 要求字段在索引中单独存储字段）
    try {
      request.add(new MultiGetRequest.Item(indexName, documentIds[0]).storedFields("title"));
      MultiGetResponse response = restClient.mget(request, RequestOptions.DEFAULT);
      MultiGetItemResponse item = response.getResponses()[0];
      // 检索title存储字段（ 要求该字段单独存储在索引中）
      String value = item.getResponse().getField("title").getValue();
      log.info("value is " + value);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }

    // 配置路由
    request.add(new MultiGetRequest.Item(indexName, documentIds[0]).routing("routing"));

    // 配置版本和版本类型
    request.add(new MultiGetRequest.Item(indexName, documentIds[0])
        .versionType(VersionType.EXTERNAL).version(10123L));

    // 配置偏好值
    request.preference("title");

    // 将实时标志设置为假（ 默认为真）
    request.realtime(false);

    // 在检索文档之前执行刷新（默认为false）
    request.refresh(true);
  }

  // 同步执行MultiGetRequest
  public void executeMultiGetRequest(String indexName, String[] documentIds) {
    if (documentIds == null || documentIds.length <= 0) {
      return;
    }

    MultiGetRequest request = new MultiGetRequest();
    for (String documentId : documentIds) {
      // 添加请求
      request.add(new MultiGetRequest.Item(indexName, documentId));
    }

    try {
      MultiGetResponse response = restClient.mget(request, RequestOptions.DEFAULT);

      // 解析MultiGetResponse
      processMultiGetResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  // 解析MultiGetResponse
  private void processMultiGetResponse(MultiGetResponse multiResponse) {
    if (multiResponse == null) {
      return;
    }

    MultiGetItemResponse[] responses = multiResponse.getResponses();
    log.info("responses is " + responses.length);

    for (MultiGetItemResponse response : responses) {
      GetResponse getResponse = response.getResponse();
      String index = response.getIndex();
      String id = response.getId();
      log.info("index is " + index + ";id is " + id);

      if (getResponse.isExists()) {
        long version = getResponse.getVersion();
        // 按字符串方式获取内容
        String sourceAsString = getResponse.getSourceAsString();
        // 按Map方式获取内容
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        // 按字节数组方式获取内容
        byte[] sourceAsBytes = getResponse.getSourceAsBytes();

        log.info("version is " + version + ";sourceAsString is " + sourceAsString);
      }
    }
  }

  // 异步执行MultiGetRequest
  public void executeMultiGetRequestAsync(String indexName, String[] documentIds) {
    if (documentIds == null || documentIds.length <= 0) {
      return;
    }

    MultiGetRequest request = new MultiGetRequest();
    for (String documentId : documentIds) {
      // 添加请求
      request.add(new MultiGetRequest.Item(indexName, documentId));
    }

    // 添加ActionListener
    ActionListener listener = new ActionListener<MultiGetResponse>() {
      @Override
      public void onResponse(MultiGetResponse response) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 执行批量获取
    try {
      MultiGetResponse response = restClient.mget(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  /*
   * 以下代码示例为Reindex的使用
   */

  // 构建ReindexRequest
  public void bulidReindexRequest(String fromIndex, String toIndex) {
    ReindexRequest request = new ReindexRequest();
    // 添加要从中复制的源的列表
    request.setSourceIndices("source1", "source2");
    // 添加目标索引
    request.setDestIndex(toIndex);

    /*
     * ReindexRequest的参数配置
     */

    // 设置目标索引的版本类型VersionType
    request.setDestVersionType(VersionType.EXTERNAL);

    // 设置目标索引的操作类型OpType为创建类型
    request.setDestOpType("create");

    // 默认情况下，版本冲突会中止重新索引进程，我们可以用以下方法计算它们
    request.setConflicts("proceed");

    // 通过添加查询来限制文档。 如下所示仅复制字段用户设置为 kimchy 的文档
    request.setSourceQuery(new TermQueryBuilder("user", "kimchy"));

    // 通过设置大小来限制已处理文档的数量。
    request.setSize(10);


    // 默认情况下，reindex使用1000个批次。可以使用sourceBatchSize更改批大小。
    request.setSourceBatchSize(100);

    // 指定管道模式
    request.setDestPipeline("my_pipeline");

    // 如果需要源索引中的一组特定文档，则需要使用sort。如果可能的话，最好选择更具选择性的查询，而不是进行大小和排序。
    request.addSortField("field1", SortOrder.DESC);
    request.addSortField("field2", SortOrder.ASC);

    // 使用切片滚动对uid进行切片。使用setslices指定要使用的切片数。
    request.setSlices(2);

    // 使用scroll参数控制“search context”保持活动的时间。
    request.setScroll(TimeValue.timeValueMinutes(10));

    // 设置超时时间
    request.setTimeout(TimeValue.timeValueMinutes(2));

    // 调用reindex后刷新索引
    request.setRefresh(true);

  }

  // 同步执行ReindexRequest
  public void executeReindexRequest(String fromIndex, String toIndex) {
    ReindexRequest request = new ReindexRequest();
    // 添加要从中复制的源的列表
    request.setSourceIndices(fromIndex);
    // 添加目标索引
    request.setDestIndex(toIndex);

    try {
      BulkByScrollResponse bulkResponse = restClient.reindex(request, RequestOptions.DEFAULT);

      // 解析 BulkByScrollResponse
      processBulkByScrollResponse(bulkResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  // 解析 BulkByScrollResponse
  private void processBulkByScrollResponse(BulkByScrollResponse bulkResponse) {
    if (bulkResponse == null) {
      return;
    }

    // 获取总耗时
    TimeValue timeTaken = bulkResponse.getTook();
    log.info("time is " + timeTaken.getMillis());

    // 检查请求是否超时
    boolean timedOut = bulkResponse.isTimedOut();
    log.info("timedOut is " + timedOut);

    // 获取已处理的文档总数
    long totalDocs = bulkResponse.getTotal();
    log.info("totalDocs is " + totalDocs);

    // 已更新的文档数
    long updatedDocs = bulkResponse.getUpdated();
    log.info("updatedDocs is " + updatedDocs);

    // 已创建的文档数
    long createdDocs = bulkResponse.getCreated();
    log.info("createdDocs is " + createdDocs);

    // 已删除的文档数
    long deletedDocs = bulkResponse.getDeleted();
    log.info("deletedDocs is " + deletedDocs);

    // 已执行的批次数
    long batches = bulkResponse.getBatches();
    log.info("batches is " + batches);

    // 跳过的文档数
    long noops = bulkResponse.getNoops();
    log.info("noops is " + noops);

    // 版本冲突数
    long versionConflicts = bulkResponse.getVersionConflicts();
    log.info("versionConflicts is " + versionConflicts);

    // 重试批量索引操作的次数
    long bulkRetries = bulkResponse.getBulkRetries();
    log.info("bulkRetries is " + bulkRetries);

    // 重试搜索操作的次数
    long searchRetries = bulkResponse.getSearchRetries();
    log.info("searchRetries is " + searchRetries);

    // 请求阻塞的总时间，不包括当前处于休眠状态的限制时间
    TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
    log.info("throttledMillis is " + throttledMillis.getMillis());

    // 查询失败数量
    List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
    log.info("searchFailures is " + searchFailures.size());

    // 批量操作失败数量
    List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
    log.info("bulkFailures is " + bulkFailures.size());
  }

  // 异步执行ReindexRequest
  public void executeReindexRequestAsync(String fromIndex, String toIndex) {
    ReindexRequest request = new ReindexRequest();
    // 添加要从中复制的源的列表
    request.setSourceIndices("source1", "source2");
    // 添加目标索引
    request.setDestIndex(toIndex);

    // 构建监听器
    ActionListener listener = new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.reindexAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  /*
   * 以下展示UpdateByQueryRequest的API使用
   */

  // 构建UpdateByQueryRequest
  public void buildUpdateByQueryRequest(String indexName) {
    UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);

    /*
     * 配置UpdateByQueryRequest
     */

    // 默认情况下， 版本冲突将中止UpdateByQueryRequest进程， 但我们可以使用以下方法来计算它们
    request.setConflicts("proceed");

    // 通过添加查询条件来限制。 如下仅更新字段用户设置为niudong的文档
    request.setQuery(new TermQueryBuilder("user", "niudong"));

    // 设置大小来限制已处理文档的数量
    request.setSize(10);

    // 默认情况下， UpdateByQueryRequest 使用的批数为1000。 可以使用 setBatchSize 更改批大小。
    request.setBatchSize(100);

    // 指定管道模式
    request.setPipeline("my_pipeline");

    // 设置分片滚动来并行化
    request.setSlices(2);

    // 使用滚动参数控制“搜索上下文” 保持连接的时间
    request.setScroll(TimeValue.timeValueMinutes(10));

    // 如果提供路由， 那么路由将被复制到滚动查询， 从而限制与该路由值匹配的分片处理。
    request.setRouting("=cat");

    // 设置 等待请求的超时时间
    request.setTimeout(TimeValue.timeValueMinutes(2));

    // 调用update by query后刷新索引
    request.setRefresh(true);

    // 设置索引选项
    request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

  }

  // 同步执行UpdateByQueryRequest
  public void executeUpdateByQueryRequest(String indexName) {
    UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);

    try {
      BulkByScrollResponse bulkResponse = restClient.updateByQuery(request, RequestOptions.DEFAULT);

      // 处理BulkByScrollResponse
      processBulkByScrollResponse(bulkResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  // 处理BulkByScrollResponse
  private void processBulkByScrollResponseInUpdateByQuery(BulkByScrollResponse bulkResponse) {
    if (bulkResponse == null) {
      return;
    }

    // 整个操作从开始到结束的毫秒数
    TimeValue timeTaken = bulkResponse.getTook();
    log.info("time is " + timeTaken.getMillis());

    // 在查询执行更新期间执行的任何请求超时 ，则将此标志设置为ture
    boolean timedOut = bulkResponse.isTimedOut();
    log.info("timedOut is " + timedOut);

    // 已成功处理的文档数
    long totalDocs = bulkResponse.getTotal();
    log.info("totalDocs is " + totalDocs);

    // 已成功更新的文档数
    long updatedDocs = bulkResponse.getUpdated();
    log.info("updatedDocs is " + updatedDocs);

    // 已成功删除的文档数
    long deletedDocs = bulkResponse.getDeleted();
    log.info("deletedDocs is " + deletedDocs);

    // 由查询更新的批次数量
    long batches = bulkResponse.getBatches();
    log.info("batches is " + batches);

    // 由查询更新拉回的滚动响应数
    long noops = bulkResponse.getNoops();
    log.info("noops is " + noops);

    // 按查询更新的版本冲突数
    long versionConflicts = bulkResponse.getVersionConflicts();
    log.info("versionConflicts is " + versionConflicts);

    // 更新尝试的重试次数
    long bulkRetries = bulkResponse.getBulkRetries();
    log.info("bulkRetries is " + bulkRetries);

    // 搜索的重试次数
    long searchRetries = bulkResponse.getSearchRetries();
    log.info("searchRetries is " + searchRetries);

    // 搜索失败的数量
    List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
    log.info("searchFailures is " + searchFailures.size());

    // 批量操作失败的次数
    List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
    log.info("bulkFailures is " + bulkFailures.size());
  }

  // 异步执行UpdateByQueryRequest
  public void executeUpdateByQueryRequestAsync(String indexName) {
    UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);

    // 添加监听器
    ActionListener listener = new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.updateByQueryAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  /*
   * 以下展示 DeleteByQueryRequest 的API使用
   */

  // 构建 DeleteByQueryRequest
  public void buildDeleteByQueryRequest(String indexName) {
    DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);

    /*
     * 配置 DeleteByQueryRequest
     */

    // 默认情况下， 版本冲突将中止 DeleteByQueryRequest 进程， 但我们可以使用以下方法来计算它们
    request.setConflicts("proceed");

    // 通过添加查询条件来限制。 如下仅删除字段用户设置为niudong的文档
    request.setQuery(new TermQueryBuilder("user", "niudong"));

    // 设置大小来限制已处理文档的数量
    request.setSize(10);

    // 默认情况下， UpdateByQueryRequest 使用的批数为1000。 可以使用 setBatchSize 更改批大小。
    request.setBatchSize(100);

    // 设置分片滚动来并行化
    request.setSlices(2);

    // 使用滚动参数控制“搜索上下文” 保持连接的时间
    request.setScroll(TimeValue.timeValueMinutes(10));

    // 如果提供路由， 那么路由将被复制到滚动查询， 从而限制与该路由值匹配的分片处理。
    request.setRouting("=cat");

    // 设置 等待请求的超时时间
    request.setTimeout(TimeValue.timeValueMinutes(2));

    // 调用update by query后刷新索引
    request.setRefresh(true);

    // 设置索引选项
    request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

  }

  // 同步执行 DeleteByQueryRequest
  public void executeDeleteByQueryRequest(String indexName) {
    DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
    // 通过添加查询条件来限制。 如下仅删除字段content设置为niudong的文档
    request.setQuery(new TermQueryBuilder("content", "niudong"));

    try {
      BulkByScrollResponse bulkResponse = restClient.deleteByQuery(request, RequestOptions.DEFAULT);

      // 处理BulkByScrollResponse
      processBulkByScrollResponse(bulkResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  // 异步执行 DeleteByQueryRequest
  public void executeDeleteByQueryRequestAsync(String indexName) {
    DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);

    // 构建监听器
    ActionListener listener = new ActionListener<BulkByScrollResponse>() {
      @Override
      public void onResponse(BulkByScrollResponse bulkResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 执行DeleteByQuery
    try {
      restClient.deleteByQueryAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  /*
   * 以下代码展示MultiTermVectorsRequest的使用
   */
  // 构建MultiTermVectorsRequest
  public void buildMultiTermVectorsRequest(String indexName, String[] documentIds, String field) {
    // 方法1：创建一个空的 MultiTermVectorsRequest，然后向其添加单个term vectors请求
    MultiTermVectorsRequest request = new MultiTermVectorsRequest();

    for (String documentId : documentIds) {
      TermVectorsRequest tvrequest = new TermVectorsRequest(indexName, documentId);
      tvrequest.setFields(field);
      request.add(tvrequest);
    }


    // 方法2：所有词向量请求共享相同参数（如索引和其他设置）
    TermVectorsRequest tvrequestTemplate = new TermVectorsRequest(indexName, "1");
    tvrequestTemplate.setFields(field);
    String[] ids = {"1", "2"};
    request = new MultiTermVectorsRequest(ids, tvrequestTemplate);
  }

  // 同步执行MultiTermVectorsRequest
  public void executeMultiTermVectorsRequest(String indexName, String[] documentIds, String field) {
    // 方法1：创建一个空的 MultiTermVectorsRequest，然后向其添加单个term vectors请求
    MultiTermVectorsRequest request = new MultiTermVectorsRequest();

    for (String documentId : documentIds) {
      TermVectorsRequest tvrequest = new TermVectorsRequest(indexName, documentId);
      tvrequest.setFields(field);
      request.add(tvrequest);
    }

    try {
      MultiTermVectorsResponse response = restClient.mtermvectors(request, RequestOptions.DEFAULT);

      // 解析MultiTermVectorsResponse
      processMultiTermVectorsResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }

  // 解析MultiTermVectorsResponse
  private void processMultiTermVectorsResponse(MultiTermVectorsResponse response) {
    if (response == null) {
      return;
    }

    List<TermVectorsResponse> tvresponseList = response.getTermVectorsResponses();

    if (tvresponseList == null) {
      return;
    }

    log.info("tvresponseList size is " + tvresponseList.size());

    for (TermVectorsResponse tvresponse : tvresponseList) {
      String id = tvresponse.getId();
      String index = tvresponse.getIndex();
      log.info("id size is " + id + "; index is " + index);
    }
  }

  // 异步执行MultiTermVectorsRequest
  public void executeMultiTermVectorsRequestAsync(String indexName, String[] documentIds,
      String field) {
    // 方法1：创建一个空的 MultiTermVectorsRequest，然后向其添加单个term vectors请求
    MultiTermVectorsRequest request = new MultiTermVectorsRequest();

    for (String documentId : documentIds) {
      TermVectorsRequest tvrequest = new TermVectorsRequest(indexName, documentId);
      tvrequest.setFields(field);
      request.add(tvrequest);
    }

    // 构建监听器
    ActionListener listener = new ActionListener<MultiTermVectorsResponse>() {
      @Override
      public void onResponse(MultiTermVectorsResponse mtvResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.mtermvectorsAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES连接
      closeEs();
    }
  }
}
