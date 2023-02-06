package com.niudong.esdemo.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.DetailAnalyzeResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import com.niudong.esdemo.service.IndexService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述索引相关API使用和测试
 *
 */
@Service
public class IndexServiceImpl implements IndexService {
  private static Log log = LogFactory.getLog(IndexServiceImpl.class);

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

  // 构建AnalyzeRequest
  public void buildAnalyzeRequest() {
    AnalyzeRequest request = new AnalyzeRequest();

    // 中文：要包含的文本。多个字符串被视为多值字段
    request.text("中国天眼系统首次探测到宇宙深处的神秘射电信号");
    // 内置分词器
    request.analyzer("standard");

    // 英文：要包含的文本。多个字符串被视为多值字段
    request.text("Some text to analyze", "Some more text to analyze");
    // 内置分词器
    request.analyzer("english");

    // 自定义分析器1
    request.text("<b>Some text to analyze</b>");
    // 配置字符筛选器
    request.addCharFilter("html_strip");
    // 配置标记器
    request.tokenizer("standard");
    // 添加内置标记筛选器
    request.addTokenFilter("lowercase");

    // 自定义分析器2
    Map<String, Object> stopFilter = new HashMap<>();
    stopFilter.put("type", "stop");
    // 自定义令牌筛选器tokenfilter的配置
    stopFilter.put("stopwords", new String[] {"to"});
    // 添加自定义标记筛选器
    request.addTokenFilter(stopFilter);

    // 可选参数
    // 将explain设置为true将为响应添加更多详细信息
    request.explain(true);
    // 设置属性允许只返回用户感兴趣的令牌属性
    request.attributes("keyword", "type");

  }

  // 构建AnalyzeRequest
  public AnalyzeRequest buildAnalyzeRequest(String text) {
    AnalyzeRequest request = new AnalyzeRequest();

    // 要包含的文本。多个字符串被视为多值字段
    request.text(text);

    // 内置分词器
    request.analyzer("standard");

    return request;

  }

  // 同步执行AnalyzeRequest
  public void executeAnalyzeRequest(String text) {
    // 构建AnalyzeRequest
    AnalyzeRequest request = buildAnalyzeRequest(text);

    try {
      AnalyzeResponse response = restClient.indices().analyze(request, RequestOptions.DEFAULT);

      // 解析AnalyzeResponse
      processAnalyzeResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析AnalyzeResponse
  private void processAnalyzeResponse(AnalyzeResponse response) {
    // analyzetoken保存有关分析生成的单个令牌的信息
    List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
    if (tokens == null) {
      return;
    }
    for (AnalyzeResponse.AnalyzeToken token : tokens) {
      log.info(token.getTerm() + " start offset is " + token.getStartOffset() + ";end offset is "
          + token.getEndOffset() + ";position is" + token.getPosition());
    }

    // 如果explain设置为true， 则通过detail()方法返回信息：
    // DetailAnalyzeResponse 包含有关分析链中不同子步骤生成的令牌的更详细信息。
    DetailAnalyzeResponse detail = response.detail();
    if (detail == null) {
      return;
    }
    log.info("detail is " + detail.toString());
  }

  // 异步执行AnalyzeRequest
  public void executeAnalyzeRequestAsync(String text) {
    // 构建AnalyzeRequest
    AnalyzeRequest request = buildAnalyzeRequest(text);

    // 构建监听器
    ActionListener<AnalyzeResponse> listener = new ActionListener<AnalyzeResponse>() {
      @Override
      public void onResponse(AnalyzeResponse analyzeTokens) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().analyzeAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是创建索引请求相关代码
   */
  // 创建索引
  public void buildIndexRequest(String index, int shardsNumber, int replicasNumber) {
    CreateIndexRequest request = new CreateIndexRequest(index);

    // 配置分片数量和副本数量
    request.settings(Settings.builder().put("index.number_of_shards", shardsNumber)
        .put("index.number_of_replicas", replicasNumber));

    // 配置映射源
    // 字符串形式
    request.mapping("{\n" + "  \"properties\": {\n" + "    \"message\": {\n"
        + "      \"type\": \"text\"\n" + "    }\n" + "  }\n" + "}", XContentType.JSON);

    // MAP形式
    Map<String, Object> message = new HashMap<>();
    message.put("type", "text");
    Map<String, Object> properties = new HashMap<>();
    properties.put("message", message);
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("properties", properties);
    request.mapping(mapping);

    // XContentBuilder形式
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.startObject("properties");
        {
          builder.startObject("message");
          {
            builder.field("type", "text");
          }
          builder.endObject();
        }
        builder.endObject();
      }
      builder.endObject();
      request.mapping(builder);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {

    }

    // 设置别名:在索引创建时设置
    request.alias(new Alias(index + "_alias").filter(QueryBuilders.termQuery("user", "niudong")));
    // 设置别名:提供整个来源
    request.source("{\n" + "    \"settings\" : {\n" + "        \"number_of_shards\" : 1,\n"
        + "        \"number_of_replicas\" : 0\n" + "    },\n" + "    \"mappings\" : {\n"
        + "        \"properties\" : {\n" + "            \"message\" : { \"type\" : \"text\" }\n"
        + "        }\n" + "    },\n" + "    \"aliases\" : {\n" + "        \"niudong_alias\" : {}\n"
        + "    }\n" + "}", XContentType.JSON);

    // 可选参数配置
    // 等待所有节点确认创建索引的超时时间
    request.setTimeout(TimeValue.timeValueMinutes(2));
    // 连接到主节点的超时时间
    request.setMasterTimeout(TimeValue.timeValueMinutes(1));


    // 在请求响应返回前等待中活动状态的分片数量
    request.waitForActiveShards(ActiveShardCount.from(2));
    // 在请求响应返回前等待中活动状态的拷贝数量
    request.waitForActiveShards(ActiveShardCount.DEFAULT);

  }

  // 创建索引请求
  public CreateIndexRequest buildIndexRequest(String index) {
    CreateIndexRequest request = new CreateIndexRequest(index);

    // 配置默认分片数量和副本数量
    request.settings(Settings.builder().put("index.number_of_shards", 3)
        .put("index.number_of_replicas", 2).put("index.blocks.write", true));

    return request;
  }

  // 同步执行创建索引的请求
  public void executeIndexRequest(String index) {
    // 创建索引请求
    CreateIndexRequest request = buildIndexRequest(index);

    try {
      CreateIndexResponse createIndexResponse =
          restClient.indices().create(request, RequestOptions.DEFAULT);

      // 解析CreateIndexResponse
      processCreateIndexResponse(createIndexResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析CreateIndexResponse
  private void processCreateIndexResponse(CreateIndexResponse createIndexResponse) {
    // 所有节点是否已确认请求
    boolean acknowledged = createIndexResponse.isAcknowledged();
    // 是否在超时前为索引中的每个分片启动了所需数量的分片副本
    boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
    log.info("acknowledged is " + acknowledged + ";shardsAcknowledged is " + shardsAcknowledged);
  }

  // 异步执行创建索引的请求
  public void executeIndexRequestAsync(String index) {
    // 创建索引请求
    CreateIndexRequest request = buildIndexRequest(index);

    // 创建监听器
    ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {

      @Override
      public void onResponse(CreateIndexResponse createIndexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.indices().createAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是获取索引请求相关代码
   */
  // 构建获取索引请求GetIndexRequest
  public GetIndexRequest buildGetIndexRequest(String index) {
    GetIndexRequest request = new GetIndexRequest(index);

    // 如果为true，则对于未在索引上显式设置的设置，将返回默认值。
    request.includeDefaults(true);
    // 控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步执行GetIndexRequest
  public void excuteGetIndexRequest(String index) {
    GetIndexRequest request = buildGetIndexRequest(index);

    try {
      GetIndexResponse getIndexResponse = restClient.indices().get(request, RequestOptions.DEFAULT);

      // 解析GetIndexResponse
      processGetIndexResponse(getIndexResponse, index);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析GetIndexResponse
  private void processGetIndexResponse(GetIndexResponse getIndexResponse, String index) {
    // 检索不同类型的映射到索引的映射元数据MappingMetadata 。
    MappingMetaData indexMappings = getIndexResponse.getMappings().get(index);
    if (indexMappings == null) {
      return;
    }

    // 检索文档类型文档属性的映射
    Map<String, Object> indexTypeMappings = indexMappings.getSourceAsMap();
    for (String str : indexTypeMappings.keySet()) {
      log.info("key is " + str);
    }
    // 获取索引的别名列表
    List<AliasMetaData> indexAliases = getIndexResponse.getAliases().get(index);
    if (indexAliases == null) {
      return;
    }
    log.info("indexAliases is " + indexAliases.size());

    // 获取为索引设置字符串index.number_shards的值。如果未显式指定设置，但该设置是默认设置的一部分（includeDefault为true），则将检索默认设置。
    String numberOfShardsString = getIndexResponse.getSetting(index, "index.number_of_shards");
    // 检索索引的所有设置。
    Settings indexSettings = getIndexResponse.getSettings().get(index);
    // 设置对象提供了更多的灵活性。在这里，它被用来提取作为整数的碎片的设置index.number。
    Integer numberOfShards = indexSettings.getAsInt("index.number_of_shards", null);
    // 获取默认设置index.refresh_interval（如果includeDefault设置为true）。如果includeDefault设置为false，getIndexResponse.defaultSettings（）将返回空映射。
    TimeValue time =
        getIndexResponse.getDefaultSettings().get(index).getAsTime("index.refresh_interval", null);
    log.info("numberOfShardsString is " + numberOfShardsString + ";indexSettings is "
        + indexSettings.toString() + ";numberOfShards is " + numberOfShards.intValue() + ";time is "
        + time.getMillis());

  }

  // 异步执行GetIndexRequest
  public void excuteGetIndexRequestAsync(String index) {
    GetIndexRequest request = buildGetIndexRequest(index);

    // 构建监听器
    ActionListener<GetIndexResponse> listener = new ActionListener<GetIndexResponse>() {
      @Override
      public void onResponse(GetIndexResponse getIndexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.indices().getAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为删除索引相关代码
   */

  // 构建删除索引请求
  public DeleteIndexRequest buildDeleteIndexRequest(String index) {
    DeleteIndexRequest request = new DeleteIndexRequest(index);

    // 配置可选参数
    // 等待所有节点将索引删除确认的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    // 等待所有节点将索引删除确认的超时时间
    request.timeout("2m");

    // 连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    // 连接到主节点的超时时间
    request.masterNodeTimeout("1m");

    // 设置IndiceOptions控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步执行DeleteIndexRequest
  public void executeDeleteIndexRequest(String index) {
    DeleteIndexRequest request = buildDeleteIndexRequest(index);
    try {
      AcknowledgedResponse deleteIndexResponse =
          restClient.indices().delete(request, RequestOptions.DEFAULT);

      // 解析AcknowledgedResponse
      processAcknowledgedResponse(deleteIndexResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析AcknowledgedResponse
  private void processAcknowledgedResponse(AcknowledgedResponse deleteIndexResponse) {
    // 所有节点是否已确认请求
    boolean acknowledged = deleteIndexResponse.isAcknowledged();
    log.info("acknowledged is " + acknowledged);
  }

  // 异步执行DeleteIndexRequest
  public void executeDeleteIndexRequestAsync(String index) {
    DeleteIndexRequest request = buildDeleteIndexRequest(index);

    // 构建监听器
    ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
      @Override
      public void onResponse(AcknowledgedResponse deleteIndexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().deleteAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是索引存在验证相关代码
   */
  // 构建索引存在验证请求
  public GetIndexRequest buildExistsIndexRequest(String index) {
    GetIndexRequest request = new GetIndexRequest(index);

    // 从主节点返回本地信息或检索状态
    request.local(false);
    // 回归到适合于人类的格式
    request.humanReadable(true);
    // 是否返回每个索引的所有默认设置
    request.includeDefaults(false);
    // 控制如何解析不可用索引以及如何展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步执行索引存在验证请求
  public void executeExistsIndexRequest(String index) {
    GetIndexRequest request = buildExistsIndexRequest(index);

    try {
      boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
      log.info("exists is " + exists);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 异步执行索引存在验证请求
  public void executeExistsIndexRequestAsync(String index) {
    GetIndexRequest request = buildExistsIndexRequest(index);

    // 构建监听器
    ActionListener<Boolean> listener = new ActionListener<Boolean>() {
      @Override
      public void onResponse(Boolean exists) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.indices().existsAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是打开索引相关的代码
   */
  // 构建打开索引请求OpenIndexRequest
  public OpenIndexRequest buildOpenIndexRequest(String index) {
    OpenIndexRequest request = new OpenIndexRequest(index);

    // 配置可选参数
    // 所有节点确认索引打开的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 从节点连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    request.masterNodeTimeout("1m");

    // 请求返回响应前活跃的分片数量
    request.waitForActiveShards(2);
    request.waitForActiveShards(ActiveShardCount.DEFAULT);

    // 设置IndiceOptions控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.strictExpandOpen());

    return request;
  }

  // 同步方式执行OpenIndexRequest
  public void executeOpenIndexRequest(String index) {
    OpenIndexRequest request = buildOpenIndexRequest(index);
    try {
      OpenIndexResponse openIndexResponse =
          restClient.indices().open(request, RequestOptions.DEFAULT);

      // 解析 OpenIndexResponse
      processOpenIndexResponse(openIndexResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析 OpenIndexResponse
  private void processOpenIndexResponse(OpenIndexResponse openIndexResponse) {
    // 所有节点是否已确认请求
    boolean acknowledged = openIndexResponse.isAcknowledged();
    // 是否在超时前为索引中的每个分片启动了所需数量的分片副本
    boolean shardsAcked = openIndexResponse.isShardsAcknowledged();
    log.info("acknowledged is " + acknowledged + ";shardsAcked is " + shardsAcked);
  }

  // 异步方式执行OpenIndexRequest
  public void executeOpenIndexRequestAsync(String index) {
    OpenIndexRequest request = buildOpenIndexRequest(index);

    // 构建监听器
    ActionListener<OpenIndexResponse> listener = new ActionListener<OpenIndexResponse>() {
      @Override
      public void onResponse(OpenIndexResponse openIndexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().openAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是和关闭索引有关的代码
   */
  // 构建CloseIndexRequest
  public CloseIndexRequest buildCloseIndexRequest(String index) {
    CloseIndexRequest request = new CloseIndexRequest(index);

    // 配置可选参数
    // 所有节点确认索引关闭的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 从节点连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    request.masterNodeTimeout("1m");

    // 用于控制解析不可用的索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 以同步方式执行关闭索引请求CloseIndexRequest
  public void executeCloseIndexRequest(String index) {
    CloseIndexRequest request = buildCloseIndexRequest(index);
    try {
      AcknowledgedResponse closeIndexResponse =
          restClient.indices().close(request, RequestOptions.DEFAULT);

      // 所有节点是否已确认请求
      boolean acknowledged = closeIndexResponse.isAcknowledged();
      log.info(index + " acknowledged is " + acknowledged);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 以异步方式执行关闭索引请求CloseIndexRequest
  public void executeCloseIndexRequestAsync(String index) {
    CloseIndexRequest request = buildCloseIndexRequest(index);

    // 构建监听器
    ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
      @Override
      public void onResponse(AcknowledgedResponse closeIndexResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.indices().closeAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是调整索引大小API的使用
   */
  // 构建ResizeRequest
  public ResizeRequest buildResizeRequest(String sourceIndex, String targetIndex) {
    ResizeRequest request = new ResizeRequest(targetIndex, sourceIndex);

    // 配置可选参数
    // 所有节点确认索引打开的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 从节点连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    request.masterNodeTimeout("1m");

    // 请求返回前需要等待的活跃状态的分片数量
    request.setWaitForActiveShards(1);
    request.setWaitForActiveShards(ActiveShardCount.DEFAULT);

    // 缩小索引上的目标索引上的分片数、删除从源索引复制的分配要求
    request.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 1)
        .putNull("index.routing.allocation.require._name"));

    // 与目标索引关联的别名
    request.getTargetIndexRequest().alias(new Alias(targetIndex + "_alias"));


    return request;
  }

  // 同步执行ResizeRequest
  public void executeResizeRequest(String sourceIndex, String targetIndex) {
    ResizeRequest request = buildResizeRequest(sourceIndex, targetIndex);

    try {
      ResizeResponse resizeResponse = restClient.indices().shrink(request, RequestOptions.DEFAULT);

      // 解析ResizeResponse
      processResizeResponse(resizeResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析ResizeResponse
  private void processResizeResponse(ResizeResponse resizeResponse) {
    // 所有节点是否已确认请求
    boolean acknowledged = resizeResponse.isAcknowledged();
    // 是否在超时前为索引中的每个分片启动了所需数量的分片副本
    boolean shardsAcked = resizeResponse.isShardsAcknowledged();
    log.info("acknowledged is " + acknowledged + ";shardsAcked is " + shardsAcked);
  }

  // 异步执行ResizeRequest
  public void executeResizeRequestAsync(String sourceIndex, String targetIndex) {
    ResizeRequest request = buildResizeRequest(sourceIndex, targetIndex);

    // 构建监听器
    ActionListener<ResizeResponse> listener = new ActionListener<ResizeResponse>() {
      @Override
      public void onResponse(ResizeResponse resizeResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().shrinkAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为拆分索引结果的使用
   */
  //
  // 构建拆分索引的ResizeRequest
  public ResizeRequest buildSplitRequest(String sourceIndex, String targetIndex) {
    ResizeRequest request = new ResizeRequest(targetIndex, sourceIndex);

    // “调整大小”类型设置为“拆分”
    request.setResizeType(ResizeType.SPLIT);

    // 配置可选参数
    // 所有节点确认索引打开的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 从节点连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    request.masterNodeTimeout("1m");

    // 请求返回前需要等待的活跃状态的分片数量
    request.setWaitForActiveShards(1);
    request.setWaitForActiveShards(ActiveShardCount.DEFAULT);

    // 拆分索引上的目标索引上的分片数（大于源索引的数据,且为整除关系）、删除从源索引复制的分配要求
    request.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 6)
        .putNull("index.routing.allocation.require._name"));

    // 与目标索引关联的别名
    request.getTargetIndexRequest().alias(new Alias(targetIndex + "_alias"));

    return request;
  }

  // 同步执行拆分索引的ResizeRequest
  public void executeSplitRequest(String sourceIndex, String targetIndex) {
    ResizeRequest request = buildSplitRequest(sourceIndex, targetIndex);

    try {
      ResizeResponse resizeResponse = restClient.indices().split(request, RequestOptions.DEFAULT);

      // 解析ResizeResponse
      processResizeResponse(resizeResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 异步执行拆分索引的ResizeRequest
  public void executeSplitRequestAsync(String sourceIndex, String targetIndex) {
    ResizeRequest request = buildSplitRequest(sourceIndex, targetIndex);

    // 构建监听器
    ActionListener<ResizeResponse> listener = new ActionListener<ResizeResponse>() {
      @Override
      public void onResponse(ResizeResponse resizeResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().splitAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为Refresh刷新索引请求相关的代码
   */
  // 构建刷新索引请求对象RefreshRequest
  public RefreshRequest buildRefreshRequest(String index) {
    // 刷新一个索引
    RefreshRequest request = new RefreshRequest(index);
    // 刷新多个索引
    RefreshRequest requestMultiple = new RefreshRequest(index, index);
    // 刷新所有索引
    RefreshRequest requestAll = new RefreshRequest();

    // 控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步方式执行RefreshRequest
  public void executeRefreshRequest(String index) {
    RefreshRequest request = buildRefreshRequest(index);

    try {
      RefreshResponse refreshResponse =
          restClient.indices().refresh(request, RequestOptions.DEFAULT);

      // 解析RefreshResponse
      processRefreshResponse(refreshResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析RefreshResponse
  private void processRefreshResponse(RefreshResponse refreshResponse) {
    // 刷新请求命中的分片总数
    int totalShards = refreshResponse.getTotalShards();
    // 刷新成功的分片数
    int successfulShards = refreshResponse.getSuccessfulShards();
    // 刷新失败的分片数
    int failedShards = refreshResponse.getFailedShards();
    // 操作在一个或多个分片上失败时的失败列表
    DefaultShardOperationFailedException[] failures = refreshResponse.getShardFailures();

    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards + "; failures is "
        + (failures == null ? 0 : failures.length));
  }

  // 异步方式执行RefreshRequest
  public void executeRefreshRequestAsync(String index) {
    RefreshRequest request = buildRefreshRequest(index);

    // 构建监听器
    ActionListener<RefreshResponse> listener = new ActionListener<RefreshResponse>() {
      @Override
      public void onResponse(RefreshResponse refreshResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.indices().refreshAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为Flush刷新索引请求相关的代码
   */
  // 构建刷新索引请求对象FlushRequest
  public FlushRequest buildFlushRequest(String index) {
    // 刷新一个索引
    FlushRequest request = new FlushRequest(index);
    // 刷新多个索引
    FlushRequest requestMultiple = new FlushRequest(index, index);
    // 刷新所有索引
    FlushRequest requestAll = new FlushRequest();

    // 控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步方式执行FlushRequest
  public void executeFlushRequest(String index) {
    FlushRequest request = buildFlushRequest(index);

    try {
      FlushResponse flushResponse = restClient.indices().flush(request, RequestOptions.DEFAULT);

      // 解析FlushResponse
      processFlushResponse(flushResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析FlushResponse
  private void processFlushResponse(FlushResponse flushResponse) {
    // 刷新请求命中的分片总数
    int totalShards = flushResponse.getTotalShards();
    // 刷新成功的分片数
    int successfulShards = flushResponse.getSuccessfulShards();
    // 刷新失败的分片数
    int failedShards = flushResponse.getFailedShards();
    // 操作在一个或多个分片上失败时的失败列表
    DefaultShardOperationFailedException[] failures = flushResponse.getShardFailures();

    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards + "; failures is "
        + (failures == null ? 0 : failures.length));
  }

  // 异步方式执行FlushRequest
  public void executeFlushRequestAsync(String index) {
    FlushRequest request = buildFlushRequest(index);

    // 构建监听器
    ActionListener<FlushResponse> listener = new ActionListener<FlushResponse>() {
      @Override
      public void onResponse(FlushResponse refreshResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.indices().flushAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为同步Flush刷新索引请求相关的代码
   */
  // 构建同步刷新索引请求对象SyncedFlushRequest
  public SyncedFlushRequest buildSyncedFlushRequest(String index) {
    // 刷新一个索引
    SyncedFlushRequest request = new SyncedFlushRequest(index);
    // 刷新多个索引
    SyncedFlushRequest requestMultiple = new SyncedFlushRequest(index, index);
    // 刷新所有索引
    SyncedFlushRequest requestAll = new SyncedFlushRequest();

    // 控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步方式执行SyncedFlushRequest
  public void executeSyncedFlushRequest(String index) {
    SyncedFlushRequest request = buildSyncedFlushRequest(index);

    try {
      SyncedFlushResponse flushResponse =
          restClient.indices().flushSynced(request, RequestOptions.DEFAULT);

      // 解析SyncedFlushResponse
      processSyncedFlushResponse(flushResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析SyncedFlushResponse
  private void processSyncedFlushResponse(SyncedFlushResponse flushResponse) {
    // 刷新请求命中的分片总数
    int totalShards = flushResponse.totalShards();
    // 刷新成功的分片数
    int successfulShards = flushResponse.successfulShards();
    // 刷新失败的分片数
    int failedShards = flushResponse.failedShards();

    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards);
  }

  // 异步方式执行SyncedFlushRequest
  public void executeSyncedFlushRequestAsync(String index) {
    SyncedFlushRequest request = buildSyncedFlushRequest(index);

    // 构建监听器
    ActionListener<SyncedFlushResponse> listener = new ActionListener<SyncedFlushResponse>() {
      @Override
      public void onResponse(SyncedFlushResponse refreshResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    // 异步执行
    try {
      restClient.indices().flushSyncedAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为清除索引缓存请求相关的代码
   */
  // 构建清除索引缓存ClearIndicesCacheRequest
  public ClearIndicesCacheRequest buildClearIndicesCacheRequest(String index) {
    // 刷新一个索引
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest(index);
    // 刷新多个索引
    ClearIndicesCacheRequest requestMultiple = new ClearIndicesCacheRequest(index, index);
    // 刷新所有索引
    ClearIndicesCacheRequest requestAll = new ClearIndicesCacheRequest();

    // 控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    // 将查询标志设置为true
    request.queryCache(true);
    // 将FieldData标志设置为true
    request.fieldDataCache(true);
    // 将请求标志设置为true
    request.requestCache(true);
    // 设置字段参数
    // request.fields("field1", "field2", "field3");

    return request;
  }

  // 同步方式执行ClearIndicesCacheRequest
  public void executeClearIndicesCacheRequest(String index) {
    ClearIndicesCacheRequest request = buildClearIndicesCacheRequest(index);

    try {
      ClearIndicesCacheResponse clearCacheResponse =
          restClient.indices().clearCache(request, RequestOptions.DEFAULT);

      // 解析ClearIndicesCacheRequest
      processClearIndicesCacheRequest(clearCacheResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析ClearIndicesCacheRequest
  private void processClearIndicesCacheRequest(ClearIndicesCacheResponse clearCacheResponse) {
    // 刷新请求命中的分片总数
    int totalShards = clearCacheResponse.getTotalShards();
    // 刷新成功的分片数
    int successfulShards = clearCacheResponse.getSuccessfulShards();
    // 刷新失败的分片数
    int failedShards = clearCacheResponse.getFailedShards();

    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards);
  }

  // 异步方式执行ClearIndicesCacheRequest
  public void executeClearIndicesCacheRequestAsync(String index) {
    ClearIndicesCacheRequest request = buildClearIndicesCacheRequest(index);

    // 构建监听器
    ActionListener<ClearIndicesCacheResponse> listener =
        new ActionListener<ClearIndicesCacheResponse>() {
          @Override
          public void onResponse(ClearIndicesCacheResponse refreshResponse) {

        }

          @Override
          public void onFailure(Exception e) {

        }
        };

    // 异步执行
    try {
      restClient.indices().clearCacheAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是强制合并索引相关的代码
   */
  // 构建ForceMergeRequest
  public ForceMergeRequest buildForceMergeRequest(String index) {
    // 强制合并一个索引
    ForceMergeRequest request = new ForceMergeRequest(index);

    // 强制合并多个索引
    ForceMergeRequest requestMultiple = new ForceMergeRequest(index + "1", index + "2");

    // 强制合并所有索引
    ForceMergeRequest requestAll = new ForceMergeRequest();

    // 配置可选参数
    // 设置IndiceOptions控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());
    // 设置max_num_segments以控制要合并到的段数。
    request.maxNumSegments(1);
    // 将“唯一删除”标志设置为“真”
    request.onlyExpungeDeletes(true);
    // 将flush标志设置为true
    request.flush(true);


    return request;
  }

  // 同步执行ForceMergeRequest
  public void executeForceMergeRequest(String index) {
    ForceMergeRequest request = buildForceMergeRequest(index);

    try {
      ForceMergeResponse forceMergeResponse =
          restClient.indices().forcemerge(request, RequestOptions.DEFAULT);

      // 解析ForceMergeResponse
      processForceMergeResponse(forceMergeResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析ForceMergeResponse
  private void processForceMergeResponse(ForceMergeResponse forceMergeResponse) {
    // 强制合并请求命中的分片总数
    int totalShards = forceMergeResponse.getTotalShards();
    // 强制合并成功的分片数
    int successfulShards = forceMergeResponse.getSuccessfulShards();
    // 强制合并失败的分片数
    int failedShards = forceMergeResponse.getFailedShards();
    // 操作在一个或多个分片上失败时的失败列表
    DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();

    log.info("totalShards is " + totalShards + ";successfulShards is " + successfulShards
        + ";failedShards is " + failedShards + ";failures size is "
        + (failures == null ? 0 : failures.length));
  }

  // 异步执行ForceMergeRequest
  public void executeForceMergeRequestAsync(String index) {
    ForceMergeRequest request = buildForceMergeRequest(index);

    // 构建监听器
    ActionListener<ForceMergeResponse> listener = new ActionListener<ForceMergeResponse>() {
      @Override
      public void onResponse(ForceMergeResponse forceMergeResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };

    try {
      restClient.indices().forcemergeAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为滚动索引相关的代码
   */

  // 构建RolloverRequest
  public RolloverRequest buildRolloverRequest(String index) {
    // 指向要滚动的索引的别名（第一个参数），以及执行滚动操作时新索引的名称。new index参数是可选的，可以设置为空。
    RolloverRequest request = new RolloverRequest(index, index + "-2");
    // 指数年龄条件
    request.addMaxIndexAgeCondition(new TimeValue(7, TimeUnit.DAYS));
    // 索引中文档数的条件
    request.addMaxIndexDocsCondition(1000);
    // 索引大小的条件
    request.addMaxIndexSizeCondition(new ByteSizeValue(5, ByteSizeUnit.GB));

    // 配置可选参数
    // 是否应执行滚动（默认）
    request.dryRun(true);
    // 所有节点确认索引打开的超时时间
    request.setTimeout(TimeValue.timeValueMinutes(2));
    // 从节点连接到主节点的超时时间
    request.setMasterTimeout(TimeValue.timeValueMinutes(1));
    // 请求返回前等待的活跃分片数量
    // request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.from(2));
    // 请求返回前等待的活跃分片数量，重置为默认值
    request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.DEFAULT);
    // 添加要应用于新索引的设置，其中包括要为其创建的分片数。
    // request.getCreateIndexRequest().settings(Settings.builder().put("index.number_of_shards",
    // 4));

    // 添加要与新索引关联的映射
    // String mappings = "{\"properties\":{\"field\":{\"type\":\"content\"}}}";
    // request.getCreateIndexRequest().mapping(mappings, XContentType.JSON);
    // 添加与新索引关联的别名
    // request.getCreateIndexRequest().alias(new Alias(index + "-2_alias"));


    return request;
  }

  // 同步执行RolloverRequest
  public void executeRolloverRequest(String index) {
    RolloverRequest request = buildRolloverRequest(index);

    try {
      RolloverResponse rolloverResponse =
          restClient.indices().rollover(request, RequestOptions.DEFAULT);

      // 解析RolloverResponse
      processRolloverResponse(rolloverResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析RolloverResponse
  private void processRolloverResponse(RolloverResponse rolloverResponse) {
    // 所有节点是否已确认请求
    boolean acknowledged = rolloverResponse.isAcknowledged();
    // 是否在超时前,为索引中的每个分片启动了所需数量的分片副本
    boolean shardsAcked = rolloverResponse.isShardsAcknowledged();
    // 旧索引的名称，最终被翻滚
    String oldIndex = rolloverResponse.getOldIndex();
    // 新索引的名称
    String newIndex = rolloverResponse.getNewIndex();
    // 索引是否已回滚
    boolean isRolledOver = rolloverResponse.isRolledOver();
    // 操作是进行还是干运行
    boolean isDryRun = rolloverResponse.isDryRun();
    // 不同的条件，是否匹配
    Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();

    log.info("acknowledged is " + acknowledged + ";shardsAcked is " + shardsAcked + ";oldIndex is "
        + oldIndex + ";newIndex is " + newIndex + ";isRolledOver is " + isRolledOver
        + ";isDryRun is " + isDryRun + ";conditionStatus size is "
        + (conditionStatus == null ? 0 : conditionStatus.size()));
  }

  // 异步执行RolloverRequest
  public void executeRolloverRequestAsync(String index) {
    RolloverRequest request = buildRolloverRequest(index);

    // 构建RolloverRequest
    ActionListener<RolloverResponse> listener = new ActionListener<RolloverResponse>() {
      @Override
      public void onResponse(RolloverResponse rolloverResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().rolloverAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为索引别名相关的代码
   */
  // 构建索引别名请求IndicatesAliasesRequest
  public IndicesAliasesRequest buildIndicatesAliasesRequest(String index, String indexAlias) {
    // 创建IndicatesAliasesRequest
    IndicesAliasesRequest request = new IndicesAliasesRequest();
    // 创建别名操作，将索引index的别名设为indexAlias ADD
    AliasActions aliasAction =
        new AliasActions(AliasActions.Type.ADD).index(index).alias(indexAlias);
    // .filter("{\"term\":{\"year\":2019}}").routing("niudong");
    // 将别名操作添加到请求中
    request.addAliasAction(aliasAction);

    // 可选参数配置
    // 所有节点确认索引操作的超时时间
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");

    // 从节点连接到主节点的超时时间
    request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
    request.masterNodeTimeout("1m");


    return request;
  }

  // 同步执行IndicatesAliasesRequest
  public void executeIndicatesAliasesRequest(String index, String indexAlias) {
    IndicesAliasesRequest request = buildIndicatesAliasesRequest(index, indexAlias);

    try {
      AcknowledgedResponse indicesAliasesResponse =
          restClient.indices().updateAliases(request, RequestOptions.DEFAULT);

      // 解析AcknowledgedResponse
      processAcknowledgedResponse(indicesAliasesResponse);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 异步执行IndicatesAliasesRequest
  public void executeIndicatesAliasesRequestAsync(String index, String indexAlias) {
    IndicesAliasesRequest request = buildIndicatesAliasesRequest(index, indexAlias);

    // 构建监听器
    ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
      @Override
      public void onResponse(AcknowledgedResponse indicesAliasesResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().updateAliasesAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下是索引别名校验相关的代码
   */
  // 构建GetAliasesRequest
  public GetAliasesRequest buildGetAliasesRequest(String indexAlias) {
    GetAliasesRequest request = new GetAliasesRequest();

    GetAliasesRequest requestWithAlias = new GetAliasesRequest(indexAlias);

    GetAliasesRequest requestWithAliases =
        new GetAliasesRequest(new String[] {indexAlias, indexAlias});

    // 配置可选参数
    // 要查找的一个或多个别名
    request.aliases(indexAlias);
    // 与别名关联的一个或多个索引
    // request.indices("index");
    // 设置IndiceOptions控制解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());
    // 本地标志（默认为false）,控制是否需要在本地群集状态或所选主节点持有的群集状态中查找别名
    request.local(true);


    return request;
  }

  // 同步执行GetAliasesRequest
  public void executeGetAliasesRequest(String indexAlias) {
    GetAliasesRequest request = buildGetAliasesRequest(indexAlias);

    try {
      boolean exists = restClient.indices().existsAlias(request, RequestOptions.DEFAULT);
      log.info("indexAlias exists is " + exists);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 异步执行GetAliasesRequest
  public void executeGetAliasesRequestAsync(String indexAlias) {
    GetAliasesRequest request = buildGetAliasesRequest(indexAlias);

    // 构建监听器
    ActionListener<Boolean> listener = new ActionListener<Boolean>() {
      @Override
      public void onResponse(Boolean exists) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().existsAliasAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  /*
   * 以下为获取索引别名相关的代码
   */
  // 同步执行GetAliasesRequest,获取索引别名
  public void executeGetAliasesRequestForAliases(String indexAlias) {
    GetAliasesRequest request = buildGetAliasesRequest(indexAlias);

    try {
      GetAliasesResponse response = restClient.indices().getAlias(request, RequestOptions.DEFAULT);

      // 解析GetAliasesResponse
      processGetAliasesResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析GetAliasesResponse
  private void processGetAliasesResponse(GetAliasesResponse response) {
    // 检索索引及其别名的映射
    Map<String, Set<AliasMetaData>> aliases = response.getAliases();

    // 空则返回
    if (aliases == null || aliases.size() <= 0) {
      return;
    }

    // 遍历MAP
    Set<Entry<String, Set<AliasMetaData>>> set = aliases.entrySet();
    for (Entry<String, Set<AliasMetaData>> entry : set) {
      String key = entry.getKey();

      Set<AliasMetaData> metaSet = entry.getValue();
      if (metaSet == null || metaSet.size() <= 0) {
        return;
      }
      for (AliasMetaData meta : metaSet) {
        String aliaas = meta.alias();
        log.info("key is " + key + ";aliaas is " + aliaas);
      }
    }
  }

  // 异步执行GetAliasesRequest，获取索引别名
  public void executeGetAliasesRequestForAliasesAsync(String indexAlias) {
    GetAliasesRequest request = buildGetAliasesRequest(indexAlias);

    // 构建监听器
    ActionListener<GetAliasesResponse> listener = new ActionListener<GetAliasesResponse>() {
      @Override
      public void onResponse(GetAliasesResponse exists) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    try {
      restClient.indices().getAliasAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
