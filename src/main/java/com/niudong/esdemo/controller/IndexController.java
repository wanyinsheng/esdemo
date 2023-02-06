package com.niudong.esdemo.controller;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.IndexService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述索引相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/indexsearch")
public class IndexController {
  @Autowired
  private IndexService indexService;

  // 同步方式执行IndexRequest
  @RequestMapping("/sr")
  public String executeIndex(String text) {
    // 参数校验
    if (Strings.isNullOrEmpty(text)) {
      return "Parameters are wrong!";
    }
    indexService.executeAnalyzeRequest(text);
    return "Execute IndexRequest success!";
  }

  // 同步方式执行CreateIndexRequest
  @RequestMapping("/create/sr")
  public String executeCreateIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeIndexRequest(indexName);

    return "Execute CreateIndexRequest success!";
  }

  // 同步方式执行GetIndexRequest
  @RequestMapping("/get/sr")
  public String executeGetIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.excuteGetIndexRequest(indexName);

    return "Execute GetIndexRequest success!";
  }

  // 同步方式执行DeleteIndexRequest
  @RequestMapping("/delete/sr")
  public String executeDeleteIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeDeleteIndexRequest(indexName);

    return "Execute DeleteIndexRequest success!";
  }

  // 同步方式执行ExistsIndexRequest
  @RequestMapping("/exists/sr")
  public String executeExistsIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeExistsIndexRequest(indexName);

    return "Execute ExistsIndexRequest success!";
  }

  // 同步方式执行OpenIndexRequest
  @RequestMapping("/open/sr")
  public String executeOpenIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeOpenIndexRequest(indexName);

    return "Execute OpenIndexRequest success!";
  }

  // 同步方式执行CloseIndexRequest
  @RequestMapping("/close/sr")
  public String executeCloseIndexRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeCloseIndexRequest(indexName);

    return "Execute CloseIndexRequest success!";
  }

  // 同步方式执行ResizeRequest
  @RequestMapping("/resize/sr")
  public String executeResizeRequest(String sourceIndexName, String targetIndexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(sourceIndexName) || Strings.isNullOrEmpty(targetIndexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeResizeRequest(sourceIndexName, targetIndexName);

    return "Execute ResizeRequest success!";
  }

  // 同步方式执行拆分索引的ResizeRequest
  @RequestMapping("/split/sr")
  public String executeSplitRequest(String sourceIndexName, String targetIndexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(sourceIndexName) || Strings.isNullOrEmpty(targetIndexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeSplitRequest(sourceIndexName, targetIndexName);

    return "Execute SplitResizeRequest success!";
  }

  // 同步方式执行刷新索引的RefreshRequest
  @RequestMapping("/refresh/sr")
  public String executeRefreshRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }
    indexService.executeRefreshRequest(indexName);

    return "Execute RefreshRequest success!";
  }

  // 同步方式执行刷新索引的FlushRequest
  @RequestMapping("/flush/sr")
  public String executeFlushRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }

    indexService.executeFlushRequest(indexName);

    return "Execute FlushRequest success!";
  }

  // 同步方式执行刷新索引的SyncedFlushRequest
  @RequestMapping("/syncedflush/sr")
  public String executeSyncedFlushRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }

    indexService.executeSyncedFlushRequest(indexName);

    return "Execute SyncedFlushRequest success!";
  }

  // 同步方式执行清除索引缓存的ClearIndicesCacheRequest
  @RequestMapping("/clearcache/sr")
  public String executeClearIndicesCacheRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }

    indexService.executeClearIndicesCacheRequest(indexName);

    return "Execute ClearIndicesCacheRequest success!";
  }

  // 同步方式执行强制合并索引的ForceMergeRequest
  @RequestMapping("/merge/sr")
  public String executeForceMergeRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }

    indexService.executeForceMergeRequest(indexName);

    return "Execute ForceMergeRequest success!";
  }

  // 同步方式执行滚动索引的RolloverRequest
  @RequestMapping("/rollover/sr")
  public String executeRolloverRequest(String indexName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName)) {
      return "Parameters are wrong!";
    }

    indexService.executeRolloverRequest(indexName);

    return "Execute RolloverRequest success!";
  }

  // 同步方式执行索引别名的请求：IndicatesAliasesRequest
  @RequestMapping("/createAlias/sr")
  public String executeIndicatesAliasesRequest(String indexName, String indexAliasName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName) || Strings.isNullOrEmpty(indexAliasName)) {
      return "Parameters are wrong!";
    }

    indexService.executeIndicatesAliasesRequest(indexName, indexAliasName);

    return "Execute IndicatesAliasesRequest success!";
  }

  // 同步方式执行索引别名存在校验的请求：GetAliasesRequest
  @RequestMapping("/existsAlias/sr")
  public String executeGetAliasesRequest(String indexAliasName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexAliasName)) {
      return "Parameters are wrong!";
    }

    indexService.executeGetAliasesRequest(indexAliasName);

    return "Execute GetAliasesRequest success!";
  }

  // 同步方式执行获取索引别名的请求：GetAliasesRequest
  @RequestMapping("/getAlias/sr")
  public String executeGetAliasesRequestForAliases(String indexAliasName) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexAliasName)) {
      return "Parameters are wrong!";
    }

    indexService.executeGetAliasesRequestForAliases(indexAliasName);

    return "Execute GetAliasesRequestForAliases success!";
  }
}
