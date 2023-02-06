package com.niudong.esdemo.util;

import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

/**
 * 
 * @author 牛冬
 * @desc:AnsjSeg API的相关操作
 *
 */
public class AnsjSegUtil {

  public static void processString(String content) {
    // 最小颗粒度的分词
    System.out.println(BaseAnalysis.parse(content));
    // 精准分词
    System.out.println(ToAnalysis.parse(content));
    // 用户自定义词典优先策略的分词
    System.out.println(DicAnalysis.parse(content));
    // 面向索引的分词
    System.out.println(IndexAnalysis.parse(content));
    // 带有新词发现功能的分词
    System.out.println(NlpAnalysis.parse(content));
  }

  public static void main(String[] args) {
    String content =
        "15年来首批由深海探险家组成的国际团队五次潜入大西洋海底3800米深处，对泰坦尼克号沉船残骸进行调查。探险队发现，虽然沉船的部分残骸状况良好，也有一些部分已消失在大海中。强大的洋流、盐蚀和细菌正不断侵蚀着这艘沉船。英媒曝光的高清图片可见，沉船的部分残骸遭腐蚀情况严重。";
    processString(content);
  }

}
