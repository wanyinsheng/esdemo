package com.niudong.esdemo.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * @author 牛冬
 * @desc:首次字+字长双Hash词典分词算法实现
 *
 */
public class TwoWordsTogetherAndLengthDictionaryUtil {

  // 字典中最长词长
  private final int wordLength = 7;

  /* 本类需要的辅助类 */
  private FileOutputStream fileOutputStream = null;


  // 构建词典接口：从文件读取词汇,构建双字哈希
  public HashMap buildChineseWordSegmentationDictionaryNoPY() {
    HashMap chineseWordSegmentationDictionaryHashMap = new HashMap();

    String chineseWordsFilePath = "FileInput/新所用词典.txt";

    try {
      File file = new File(chineseWordsFilePath);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String readOutString = bufferedReader.readLine();
      while (readOutString != null) {

        String twoLetters = readOutString.substring(0, 2);

        // 求当前词汇的字长
        int readOutStringLength = readOutString.length();

        TreeMap wordLengthTreeMap =
            (TreeMap) chineseWordSegmentationDictionaryHashMap.get(twoLetters);
        if (wordLengthTreeMap == null) {
          // 如果尚未写入第一级Map
          wordLengthTreeMap = new TreeMap(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
              // 降序排列
              return o2.compareTo(o1);
            }
          });
          wordLengthTreeMap.put(readOutStringLength, readOutString + ",");
          chineseWordSegmentationDictionaryHashMap.put(twoLetters, wordLengthTreeMap);
          // System.out.println("如果尚未写入第一级Map,readOutString="+readOutString+",readOutStringLength="+readOutStringLength);
        } else {
          // 如果已经写如了第一级Map，开始判断是不是写入了第二级Map
          // 获取二级Map中的词，默认升序排序
          String words = (String) wordLengthTreeMap.get(readOutStringLength);
          // 看是否有该字长的词
          if (words == null || words == "") {
            wordLengthTreeMap.put(readOutStringLength, readOutString + ",");
            chineseWordSegmentationDictionaryHashMap.remove(twoLetters);
            chineseWordSegmentationDictionaryHashMap.put(twoLetters, wordLengthTreeMap);
            // System.out.println("如果已经写如了第一级Map，开始判断是不是写入了第二级Map,readOutString="+readOutString+",readOutStringLength="+readOutStringLength);
          } else {
            // 如果两级Map均已有值，则取出后按升序组合在放入Map
            String newAScWords = getASCStringNoPY(words, readOutString);
            // System.out.println("原有words="+words+",当前readOutString="+readOutString+"，排序后newAScWords="+newAScWords);
            wordLengthTreeMap.remove(readOutStringLength);
            wordLengthTreeMap.put(readOutStringLength, newAScWords);
            chineseWordSegmentationDictionaryHashMap.remove(twoLetters);
            chineseWordSegmentationDictionaryHashMap.put(twoLetters, wordLengthTreeMap);
            // System.out.println("如果两级Map均已有值，则取出后按升序组合在放入Map,readOutString="+readOutString+",readOutStringLength="+readOutStringLength);
          }
        }
        // 继续下一个关键词的插入词典
        readOutString = bufferedReader.readLine();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("文件读取失败！");
    }

    return chineseWordSegmentationDictionaryHashMap;
  }

  // 插入新值后依然按升序排序
  String getASCStringNoPY(String oldString, String inputString) {
    String outputString = "";
    String[] oldStringArray = oldString.split(",");
    int length = oldStringArray.length;

    // 记录当前比新词小的老词
    String recordString = "";

    int i = 0;
    for (; i < length; i++) {
      String temp = oldStringArray[i];

      if (temp.compareTo(inputString) >= 0) {
        if (i == 0) {
          outputString = inputString + "," + oldString;
          break;
        } else {
          outputString = recordString + inputString + ",";
          // 将大于新值的老词加入词典
          for (int j = i; j < length; j++) {
            outputString += oldStringArray[j];
            outputString += ",";
          }
          break;
        }
      } else {
        recordString += oldStringArray[i];
        recordString += ",";
      }
    }
    if (i == length) {
      outputString += recordString;
      outputString += inputString;
      outputString += ",";
    }
    // System.out.println("length="+length+",oldString="+oldString+",inputString="+inputString+",outputString="+outputString);
    return outputString;
  }

  // 逆向最大匹配算法3
  public String forwardMaxMatchMethodNoPY(String testString, HashMap dictionary) {
    // 返回的匹配结果字符串，各字符以逗号分隔
    String resultString = "";

    int testStringLength = testString.length();

    // System.out.println("testString="+testString+",testStringLength="+testStringLength);

    if (testStringLength <= wordLength) {
      resultString += forwardMaxMatchNoPY(testString, dictionary);
    } else {
      for (int i = 0; i <= testStringLength / wordLength; i++) {
        String subString = "";
        if ((i + 1) * wordLength <= testStringLength) {
          subString = testString.substring(i * wordLength, (i + 1) * wordLength);
        }
        if ((i * wordLength < testStringLength) && ((i + 1) * wordLength > testStringLength)) {
          subString = testString.substring(i * wordLength);
        }
        resultString += forwardMaxMatchNoPY(subString, dictionary);
      }
    }

    return resultString;
  }

  // 最大逆向匹配子函数2.1
  public String forwardMaxMatchNoPY(String inputString, HashMap dictionary) {
    String resultString = "";
    String databaseString = "";
    HashMap subDictionary = new HashMap();
    HashMap wordLengthHashMap = new HashMap();

    int inputStringLength = inputString.length();
    int i = 0;
    while (i < inputStringLength) {
      String subString = inputString.substring(i);

      // System.out.println("subString="+subString);
      int subStringLength = subString.length();
      if (subStringLength > 1) {
        String first_letter = subString.substring(0, 1);
        subDictionary = (HashMap) dictionary.get(first_letter);

        if (subDictionary != null) {
          String second_letter = subString.substring(1, 2);

          wordLengthHashMap = (HashMap) subDictionary.get(second_letter);

          int flag = 0;

          if (wordLengthHashMap != null) {
            for (int k = subStringLength; k > 1; k--) {
              databaseString = (String) wordLengthHashMap.get(k);

              // System.out.println("first_letter="+first_letter+",second_letter="+second_letter+",字长="+k+",databaseString="+databaseString);

              if (databaseString != null) {
                String[] database = databaseString.split(",");
                int databaseLength = database.length;
                String inputStringTemp = subString.substring(0, k);
                // System.out.println("inputStringTemp="+inputStringTemp);
                for (int j = 0; j < databaseLength; j++) {

                  if (database[j].equals(inputStringTemp)) {
                    resultString += database[j];
                    resultString += ",";
                    flag = 1;
                    i += k;
                    // System.out.println("匹配成功，k="+k);
                    i--;
                    break;
                  }
                }
              }
              if (flag == 1) {
                break;
              }
            }
          }
        }
      }
      i++;
    }
    return resultString;
  }

  // 逆向最大匹配算法3
  public String forwardMaxMatchMethodNoPY2(String testString, HashMap dictionary) {
    // 返回的匹配结果字符串，各字符以逗号分隔
    String resultString = "";

    int testStringLength = testString.length();

    // System.out.println("testString="+testString+",testStringLength="+testStringLength);

    if (testStringLength <= wordLength) {
      resultString += forwardMaxMatchNoPY2(testString, dictionary);
    } else {
      for (int i = 0; i <= testStringLength / wordLength; i++) {
        String subString = "";
        if ((i + 1) * wordLength <= testStringLength) {
          subString = testString.substring(i * wordLength, (i + 1) * wordLength);
        }
        if ((i * wordLength < testStringLength) && ((i + 1) * wordLength > testStringLength)) {
          subString = testString.substring(i * wordLength);
        }
        resultString += forwardMaxMatchNoPY2(subString, dictionary);
      }
    }

    return resultString;
  }

  // 最大逆向匹配子函数2.2 改进
  public String forwardMaxMatchNoPY2(String inputString, HashMap dictionary) {
    String resultString = "";
    String databaseString = "";
    TreeMap subDictionary = new TreeMap();

    int inputStringLength = inputString.length();

    int i = 0;
    while (i < inputStringLength) {
      // for(int i=0;i<inputStringLength;i++){
      String subString = inputString.substring(i);

      int len = subString.length();
      // if( subString.length()>1){
      if (len > 1) {
        String twoLetters = subString.substring(0, 2);
        subDictionary = (TreeMap) dictionary.get(twoLetters);

        if (subDictionary != null) {
          databaseString = (String) subDictionary.get(len);
          System.out.println("twoLetters=" + twoLetters + "databaseString=" + databaseString);

          if (databaseString != null) {
            String[] database = databaseString.split(",");
            int databaseLength = database.length;

            // modify 2012.03.13
            int flag = 0;
            while (subString.length() > 1 && flag == 0) {
              for (int j = 0; j < databaseLength; j++) {
                if (subString.equals(database[j])) {
                  resultString += database[j];
                  resultString += ",";
                  flag = 1;
                  i += database[j].length();
                  i--;
                  break;
                }
              }
              if (flag == 0) {
                subString = subString.substring(0, subString.length() - 1);
              }
            }
          }
        }
      }
      i++;
    }
    return resultString;
  }

  // 遍历字典中所有的词汇
  public void readFromDictionary(HashMap chineseWordSegmentationDictionaryHashMap) {
    Iterator iter_big = chineseWordSegmentationDictionaryHashMap.entrySet().iterator();
    while (iter_big.hasNext()) {
      Map.Entry entry_first = (Map.Entry) iter_big.next();
      String twoLetters = (String) entry_first.getKey();
      TreeMap wordLengthTreeMap = (TreeMap) entry_first.getValue();
      Iterator iter_small = wordLengthTreeMap.entrySet().iterator();
      while (iter_small.hasNext()) {
        Map.Entry entry_second = (Map.Entry) iter_small.next();
        int wordLength = (Integer) entry_second.getKey();
        String words = (String) entry_second.getValue();

        dataIntoFile("dist/FileOutput/DictionaryTwoWordAndLengthTreeMap.txt",
            twoLetters + ",字长：" + wordLength + ":" + words);
      }
    }
  }

  // 正向最大匹配
  public String forwardMaxMatchMethod(String testString, HashMap dictionary) {
    String resultString = "";

    // 传入字符串所有字长
    int wordLength = testString.length();
    int index = 0;
    // System.out.println("wordLength="+wordLength);
    while (wordLength > 1 && index + 2 < wordLength) {
      String twoLetter = testString.substring(index, index + 2);

      // System.out.println("twoLetter="+twoLetter);

      TreeMap wordLengthHash = (TreeMap) dictionary.get(twoLetter);

      // System.out.println(wordLengthHash==null);

      if (wordLengthHash != null) {
        Iterator iterator = wordLengthHash.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry entry = (Map.Entry) iterator.next();
          int wordLengthTemp = (Integer) entry.getKey();
          String databaseString = (String) entry.getValue();

          // System.out.println("twoLetter="+twoLetter+",wordLengthTemp="+wordLengthTemp+",databaseString="+databaseString);
          // System.out.println("index="+index+",index+wordLengthTemp="+(index+wordLengthTemp));

          int endIndex = index + wordLengthTemp;
          if (endIndex <= wordLength) {
            String subString = testString.substring(index, endIndex);
            int flag = 0;
            String[] database = databaseString.split(",");
            int databaseLength = database.length;
            // 二分查找
            // int low=0;
            // int high=--databaseLength;
            // dataIntoFile("dist/FileOutput/Test1.txt","low="+low+",high="+high+",subString="+subString+",databaseString="+databaseString);
            // while(low<=high && high!=0){
            // int middle=(low+high)>>1;
            // int compareValue=subString.compareTo(database[middle]);
            // dataIntoFile("dist/FileOutput/Test1.txt","low="+low+",high="+high+",middle="+middle+",compareValue="+compareValue);
            // System.out.println("compareValue="+compareValue);
            // if(compareValue==0){
            // resultString+=database[middle];
            // resultString+=",";
            // flag=1;
            // break;
            // }else if(compareValue<0){
            // high=--middle;
            // }else{
            // ow=++middle;
            // }
            // }
            for (int i = 0; i < databaseLength; i++) {
              if (subString.equals(database[i])) {
                resultString += subString;
                resultString += ",";
                // System.out.println("resultString="+resultString);
                flag = 1;
                break;
              }
            }

            if (flag == 1) {
              index += wordLengthTemp;
              index--;
              // System.out.println("index="+index);
              break;
            } // else{
              // index+=1;
              // }
          }
        }
        index++;
      } else {
        index++;
      }
    }

    return resultString;
  }

  /*
   * 将一条数据写入指定的文件路径(dataPath)
   */
  public void dataIntoFile(String dataPath, String data) {

    try {
      fileOutputStream = new FileOutputStream(dataPath, true);
      byte[] dataBytes = data.getBytes();
      fileOutputStream.write(dataBytes);
      fileOutputStream.write("\r\n".getBytes());
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("指定文件没有找到，当前文件路径是：" + dataPath);
    } finally {
      try {
        fileOutputStream.close();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("指定文件关闭异常，当前文件路径是：" + dataPath);
      }
    }
  }

  public static void main(String[] args) {
    TwoWordsTogetherAndLengthDictionaryUtil twoWordsTogetherAndLengthDictionary =
        new TwoWordsTogetherAndLengthDictionaryUtil();

    HashMap directory =
        twoWordsTogetherAndLengthDictionary.buildChineseWordSegmentationDictionaryNoPY();

    // 遍历字典
    //twoWordsTogetherAndLengthDictionary.readFromDictionary(directory);
    String testString = "再保险概述随着社会经济和科学技术的不断发展，社会财富日益增长，危险事故造成的";

    String resultString =
        twoWordsTogetherAndLengthDictionary.forwardMaxMatchMethod(testString, directory);
    System.out.println("resultString=" + resultString);
  }

}
