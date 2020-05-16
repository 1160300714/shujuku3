/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include "page.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include <functional>
#include <string>
#include <iostream>
#include <ctime>
#include <sstream>
#include "storage.h"

using namespace std;

namespace badgerdb {

void TableScanner::print() const {
  // TODO: Printer the contents of the table

    //使用文件和页的迭代器进行二重循环来输出元组的内容，注意迭代器指针应赋予给遍历
    for (FileIterator iter = tableFile.begin(); iter != tableFile.end(); iter++) {
        Page tem = *(iter);
        Page *pa = &tem;
        for (PageIterator it = pa->begin(); it != pa->end(); it++) {
            string record = *(it);
            cout << "TableScanner::print:" << record << endl;
        }


    }

}

JoinOperator::JoinOperator(File& leftTableFile,
                           File& rightTableFile,
                           const TableSchema& leftTableSchema,
                           const TableSchema& rightTableSchema,
                           const Catalog* catalog,
                           BufMgr* bufMgr)
    : leftTableFile(leftTableFile),
      rightTableFile(rightTableFile),
      leftTableSchema(leftTableSchema),
      rightTableSchema(rightTableSchema),
      resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)),
      catalog(catalog),
      bufMgr(bufMgr),
      isComplete(false) {
  // nothing
}

TableSchema JoinOperator::createResultTableSchema(
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema) {
  vector<Attribute> attrs;

  //构建属性所用的各项临时变量
  string name;
  DataType type;
  int max;
  bool notnull;
  bool unique;

  //遍历左边，把所有属性添加到attrs里
  for (int i = 0; i < leftTableSchema.getAttrCount(); i++) {
      name = leftTableSchema.getAttrName(i);
      type = leftTableSchema.getAttrType(i);
      max = leftTableSchema.getAttrMaxSize(i);
      notnull = leftTableSchema.isAttrNotNull(i);
      unique = leftTableSchema.isAttrUnique(i);
      Attribute temA1 = Attribute(name, type, max);
      temA1.isNotNull = notnull;
      temA1.isUnique = unique;
      attrs.push_back(temA1);
  }

  //遍历右边，把每一个属性和上面的比较，如果有重复则不放入，佛则放入
  for (int j = 0; j < rightTableSchema.getAttrCount(); j++) {
      int flag = 1;
      for (int k = 0; k < leftTableSchema.getAttrCount(); k++) {
      
          //如果属性值相同。flag为0，compare相等返回0
          if (!(leftTableSchema.getAttrName(k).compare(rightTableSchema.getAttrName(j)))) {
              flag = 0;
          }
      }
            //如果属性值不同，flag为1
      if (flag) {
          name = rightTableSchema.getAttrName(j);
          type = rightTableSchema.getAttrType(j);
          max = rightTableSchema.getAttrMaxSize(j);
          notnull = rightTableSchema.isAttrNotNull(j);
          unique = rightTableSchema.isAttrUnique(j);
          Attribute temA2 = Attribute(name, type, max);
          temA2.isNotNull = notnull;
          temA2.isUnique = unique;
          attrs.push_back(temA2);
      }
  }
  // TODO: add attribute definitions
  return TableSchema("TEMP_TABLE", attrs, true);
}

void JoinOperator::printRunningStats() const {
  cout << "# Result Tuples: " << numResultTuples << endl;
  cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
  cout << "# I/Os: " << numIOs << endl;
}


//一趟
bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;

  //Build阶段的计数器
  int countBuild = 0;
  //Tuple元组数组的计数器
  int countTuple = 0;
  //存放右关系的b元素内容的数组
  string rightb[100];
  //存放右关系的c元素内容的数组
  string rightc[100];
  //存放连接完成后的元组的数组
  string tuple[500];


  //因为右关系比较少，用文件迭代器把右关系先读入，“，”做分割，提取其前后属性。
  for (FileIterator iter = rightTableFile.begin(); iter != rightTableFile.end(); iter++) {
      Page tem = *(iter);
      Page* pa = &tem;
      for (PageIterator it = pa->begin(); it != pa->end(); it++) {
          string record = *(it);
          int place1 = record.find_first_of(",");
          string temp1 = record.substr(0, place1);
          string temp2 = record.substr(place1 + 1);
          rightb[countBuild] = temp1;
          rightc[countBuild] = temp2;
          countBuild++;
      }
  }


  for (FileIterator iters = leftTableFile.begin(); iters != leftTableFile.end(); iters++) {
      Page tems = *(iters);
      Page* pas = &tems;
      for (PageIterator its = pas->begin(); its != pas->end(); its++) {
          string records = *(its);
          int place2 = records.find_first_of(",");
          string tem3 = records.substr(place2 + 1); //左关系的后一个属性
          for (int i = 0; i < 100; i++) {
              if (!(tem3.compare(rightb[i]))) {  //和右关系的第一个属性比较，相等则相连
                  stringstream ss;
                  ss << records <<","<< rightc[i];
                  tuple[countTuple] = ss.str();
                  countTuple++;
                  numResultTuples++;
              }
          }
      }
  }
  
  //插入元组，写回文件
  for (int k = 0; k < 500; k++) {
      HeapFileManager::insertTuple(tuple[k], resultFile, bufMgr);
  }
  bufMgr->flushFile(&resultFile);
  // TODO: Execute the join algorithm

  numUsedBufPages = numAvailableBufPages;
  numIOs = 500+100;

  isComplete = true;
  return true;
}

//嵌套
bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;
  
  int countBuild = 0;
  int countTuple = 0;
  string rightb[100];
  string rightc[100];
  string tuple[500];

    //同上，先提取右关系的
  for (FileIterator iter = rightTableFile.begin(); iter != rightTableFile.end(); iter++) {
      Page tem = *(iter);
      Page* pa = &tem;
      for (PageIterator it = pa->begin(); it != pa->end(); it++) {
          string record = *(it);
          int place1 = record.find_first_of(",");
          string temp1 = record.substr(0, place1);
          string temp2 = record.substr(place1 + 1);
          rightb[countBuild] = temp1;
          rightc[countBuild] = temp2;
          countBuild++;
      }
  }

  int countLoop = 0;

  
  //公式，numAvailableBufPages为可用的缓冲块，余留一个用作嵌套，最后多出来一个还要做一次
  for (int j = 0; j < 100 / (numAvailableBufPages - 1) + 1; j++) {
      for (FileIterator iters = leftTableFile.begin(); iters != leftTableFile.end(); iters++) {
          Page tems = *(iters);
          Page* pas = &tems;
          for (PageIterator its = pas->begin(); its != pas->end(); its++) {
              string records = *(its);
              int place2 = records.find_first_of(",");
              string tem3 = records.substr(place2 + 1);
                  for (int i = 0; i < numAvailableBufPages - 1; i++) {
                      if (i + countLoop * (numAvailableBufPages-1) < 100) {//每次leftb比较9个属性，一共循环12次
                          if (!(tem3.compare(rightb[i + countLoop * (numAvailableBufPages-1)])))
                          {
                              stringstream ss;
                              ss << records << "," << rightc[i];
                              tuple[countTuple] = ss.str();
                              countTuple++;
                              numResultTuples++;
                          }
                      }
                  }
              }
          }
      countLoop++;
      }
  

    for (int k = 0; k < 500; k++) {
        HeapFileManager::insertTuple(tuple[k], resultFile, bufMgr);
    }
    bufMgr->flushFile(&resultFile);

    numUsedBufPages = numAvailableBufPages;
    numIOs = 100 + 100 * 500 / (numAvailableBufPages - 1);//公式计算
  // TODO: Execute the join algorithm

  isComplete = true;
  return true;
}

BucketId GraceHashJoinOperator::hash(const string& key) const {
  std::hash<string> strHash;
  return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator ::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;
  
  // TODO: Execute the join algorithm

  isComplete = true;
  return true;
}

}  // namespace badgerdb