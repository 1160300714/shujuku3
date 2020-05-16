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

    //ʹ���ļ���ҳ�ĵ��������ж���ѭ�������Ԫ������ݣ�ע�������ָ��Ӧ���������
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

  //�����������õĸ�����ʱ����
  string name;
  DataType type;
  int max;
  bool notnull;
  bool unique;

  //������ߣ�������������ӵ�attrs��
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

  //�����ұߣ���ÿһ�����Ժ�����ıȽϣ�������ظ��򲻷��룬�������
  for (int j = 0; j < rightTableSchema.getAttrCount(); j++) {
      int flag = 1;
      for (int k = 0; k < leftTableSchema.getAttrCount(); k++) {
      
          //�������ֵ��ͬ��flagΪ0��compare��ȷ���0
          if (!(leftTableSchema.getAttrName(k).compare(rightTableSchema.getAttrName(j)))) {
              flag = 0;
          }
      }
            //�������ֵ��ͬ��flagΪ1
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


//һ��
bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;

  //Build�׶εļ�����
  int countBuild = 0;
  //TupleԪ������ļ�����
  int countTuple = 0;
  //����ҹ�ϵ��bԪ�����ݵ�����
  string rightb[100];
  //����ҹ�ϵ��cԪ�����ݵ�����
  string rightc[100];
  //���������ɺ��Ԫ�������
  string tuple[500];


  //��Ϊ�ҹ�ϵ�Ƚ��٣����ļ����������ҹ�ϵ�ȶ��룬���������ָ��ȡ��ǰ�����ԡ�
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
          string tem3 = records.substr(place2 + 1); //���ϵ�ĺ�һ������
          for (int i = 0; i < 100; i++) {
              if (!(tem3.compare(rightb[i]))) {  //���ҹ�ϵ�ĵ�һ�����ԱȽϣ����������
                  stringstream ss;
                  ss << records <<","<< rightc[i];
                  tuple[countTuple] = ss.str();
                  countTuple++;
                  numResultTuples++;
              }
          }
      }
  }
  
  //����Ԫ�飬д���ļ�
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

//Ƕ��
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

    //ͬ�ϣ�����ȡ�ҹ�ϵ��
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

  
  //��ʽ��numAvailableBufPagesΪ���õĻ���飬����һ������Ƕ�ף��������һ����Ҫ��һ��
  for (int j = 0; j < 100 / (numAvailableBufPages - 1) + 1; j++) {
      for (FileIterator iters = leftTableFile.begin(); iters != leftTableFile.end(); iters++) {
          Page tems = *(iters);
          Page* pas = &tems;
          for (PageIterator its = pas->begin(); its != pas->end(); its++) {
              string records = *(its);
              int place2 = records.find_first_of(",");
              string tem3 = records.substr(place2 + 1);
                  for (int i = 0; i < numAvailableBufPages - 1; i++) {
                      if (i + countLoop * (numAvailableBufPages-1) < 100) {//ÿ��leftb�Ƚ�9�����ԣ�һ��ѭ��12��
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
    numIOs = 100 + 100 * 500 / (numAvailableBufPages - 1);//��ʽ����
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