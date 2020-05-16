/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "page.h"
#include "file_iterator.h"
#include "storage.h"
#include <sstream>
using namespace std;

namespace badgerdb {

RecordId HeapFileManager::insertTuple(const string& tuple,
                                      File& file,
                                      BufMgr* bufMgr) {
  // TODO


  Page *page;
  PageId pageNo;


  //有空余空间的时候
  for(FileIterator it = file.begin();it!=file.end();it++)
  {
	//使用文件迭代器，不然会出错
	Page tem = *(it);
	Page *pa = &tem;

	//如果有空页，获取页号
	PageId pageNum = pa->page_number();
	bufMgr->readPage(&file,pageNum,pa); //pin++.读文件，把pagenum赋值到pa
    //插入元组
	if(pa->hasSpaceForRecord(tuple))
	{
		RecordId id = pa->insertRecord(tuple);
		bufMgr->unPinPage(&file,pageNum,true); //pin--
		return id;
	}
	//没找到则只是释放pincnt
	bufMgr->unPinPage(&file,pageNum,true); //pin--
  }

  //for之后，没有空余空间，申请新页，再将元组插入新页
  bufMgr->allocPage(&file,pageNo,page);
  RecordId id = page->insertRecord(tuple);
  bufMgr->unPinPage(&file,pageNo,true);//pin--
  return id;





}



void HeapFileManager::deleteTuple(const RecordId& rid,
                                  File& file,
                                  BufMgr* bugMgr) {
  // TODO

  Page *pa;
  PageId pageNum = rid.page_number;//获取元组所在页号 page.h中
  bugMgr->readPage(&file,pageNum,pa);  //读入缓冲区
  pa->deleteRecord(rid);  //删除元组
  bugMgr->unPinPage(&file,pageNum,true);  //pin--


}

string HeapFileManager::createTupleFromSQLStatement(const string& sql,
                                                    const Catalog* catalog) {
  // TODO

  //两个括号之间的内容
	size_t position1 = sql.find_first_of("(");
    size_t position2 = sql.find_first_of(")");
    string temp = sql.substr(position1 + 1, position2 - position1-1);

    //逗号分隔
    size_t position3 = temp.find_first_of(",");

    //提取第一部分，去除单引号
    string value1 = temp.substr(0, position3);
    size_t position4 = value1.find_first_of("'");
    size_t position5 = value1.find_last_of("'");
    if(position4!=-1)
        value1 = value1.substr(position4 + 1, position5 - position4 - 1);

    //第二部分
    string value2 = temp.substr(position3 + 2);
    size_t position6 = value2.find_first_of("'");
    size_t position7 = value2.find_last_of("'");
    if (position6 != -1)
        value2 = value2.substr(position6 + 1, position7 - position6 - 1);

    //将两部分拼接，并用逗号间隔
    stringstream s1;
    s1 << value1 << "," << value2;

    return s1.str();





}
}  // namespace badgerdb