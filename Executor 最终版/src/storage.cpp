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


  //�п���ռ��ʱ��
  for(FileIterator it = file.begin();it!=file.end();it++)
  {
	//ʹ���ļ�����������Ȼ�����
	Page tem = *(it);
	Page *pa = &tem;

	//����п�ҳ����ȡҳ��
	PageId pageNum = pa->page_number();
	bufMgr->readPage(&file,pageNum,pa); //pin++.���ļ�����pagenum��ֵ��pa
    //����Ԫ��
	if(pa->hasSpaceForRecord(tuple))
	{
		RecordId id = pa->insertRecord(tuple);
		bufMgr->unPinPage(&file,pageNum,true); //pin--
		return id;
	}
	//û�ҵ���ֻ���ͷ�pincnt
	bufMgr->unPinPage(&file,pageNum,true); //pin--
  }

  //for֮��û�п���ռ䣬������ҳ���ٽ�Ԫ�������ҳ
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
  PageId pageNum = rid.page_number;//��ȡԪ������ҳ�� page.h��
  bugMgr->readPage(&file,pageNum,pa);  //���뻺����
  pa->deleteRecord(rid);  //ɾ��Ԫ��
  bugMgr->unPinPage(&file,pageNum,true);  //pin--


}

string HeapFileManager::createTupleFromSQLStatement(const string& sql,
                                                    const Catalog* catalog) {
  // TODO

  //��������֮�������
	size_t position1 = sql.find_first_of("(");
    size_t position2 = sql.find_first_of(")");
    string temp = sql.substr(position1 + 1, position2 - position1-1);

    //���ŷָ�
    size_t position3 = temp.find_first_of(",");

    //��ȡ��һ���֣�ȥ��������
    string value1 = temp.substr(0, position3);
    size_t position4 = value1.find_first_of("'");
    size_t position5 = value1.find_last_of("'");
    if(position4!=-1)
        value1 = value1.substr(position4 + 1, position5 - position4 - 1);

    //�ڶ�����
    string value2 = temp.substr(position3 + 2);
    size_t position6 = value2.find_first_of("'");
    size_t position7 = value2.find_last_of("'");
    if (position6 != -1)
        value2 = value2.substr(position6 + 1, position7 - position6 - 1);

    //��������ƴ�ӣ����ö��ż��
    stringstream s1;
    s1 << value1 << "," << value2;

    return s1.str();





}
}  // namespace badgerdb