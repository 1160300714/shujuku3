/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */


#include "schema.h"

#include <string>
#include <iostream>
using namespace std;

namespace badgerdb {

TableSchema TableSchema::fromSQLStatement(const string& sql) {
  string tableName;
  vector<Attribute> attrs;
  bool isTemp = false;

    //datatype ��������
    DataType dataType1 ;
    DataType dataType2 ;

    //C��13λname
    size_t pos1 = sql.find_first_of("C");
    tableName = sql.substr(pos1 + 13, 1);

    //����
    string tem1 = sql.substr(pos1 + 16);
    tem1 = tem1.substr(0, tem1.length() - 2);

    //��������
    size_t pos2 = tem1.find_first_of(",");
    string left = tem1.substr(0, pos2);
    string right = tem1.substr(pos2 + 2);

    //��һ������name
    string namel = left.substr(0, 1);

    //type�����ͣ�important��Ϊ�գ�Ψһ��maxΪmaxsize��
    string type_l;
    string important_l;
    int maxl = 1;
    bool not_null_l=false;
    bool unique_l=false;

    //ȥ�������ͱ�����Ŀո�
    left = left.substr(2);

    //�ҵ�type��UNIQUE NOT NULL֮��Ŀո�
    size_t pos3 = left.find(" ");
    //������type�Ĳ���
    type_l = left.substr(0,pos3);
    //û��UNIQUE NOT NULL��������special����Ϊ�ո񣬷���-1��
    if (pos3 == -1)
        important_l = " ";
    //�ҵ���
    else
        important_l = left.substr(pos3 + 1);

    //�����ţ�ȡ���һλ��������maxsize
    size_t pos5 = type_l.find("(");
    string numl = type_l.substr(pos5 + 1, 1);
    int nl = atoi(numl.c_str());
    //û�ҵ�����INT��maxsizeΪ1
    if (pos5 == -1)
        maxl = 1;
    //�ҵ�����������Ϊmaxsize
    else
        maxl = nl;

    //����important��������ȷ��UNIQUE �� NOTNULL ������
    if (!important_l.compare("UNIQUE NOT NULL"))
    {
        not_null_l = true;
        unique_l = true;
    }
    if (!important_l.compare("NOT NULL"))
    {
        not_null_l = true;
    }
    if (!important_l.compare("UNIQUE"))
    {
        unique_l = true;
    }

    //ȷ��type�ľ�������
    if (pos5 == -1)
        dataType1 = INT;
    else
    {
        type_l = type_l.substr(0, pos5);
        if (!type_l.compare("CHAR"))
            dataType1 = CHAR;
        else
            dataType1 = VARCHAR;
    }

    //ͬ��
    string namer = right.substr(0, 1);
    string type_r;
    string important_r;
    int maxr = 1;
    bool not_null_r = false;
    bool unique_r = false;

    right = right.substr(2);
    size_t pos4 = right.find_first_of(" ");
    type_r = right.substr(0, pos4);
    if (pos4 == -1)
        important_r = " ";
    else
        important_r = right.substr(pos4 + 1);

    size_t pos6 = type_r.find("(");
    string numr = type_r.substr(pos6 + 1, 1);
    int nr = atoi(numr.c_str());
    if (pos6 == -1)
        maxr = 1;
    else
        maxr = nr;


    if (!important_r.compare("UNIQUE NOT NULL"))
    {
        not_null_r = true;
        unique_r = true;
    }
    if (!important_r.compare("NOT NULL"))
    {
        not_null_r = true;
    }
    if (!important_r.compare("UNIQUE"))
    {
        unique_r = true;
    }


    if (pos6 == -1)
        dataType2 = INT;
    else
    {
        type_r = type_r.substr(0, pos6);
        if (!type_r.compare("CHAR"))
            dataType2 = CHAR;
        else
            dataType2 = VARCHAR;
    }

    //�������ԣ����뵽attrs��
	Attribute temA1 = Attribute(namel,dataType1,maxl);
	temA1.isNotNull = not_null_l;
	temA1.isUnique = unique_l;

	Attribute temA2 = Attribute(namer,dataType2,maxr);
	temA2.isNotNull = not_null_r;
	temA2.isUnique = unique_r;

	attrs.push_back(temA1);
	attrs.push_back(temA2);

    

  // TODO: create attribute definitions from sql


  return TableSchema(tableName, attrs, isTemp);

}

void TableSchema::print() const {

    //ʹ��get������ӡ��������
	for(int i=0;i<getAttrCount();i++){
        cout << "[" << i + 1 << "th]:" << endl;
		cout<<"name:"<<getAttrName(i)<<endl;
        cout<<"size:"<<getAttrMaxSize(i)<<endl;
		cout<<"type:"<<getAttrType(i)<<endl; 
		cout<<"isNotNull?:"<<isAttrNotNull(i)<<endl;
		cout<<"isUnique?:"<<isAttrUnique(i)<<endl;
        cout<< endl;
	}
  // TODO
}

}  // namespace badgerdb