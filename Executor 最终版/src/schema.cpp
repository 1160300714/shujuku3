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

    //datatype 属性类型
    DataType dataType1 ;
    DataType dataType2 ;

    //C后13位name
    size_t pos1 = sql.find_first_of("C");
    tableName = sql.substr(pos1 + 13, 1);

    //括号
    string tem1 = sql.substr(pos1 + 16);
    tem1 = tem1.substr(0, tem1.length() - 2);

    //左右属性
    size_t pos2 = tem1.find_first_of(",");
    string left = tem1.substr(0, pos2);
    string right = tem1.substr(pos2 + 2);

    //第一个属性name
    string namel = left.substr(0, 1);

    //type是类型，important是为空，唯一，max为maxsize。
    string type_l;
    string important_l;
    int maxl = 1;
    bool not_null_l=false;
    bool unique_l=false;

    //去掉表名和表名后的空格
    left = left.substr(2);

    //找到type和UNIQUE NOT NULL之间的空格
    size_t pos3 = left.find(" ");
    //剪切下type的部分
    type_l = left.substr(0,pos3);
    //没有UNIQUE NOT NULL等字样，special设置为空格，返回-1，
    if (pos3 == -1)
        important_l = " ";
    //找到了
    else
        important_l = left.substr(pos3 + 1);

    //找括号，取后的一位，数字是maxsize
    size_t pos5 = type_l.find("(");
    string numl = type_l.substr(pos5 + 1, 1);
    int nl = atoi(numl.c_str());
    //没找到，是INT，maxsize为1
    if (pos5 == -1)
        maxl = 1;
    //找到了则将其设置为maxsize
    else
        maxl = nl;

    //根据important的内容来确定UNIQUE 和 NOTNULL 的属性
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

    //确定type的具体内容
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

    //同上
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

    //构造属性，加入到attrs中
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

    //使用get函数打印属性内容
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