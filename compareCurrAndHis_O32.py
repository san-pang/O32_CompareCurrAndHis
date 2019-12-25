# -*- coding: utf-8 -*-
# @File  : compareCurrAndHis_O32.py
# @Author: 王三胖
# @Date  : 2019/12/24
# @Desc  : 比对O32当前表和历史表的表结构是否一致

import cx_Oracle

class O32:
    def __init__(self, dbuser, dbpass, tns):
        self.db = cx_Oracle.connect(dbuser, dbpass, tns)
        self.cursor = self.db.cursor()
        # 是否只打印比对不一致的表，如果设置为False，比对一致的表也会打印，例如“TFUTURESINFO 当前表与历史表比对一致”
        self.onlyPrintDiffrent = False
        # 忽略不比对的字段
        # L_DATE和D_DATETIME在几乎全部当前表和历史表都不一样，当前表不允许为NULL，而历史表允许为NULL，导致比对不通过，暂不比对这两个字段
        self.ignoreField = ["L_DATE", "D_DATETIME"]

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

    def getTableStruct(self, tableName):
        '''
        获取表结构（字段长度、字段类型、小数位精度、是否为NULL等）
        :param tableName: 表名
        :return: 表结构数据字典
        '''
        sql = '''
            SELECT --T1.TABLE_NAME,
                   T1.COLUMN_NAME,
                   T1.DATA_TYPE,
                   T1.DATA_LENGTH,
                   T1.DATA_PRECISION,
                   t1.DATA_SCALE,
                   T1.NULLABLE
              FROM USER_TAB_COLS T1, USER_COL_COMMENTS T2
             WHERE T1.TABLE_NAME = T2.TABLE_NAME
               AND T1.COLUMN_NAME = T2.COLUMN_NAME
               AND T1.TABLE_NAME = upper('{0}')'''.format(tableName)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        cols = [d[0] for d in self.cursor.description]
        res = {}
        for row in data:
            b = dict(zip(cols, row))
            # 这里需要做一下兼容处理，如果字段是VARCHAR类型，DATA_PRECISION的值会是None
            if b.has_key("DATA_PRECISION") and (b["DATA_PRECISION"] == None or b["DATA_PRECISION"] == "None"):
                b["DATA_PRECISION"] = b["DATA_LENGTH"]
            # varchar类型的字段小数精度是None，需要默认为0去比对
            if b.has_key("DATA_SCALE") and (b["DATA_SCALE"] == None or b["DATA_SCALE"] == "None"):
                b["DATA_SCALE"] = 0
            # DATA_LENGTH这个字段只是用来处理字段长度的中间值，如果字段类型不一样，是肯定比对不上的，因此无需比对
            b.pop("DATA_LENGTH", None)
            res[b["COLUMN_NAME"]] = b
        return res

    def getToHisTable(self):
        '''
        获取O32系统要归历史的表名
        :return: 要归历史的表名
        '''
        # 在O32里面类别为4的表不归历史
        sql = 'select vc_table_name from TARCHIVE where c_archive_type != \'4\''
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return [d[0] for d in data]

    def translate(self, key):
        '''
        对比对差异部分字段进行翻译展示
        :return: 翻译后的值
        '''
        if key == "DATA_TYPE":
            return "字段类型"
        elif key == "DATA_PRECISION":
            return "字段总长度"
        elif key == "DATA_SCALE":
            return "小数位精度"
        elif key == "NULLABLE":
            return "是否允许为NULL"
        else:
            return key

    def compare(self):
        '''
        比对当前表和历史表的表结构差异
        :return: None
        '''
        currTables = self.getToHisTable()
        for currTable in currTables:
            hisTable = currTable[:1] + "his" + currTable[1:]
            currStruct = self.getTableStruct(currTable)
            hisStruct = self.getTableStruct(hisTable)
            for x in self.ignoreField:
                currStruct.pop(x, None)
                hisStruct.pop(x, None)
            if currStruct == hisStruct:
                if not self.onlyPrintDiffrent:
                    print currTable, "当前表与历史表比对一致\n"
            else:
                print currTable, "当前表与历史表比对不一致"
                for currField, currValue in currStruct.items():
                    if hisStruct.has_key(currField):
                        hisValue = hisStruct[currField]
                        for k, v in currValue.items():
                            if v != hisValue[k]:
                                print currField, "字段在当前表和历史表定义不一致：", self.translate(k), "在当前表为", v, "，历史表为", hisValue[k]
                        del currStruct[currField]
                        del hisStruct[currField]
                    else:
                        # 只存在于当前表的字段
                        print currField, "字段在历史表不存在"
                        del currStruct[currField]
                if len(hisStruct) > 0:
                    print "".join([k for k, v in hisStruct.items()]), "字段在当前表不存在"
                print  # 纯粹为了换行

if __name__ == "__main__":
    o32 = O32(dbuser="用户名", dbpass="密码", tns="数据库IP:数据库监听端口/数据库实例名")
    o32.onlyPrintDiffrent = True
    o32.compare()