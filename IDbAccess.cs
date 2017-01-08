using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;

namespace DBUtil
{
    public interface IDbAccess : IDisposable
    {
        bool IsKeepConnect { get; set; }
        IDbTransaction tran { get; set; }

        string ConnectionString { get; set; }

        IDbConnection conn { get; set; }

        DataBaseType DataBaseType { get; set; }

        /// <summary>记录是否打开了连接,防止多次打开连接</summary>
        bool IsOpen { get; set; }

        /// <summary>记录是否开启了事务,防止多次开启事务</summary>
        bool IsTran { get; set; }

        /// <summary>当前数据库使用的参数的前缀符号</summary>
        string paraPrefix { get; }

        /// <summary>创建参数</summary>
        /// <returns>针对当前数据库类型的空参数对象</returns>
        IDbDataParameter CreatePara();

        /// <summary>根据指定日期范围生成过滤字符串</summary>
        /// <param name="dateColumn">要进行过滤的字段名称</param>
        /// <param name="minDate">最小日期</param>
        /// <param name="MaxDate">最大日期</param>
        /// <param name="isMinInclude">最小日期是否包含</param>
        /// <param name="isMaxInclude">最大日期是否包含</param>
        /// <returns>返回生成的过滤字符串</returns>
        string GetDateFilter(string dateColumn, string minDate, string MaxDate, bool isMinInclude, bool isMaxInclude);

        /// <summary>执行sql语句</summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        int ExecuteSql(string strSql);

        /// <summary>执行sql语句</summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        int ExecuteSql(string strSql, IDataParameter[] paramArr);

        /// <summary>执行多个sql语句</summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        /// <returns>返回是否执行成功</returns>
        void ExecuteSql(string[] strSql);

        /// <summary>执行多个sql语句</summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        /// <param name="paraArrs">多个SQL语句的参数对应的二维数组</param>
        /// <returns>返回是否执行成功</returns>
        void ExecuteSql(string[] strSql, IDataParameter[][] paraArrs);

        /// <summary>向一个表中添加一行数据</summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">列名和值得键值对</param>
        /// <returns>返回是受影响的行数</returns>
        bool AddData(string tableName, Hashtable ht);

        /// <summary>根据键值表ht中的数据向表中更新数据</summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, string filterStr);

        /// <summary>根据键值表ht中的数据向表中更新数据</summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, string filterStr, IDbDataParameter[] paraArr);

        /// <summary>向表中更新数据并根据ht里面的键值对作为关键字更新(关键字默认不参与更新)</summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false);

        /// <summary>删除一行</summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <returns>返回受影响的行数</returns>
        int DeleteTableRow(string tableName, string strFilter);

        /// <summary>删除一行</summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <param name="paraArr">过滤条件中的参数集合</param>
        /// <returns>返回受影响的行数</returns>
        int DeleteTableRow(string tableName, string strFilter, IDbDataParameter[] paraArr);

        /// <summary>返回查到的第一行第一列的值</summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        object GetFirstColumn(string strSql);

        /// <summary>返回查到的第一行第一列的值</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        object GetFirstColumn(string strSql, IDbDataParameter[] paraArr);

        /// <summary>返回查到的第一行第一列的字符串值</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        string GetFirstColumnString(string strSql, bool isReturnNull = false);

        /// <summary>返回查到的第一行第一列的字符串值</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        string GetFirstColumnString(string strSql, IDbDataParameter[] paraArr, bool isReturnNull = false);

        /// <summary>获取阅读器</summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        IDataReader GetDataReader(string strSql);

        /// <summary>获取阅读器</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回阅读器</returns>
        IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr);

        /// <summary>返回查询结果的数据集</summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        DataSet GetDataSet(string strSql);

        /// <summary>返回查询结果的数据集</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回的查询结果集</returns>
        DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr);

        /// <summary>返回查询结果的数据表</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回的查询结果集</returns>
        DataTable GetDataTable(string strSql);

        /// <summary>返回查询结果的数据表</summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回的查询结果集</returns>
        DataTable GetDataTable(string strSql, IDbDataParameter[] paraArr);

        /// <summary>
        /// 开启事务
        /// </summary>
        void BeginTrans();

        /// <summary>
        /// 提交事务
        /// </summary>
        void Commit();

        /// <summary>
        /// 回滚事务
        /// </summary>
        void Rollback();


        /// <summary>获得分页的查询语句</summary>
        /// <param name="tableName">表名</param>
        /// <param name="selectColumns">要查询的列,为null是表示所有列</param>
        /// <param name="PageSize">分页大小</param>
        /// <param name="PageIndex">分页索引</param>
        /// <param name="strWhere">过滤条件</param>
        /// <param name="strOrder">排序条件</param>
        /// <returns>返回经过分页的语句</returns>
        string GetSqlForPageSize(string tableName, string[] selectColumns, int PageSize, int PageIndex, string strWhere, string strOrder);

        string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex);

        /// <summary>判断指定表或视图中是否有某一列</summary>
        /// <param name="tableName">表或视图名</param>
        /// <param name="columnName">列名</param>
        /// <returns>返回列是否存在</returns>
        bool JudgeColumnExist(string tableName, string columnName);

        /// <summary>返回表或视图是否存在</summary>
        /// <param name="tableName">表或视图名</param>
        /// <returns>返回表或视图是否存在</returns>
        bool JudgeTableOrViewExist(string tableName);

        /// <summary>返回所有的表</summary>
        /// <returns>返回所有的表</returns>
        List<TableStruct> ShowTables();

        /// <summary>返回所有的视图</summary>
        /// <returns>返回所有的视图</returns>
        List<DataView> ShowViews();

        /// <summary>生成一个表的新纪录的ID</summary>
        /// <param name="TableName">要生成ID的表名</param>
        /// <returns>返回新的ID</returns>
        int SetID(string TableName);

        /// <summary>重设置一个表的ID值</summary>
        /// <param name="TableName">表名</param>
        /// <param name="IDColumn">列名</param>
        /// <returns>是否重置成功</returns>
        bool ResetID(string TableName, string IDColumn);
    }
}
