using System.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using System.Text.RegularExpressions;

namespace DBUtil
{
    public class SqlServerIDbAccess : IDbAccess
    {
        public IDSNOManager IDSNOManager { get { return IDBFactory.IDSNOManage; } }
        public bool IsKeepConnect { set; get; }
        public IDbTransaction tran { set; get; }
        public string ConnectionString { get; set; }
        public IDbConnection conn { set; get; }
        public DataBaseType DataBaseType { get; set; }

        public bool IsOpen { set; get; }

        public bool IsTran { set; get; }

        /// <summary>打开连接测试</summary>
        /// <returns></returns>
        public Result OpenTest()
        {
            try
            {
                conn.Open();
                conn.Close();
                return new Result()
                {
                    Success = true
                };
            }
            catch (Exception ex)
            {
                return new Result()
                {
                    Success = false,
                    Data = ex.ToString()
                };
            }
        }

        /// <summary>当前数据库使用的参数的前缀符号</summary>
        public string paraPrefix { get { return "@"; } }

        #region 创建参数 public IDbDataParameter CreatePara()
        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreatePara()
        {
            return new SqlParameter();
        }
        #endregion

        #region 创建参数 public IDbDataParameter CreatePara(string name, object value)
        /// <summary>创建具有名称和值的参数</summary>
        /// <returns>针对当前数据库类型的参数对象</returns>
        public IDbDataParameter CreatePara(string name, object value)
        {
            return new SqlParameter(name, value);
        }
        #endregion
        #region 根据指定日期范围生成过滤字符串 public string GetDateFilter(string dateColumn, string minDate, string MaxDate, bool isMinInclude, bool isMaxInclude)
        /// <summary>根据指定日期范围生成过滤字符串</summary>
        /// <param name="dateColumn">要进行过滤的字段名称</param>
        /// <param name="minDate">最小日期</param>
        /// <param name="MaxDate">最大日期</param>
        /// <param name="isMinInclude">最小日期是否包含</param>
        /// <param name="isMaxInclude">最大日期是否包含</param>
        /// <returns>返回生成的过滤字符串</returns>
        public string GetDateFilter(string dateColumn, string minDate, string maxDate, bool isMinInclude, bool isMaxInclude)
        {
            DateTime dt;
            if (DateTime.TryParse(minDate, out dt) && DateTime.TryParse(maxDate, out dt))
            {
                string res = "";
                if (isMinInclude)
                {
                    res += " and " + dateColumn + ">='" + minDate + "'";
                }
                else
                {
                    res += " and " + dateColumn + ">'" + minDate + "'";
                }
                if (isMaxInclude)
                {
                    res += " and " + dateColumn + "<='" + maxDate + "'";
                }
                else
                {
                    res += " and " + dateColumn + "<'" + maxDate + "'";
                }
                return res;
            }
            else
            {
                throw new Exception("非正确的格式:[" + minDate + "]或[" + maxDate + "]");
            }
        }
        #endregion
        #region 执行sql语句 public int ExecuteSql(string strSql)
        /// <summary>
        /// 执行sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <returns>受影响的行数</returns>
        public int ExecuteSql(string strSql)
        {
            try
            {
                SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (SqlTransaction)tran;
                }
                if (!IsOpen)
                {
                    conn.Open();
                    IsOpen = true;
                }
                int r = cmd.ExecuteNonQuery();
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
                return r;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }

        }
        #endregion

        #region 执行多个sql语句 public void ExecuteSql(string[] strSql)
        /// <summary>
        /// 执行多个sql语句
        /// </summary>
        /// <param name="strSql">多个SQL语句的数组</param>
        public void ExecuteSql(string[] strSql)
        {
            try
            {
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = (SqlConnection)conn;
                if (IsTran)
                {
                    cmd.Transaction = (SqlTransaction)tran;
                }
                if (!IsOpen)
                {
                    conn.Open();
                }
                foreach (string sql in strSql)
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
        }
        #endregion

        #region 执行带参数的sql语句 public int ExecuteSql(string strSql, IDataParameter[] paramArr)
        /// <summary>
        /// 执行带参数的sql语句
        /// </summary>
        /// <param name="strSql">要执行的sql语句</param>
        /// <param name="paramArr">参数数组</param>
        /// <returns></returns>
        public int ExecuteSql(string strSql, IDataParameter[] paramArr)
        {
            try
            {
                SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (SqlTransaction)tran;
                }
                cmd.Parameters.AddRange(paramArr);
                if (!IsOpen)
                {
                    conn.Open();
                    IsOpen = true;
                }
                int r = cmd.ExecuteNonQuery();
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
                return r;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (!IsTran && !IsKeepConnect)
                {
                    conn.Close();
                    this.IsOpen = false;
                }
            }
        }
        #endregion

        #region 批量执行带参数的sql语句 public void ExecuteSql(string[] strSql, IDataParameter[][] paraArrs)
        /// <summary>
        /// 批量执行带参数的sql语句
        /// </summary>
        /// <param name="strSql"></param>
        /// <param name="paraArrs"></param>
        public void ExecuteSql(string[] strSql, IDataParameter[][] paraArrs)
        {
            for (int i = 0; i < strSql.Length; i++)
            {
                ExecuteSql(strSql[i], paraArrs[i]);
            }
        }
        #endregion

        #region 向一个表中添加一行数据 public bool AddData(string tableName, Hashtable ht)
        /// <summary>
        /// 向一个表中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">列名和值得键值对</param>
        /// <returns>返回是受影响的行数</returns>
        public bool AddData(string tableName, Hashtable ht)
        {
            string insertTableOption = "";
            string insertTableValues = "";
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                insertTableOption += " " + item.Key.ToString() + ",";
                insertTableValues += "@" + item.Key.ToString() + ",";
                paras.Add(new SqlParameter()
                {
                    ParameterName = item.Key.ToString(),
                    Value = item.Value
                });
            }
            insertTableOption = insertTableOption.TrimEnd(new char[] { ',' });
            insertTableValues = insertTableValues.TrimEnd(new char[] { ',' });

            string strSql = string.Format("insert into {0} ({1}) values ({2})", tableName, insertTableOption, insertTableValues);
            return ExecuteSql(strSql, paras.ToArray()) > 0 ? true : false;
        }
        #endregion

        #region 根据键值表ht中的数据向表中更新数据 public bool UpdateData(string tableName, Hashtable ht, string filterStr)
        /// <summary>
        /// 根据键值表ht中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, string filterStr)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    paras.Add(new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    });
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            sql += filterStr;
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }
        #endregion

        #region 根据键值表ht中的数据向表中更新数据 public bool UpdateData(string tableName, Hashtable ht, string filterStr, IDbDataParameter[] paraArr)
        /// <summary>
        /// 根据键值表ht中的数据向表中更新数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="filterStr">过滤条件以and开头</param>
        /// <param name="paraArr">过滤条件中的参数数组</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, string filterStr, IDbDataParameter[] paraArr)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    paras.Add(new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    });
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            sql += filterStr;
            foreach (var item in paraArr)
            {
                if (!ContainsDBParameter(paras, item))
                {
                    paras.Add(item);
                }
            }
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }
        #endregion

        #region 向表中更新数据并根据ht里面的键值对作为关键字更新(关键字默认不参与更新) public bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false)
        /// <summary>
        /// 向表中更新数据并根据ht里面的键值对作为关键字更新(关键字默认不参与更新)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ht">键值表</param>
        /// <param name="keys">关键字集合</param>
        /// <param name="isKeyAttend">关键字是否参与到更新中</param>
        /// <returns>是否更新成功</returns>
        public bool UpdateData(string tableName, Hashtable ht, List<string> keys, bool isKeyAttend = false)
        {
            string sql = string.Format("update {0} set ", tableName);
            List<IDbDataParameter> paras = new List<IDbDataParameter>();
            foreach (System.Collections.DictionaryEntry item in ht)
            {
                if (keys.Contains(item.Key.ToString()) && !isKeyAttend)
                {
                    continue;
                }
                if (item.Value == null)
                {
                    sql += " " + item.Key.ToString() + "=null,";
                }
                else
                {
                    sql += " " + item.Key.ToString() + "=@" + item.Key.ToString() + ",";
                    IDbDataParameter para = new SqlParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    };
                    if (!ContainsDBParameter(paras, para)) ;
                    paras.Add(para);
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            foreach (var item in keys)
            {
                sql += " and " + item + "=@" + item;
                paras.Add(new SqlParameter()
                {
                    ParameterName = item,
                    Value = ht[item]
                });
            }
            return ExecuteSql(sql, paras.ToArray()) > 0 ? true : false;
        }
        #endregion

        #region  判断参数集合list中是否包含同名的参数para,如果已存在返回true,否则返回false private bool ContainsDBParameter(List<IDbDataParameter> list, IDbDataParameter para)
        /// <summary>
        /// 判断参数集合list中是否包含同名的参数para,如果已存在返回true,否则返回false
        /// </summary>
        /// <param name="list">参数集合</param>
        /// <param name="para">参数模型</param>
        /// <returns>参数集合中是否包含参数模型</returns>
        private bool ContainsDBParameter(List<IDbDataParameter> list, IDbDataParameter para)
        {
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i].ParameterName == para.ParameterName)
                {
                    return true;
                }
            }
            return false;
        }
        #endregion

        #region 删除一行 public int DeleteTableRow(string tableName, string strFilter)
        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件以and开头</param>
        /// <returns>返回受影响的行数</returns>
        public int DeleteTableRow(string tableName, string strFilter)
        {
            string sql = string.Format("delete from {0} where 1=1 {1}", tableName, strFilter);
            return ExecuteSql(sql);
        }
        #endregion

        #region 删除一行 public int DeleteTableRow(string tableName, string strFilter, IDbDataParameter[] paras)
        /// <summary>
        /// 删除一行
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="strFilter">过滤条件</param>
        /// <param name="paras">过滤条件中的参数集合</param>
        /// <returns>返回受影响的行数</returns>
        public int DeleteTableRow(string tableName, string strFilter, IDbDataParameter[] paras)
        {
            string sql = string.Format("delete from {0} where 1=1 {1}", tableName, strFilter);
            return ExecuteSql(sql, paras.ToArray());
        }
        #endregion

        #region 返回查到的第一行第一列的值 public object GetFirstColumn(string strSql)
        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public object GetFirstColumn(string strSql)
        {

            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            object obj = cmd.ExecuteScalar();
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                IsOpen = false;
            }
            return obj;
        }
        #endregion

        #region 返回查到的第一行第一列的值 public object GetFirstColumn(string strSql, IDbDataParameter[] paraArr)
        /// <summary>
        /// 返回查到的第一行第一列的值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句参数</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public object GetFirstColumn(string strSql, IDbDataParameter[] paraArr)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            object obj = cmd.ExecuteScalar();
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                IsOpen = false;
            }
            return obj;
        }
        #endregion

        #region 返回查到的第一行第一列的字符串值(调用GetFirstColumn,将返回的对象转换成字符串,如果为null就转化为"") public string GetFirstColumnString(string strSql, bool isReturnNull = false)
        /// <summary>
        /// 返回查到的第一行第一列的字符串值(调用GetFirstColumn,将返回的对象转换成字符串,如果为null就转化为"")
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="isReturnNull">当查询结果为null是是否将null返回,为true则返回null,为false则返回"",默认为false</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public string GetFirstColumnString(string strSql, bool isReturnNull = false)
        {

            object obj = GetFirstColumn(strSql);
            if (obj == null)
            {
                if (isReturnNull)
                {
                    return null;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return obj.ToString();
            }
        }
        #endregion

        #region 返回查到的第一行第一列的字符串值 public string GetFirstColumnString(string strSql, IDbDataParameter[] paraArr, bool isReturnNull = false)
        /// <summary>
        /// 返回查到的第一行第一列的字符串值
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">sql语句中的参数数组</param>
        /// <param name="isReturnNull">false:查询结果为null就返回""否则返回null</param>
        /// <returns>返回查到的第一行第一列的值</returns>
        public string GetFirstColumnString(string strSql, IDbDataParameter[] paraArr, bool isReturnNull = false)
        {
            object obj = GetFirstColumn(strSql, paraArr);
            if (obj == null)
            {
                if (isReturnNull)
                {
                    return null;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return obj.ToString();
            }
        }
        #endregion

        #region 获取阅读器 public IDataReader GetDataReader(string strSql)
        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            return cmd.ExecuteReader();
        }
        #endregion

        #region 获取阅读器 public IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr)
        /// <summary>
        /// 获取阅读器
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回阅读器</returns>
        public IDataReader GetDataReader(string strSql, IDbDataParameter[] paraArr)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            return cmd.ExecuteReader();
        }
        #endregion

        #region 返回查询结果的数据集 public DataSet GetDataSet(string strSql)
        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            SqlDataAdapter adp = new SqlDataAdapter(cmd);
            DataSet set = new DataSet();
            adp.Fill(set);
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                this.IsOpen = false;
            }
            return set;
        }
        #endregion

        #region 返回查询结果的数据集 public DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
        /// <summary>
        /// 返回查询结果的数据集
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">SQL语句中的参数集合</param>
        /// <returns>返回的查询结果集</returns>
        public DataSet GetDataSet(string strSql, IDbDataParameter[] paraArr)
        {
            SqlCommand cmd = new SqlCommand(strSql, (SqlConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (SqlTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            SqlDataAdapter adp = new SqlDataAdapter(cmd);
            DataSet set = new DataSet();
            adp.Fill(set);
            if (!IsTran && !IsKeepConnect)
            {
                conn.Close();
                this.IsOpen = false;
            }
            return set;
        }
        #endregion

        #region 返回查询结果的数据表 public DataTable GetDataSet(string strSql)
        /// <summary>
        /// 返回查询结果的数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <returns>返回的查询数据表</returns>
        public DataTable GetDataTable(string strSql)
        {
            DataSet ds = GetDataSet(strSql);
            if (ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                ds.Tables.Remove(dt);
                return dt;
            }
            return null;
        }
        #endregion

        #region 返回查询结果的数据集 public DataTable GetDataTable(string strSql, IDbDataParameter[] paraArr)
        /// <summary>
        /// 返回的查询数据表
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="paraArr">SQL语句中的参数集合</param>
        /// <returns>返回的查询数据表</returns>
        public DataTable GetDataTable(string strSql, IDbDataParameter[] paraArr)
        {
            DataSet ds = GetDataSet(strSql, paraArr);
            if (ds.Tables.Count > 0)
            {
                DataTable dt = ds.Tables[0];
                ds.Tables.Remove(dt);
                return dt;
            }
            return null;
        }
        #endregion

        #region 开启事务 public void BeginTrans()
        /// <summary>
        /// 开启事务
        /// </summary>
        public void BeginTrans()
        {
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            if (IsTran)
            {
                tran.Commit();
            }
            tran = conn.BeginTransaction();
            IsTran = true;
        }
        #endregion

        #region 提交事务 public void Commit()
        /// <summary>
        /// 提交事务
        /// </summary>
        public void Commit()
        {
            tran.Commit();
        }
        #endregion

        #region 回滚事务 public void Rollback()
        /// <summary>
        /// 回滚事务
        /// </summary>
        public void Rollback()
        {
            tran.Rollback();
        }
        #endregion

        #region 判断指定表中是否有某一列 public bool JudgeColumnExist(string tableName, string columnName)
        /// <summary>
        /// 判断指定表中是否有某一列
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <returns>返回列是否存在</returns>
        public bool JudgeColumnExist(string tableName, string columnName)
        {
            string sql = string.Format("select count(1) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}' and COLUMN_NAME='{1}'", tableName, columnName);
            int r = int.Parse(GetFirstColumn(sql).ToString());
            if (r > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        #endregion

        #region 判断表是否存在 public bool JudgeTableOrViewExist(string tableName)
        /// <summary>
        /// 判断表是否存在
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <returns>返回表是否存在</returns>
        public bool JudgeTableOrViewExist(string tableName)
        {
            string sql = string.Format("select count(1) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='{0}'", tableName);
            int r = int.Parse(GetFirstColumn(sql).ToString());
            if (r > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        #endregion

        #region 获得分页的查询语句 public string GetSqlForPageSize(string tableName, string[] selectColumns, int PageSize, int PageIndex, string strWhere, string strOrder)
        /// <summary>
        /// 获得分页的查询语句
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="selectColumns">要查询的列,为null是表示所有列</param>
        /// <param name="PageSize">分页大小</param>
        /// <param name="PageIndex">分页索引</param>
        /// <param name="strWhere">过滤条件</param>
        /// <param name="strOrder">排序条件</param>
        /// <returns>返回经过分页的语句</returns>
        public string GetSqlForPageSize(string tableName, string[] selectColumns, int PageSize, int PageIndex, string strWhere, string strOrder)
        {
            string sql = "";
            string sqlSelect = "select ";
            if (selectColumns == null)
            {
                sqlSelect += "* ";
            }
            else
            {
                foreach (string columnName in selectColumns)
                {
                    sqlSelect += columnName + ",";
                }
            }
            sqlSelect = sqlSelect.Trim(new char[] { ',' });
            sql = string.Format("{0} from (select *,ROW_NUMBER() OVER({1}) AS rownumber  from {2} t where 1=1 {3}) as tfix WHERE tfix.rownumber BETWEEN ({4}*{5}+1) AND ({6}*{7})", sqlSelect, strOrder, tableName, strWhere, PageIndex - 1, PageSize, PageIndex, PageSize);

            return sql;
        }
        #endregion

        #region 获得分页的查询语句 public string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex)
        /// <summary>
        /// 获得分页的查询语句
        /// </summary>
        /// <param name="selectSql">查询sql如:select name,id from test where id>5</param>
        /// <param name="strOrder">排序字句如:order by id desc</param>
        /// <param name="PageSize">页面大小</param>
        /// <param name="PageIndex">页面索引从1开始</param>
        /// <returns>经过分页的sql语句</returns>
        public string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex)
        {
            string sql = string.Format("select * from (select *,ROW_NUMBER() OVER({0}) as RNO__ from ({1}) as inner__ ) outer__ WHERE outer__.RNO__ BETWEEN ({2}*{3}+1) AND ({4}*{5})", strOrder, selectSql, PageIndex - 1, PageSize, PageIndex, PageSize);
            return sql;
        }
        #endregion

        #region 实现释放资源的方法 public void Dispose()
        /// <summary>
        /// 实现释放资源的方法
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (this.conn.State != ConnectionState.Closed)
                {
                    this.conn.Close();
                    this.IsOpen = false;
                }
            }
            catch (Exception e)
            {
            }
        }
        #endregion

        #region  获得所有表,注意返回的集合中的表模型中只有表名 public List<DataTable> ShowTables()
        /// <summary>
        /// 获得所有表,注意返回的集合中的表模型中只有表名
        /// </summary>
        /// <returns></returns>
        public List<TableStruct> ShowTables()
        {
            DataSet ds = GetDataSet("select TABLE_NAME from INFORMATION_SCHEMA.TABLES t where t.TABLE_TYPE ='BASE TABLE'");
            TableStruct tbl = null;
            List<TableStruct> list = new List<TableStruct>();
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                tbl = new TableStruct();
                tbl.Name = ds.Tables[0].Rows[i][0].ToString();
                list.Add(tbl);
            }
            return list;
        }
        #endregion

        public List<DataView> ShowViews()
        {
            throw new NotImplementedException();
        }

        #region 获得指定表的表结构说明 public TableStruct GetTableStruct(string tableName)
        /// <summary>获得指定表的表结构说明</summary>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        public TableStruct GetTableStruct(string tableName)
        {
            string sql = string.Format(@"
select 
	序号=ROW_NUMBER() OVER(order BY ORDINAL_POSITION),
	列名=t.COLUMN_NAME,
	类型=DATA_TYPE,
    最大长度=tt.max_length,
	说明=tt.说明,
	是否可空=IS_NULLABLE,
    自增种子=IDENT_SEED('{0}'),
    增量=IDENT_INCR('{0}'),
	默认值=COLUMN_DEFAULT,
	是否自增 
from INFORMATION_SCHEMA.COLUMNS T
left outer join
(select 列名=c.name,说明=p.value ,max_length
from sys.columns c 
	left outer join sys.objects o on c.object_id =o.object_id
	left outer join sys.extended_properties p on c.column_id = p.minor_id and c.object_id=p.major_id
where o.name='{0}')  tt
on t.COLUMN_NAME=tt.列名 
left outer join
(SELECT TABLE_NAME,COLUMN_NAME,是否自增=case (COLUMNPROPERTY(      
      OBJECT_ID('{0}'),COLUMN_NAME,'IsIdentity')) when 1 then '是' else '否' end  FROM INFORMATION_SCHEMA.columns) c
on c.COLUMN_NAME =T.COLUMN_NAME

where t.TABLE_NAME='{0}'
and c.TABLE_NAME='{0}' 
", tableName);
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count == 0)
            {
                return null;
            }
            TableStruct tbl = new TableStruct();
            tbl.Name = tableName;
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                TableStruct.Column col = new TableStruct.Column()
                {
                    Name = dt.Rows[i]["列名"].ToString(),
                    Desc = dt.Rows[i]["说明"].ToString(),
                    IsIdentity = dt.Rows[i]["是否自增"].ToString() == "是" ? true : false,
                    IsNullable = dt.Rows[i]["是否可空"].ToString() == "YES" ? true : false,
                    Type = dt.Rows[i]["类型"].ToString(),
                    Default = dt.Rows[i]["默认值"].ToString(),
                    MaxLength = int.Parse(dt.Rows[i]["最大长度"].ToString())
                };
                if (col.IsIdentity)
                {
                    col.Start = int.Parse(dt.Rows[0]["自增种子"].ToString());
                    col.Incre = int.Parse(dt.Rows[0]["增量"].ToString());
                }
                col.Default = (col.Default ?? "").Trim(new char[] { '(', ')', ' ', '\'' });
                tbl.Columns.Add(col);
                string sqltmp = string.Format(@"SELECT
  count(1)
FROM
  sys.indexes idx
    JOIN sys.index_columns idxCol 
      ON (idx.object_id = idxCol.object_id 
          AND idx.index_id = idxCol.index_id 
          AND idx.is_unique_constraint = 1)
    JOIN sys.tables tab
      ON (idx.object_id = tab.object_id)
    JOIN sys.columns col
      ON (idx.object_id = col.object_id
          AND idxCol.column_id = col.column_id)
WHERE
  tab.name = '{0}'
  and
  col.name='{1}'", tableName, col.Name);
                if (GetFirstColumnString(sqltmp) == "1")
                {
                    col.IsUnique = true;
                }
                else
                {
                    col.IsUnique = false;
                }
            }
            sql = string.Format(@"
select b.column_name
from information_schema.table_constraints a
inner join information_schema.constraint_column_usage b
on a.constraint_name = b.constraint_name
where a.constraint_type = 'PRIMARY KEY' and a.table_name = '{0}'
", tableName);
            dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string colname = dt.Rows[i][0].ToString();
                    tbl.PrimaryKey += "," + colname;
                    TableStruct.Column col = tbl.Columns.First<TableStruct.Column>(j => j.Name == colname);
                    col.IsPrimaryKey = true;
                    if (dt.Rows.Count == 1)
                    {
                        col.IsUnique = true;
                    }

                }
            }
            tbl.PrimaryKey = (tbl.PrimaryKey ?? "").Trim(',');
            sql = @"select 
	说明=(SELECT value   
FROM sys.extended_properties ds  
LEFT JOIN sysobjects tbs ON ds.major_id=tbs.id  
WHERE  ds.minor_id=0 and  
 tbs.name=table_name )
from INFORMATION_SCHEMA.TABLES t where t.TABLE_TYPE='BASE TABLE' and t.TABLE_NAME='" + tbl.Name + "'";
            tbl.Desc = GetFirstColumnString(sql);
            //构建表索引集合
            sql = "sp_helpindex '" + tableName + "'";
            dt = GetDataTable(sql);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = dt.Rows.Count - 1; i >= 0; i--)
                {
                    if (dt.Rows[i][1].ToString().Contains("primary key located on PRIMARY") || dt.Rows[i][1].ToString().Contains("unique key located on PRIMARY"))
                    {
                        continue;
                    }
                    TableStruct.Index index = new TableStruct.Index();
                    index.Name = dt.Rows[i][0].ToString();
                    index.Desc = dt.Rows[i][1].ToString();
                    index.Keys = dt.Rows[i][2].ToString();
                    tbl.Indexs.Add(index);
                }
            }

            //构建触发器集合
            sql = @" sp_helptrigger " + tableName;
            dt = GetDataTable(sql);
            if (dt != null && dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    TableStruct.Trigger tri = new TableStruct.Trigger();
                    tri.Name = dt.Rows[i]["trigger_name"].ToString();
                    string desc = "";
                    if (dt.Rows[i]["isupdate"].ToString() == "1")
                    {
                        desc += "update/";
                    }
                    if (dt.Rows[i]["isdelete"].ToString() == "1")
                    {
                        desc += "delete/";
                    }
                    if (dt.Rows[i]["isinsert"].ToString() == "1")
                    {
                        desc += "insert/";
                    }
                    if (dt.Rows[i]["isafter"].ToString() == "1")
                    {
                        desc += "after";
                    }
                    tri.Type = desc;
                    tbl.Triggers.Add(tri);
                }
            }
            //构建约束集合
            sql = "sp_helpconstraint @objname='" + tableName + "'";
            DataSet ds = GetDataSet(sql);
            if (ds.Tables.Count > 1 && ds.Tables[1].Rows.Count > 0)
            {

                for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                {
                    TableStruct.Constraint constraint = new TableStruct.Constraint();

                    string type = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString();
                    if (string.IsNullOrWhiteSpace(type))
                    {
                        continue;
                    }
                    if (type.StartsWith("CHECK"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "检查约束";
                        string[] arr = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString().Split(new String[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                        constraint.Keys = arr[arr.Length - 1];
                        constraint.Remark = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        tbl.Constraints.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("DEFAULT"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "默认约束";
                        string[] arr = (ds.Tables[1].Rows[i]["constraint_type"] ?? "").ToString().Split(new String[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                        constraint.Keys = arr[arr.Length - 1];
                        constraint.Remark = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        tbl.Constraints.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("FOREIGN"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "外键约束";
                        if (ds.Tables[1].Rows.Count > i)
                        {
                            constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                            constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                            constraint.DelType = (ds.Tables[1].Rows[i]["delete_action"] ?? "").ToString();
                            constraint.UpdateType = (ds.Tables[1].Rows[i]["update_action"] ?? "").ToString();

                            DataTable dt2 = GetDataTable("select delete_referential_action,update_referential_action from sys.foreign_keys where name='" + constraint.Name + "'");
                            switch (dt2.Rows[0]["delete_referential_action"].ToString())
                            {
                                case "0":
                                    {
                                        constraint.DelType = "NO ACTION";
                                        break;
                                    }
                                case "1":
                                    {
                                        constraint.DelType = "CASCADE";
                                        break;
                                    }
                                case "2":
                                    {
                                        constraint.DelType = "SET NULL";
                                        break;
                                    }
                                case "3":
                                    {
                                        constraint.DelType = "SET DEFAULT";
                                        break;
                                    }
                            }
                            switch (dt2.Rows[0]["update_referential_action"].ToString())
                            {
                                case "0":
                                    {
                                        constraint.UpdateType = "NO ACTION";
                                        break;
                                    }
                                case "1":
                                    {
                                        constraint.UpdateType = "CASCADE";
                                        break;
                                    }
                                case "2":
                                    {
                                        constraint.UpdateType = "SET NULL";
                                        break;
                                    }
                                case "3":
                                    {
                                        constraint.UpdateType = "SET DEFAULT";
                                        break;
                                    }
                            }
                            constraint.RefStr = (ds.Tables[1].Rows[i + 1]["constraint_keys"] ?? "").ToString();
                        }
                        constraint.Remark = "Update(" + constraint.UpdateType + ")," + "Delete(" + constraint.DelType + "),Ref(" + constraint.RefStr + ")";
                        tbl.Constraints.Add(constraint);
                        continue;
                    }
                    else if (type.StartsWith("PRIMARY"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "主键约束";
                        constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        tbl.Constraints.Add(constraint);
                    }
                    else if (type.StartsWith("UNIQUE"))
                    {
                        constraint.Name = (ds.Tables[1].Rows[i]["constraint_name"] ?? "").ToString();
                        constraint.Type = "唯一约束";
                        constraint.Keys = (ds.Tables[1].Rows[i]["constraint_keys"] ?? "").ToString();
                        tbl.Constraints.Add(constraint);
                    }
                }
            }
            return tbl;
        }
        #endregion

        /// <summary>获取当前数据库的用户自定义函数</summary>
        /// <returns></returns>
        public List<Func> GetFuncs()
        {
            List<Func> res = new List<Func>();
            string sql = "select 名称=name,类型=Case [TYPE] when 'FN' then '标量函数' when 'IF' then '表值函数' end ,modify_date from sys.objects where type='FN' or type='IF' order by name asc";
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Func func = new Func();
                    func.Name = dt.Rows[i]["名称"].ToString();
                    func.Type = dt.Rows[i]["类型"].ToString();
                    func.LastUpdate = dt.Rows[i]["modify_date"].ToString();
                    string sql2 = @"sp_helptext '" + func.Name + "'";
                    StringBuilder sb = new StringBuilder();
                    DataTable dt2 = GetDataTable(sql);
                    if (dt2.Rows.Count > 0)
                    {
                        for (int ii = 0; ii < dt.Rows.Count; ii++)
                        {
                            sb.AppendLine(dt.Rows[i][0].ToString());
                        }
                    }
                    func.CreateSql = sb.ToString();
                    res.Add(func);
                }
            }
            return res;
        }

        /// <summary>获取当前数据库的用户自定义存储过程</summary>
        /// <returns></returns>
        public List<Proc> GetProcs()
        {
            List<Proc> res = new List<Proc>();
            string sql = @"select 名称=ROUTINE_NAME,最近更新=LAST_ALTERED
from INFORMATION_SCHEMA.ROUTINES
where ROUTINE_TYPE='PROCEDURE'";
            DataTable dt = GetDataTable(sql);
            if (dt.Rows.Count > 0)
            {
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Proc proc = new Proc();
                    proc.Name = dt.Rows[i]["名称"].ToString();
                    proc.LastUpdate = dt.Rows[i]["最近更新"].ToString();
                    sql = @"sp_helptext '" + proc.Name + "'";
                    StringBuilder sb = new StringBuilder("");
                    DataTable dt2 = GetDataTable(sql);
                    if (dt.Rows.Count > 0)
                    {
                        for (int ii = 0; ii < dt.Rows.Count; ii++)
                        {
                            sb.AppendLine(dt.Rows[ii][0].ToString());
                        }
                        proc.CreateSql = sb.ToString();
                    }
                    res.Add(proc);
                }
            }
            return res;
        }
        #region 批量获得指定表的表结构说明 public List<TableStruct> GetTableStructs(List<string> tableNames)
        /// <summary>批量获得指定表的表结构说明</summary>
        /// <param name="tableNames">表名集合</param>
        /// <returns></returns>
        public List<TableStruct> GetTableStructs(List<string> tableNames)
        {
            List<TableStruct> res = new List<TableStruct>();
            tableNames.ForEach(i =>
            {
                res.Add(GetTableStruct(i));
            });
            return res;
        }
        #endregion

        #region 重命名指定表 public void RenameTable(string oldTableName, string newTableName)
        /// <summary>重命名指定表</summary>
        /// <param name="oldTableName">旧表名</param>
        /// <param name="newTableName">新表名</param>
        public void RenameTable(string oldTableName, string newTableName)
        {
            string sql = "EXEC   sp_rename   '" + oldTableName + "',   '" + newTableName + "'";
            ExecuteSql(sql);
        }
        #endregion

        #region 删除指定表 public void DropTable(string tableName)
        /// <summary>删除指定表</summary>
        /// <param name="tableName">要删除的表</param>
        /// <returns></returns>
        public void DropTable(string tableName)
        {
            string sql = "drop  table   " + tableName + "";
            ExecuteSql(sql);
        }
        #endregion

        #region 保存表说明,如果不存在旧的说明信息就创建否则就覆盖 public void SaveTableDesc(string tableName, string desc)
        /// <summary>保存表说明,如果不存在旧的说明信息就创建否则就覆盖</summary>
        /// <param name="tableName">表名</param>
        /// <param name="desc">说明信息</param>
        /// <returns></returns>
        public void SaveTableDesc(string tableName, string desc)
        {
            string sql = string.Format(@"SELECT count(1)
FROM fn_listextendedproperty ('MS_Description', 'schema', 'dbo', 'table', '{0}',null,null);", tableName);
            if (GetFirstColumnString(sql) == "1")
            {
                sql = string.Format(@"EXEC sp_dropextendedproperty @name=N'MS_Description', @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{0}', @level2type=null,@level2name=null", tableName);
                ExecuteSql(sql);
            }
            sql = string.Format(@"EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", desc, tableName);
            ExecuteSql(sql);
        }
        #endregion
        #region 重命名列名 public void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        /// <summary>重命名列名</summary>
        /// <param name="tableName">表名</param>
        /// <param name="oldColumnName">旧的列名</param>
        /// <param name="newColumnName">新的列名</param>
        /// <returns></returns>
        public void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            string sql = string.Format("sp_rename '{0}.{1}','{2}','column' ", tableName, oldColumnName, newColumnName);
            ExecuteSql(sql);
        }
        #endregion
        #region 删除指定表的指定列 public void DropColumn(string tableName, string columnName)
        /// <summary>删除指定表的指定列</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">要删除的列名</param>
        /// <returns></returns>
        public void DropColumn(string tableName, string columnName)
        {
            string sql = string.Format(" ALTER TABLE {0} DROP COLUMN {1}", tableName, columnName);
            ExecuteSql(sql);

        }
        #endregion
        #region 保存指定表的指定列的说明信息 public void SaveColumnDesc(string tableName, string columnName, string desc)
        /// <summary>保存指定表的指定列的说明信息</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="desc">说明信息</param>
        /// <returns></returns>
        public void SaveColumnDesc(string tableName, string columnName, string desc)
        {
            string sql = string.Format(@"SELECT count(1)
FROM fn_listextendedproperty ('MS_Description', 'schema', 'dbo', 'table', '{0}','column','{1}');", tableName, columnName);
            if (GetFirstColumnString(sql) == "1")
            {
                sql = string.Format(@"--删除列说明
  EXEC sp_dropextendedproperty @name=N'MS_Description', @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{0}', @level2type='column',@level2name='{1}'", tableName, columnName);
                ExecuteSql(sql);
            }
            sql = string.Format(@"EXECUTE sp_addextendedproperty N'MS_Description', '{0}', N'schema', N'dbo', N'table', N'{1}', N'column', N'{2}'", desc, tableName, columnName);
            ExecuteSql(sql);
        }
        #endregion
        /// <summary>改变指定表的指定列类型</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="isForce">是否暴力修改列类型,暴力修改:在正常修改不成功的情况下会删掉这个列并重建这个列,数据将会丢失</param>
        /// <returns></returns>
        public Result AlterColumnType(string tableName, string columnName, string columnType, bool isForce)
        {
            string sql = string.Format("alter table {0} alter column {1} {2}", tableName, columnName, columnType);
            try
            {
                ExecuteSql(sql);
                return new Result() { Success = true };
            }
            catch (Exception ex)
            {
                if (isForce)
                {
                    sql = string.Format("ALTER TABLE {0} DROP COLUMN {1}", tableName, columnName);
                    ExecuteSql(sql);
                    sql = string.Format("ALTER TABLE {0} ADD {1} {2}", tableName, columnName, columnType);
                    ExecuteSql(sql);
                    return new Result() { Success = true };
                }
                else
                {
                    return new Result() { Success = false, Data = ex.ToString() };
                }
            }
        }

        /// <summary>修改指定表的指定列是否可以为空</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="canNull">是否可空</param>
        /// <returns></returns>
        public void AlterColumnNullAble(string tableName, string columnName, string columnType, bool canNull)
        {
            string sql = string.Format("alter table {0} alter column {1} {2} {3}", tableName, columnName, columnType, canNull ? "null" : "not null");
            ExecuteSql(sql);
        }

        /// <summary>给指定表增加自增列</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="columnType">列类型</param>
        /// <param name="start">种子</param>
        /// <param name="end">增量</param>
        /// <returns></returns>
        public void AddIdentityColumn(string tableName, string columnName, string columnType, string start, string end)
        {
            string sql = string.Format(" alter table {0} add {1} int identity({2},{3}) ", tableName, columnName, columnType, start, end);
            ExecuteSql(sql);
        }

        /// <summary>给指定列修改默认值</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="def">默认值</param>
        /// <returns></returns>
        public void SaveColumnDefault(string tableName, string columnName, string def)
        {
            string sql = string.Format(@"SELECT name FROM sysobjects WHERE id = ( SELECT syscolumns.cdefault FROM sysobjects 
    INNER JOIN syscolumns ON sysobjects.Id=syscolumns.Id 
    WHERE sysobjects.name=N'{0}' AND syscolumns.name=N'{1}' )", tableName, columnName);
            string defname = GetFirstColumnString(sql);
            if (string.IsNullOrEmpty(def))
            {
                if (defname != "")
                {
                    sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                    ExecuteSql(sql);
                }
            }
            else
            {
                if (defname != "")
                {
                    sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                    ExecuteSql(sql);
                }
                sql = string.Format(" ALTER TABLE {0} ADD CONSTRAINT DF_gene_{0}_{1} DEFAULT ('{2}') FOR {1}", tableName, columnName, def);
                ExecuteSql(sql);
            }
        }

        /// <summary>删除指定表指定列的默认值</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="def">默认值</param>
        /// <returns></returns>
        public void DropColumnDefault(string tableName, string columnName)
        {
            string sql = string.Format(@"SELECT name FROM sysobjects WHERE id = ( SELECT syscolumns.cdefault FROM sysobjects 
    INNER JOIN syscolumns ON sysobjects.Id=syscolumns.Id 
    WHERE sysobjects.name=N'{0}' AND syscolumns.name=N'{1}' )", tableName, columnName);
            string defname = GetFirstColumnString(sql);
            if (defname != "")
            {
                sql = string.Format(" ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, defname);
                ExecuteSql(sql);
            }
        }

        /// <summary>创建新表</summary>
        /// <param name="tableStruct">表结构说明</param>
        /// <returns></returns>
        public void CreateTable(TableStruct tableStruct)
        {
            string sql = string.Format(@" create table {0} (
", tableStruct.Name);
            string sqlPri = @"
ALTER TABLE {0} ADD CONSTRAINT PK_gene_{0}_{1} PRIMARY KEY({2})";
            string priname = "";
            string prikey = "";
            string sqldesc = "";
            if (!string.IsNullOrWhiteSpace(tableStruct.Desc))
            {
                sqldesc += string.Format(@"
 EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", tableStruct.Desc, tableStruct.Name);
            }

            tableStruct.Columns.ForEach(i =>
            {
                string ideSql = "";
                string nullSql = "";
                string defSql = "";
                string uniSql = "";
                if (i.IsIdentity)
                {
                    ideSql = "identity(" + i.Start + "," + i.Incre + ")";
                }
                if (i.IsUnique)
                {
                    uniSql = "unique";
                }
                if (!i.IsNullable)
                {
                    nullSql = "not null";
                }
                if (!string.IsNullOrWhiteSpace(i.Default))
                {
                    defSql = " default '" + i.Default + "'";
                }
                if (i.IsPrimaryKey)
                {
                    priname += "_" + i.Name;
                    prikey += "," + i.Name;
                }

                sql += string.Format(@" {0} {1} {2} {3} {4} {5},
", i.Name, i.Type, nullSql, defSql, ideSql, uniSql);
                if (i.Desc != "" && i.Desc != null)
                {
                    sqldesc += string.Format(@"
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'
", i.Desc, tableStruct.Name, i.Name);
                }
            });
            priname = priname.Trim('_');
            prikey = prikey.Trim(',');
            sqlPri = string.Format(sqlPri, tableStruct.Name, priname, prikey);
            if (prikey == "")
            {
                sqlPri = "";
            }
            sql += @"
)
";
            ExecuteSql(sql);
            ExecuteSql(sqlPri);
            ExecuteSql(sqldesc);

        }

        /// <summary>给指定表添加一列</summary>
        /// <param name="tableName">表名</param>
        /// <param name="column">列名</param>
        /// <returns></returns>
        public void AddColumn(string tableName, TableStruct.Column column)
        {
            string sql = " alter table " + tableName + " add " + column.Name + " " + column.Type;
            string sqlDesc = "";
            if (!string.IsNullOrWhiteSpace(column.Desc))
            {
                sqlDesc = string.Format("EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'", column.Desc, tableName, column.Name);
            }
            if (column.IsNullable)
            {
                sql += " null";
            }
            else
            {
                sql += " not null";
            }
            if (column.IsIdentity)
            {
                sql += " identity(" + column.Start + "," + column.Incre + ")";
            }
            if (column.IsUnique)
            {
                sql += " unique";
            }
            if (!string.IsNullOrEmpty(column.Default))
            {
                if (column.Type.Contains("char") || column.Type.Contains("date") || column.Type.Contains("text"))
                {
                    sql += " default '" + column.Default + "'";
                }
                else
                {
                    sql += " default " + column.Default;
                }
            }
            ExecuteSql(sql);
            if (sqlDesc != "")
            {
                ExecuteSql(sqlDesc);
            }
        }

        /// <summary>设置指定列是否是唯一的</summary>
        /// <param name="tableName">表名</param>
        /// <param name="columnName">列名</param>
        /// <param name="canUnique">是否是唯一的</param>
        public void SaveColumnUnique(string tableName, string columnName, bool canUnique)
        {
            string sql = @"
SELECT idx.name
  FROM sys.indexes idx
  JOIN sys.index_columns idxCol ON (idx.object_id = idxCol.object_id AND
                                   idx.index_id = idxCol.index_id AND
                                   idx.is_unique_constraint = 1)
  JOIN sys.tables tab ON (idx.object_id = tab.object_id)
  JOIN sys.columns col ON (idx.object_id = col.object_id AND
                          idxCol.column_id = col.column_id)
 WHERE tab.name = 'test'
   and col.name = 'gh'";
            string constraintName = GetFirstColumnString(sql);
            if (!canUnique && constraintName != "")
            {
                //删除唯一约束
                ExecuteSql(string.Format("ALTER TABLE {0} DROP CONSTRAINT {1}", tableName, constraintName));
            }
            if (canUnique && constraintName == "")
            {
                //增加唯一约束
                ExecuteSql(string.Format("ALTER TABLE {0} ADD CONSTRAINT UQ_gene_{0}_{1} UNIQUE ({1})", tableName, columnName));
            }
        }

        /// <summary>准备数据导出的存储过程,这将重建usp_CreateInsertScript</summary>
        public void PreExportDataProc()
        {
            ExecuteSql(@"if exists (select 1  
            from  sys.procedures  
           where  name = 'usp_CreateInsertScript')  
   drop proc usp_CreateInsertScript");
            ExecuteSql(sqlExportDataProc);
        }

        /// <summary>根据表结构对象生成建表语句</summary>
        /// <param name="tableStruct"></param>
        /// <returns></returns>
        public string CreateTableSql(TableStruct tableStruct)
        {
            string sql = string.Format(@"create table {0} (
", tableStruct.Name);
            string sqlPri = @"
ALTER TABLE {0} ADD CONSTRAINT PK_gene_{0}_{1} PRIMARY KEY({2})";
            string priname = "";
            string prikey = "";
            string sqldesc = "";
            if (!string.IsNullOrWhiteSpace(tableStruct.Desc))
            {
                sqldesc += string.Format(@"
 EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=null,@level2name=null", tableStruct.Desc, tableStruct.Name);
            }

            tableStruct.Columns.ForEach(i =>
            {
                string ideSql = "";
                string nullSql = "";
                string defSql = "";
                string uniSql = "";
                ideSql = i.FinalIdentity;
                if (i.IsUnique)
                {
                    uniSql = "unique";
                }
                if (!i.IsNullable)
                {
                    nullSql = "not null";
                }
                if (!string.IsNullOrWhiteSpace(i.Default))
                {
                    defSql = " default '" + i.Default + "'";
                }
                if (i.IsPrimaryKey)
                {
                    priname += "_" + i.Name;
                    prikey += "," + i.Name;
                }

                sql += string.Format(@"    {0} {1} {2} {3} {4} {5},
", i.Name, i.FinalType, nullSql, defSql, ideSql, uniSql);
                if (i.Desc != "" && i.Desc != null)
                {
                    sqldesc += string.Format(@"
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'{0}' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'{1}', @level2type=N'COLUMN',@level2name=N'{2}'
", i.Desc, tableStruct.Name, i.Name);
                }
            });
            sql = sql.Trim('\n', '\r', ',');
            priname = priname.Trim('_');
            prikey = prikey.Trim(',');
            sqlPri = string.Format(sqlPri, tableStruct.Name, priname, prikey);
            if (prikey == "")
            {
                sqlPri = "";
            }
            sql += @"
)
";
            string res = string.Format("{0}\r\n {1}\r\n {2}", sql, sqlPri, sqldesc);

            //构建约束语句
            //六类约束:主键、非空、默认、唯一、检查、外键,这里只对后两个约束生成语句
            string sqlConstraint = "";
            tableStruct.Constraints.ForEach(i =>
            {
                if (i.Type == "检查约束")
                {
                    sqlConstraint += "\r\n--************检查约束<" + i.Name + ">*****************\r\n";
                    string tmp = string.Format("ALTER TABLE {0} ADD CONSTRAINT {1} CHECK({2})", tableStruct.Name, i.Name, i.Remark.Trim('(', ')'));
                    sqlConstraint += tmp;
                    sqlConstraint += "\r\n--************检查约束</" + i.Name + ">*****************\r\n";
                }
                else if (i.Type == "外键约束")
                {
                    sqlConstraint += "\r\n--************外键约束<" + i.Name + ">*****************\r\n";
                    string tmp = string.Format("ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY({2}) {3} ON DELETE {4} ON UPDATE {5}", tableStruct.Name, i.Name, i.Keys, i.RefStr, i.DelType, i.UpdateType);
                    sqlConstraint += tmp;
                    sqlConstraint += "\r\n--************外键约束</" + i.Name + ">*****************\r\n";
                }
            });
            //构建触发器语句
            string sqlTrigger = "";
            tableStruct.Triggers.ForEach(i =>
            {
                sqlTrigger += "\r\n--************触发器<" + i.Name + ">*****************\r\ngo\r\n";
                DataTable dt3 = GetDataTable("sp_helptext '" + i.Name + "'");
                for (int k = 0; k < dt3.Rows.Count; k++)
                {
                    sqlTrigger += dt3.Rows[k][0].ToString();
                }
                sqlTrigger += "go\r\n--************触发器</" + i.Name + ">*****************\r\ngo\r\n";
            });
            //构建索引语句
            string sqlIndex = "";
            tableStruct.Indexs.ForEach(i =>
            {
                sqlIndex += "\r\n--************索引<" + i.Name + ">*****************\r\n";
                if (i.Desc.Contains("unique"))
                {
                    sqlIndex += string.Format("CREATE UNIQUE NONCLUSTERED INDEX {0} ON test({1})", i.Name, i.Keys);
                }
                else
                {
                    sqlIndex += string.Format("CREATE NONCLUSTERED INDEX {0} ON test({1})", i.Name, i.Keys);
                }

                sqlIndex += "\r\n--************索引</" + i.Name + ">*****************\r\n";
            });
            res += string.Format("\r\n{0}\r\n{1}\r\n{2}", sqlConstraint, sqlTrigger, sqlIndex);
            return res;
        }

        /// <summary>根据视图名称生成视图建立语句</summary>
        /// <param name="viewName">视图名称</param>
        /// <returns></returns>
        public string CreateViewSql(string viewName)
        {
            StringBuilder sb = new StringBuilder();

            DataTable dt = GetDataTable("sp_helptext " + viewName);
            for (int ii = 0; ii < dt.Rows.Count; ii++)
            {
                sb.Append(dt.Rows[ii][0].ToString());
            }
            return sb.ToString();
        }

        /// <summary>根据存储过程名字生成存储过程的创建语句</summary>
        /// <param name="procName">存储过程名字</param>
        /// <returns></returns>
        public string CreateProcSql(string procName)
        {
            string sql = @"select ROUTINE_DEFINITION
from INFORMATION_SCHEMA.ROUTINES
where ROUTINE_TYPE='PROCEDURE' and ROUTINE_NAME='" + procName + "'\r\n";
            return GetFirstColumnString(sql);
        }

        /// <summary>根据函数名生成函数的创建语句</summary>
        /// <param name="funcName">函数名称</param>
        /// <returns></returns>
        public string CreateFuncSql(string funcName)
        {
            string sql = @"sp_helptext '" + funcName + "'";
            DataTable dt = GetDataTable(sql);
            StringBuilder sb = new StringBuilder();
            if (dt.Rows.Count > 0)
            {
                for (int ii = 0; ii < dt.Rows.Count; ii++)
                {
                    sb.Append(dt.Rows[ii][0].ToString());
                }
            }
            return sb.ToString();
        }

        /// <summary>根据表名称和过滤条件生成表数据的insert语句</summary>
        /// <param name="tblName">表结构</param>
        /// <param name="Count">生成的insert语句的个数</param>
        /// <param name="filter">过滤条件</param>
        /// <returns></returns>
        public string GeneInsertSql(string tblName, ref int Count, string filter = "1=1")
        {
            DataTable dt = GetDataTable("exec usp_CreateInsertScript '" + tblName + "','" + filter + "'");
            StringBuilder sb = new StringBuilder();
            sb.AppendLine();
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                sb.AppendLine(dt.Rows[i][0].ToString());
            }
            sb.AppendLine();
            Count = dt.Rows.Count;
            return sb.ToString();
        }

        #region string sqlExportDataProc 表数据导出insert语句的存储过程语句
        string sqlExportDataProc = @"
-- ============================================= 
-- Author: 胡庆杰 
-- Create date: 2017-1-10 
-- Description: 将表数据生成Insert脚本 
-- Demo : exec usp_CreateInsertScript 'SYSUSER','1=1' 
-- ============================================= 
create proc [dbo].usp_CreateInsertScript (@tablename varchar(400),@con nvarchar(1000)) 
as 
begin 
set nocount on 
declare @sqlstr varchar(max) 
declare @sqlstr1 varchar(max) 
declare @sqlstr2 varchar(max) 
select @sqlstr='select ''insert '+@tablename 
select @sqlstr1='' 
select @sqlstr2='(' 
select @sqlstr1='values (''+' 
select @sqlstr1=@sqlstr1+col+'+'',''+' ,@sqlstr2=@sqlstr2+name +',' from (select case 
when a.xtype =173 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.length*2+2)+'),'+a.name +')'+' end' 
when a.xtype =104 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(1),'+a.name +')'+' end' 
when a.xtype =175 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end' 
when a.xtype =61 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'convert(varchar(23),'+a.name +',121)'+ '+'''''''''+' end' 
when a.xtype =106 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.xprec+2)+'),'+a.name +')'+' end' 
when a.xtype =62 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(23),'+a.name +',2)'+' end' 
when a.xtype =56 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(11),'+a.name +')'+' end' 
when a.xtype =60 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(22),'+a.name +')'+' end' 
when a.xtype =239 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end' 
when a.xtype =108 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.xprec+2)+'),'+a.name +')'+' end' 
when a.xtype =231 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end' 
when a.xtype =59 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(23),'+a.name +',2)'+' end' 
when a.xtype =58 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'convert(varchar(23),'+a.name +',121)'+ '+'''''''''+' end' 
when a.xtype =52 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(12),'+a.name +')'+' end' 
when a.xtype =122 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(22),'+a.name +')'+' end' 
when a.xtype =127 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(6),'+a.name +')'+' end' 
when a.xtype =48 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar(6),'+a.name +')'+' end' 
when a.xtype =165 then 'case when '+a.name+' is null then ''NULL'' else '+'convert(varchar('+convert(varchar(4),a.length*2+2)+'),'+a.name +')'+' end' 
when a.xtype =167 then 'case when '+a.name+' is null then ''NULL'' else '+'''''''''+'+'replace('+a.name+','''''''','''''''''''')' + '+'''''''''+' end' 
else '''NULL''' 
end as col,a.colid,a.name 
from syscolumns a where a.id = object_id(@tablename) 
--
and a.name not in(SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns c where c.TABLE_NAME=@tablename and COLUMNPROPERTY(      
      OBJECT_ID(@tablename),COLUMN_NAME,'IsIdentity')=1)
--
and a.xtype <>189 and a.xtype <>34 and a.xtype <>35 and a.xtype <>36 
)t order by colid 
select @sqlstr=@sqlstr+left(@sqlstr2,len(@sqlstr2)-1)+') '+left(@sqlstr1,len(@sqlstr1)-3)+')'' from '+@tablename + ' where 1=1 and ' + isnull(@con,'') 
print @sqlstr 
exec( @sqlstr) 
set nocount off 
end

";
        #endregion
    }
}