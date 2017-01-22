using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using Oracle.DataAccess.Client;

namespace DBUtil
{
    public class OracleIDbAccess : IDbAccess
    {
        public IDSNOManager IDSNOManager { get { return IDBFactory.IDSNOManage; } }
        public bool IsKeepConnect { set; get; }
        public IDbTransaction tran { set; get; }
        public string ConnectionString { get; set; }
        public IDbConnection conn { set; get; }
        public DataBaseType DataBaseType { get; set; }

        public bool IsOpen { set; get; }

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

        public bool IsTran { set; get; }

        /// <summary>当前数据库使用的参数的前缀符号</summary>
        public string paraPrefix { get { return ":"; } }

        #region 创建参数 public IDbDataParameter CreatePara()
        /// <summary>
        /// 创建参数
        /// </summary>
        /// <returns></returns>
        public IDbDataParameter CreatePara()
        {
            return new OracleParameter();
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
                    res += " and " + dateColumn + ">=to_date('" + minDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                else
                {
                    res += " and " + dateColumn + ">to_date('" + minDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                if (isMaxInclude)
                {
                    res += " and " + dateColumn + "<=to_date('" + maxDate + "','yyyy-MM-dd HH24:mi:ss')";
                }
                else
                {
                    res += " and " + dateColumn + "<to_date('" + maxDate + "','yyyy-MM-dd HH24:mi:ss')";
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
                OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
                OracleCommand cmd = new OracleCommand();
                cmd.Connection = (OracleConnection)conn;
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
                OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
                if (IsTran)
                {
                    cmd.Transaction = (OracleTransaction)tran;
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
                insertTableValues += ":" + item.Key.ToString() + ",";
                paras.Add(new OracleParameter()
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
                    sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    paras.Add(new OracleParameter()
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
                    sql += " " + item.Key.ToString() + "=:" + item.Key.ToString() + ",";
                    paras.Add(new OracleParameter()
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
                    sql += " " + item.Key.ToString() + ":" + item.Key.ToString() + ",";
                    IDbDataParameter para = new OracleParameter()
                    {
                        ParameterName = item.Key.ToString(),
                        Value = item.Value
                    };
                    if (!ContainsDBParameter(paras, para))
                    {
                        paras.Add(para);
                    }
                }
            }
            sql = sql.TrimEnd(new char[] { ',' });
            sql += " where 1=1 ";
            foreach (var item in keys)
            {
                sql += " and " + item + "=:" + item;
                paras.Add(new OracleParameter()
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

            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            OracleDataAdapter adp = new OracleDataAdapter(cmd);
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
            OracleCommand cmd = new OracleCommand(strSql, (OracleConnection)conn);
            cmd.Parameters.AddRange(paraArr);
            if (IsTran)
            {
                cmd.Transaction = (OracleTransaction)tran;
            }
            if (!IsOpen)
            {
                conn.Open();
                IsOpen = true;
            }
            OracleDataAdapter adp = new OracleDataAdapter(cmd);
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
            string sql = string.Format("select count(1) from user_tab_cols t where t.table_name='{0}' and t.column_name='{1}'", tableName, columnName);
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
            string sql = string.Format("select count(1) from user_tables t where t.TABLE_NAME ='{0}'", tableName);
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
            throw new NotImplementedException("不建议使用这个分页,请选择其他的分页!");
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
            string sql = string.Format(@"select * from (select inner__.*,ROWNUM RNO__ from({0} {1}) inner__) outer__ where outer__.RNO__ between {2} and {3} ", selectSql, strOrder, (PageIndex - 1) * PageSize + 1, PageSize * PageIndex);
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
            DataSet ds = GetDataSet("select TABLE_NAME from user_tables");
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
    }
}