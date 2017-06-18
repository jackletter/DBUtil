using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace DBUtil
{
    public class SimpleIDSNOManager : IDSNOManager
    {
        #region ID生成控制管理 根据表名和列名生成ID,第一次生成后就不需要再访问数据库
        private static System.Collections.Concurrent.ConcurrentDictionary<string, object> ht_locks = new System.Collections.Concurrent.ConcurrentDictionary<string, object>();
        private static System.Collections.Concurrent.ConcurrentDictionary<string, object> ht_ids = new System.Collections.Concurrent.ConcurrentDictionary<string, object>();
        private string GeneKey(string tableName, string colName)
        {
            return "__^^-^^__" + tableName + "__^^-^^__" + colName;
        }
        private object GetLock(string tableName, string colName)
        {
            string key = GeneKey(tableName, colName);
            object objtmp;
            if (!ht_locks.TryGetValue(key, out objtmp))
            {
                lock (typeof(IDBFactory))
                {
                    if (!ht_locks.TryGetValue(key, out objtmp))
                    {
                        ht_locks.TryAdd(key, new object());
                    }
                    return ht_locks[key];
                }
            }
            else
            {
                return objtmp;
            }
        }

        /// <summary>根据表名和列名生成ID,第一次生成后就不需要再访问数据库,频率高时使用</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <returns></returns>
        public int NewID(IDbAccess iDb, string tableName, string colName)
        {
            string key = GeneKey(tableName, colName);
            object lockobj = GetLock(tableName, colName);
            lock (lockobj)
            {
                object obj;
                if (!ht_ids.TryGetValue(key, out obj))
                {
                    string str = iDb.GetFirstColumnString(string.Format("select max({0}) from {1}", colName, tableName));
                    if (string.IsNullOrWhiteSpace(str))
                    {
                        obj = 1;
                    }
                    else
                    {
                        obj = int.Parse(str) + 1;
                    }
                    ht_ids.TryAdd(key, obj);
                }
                else
                {
                    obj = int.Parse(obj.ToString()) + 1;
                    ht_ids[key] = obj;
                }
                return int.Parse(obj.ToString());
            }

        }

        /// <summary>使用程序锁直接从表的字段里面算得递增值,频率低时使用</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <returns></returns>
        public int NewIDForce(IDbAccess iDb, string tableName, string colName)
        {
            string key = GeneKey(tableName, colName);
            object lockObj = GetLock(tableName, colName);
            lock (lockObj)
            {
                string str = iDb.GetFirstColumnString(string.Format("select max({0}) from {1}", colName, tableName));
                if (str == "")
                {
                    str = "0";
                }
                int id = int.Parse(str) + 1;
                object obj;
                if (ht_ids.TryGetValue(key, out obj))
                {
                    ht_ids[key] = id;
                }
                else
                {
                    ht_ids.TryAdd(key, id);
                }
                return id;
            }
        }

        /// <summary>重置一个表的ID</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="val">为null时直接删除这个表和这个列的ID生成控制</param>
        /// <returns></returns>
        public void ResetID(string tableName, string colName, int? val)
        {
            string key = GeneKey(tableName, colName);
            object lockobj = GetLock(tableName, colName);
            lock (lockobj)
            {
                if (val == null)
                {
                    if (ht_ids.ContainsKey(key))
                    {
                        object obj;
                        ht_ids.TryRemove(key, out obj);
                    }
                }
                else
                {
                    if (ht_ids.ContainsKey(key))
                    {
                        ht_ids[key] = val;
                    }
                    else
                    {
                        ht_ids.TryAdd(key, val);
                    }
                }
            }
        }

        /// <summary>显示当前环境下的当前ID</summary>
        /// <param name="tableName">如果指定了tableName就只显示这个表的ID控制情况</param>
        /// <param name="colName">如果指定了colName就显示这个表的这个字段的ID控制情况</param>
        /// <returns></returns>
        public List<string[]> ShowCurrentIDs(string tableName, string colName)
        {
            List<string[]> res = new List<string[]>();
            foreach (var key in ht_ids.Keys)
            {
                string id = ht_ids[key].ToString();
                string[] arr = key.ToString().Split(new string[] { "__^^-^^__" }, StringSplitOptions.RemoveEmptyEntries);
                string table = arr[0];
                string col = arr[1];
                res.Add(new string[] { table, col, id });
            }
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                for (int i = res.Count - 1; i >= 0; i--)
                {
                    if (!res[i][0].StartsWith(tableName))
                    {
                        res.RemoveAt(i);
                    }
                }
            }
            if (!string.IsNullOrWhiteSpace(colName))
            {
                for (int i = res.Count - 1; i >= 0; i--)
                {
                    if (!res[i][1].StartsWith(colName))
                    {
                        res.RemoveAt(i);
                    }
                }
            }
            return res;
        }

        /// <summary>(慎用,必须填写正确的表名和字段名,否则无法在故障修复后恢复ID控制)添加一个ID控制项,并指定初始值(默认为0,即下一个生成使用的为1)</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="val">初始化值,默认0</param>
        public void AddID(string tableName, string colName, int val = 0)
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(colName))
            {
                throw new Exception("添加ID生成控制项时,表名和列名不能为空!");
            }
            string key = GeneKey(tableName, colName);
            if (!ht_ids.ContainsKey(key))
            {
                object lockobj = GetLock(tableName, colName);
                lock (lockobj)
                {
                    if (!ht_ids.ContainsKey(key))
                    {
                        ht_ids.TryAdd(key, val);
                    }
                }
            }
        }
        #endregion

        #region 自动编号生成管理 根据表名列名以及设置的区块生成编号,第一次生成后就不需要再访问数据库
        private static System.Collections.Concurrent.ConcurrentDictionary<string, object> ht_SNO_locks = new System.Collections.Concurrent.ConcurrentDictionary<string, object>();
        private static System.Collections.Concurrent.ConcurrentDictionary<string, object> ht_SNO_nos = new System.Collections.Concurrent.ConcurrentDictionary<string, object>();

        private object GetSNOLock(string tableName, string colName, List<SerialChunk> chunks)
        {
            string key = GeneSNOKey(tableName, colName, chunks);
            object objtmp;
            if (!ht_SNO_locks.TryGetValue(key, out objtmp))
            {
                lock (typeof(IDBFactory))
                {
                    if (!ht_SNO_locks.TryGetValue(key, out objtmp))
                    {
                        ht_SNO_locks.TryAdd(key, new object());
                    }
                    return ht_SNO_locks[key];
                }
            }
            else
            {
                return objtmp;
            }
        }

        private string GeneSNOKey(string tableName, string colName, List<SerialChunk> chunks)
        {
            string res = tableName + "__^^-^^__" + colName;
            chunks.ForEach(i =>
            {
                res += "__^^-^^__" + i.Name + "__^^-^^__";
            });
            res = res.Substring(0, res.Length - 9);
            return res;
        }

        public string NewSNO(IDbAccess iDb, string tableName, string colName, List<SerialChunk> chunks)
        {
            ValiAndPreDealPara(tableName, colName, chunks);
            string key = GeneSNOKey(tableName, colName, chunks);
            object lockobj = GetSNOLock(tableName, colName, chunks);
            lock (lockobj)
            {
                DateTime now = DateTime.Now;
                object obj;
                bool b = ht_SNO_nos.TryGetValue(key, out obj);

                string cycleModel = "";//序列体循环模式
                SerialChunk Serialchunk = null;//序列块
                SerialChunk Datechunk = null; //日期循环块
                int NoStart = 0;//序列块开始索引
                int NoLen = -1;//序列块长度
                int SerialTotalLen = 0;//序列体总长度
                int DateStart = 0;//日期块开始索引
                int DateLen = 0;//日期块长度
                int max = -1;//有效的最大序列码
                //第一次循环检查
                for (int i = 0; i < chunks.Count; i++)
                {
                    var chunk = chunks[i];
                    if (chunk.Type == "SerialNo")
                    {
                        Serialchunk = chunk;
                        cycleModel = chunk.CycleModel;
                        for (int ii = 0; ii < chunks.Count; ii++)
                        {
                            var chu = chunks[ii];
                            if (chu.Type != "SerialNo")
                            {
                                NoStart += chu.Len;
                                SerialTotalLen += chu.Len;
                            }
                            else
                            {
                                if (chu.Varlen)
                                {
                                    //如果序列块变长的
                                    NoLen = -1;
                                    SerialTotalLen = -1;
                                }
                                else
                                {
                                    //序列块是定长的
                                    NoLen = chu.Len;
                                    SerialTotalLen += chu.Len;
                                }
                            }
                        }
                    }
                    else if (chunk.Type == "DateTime" && chunk.Incyle)
                    {
                        Datechunk = chunk;
                        DateLen = chunk.Len;
                        for (int ii = 0; ii < i; ii++)
                        {
                            var chu = chunks[ii];
                            DateStart += chu.Len;
                        }
                    }
                }
                if (!b)
                {
                    //缓存中没有当前类型的编号
                    string likeStr = "";//去后台查询已经存在的匹配编号时用

                    //第二次循环开始生成匹配串
                    for (int j = 0; j < chunks.Count; j++)
                    {
                        var chunk = chunks[j];
                        if (chunk.Type == "Text")
                        {
                            likeStr += chunk.TextVal;
                        }
                        else if (chunk.Type == "SerialNo")
                        {
                            if (chunk.Varlen)
                            {
                                likeStr += "%";
                            }
                            else
                            {
                                for (int i = 0; i < chunk.Len; i++)
                                {
                                    likeStr += "_";
                                }
                            }
                        }
                        else if (chunk.Type == "DateTime")
                        {
                            if (cycleModel == "none" || string.IsNullOrWhiteSpace(cycleModel))
                            {
                                //序列码不循环
                                for (int i = 0; i < chunk.Len; i++)
                                {
                                    likeStr += "_";
                                }
                            }
                            else
                            {
                                //序列码循环
                                if (!chunk.Incyle)
                                {
                                    //当前日期不在循环中
                                    for (int i = 0; i < chunk.Len; i++)
                                    {
                                        likeStr += "_";
                                    }
                                }
                                else
                                {
                                    //当前日期在循环中
                                    string str = now.ToString(chunk.DateFormate);
                                    if (str.Length != chunk.Len) { throw new Exception("格式化当前日期后的长度与制定的日期块所占长度不一致:[" + str + "][" + chunk.Len + "]"); }
                                    likeStr += now.ToString(chunk.DateFormate);
                                }
                            }
                        }
                    }

                    //根据生成的序列号匹配串去后台查找相近的编号
                    string sql = string.Format("select distinct {0} from {1} where {0} like '{2}'", colName, tableName, likeStr);
                    DataTable dt = iDb.GetDataTable(sql);

                    max = FindMaxSerialNo(cycleModel, dt, now, Serialchunk, NoStart, NoLen, Datechunk, DateStart, DateLen);
                }
                else
                {
                    //缓存中有当前类型的编号
                    DataTable dt = new DataTable();
                    dt.Columns.Add(new DataColumn());
                    DataRow row = dt.NewRow();
                    row[0] = obj.ToString();
                    dt.Rows.Add(row);
                    max = FindMaxSerialNo(cycleModel, dt, now, Serialchunk, NoStart, NoLen, Datechunk, DateStart, DateLen);
                }
                //根据已经找到的有效最大序列码进行编号的自动生成
                string res = "";
                for (int j = 0; j < chunks.Count; j++)
                {
                    var chunk = chunks[j];
                    if (chunk.Type == "Text")
                    {
                        res += chunk.TextVal;
                    }
                    else if (chunk.Type == "SerialNo")
                    {
                        string no = "";
                        if (max == -1)
                        {
                            no = chunk.Start.ToString();
                            if (!chunk.Varlen)
                            {
                                no = no.PadLeft(chunk.Len, '0');
                            }
                        }
                        else
                        {
                            no = (max + chunk.Incr).ToString();
                            if (!chunk.Varlen)
                            {
                                no = no.PadLeft(chunk.Len, '0');
                            }
                        }
                        if (!chunk.Varlen)
                        {
                            if (no.Length != chunk.Len) { throw new Exception("生成的序列码和指定的序列码的长度不一致[" + no.ToString() + "][" + chunk.Len + "]!"); }
                        }
                        res += no;
                    }
                    else if (chunk.Type == "DateTime")
                    {
                        string dtstr = now.ToString(chunk.DateFormate);
                        if (dtstr.Length != chunk.Len) { throw new Exception("生成的日期块的长度与指定的长度不一致[" + dtstr + "][" + chunk.Len + "]!"); }
                        res += dtstr;
                    }
                }
                if (ht_SNO_nos.TryGetValue(key, out obj))
                {
                    ht_SNO_nos[key] = res;
                }
                else
                {
                    ht_SNO_nos.TryAdd(key, res);
                }
                return res;
            }
        }

        private int FindMaxSerialNo(string cycleModel, DataTable dt, DateTime now, SerialChunk Serialchunk, int NoStart, int NoLen, SerialChunk Datechunk, int DateStart, int DateLen)
        {
            int max = -1;
            //找出最大的序列码
            if (dt.Rows.Count > 0)
            {
                //从已存在的记录中找出有效的最大序列码
                if (cycleModel == "none" || string.IsNullOrWhiteSpace(cycleModel))
                {
                    //序列码不循环时
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        string str = dt.Rows[i][0].ToString();
                        try
                        {
                            int cu;
                            if (Serialchunk.Varlen)
                            {
                                cu = int.Parse(str.Substring(NoStart, str.Length - NoStart));
                            }
                            else
                            {
                                cu = int.Parse(str.Substring(NoStart, NoLen));
                            }
                            max = max > cu ? max : cu;
                        }
                        catch
                        {
                            continue;
                        }
                    }
                    return max;
                }
                string format = "";
                switch (cycleModel)
                {
                    case "day":
                        {
                            format = "yyyyMMdd";
                            break;
                        }
                    case "month":
                        {
                            format = "yyyyMM";
                            break;
                        }
                    case "year":
                        {
                            format = "yyyy";
                            break;
                        }
                    case "hour":
                        {
                            format = "yyyyMMddHH";
                            break;
                        }
                    case "minute":
                        {
                            format = "yyyyMMddHHmm";
                            break;
                        }
                }
                //序列码循环时
                DateTime dtcp = DateTime.ParseExact(now.ToString(format), format, null);
                DateTime dttmp;
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string str = dt.Rows[i][0].ToString();
                    try
                    {
                        dttmp = DateTime.ParseExact(DateTime.ParseExact(str.Substring(DateStart, DateLen), Datechunk.DateFormate, null).ToString(format), format, null);
                        if (dttmp == dtcp)
                        {
                            int cu;
                            if (Serialchunk.Varlen)
                            {
                                cu = int.Parse(str.Substring(NoStart, str.Length - NoStart));
                            }
                            else
                            {
                                cu = int.Parse(str.Substring(NoStart, NoLen));
                            }
                            max = max > cu ? max : cu;
                        }
                    }
                    catch
                    {
                        continue;
                    }
                }
            }
            return max;
        }

        //验证和预处理参数
        private void ValiAndPreDealPara(string tableName, string colName, List<SerialChunk> chunks)
        {
            if (chunks == null || chunks.Count == 0) { throw new Exception("必须存在用于编号控制的chunk"); }
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(colName)) { throw new Exception("表名或列名不能为空"); }
            int serialNoCount = 0;
            int dateincycleCount = 0;
            for (int i = 0; i < chunks.Count; i++)
            {
                var chunk = chunks[i];
                if (string.IsNullOrWhiteSpace(chunk.Name)) { throw new Exception("用于编号生成的每个chunk的name不能为空"); }
                if (string.IsNullOrWhiteSpace(chunk.FormatStr)) { throw new Exception("用于编号生成的每个chunk的FormatStr属性不能为空"); }
                chunk.PraseSelf();//进行参数解析
                if (chunk.Type == "SerialNo")
                {
                    serialNoCount++;
                    if (serialNoCount > 1) { throw new Exception("编号体中必须有且只有一个序列块!"); }
                    if (chunk.Varlen && i != chunks.Count - 1) { throw new Exception("当编号体中的序列块为变长时,这个序列块必须放在编号体的最后!"); }
                    if (chunk.CycleModel != "none" && !string.IsNullOrWhiteSpace(chunk.CycleModel))
                    {
                        var chun = chunks.FirstOrDefault<SerialChunk>(item =>
                        {
                            return item.Type == "DateTime" && item.Incyle;
                        });
                        if (chun == null) { throw new Exception("当编号体中序列块为循环状态时,必须存在一个与之对应的DateTime块,并且这个DateTime块在循环中!"); }
                    }
                }
                else if (chunk.Type == "DateTime" && chunk.Incyle)
                {
                    dateincycleCount++;
                    if (dateincycleCount > 1) { throw new Exception("编号体中只可以存在一个属于循环的DateTime块!"); }
                }
            }
            if (serialNoCount != 1) { throw new Exception("编号体中必须有且只有一个序列块!"); }
        }


        /// <summary>重置一个序列号控制项的当前编号</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="trunks">chunk集合(这里的每个chunk只要求Name属性不为空即可)</param>
        /// <returns></returns>
        public void ResetSNO(string tableName, string colName, List<SerialChunk> trunks, string sno)
        {
            string key = GeneSNOKey(tableName, colName, trunks);
            object lockobj = GetSNOLock(tableName, colName, trunks);
            lock (lockobj)
            {
                if (string.IsNullOrWhiteSpace(sno))
                {
                    if (ht_SNO_nos.ContainsKey(key))
                    {
                        object obj;
                        ht_SNO_nos.TryRemove(key, out obj);
                    }
                }
                else
                {
                    ht_SNO_nos[key] = sno;
                }
            }
        }

        /// <summary>显示当前环境下的当前序列号情况</summary>
        /// <param name="tableName">如果指定了tableName就只显示这个表的序列号控制情况</param>
        /// <param name="colName">如果指定了colName就显示这个表的这个字段的序列号控制情况</param>
        /// <param name="trunks">如果指定了trunks就显示当前格式控制下的序列号情况(每个trunk只要求Name属性)</param>
        /// <returns></returns>
        public List<string[]> ShowCurrentSNOs(string tableName, string colName, List<SerialChunk> trunks)
        {
            List<string[]> res = new List<string[]>();
            foreach (var key in ht_SNO_nos.Keys)
            {
                string sno = ht_SNO_nos[key].ToString();
                string[] arr = key.ToString().Split(new string[] { "__^^-^^__" }, StringSplitOptions.RemoveEmptyEntries);
                string table = arr[0];
                string col = arr[1];
                string trunkStr = "";
                for (int i = 2; i < arr.Length; i++)
                {
                    trunkStr += (arr[i] + ",");
                }
                trunkStr = trunkStr.Trim(',');
                res.Add(new string[] { table, col, trunkStr, sno });
            }
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                for (int i = res.Count - 1; i >= 0; i--)
                {
                    if (!res[i][0].StartsWith(tableName))
                    {
                        res.RemoveAt(i);
                    }
                }
            }
            if (!string.IsNullOrWhiteSpace(colName))
            {
                for (int i = res.Count - 1; i >= 0; i--)
                {
                    if (!res[i][1].StartsWith(colName))
                    {
                        res.RemoveAt(i);
                    }
                }
            }
            if (trunks != null && trunks.Count > 0)
            {
                string trunkStr = "";
                for (int i = 0; i < trunks.Count; i++)
                {
                    trunkStr += (trunks[i] + ",");
                }
                trunkStr = trunkStr.Trim(',');
                for (int i = res.Count - 1; i >= 0; i--)
                {
                    if (!res[i][2].StartsWith(trunkStr))
                    {
                        res.RemoveAt(i);
                    }
                }
            }
            return res;
        }
        #endregion
    }
}
