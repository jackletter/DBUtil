using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public class AccessIDbAccess:OleIDbBase
    {
        public override string GetSqlForPageSize(string selectSql, string strOrder, int PageSize, int PageIndex)
        {
            string sql = string.Format("select * from (select *,ROW_NUMBER() OVER({0}) as RNO__ from ({1}) as inner__ ) outer__ WHERE outer__.RNO__ BETWEEN ({2}*{3}+1) AND ({4}*{5})", strOrder, selectSql, PageIndex - 1, PageSize, PageIndex, PageSize);
            return sql;
        }
    }
}
