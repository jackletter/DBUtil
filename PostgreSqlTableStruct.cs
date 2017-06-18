using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBUtil
{
    public class PostgreSqlTableStruct : TableStruct
    {
        public List<Column> Columns = new List<Column>();
        public List<Constraint> Constraints = new List<Constraint>();
        public List<Trigger> Triggers = new List<Trigger>();
        public List<Index> Indexs = new List<Index>();
        public class Column
        {
            public string Name { set; get; }
            public string Type { set; get; }
            public string Desc { set; get; }

            public bool IsNullable { set; get; }

            public int MaxLength { set; get; }

            public string Default { set; get; }

            public bool HasDefault { get; set; }

            public bool IsUnique { set; get; }

            public bool IsUniqueUion { get; set; }
            public String UniqueCols { get; set; }
        }

        public class Constraint
        {
            public string Name { set; get; }
            public string Type { set; get; }
            public string DelType { set; get; }
            public string UpdateType { set; get; }
            public string Keys { set; get; }
            public string RefStr { set; get; }
            public string Remark { set; get; }
        }

        public class Trigger
        {
            public string Name { set; get; }
            public string Type { set; get; }
        }

        public class Index
        {
            public string Name { set; get; }
            public string Desc { set; get; }
            public string Keys { set; get; }
        }
    }
}
