using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public class TableStruct
    {
        public string Name { set; get; }
        public string Desc { set; get; }
        public string PrimaryKey { set; get; }
        public List<Column> Columns = new List<Column>();
        public class Column
        {
            public string Name { set; get; }
            public string Type { set; get; }
            public string Desc { set; get; }

            public bool IsIdentity { set; get; }

            public bool IsNullable { set; get; }

            public bool IsPrimaryKey { set; get; }

            public string Default { set; get; }

            public int MaxLength { set; get; }

            public int Start { set; get; }

            public int Incre { set; get; }

            public bool IsUnique { set; get; }
        }
    }
}
