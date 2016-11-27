using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public class SearchResult<T>
    {
        public int Count { set; get; }
        public List<T> DataList = new List<T>();
    }
}
