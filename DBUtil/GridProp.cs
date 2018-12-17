using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel.DataAnnotations;

namespace DBUtil
{
    public class GridProp
    {
        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public bool IsPage { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string PageSize { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string PageIndex { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string FilterStr { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string OrderStr { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string OrderColumn { set; get; }

        [DisplayFormat(ConvertEmptyStringToNull = false)]
        public string OrderType { set; get; }
    }
}
