using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBUtil
{
    public interface IDSNOManager
    {
        /// <summary>根据表名和列名生成ID,第一次生成后就不需要再访问数据库,频率高时使用</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <returns></returns>
        int NewID(IDbAccess iDb, string tableName, string colName);

        /// <summary>使用程序锁直接从表的字段里面算得递增值,频率低时使用</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <returns></returns>
        int NewIDForce(IDbAccess iDb, string tableName, string colName);

        /// <summary>重置一个表的ID</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="val">为null时直接删除这个表和这个列的ID生成控制</param>
        /// <returns></returns>
        void ResetID(string tableName, string colName, int? val);

        /// <summary>显示当前环境下的当前ID</summary>
        /// <param name="tableName">如果指定了tableName就只显示这个表的ID控制情况</param>
        /// <param name="colName">如果指定了colName就显示这个表的这个字段的ID控制情况</param>
        /// <returns></returns>
        List<string[]> ShowCurrentIDs(string tableName, string colName);

        /// <summary>(慎用,必须填写正确的表名和字段名,否则无法在故障修复后恢复ID控制)添加一个ID控制项,并指定初始值(默认为0,即下一个生成使用的为1)</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="val">初始化值,默认0</param>
        void AddID(string tableName, string colName, int val = 0);


        /// <summary>
        /// 根据表名列名和格式块创建新的自动编号
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="chunks">格式块,格式块的格式参照SerialTrunk的构造函数</param>
        /// <returns></returns>
        string NewSNO(IDbAccess iDb, string tableName, string colName, List<SerialChunk> chunks);

        /// <summary>重置一个序列号控制项的当前编号</summary>
        /// <param name="tableName">表名</param>
        /// <param name="colName">列名</param>
        /// <param name="trunks">chunk集合(这里的每个chunk只要求Name属性不为空即可)</param>
        /// <returns></returns>
        void ResetSNO(string tableName, string colName, List<SerialChunk> trunks, string sno);

        /// <summary>显示当前环境下的当前序列号情况</summary>
        /// <param name="tableName">如果指定了tableName就只显示这个表的序列号控制情况</param>
        /// <param name="colName">如果指定了colName就显示这个表的这个字段的序列号控制情况</param>
        /// <param name="trunks">如果指定了trunks就显示当前格式控制下的序列号情况(每个trunk只要求Name属性)</param>
        /// <returns></returns>
        List<string[]> ShowCurrentSNOs(string tableName, string colName, List<SerialChunk> trunks);


    }
}
