using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

/**
 * oracle为vs提供的访问组件,下面是下载的地址
 * http://www.oracle.com/technetwork/topics/dotnet/whatsnew/vs2012welcome-1835382.html
 * .net 4.5下可以连接,但是.net4.0下连接报错:其他信息: 未在本地计算机上注册“MSDAORA”提供程序。
 * 解决办法是将.net4.0版本工程属性的生成里的目标平台调整为x86
 * 但它访问数据库仍然需要oracle客户端安装配置tnsnames.ora
 * 
 * 
 * 
 * */
namespace DBUtil
{

    class Test
    {
        static string connstr = "Provider=MSDAORA;Data Source=ORCLmyvm;Password=123;User ID=scott;";
        static string dbtype = "ORACLE";
        static IDbAccess iDb = IDBFactory.CreateIDB(connstr, dbtype);
        //特权身份连接字符串还不会写
        //static string privilegeStr = "Provider=MSDAORA;Data Source=ORCLmyvm;Password=sys123;User ID=sys;DBA Privilege=SYSDBA;";        
        public static void Main()
        {

            string res = iDb.GetFirstColumnString("select 3 from dual");
            Console.WriteLine(res);
            Console.WriteLine("ok");
            Console.ReadLine();
        }

        public static void Test1()
        {
            iDb.ExecuteSql(@"create table ");
        }
    }

}
