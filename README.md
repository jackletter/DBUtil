#最新版本请移步：https://gitee.com/jackletter/DBUtil

# DBUtil
.net 下常用的数据库访问工具,支持sqlserver、oracle、mysql、postgresql、sqlite、access
运行平台：net framework4.5
## 另外：.netcore版本： https://github.com/jackletter/DBUtil.Standard
# 使用说明

1. 安装依赖：
```shell
Install-Package DBUtil
```
2. 创建数据库操作对象
```c#
DBUtil.IDbAccess iDb = DBUtil.IDBFactory.CreateIDB("Data Source=.;Initial Catalog=JACKOA;User ID=sa;Password=xx;","SQLSERVER");
```
3. 查询
```c#
String str=iDb.GetFirstColumnString("select Name from SysUser");
DataTable dt = iDb.GetDataTable("select * from test2");
DataSet ds = iDb.GetDataSet("select * from test2;select * from test2;"); 
```
4. 分页查询
```c#
DBUtil.IDbAccess iDb = DBUtil.IDBFactory.CreateIDB(@"Data Source=localhost;Initial Catalog=imgserver2;User ID=root;Password=123456;", "MYSQL");
string selectSql = "select * from test2";
string orderSql = "order by id desc";
int pageSize = 10;
int pageIndex = 1;
string sqlFinal = iDb.GetSqlForPageSize(selectSql, orderSql, pageSize, pageIndex);
Console.WriteLine(sqlFinal);//select * from test2 order by id desc limit 0,10
```
5. 参数化sql
```c#
DataTable dt = iDb.GetDataTable(string.Format("select * from test2 where name like {0}", iDb.paraPrefix + "name"), new IDbDataParameter[] {
      iDb.CreatePara("name","%小%")
 });
```
6. ID生成
```c#
int id = iDb.IDSNOManager.NewID(iDb, "test", "id");
```




