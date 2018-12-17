# DBUtil
.net 下常用的数据库访问工具,支持sqlserver2008、oracle、mysql、postgresql、sqlite、access,net framework4.5
# 使用说明
http://blog.csdn.net/u010476739/article/details/54882950

# 下载编译
1. git clone https://github.com/jackletter/DBUtil.git
2. 使用vs2017打开DBUtil.sln
3. 重新生成解决方案即可

如果是操作sqlite注意观察是否输出了SQLite.Interop.dll,如果没有的话
  1.直接手动拷贝DBUtil\packages\System.Data.SQLite.Core.1.0.109.2\build\net45\x64\SQLite.Interop.dll到debug目录即可
或者
  2.编辑生成管理器，先将Any CPU 改为x64再改为 Any CPU即可
