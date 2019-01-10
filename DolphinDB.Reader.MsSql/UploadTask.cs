using dolphindb;
using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    public class UploadTask
    {
        public static Task getTask()
        {
            return new Task(() =>
            {
                DBConnection conn = new DBConnection();
                conn.connect("115.239.209.223", 8951, "admin", "123456");

                var variable = new Dictionary<string, IEntity>() ;
                variable.Add("table1", Program.bt);
                conn.upload(variable);
                conn.run("share table1 as sql_table");
                //DateTime dt_end2 = DateTime.Now;

                //TimeSpan readtime = dt_end1 - dt_st1;
                //TimeSpan uploadtime = dt_end2 - dt_end1;

                //Console.WriteLine("read cost : ");
                //Console.WriteLine(readtime.TotalSeconds);
                //Console.WriteLine("upload cost : ");
                //Console.WriteLine(uploadtime.TotalSeconds);
                //Console.ReadLine();
            });
        }
    }
}
