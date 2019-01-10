using OrcaMDF.Core.Engine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    /// <summary>
    /// read all row data and keep putting to blockingCollection
    /// </summary>
    public class ReadTask
    {
        public static Task getTask()
        {
            return new Task(() =>
            {
                var db = new Database(@"C:\work\DDB.mdf");
                var scanner = new DataScanner(db);
                var rows = scanner.ScanTable("stu");
                DateTime st = DateTime.Now;
                Console.WriteLine("take all! " + (DateTime.Now - st).Milliseconds);
                int i = 0;
                foreach (var row in rows)
                {
                    i++;
                    if (i % 100000 == 0 && i > 0)
                    {
                        Console.WriteLine("read 100000!" + i.ToString());
                    }
                    //RowQueue.queue.Add(row);
                }
            }
            );
        }
    }
}
