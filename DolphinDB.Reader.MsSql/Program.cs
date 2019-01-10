using dolphindb;
using dolphindb.data;
using OrcaMDF.Core.Engine;
using OrcaMDF.RawCore;
using OrcaMDF.RawCore.Records;
using OrcaMDF.RawCore.Types;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    class Program
    {
        public static BasicTable bt = null;
        static void Main(string[] args)
        {
            var db = new OrcaMDF.Core.Engine.Database(@"C:\work\DDB.mdf");
            var scanner = new DDB_DataScanner(db);
            var rows = scanner.ScanTable("stu");

            List<String> colNames = new List<string>();
            colNames.Add("aid");
            colNames.Add("namec");
            int rowSize = 20000000;
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(rowSize));
            cols.Add(new BasicStringVector(rowSize));
            bt = new BasicTable(colNames, cols);
            Console.WriteLine("prepared basicTable ");
            //int rowIndex = 0;
            //foreach (var scannedRow in tb)
            //{
            //    for (int colIndex = 0;colIndex < cols.Count;colIndex++)
            //    {
            //        IVector v = cols[colIndex];
            //        switch (v.getDataType())
            //        {
            //            case DATA_TYPE.DT_INT:
            //                v.set(rowIndex, new BasicInt((int)scannedRow[scannedRow.Columns[colIndex]]));
            //                break;
            //            case DATA_TYPE.DT_STRING:
            //                v.set(rowIndex, new BasicString((string)scannedRow[scannedRow.Columns[colIndex]]));
            //                break;
            //        }
            //    }
            //    rowIndex++;
            //}
            //BasicTable bt = new BasicTable(colNames, cols);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //人工确定size
            Task t1 = ReadTask.getTask();
            t1.Start();
            Task t2 = ParserTask.getTask();
            t2.Start();
            Task.WaitAll(t1, t2);
            sw.Stop();
            Console.WriteLine("read and parse cost: " + sw.ElapsedMilliseconds);
            sw.Reset();
            sw.Restart();
            Task t3 = UploadTask.getTask();
            t3.Start();
            Task.WaitAll(t3);
            sw.Stop();
            Console.WriteLine("upload cost: " + sw.ElapsedMilliseconds);
            Console.ReadLine();
            //Console.WriteLine("starting upload data!");
            //DateTime dt_end1 = DateTime.Now;

            //DBConnection conn = new DBConnection();
            //conn.connect("192.168.1.135", 8981, "admin", "123456");

            //var variable = new Dictionary<string, IEntity>() ;
            //variable.Add("table1", bt);
            //conn.upload(variable);
            //conn.run("share table1 as sql_table");
            //DateTime dt_end2 = DateTime.Now;

            //TimeSpan readtime = dt_end1 - dt_st1;
            //TimeSpan uploadtime = dt_end2 - dt_end1;

            //Console.WriteLine("read cost : ");
            //Console.WriteLine(readtime.TotalSeconds);
            //Console.WriteLine("upload cost : ");
            //Console.WriteLine(uploadtime.TotalSeconds);
            //Console.ReadLine();
        }

        static IVector getColumn(OrcaMDF.Core.MetaData.ColumnType colType,int count)
        {
            switch (colType)
            {
                case OrcaMDF.Core.MetaData.ColumnType.Int:
                    return new BasicIntVector(count);
                    break;
                default:
                    return new BasicStringVector(count);
                    break;
            }
        }
    }
}
