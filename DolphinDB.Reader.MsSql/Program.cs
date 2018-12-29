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
using System.Linq;


namespace DolphinDB.Reader.MsSql
{
    class Program
    {
        static void Main(string[] args)
        {
            //var db = new Database("F:\\BaiduYunDownload\\TushareData.mdf");
            string fileName = ConfigurationManager.AppSettings["MDF_File"];
            string tableName = ConfigurationManager.AppSettings["Table_Name"];
            
            //var db = new Database(@"G:\SQLServerDatabases\AWLT2008.mdf");
            var db = new Database(@"H:\work\DDB.mdf");
            
            //var db = new Database(@"G:\SQLServerDatabases\TushareData.mdf");
            var scanner = new DDB_DataScanner(db);
            //var rows = scanner.ScanTable("Address").Take(1000);
            Console.WriteLine("starting read data!");
            DateTime dt_st1 = DateTime.Now;
            List<IVector> cols = new List<IVector>();


            var bt = scanner.ScanTable("tb",20000000);
      
            Console.WriteLine("take over into memory!");
            DateTime dt_st2 = DateTime.Now;
            Console.WriteLine((dt_st2 - dt_st1).Seconds);
            Console.ReadLine();

            //List<String> colNames = new List<string>();
            //foreach (var col in firstRow.Columns)
            //{
            //    colNames.Add(col.Name);
            //    IVector c = getColumn(col.Type, rowCount);
            //    cols.Add(c);
            //}
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
