using dolphindb.data;
using OrcaMDF.Core.MetaData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    public class ParserTask
    {
        public static Task getTask()
        {
            return new Task(() => {
                int i = 0;
                
                while (true)
                {
                    if (i == 20000000) break;
                    Row row;
                   
                    if (RowQueue.queue.TryTake(out row))
                    {
                        BasicIntVector v1 = (BasicIntVector)Program.bt.getColumn(0);
                        BasicStringVector v2 = (BasicStringVector)  Program.bt.getColumn(1);
                        v1.setInt(i, (int)row["aid"]);
                        v2.setString(i, (string)row["namec"]);
                        i++;
                    }
                    if(i%100000 == 0&&i>0)
                    {
                        Console.WriteLine("100000 rows parsed! left " + RowQueue.queue.Count().ToString());
                    }
                 }
            });
        }


        private static List<IVector> getAllColumn(DataRow schema, int size)
        {
            List<IVector> lst = new List<IVector>();
            foreach (var col in schema.Columns)
            {
                switch (col.Type)
                {
                    case ColumnType.BigInt:
                        lst.Add(new BasicLongVector(size));
                        break;
                    case ColumnType.Binary:
                        break;
                    case ColumnType.Bit:
                        lst.Add(new BasicByteVector(size));
                        break;
                    case ColumnType.Char:
                        lst.Add(new BasicByteVector(size));
                        break;
                    case ColumnType.DateTime:
                        lst.Add(new BasicDateTimeVector(size));
                        break;
                    case ColumnType.Decimal:
                        lst.Add(new BasicDoubleVector(size));
                        break;
                    case ColumnType.Image:
                        break;
                    case ColumnType.Int:
                        lst.Add(new BasicIntVector(size));
                        break;
                    case ColumnType.Money:
                        break;
                    case ColumnType.NChar:
                        break;
                    case ColumnType.NText:
                        lst.Add(new BasicStringVector(size));
                        break;
                    case ColumnType.NVarchar:
                        lst.Add(new BasicStringVector(size));
                        break;
                    case ColumnType.RID:
                        break;
                    case ColumnType.SmallDatetime:
                        break;
                    case ColumnType.SmallInt:
                        lst.Add(new BasicShortVector(size));
                        break;
                    case ColumnType.SmallMoney:
                        break;
                    case ColumnType.Text:
                        lst.Add(new BasicStringVector(size));
                        break;
                    case ColumnType.TinyInt:
                        lst.Add(new BasicShortVector(size));
                        break;
                    case ColumnType.UniqueIdentifier:
                        break;
                    case ColumnType.Uniquifier:
                        break;
                    case ColumnType.VarBinary:
                        break;
                    case ColumnType.Varchar:
                        lst.Add(new BasicStringVector(size));
                        break;
                    case ColumnType.Variant:
                        break;
                    default:
                        lst.Add(new BasicStringVector(size));
                        break;
                }
            }
            return lst;
        }
    }
}
