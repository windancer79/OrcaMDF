using dolphindb.data;
using OrcaMDF.Core.Engine;
using OrcaMDF.Core.MetaData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    public class MDFReader :Scanner
    {
        public MDFReader(Database database)
			: base(database)
		{ }

        public BasicTable ScanTable(string tableName)
        {
            var schema = MetaData.GetEmptyDataRow(tableName);

            return scanTable(tableName, schema);
        }

        private BasicTable scanTable(string tableName, Row schema)
        {
            // Get object
            var tableObject = Database.BaseTables.sysschobjs
                .Where(x => x.name == tableName)
                .Where(x => x.type.Trim() == ObjectType.INTERNAL_TABLE || x.type.Trim() == ObjectType.SYSTEM_TABLE || x.type.Trim() == ObjectType.USER_TABLE)
                .SingleOrDefault();

            if (tableObject == null)
                throw new ArgumentException("Table does not exist.");

            // Get rowset, prefer clustered index if exists
            var partitions = Database.Dmvs.Partitions
                .Where(x => x.ObjectID == tableObject.id && x.IndexID <= 1)
                .OrderBy(x => x.PartitionNumber);

            if (partitions.Count() == 0)
                throw new ArgumentException("Table has no partitions.");

            // Loop all partitions and return results one by one
            return partitions.SelectMany(partition => scanPartition(partition.PartitionID, partition.PartitionNumber, schema));
        }
    }
}
