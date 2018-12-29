using dolphindb.data;
using OrcaMDF.Core.Engine.Pages.PFS;
using OrcaMDF.Core.Engine.Records.Parsers;
using OrcaMDF.Core.MetaData;
using OrcaMDF.Core.MetaData.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OrcaMDF.Core.Engine
{
    public class DDB_DataScanner :Scanner
    {
        private List<IVector> cols = null;
        public DDB_DataScanner(Database database)
			: base(database)
		{ }

        /// <summary>
        /// Will scan any table - heap or clustered - and return an IEnumerable of generic rows with data & schema
        /// </summary>
        public BasicTable ScanTable(string tableName,int size = 1000)
        {
            var schema = MetaData.GetEmptyDataRow(tableName);
            cols = getAllColumn(schema, size);
            return scanTable(tableName, schema);
        }

        private List<IVector> getAllColumn(DataRow schema,int size)
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
        /// <summary>
        /// Will scan any table - heap or clustered - and return an IEnumerable of typed rows with data & schema
        /// </summary>
        //internal IEnumerable<TDataRow> ScanTable<TDataRow>(string tableName) where TDataRow : Row, new()
        //{
        //    //var schema = new TDataRow();

        //    //return scanTable(tableName, schema).Cast<TDataRow>();
        //}

        /// <summary>
        /// Scans a linked list of pages returning an IEnumerable of typed rows with data & schema
        /// </summary>
        internal IEnumerable<TDataRow> ScanLinkedDataPages<TDataRow>(PagePointer loc, CompressionContext compression) where TDataRow : Row, new()
        {
            return ScanLinkedDataPages(loc, new TDataRow(), compression).Cast<TDataRow>();
        }

        /// <summary>
        /// Starts at the data page (loc) and follows the NextPage pointer chain till the end.
        /// </summary>
        internal IEnumerable<Row> ScanLinkedDataPages(PagePointer loc, Row schema, CompressionContext compression)
        {
            while (loc != PagePointer.Zero)
            {
                var recordParser = RecordEntityParser.CreateEntityParserForPage(loc, compression, Database);

                foreach (var dr in recordParser.GetEntities(schema))
                    yield return dr;

                loc = recordParser.NextPage;
            }
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
            List<string> colNames = new List<string>();
            foreach(var n in schema.Columns)
            {
                colNames.Add(n.Name);
            }

            foreach (var partition in partitions)
            {
                scanPartition(partition.PartitionID, partition.PartitionNumber, schema);
            }
            BasicTable bt = new BasicTable(colNames, cols);
            return bt;

        }

        private void scanPartition(long partitionID, int partitionNumber, Row schema)
        {
            // Lookup partition
            var partition = Database.Dmvs.Partitions
                .Where(p => p.PartitionID == partitionID && p.PartitionNumber == partitionNumber)
                .SingleOrDefault();

            if (partition == null)
                throw new ArgumentException("Partition (" + partitionID + "." + partitionNumber + " does not exist.");

            // Get allocation unit for in-row data
            var au = Database.Dmvs.SystemInternalsAllocationUnits
                .Where(x => x.ContainerID == partition.PartitionID && x.Type == 1)
                .SingleOrDefault();

            if (au == null)
                throw new ArgumentException("Partition (" + partition.PartitionID + "." + partition.PartitionNumber + " has no HOBT allocation unit.");

            // Before we can scan either heaps or indices, we need to know the compression level as that's set at the partition level, and not at the record/page level.
            // We also need to know whether the partition is using vardecimals.
            var compression = new CompressionContext((CompressionLevel)partition.DataCompression, MetaData.PartitionHasVardecimalColumns(partition.PartitionID));

            // Heap tables won't have root pages, thus we can check whether a root page is defined for the HOBT allocation unit
            if (au.RootPage != PagePointer.Zero)
            {
                var Index = 0;
                foreach (var row in ScanLinkedDataPages(au.FirstPage, schema, compression))
                {
                    addRow(row, Index);
                    Index++;

                }

            }
            else
            {
                var index = 0;
                foreach (var row in scanHeap(au.FirstIamPage, schema, compression))
                {
                    addRow(row,index);
                    index++;
                }
}
        }

        private void addRow(Row row,int Pos)
        {
            BasicIntVector v = (BasicIntVector)cols[0];
            BasicStringVector v1 = (BasicStringVector)cols[1];
            v.setInt(Pos, (int)row[row.Columns[0]]);
            v1.setString(Pos, (string)row[row.Columns[1]]);
        }
        
        /// <summary>
        /// Scans a heap beginning from the provided IAM page and onwards.
        /// </summary>
        private IEnumerable<Row> scanHeap(PagePointer loc, Row schema, CompressionContext compression)
        {
            // Traverse the linked list of IAM pages untill the tail pointer is zero
            while (loc != PagePointer.Zero)
            {
                // Before scanning, check that the IAM page itself is allocated
                var pfsPage = Database.GetPfsPage(PfsPage.GetPfsPointerForPage(loc));

                // If IAM page isn't allocated, there's nothing to return
                if (!pfsPage.GetPageDescription(loc.PageID).IsAllocated)
                    yield break;

                var iamPage = Database.GetIamPage(loc);

                // Create an array with all of the header slot pointers
                var iamPageSlots = new[]
                    {
                        iamPage.Slot0,
                        iamPage.Slot1,
                        iamPage.Slot2,
                        iamPage.Slot3,
                        iamPage.Slot4,
                        iamPage.Slot5,
                        iamPage.Slot6,
                        iamPage.Slot7
                    };

                // Loop each header slot and yield the results, provided the header slot is allocated
                foreach (var slot in iamPageSlots.Where(x => x != PagePointer.Zero))
                {
                    var recordParser = RecordEntityParser.CreateEntityParserForPage(slot, compression, Database);

                    foreach (var dr in recordParser.GetEntities(schema))
                        yield return dr;
                }

                // Then loop through allocated extents and yield results
                foreach (var extent in iamPage.GetAllocatedExtents())
                {
                    // Get PFS page that tracks this extent
                    var pfs = Database.GetPfsPage(PfsPage.GetPfsPointerForPage(extent.StartPage));

                    foreach (var pageLoc in extent.GetPagePointers())
                    {
                        // Check if page is allocated according to PFS page
                        var pfsDescription = pfs.GetPageDescription(pageLoc.PageID);

                        if (!pfsDescription.IsAllocated)
                            continue;

                        var recordParser = RecordEntityParser.CreateEntityParserForPage(pageLoc, compression, Database);

                        foreach (var dr in recordParser.GetEntities(schema))
                            yield return dr;
                    }
                }

                // Update current IAM chain location to the tail pointer
                loc = iamPage.Header.NextPage;
            }
        }
    }
}
