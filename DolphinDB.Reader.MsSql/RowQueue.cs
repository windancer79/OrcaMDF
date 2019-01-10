using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DolphinDB.Reader.MsSql
{
    public class RowQueue
    {
        public static BlockingCollection<OrcaMDF.Core.MetaData.Row> queue = new BlockingCollection<OrcaMDF.Core.MetaData.Row>();

    }
}
