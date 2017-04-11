using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickEtl
{
    public interface IBulkCopy
    {
        
    }

    public class BulkCopy : IBulkCopy
    {
        public void Copy(string sourceConnectionString, string sourceTable, string targetConnectionString, string targetTable)
        {
            using (var testConnection = new SqlConnection(sourceConnectionString))
            {
                testConnection.Open();
                var command = new SqlCommand($"select * from {sourceTable};", testConnection);
                using (var reader = command.ExecuteReader())
                {
                    using (var destinationConnection = new SqlConnection(targetConnectionString))
                    {
                        using (var bc = new SqlBulkCopy(destinationConnection))
                        {
                            destinationConnection.Open();
                            bc.DestinationTableName = targetTable;
                            bc.WriteToServer(reader);
                        }
                    }
                }
            }
        }
    }
}
