using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
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
            Stopwatch watch = new Stopwatch();
            watch.Start();

            using (var testConnection = new SqlConnection(sourceConnectionString))
            {
                testConnection.Open();
                var command = new SqlCommand($"select * from {sourceTable};", testConnection);
                using (var reader = command.ExecuteReader())
                {
                    //using (var destinationConnection = new SqlConnection(targetConnectionString))
                    //{
                    
                    using (var bc = new SqlBulkCopy(targetConnectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.TableLock))
                    {

                        //destinationConnection.Open();
                        bc.BatchSize = 0;
                        bc.BulkCopyTimeout = 1800;
                        bc.DestinationTableName = targetTable;
                        bc.WriteToServer(reader);
                    }
                    //}
                }
            }

            watch.Stop();
            Console.WriteLine("Elapsed: " + watch.ElapsedMilliseconds);
            Console.ReadLine();

            // 1. 105420ms ~1.75min (100K batchsize)
            // 2. 84861 (default batch size)

            //WITH OrderedOrders AS
            //(
            //    SELECT SalesOrderID, OrderDate,
            //    ROW_NUMBER() OVER (ORDER BY OrderDate) AS 'RowNumber'
            //    FROM Sales.SalesOrderHeader 
            //) 
            //SELECT * 
            //FROM OrderedOrders 
            //WHERE RowNumber BETWEEN 51 AND 60;

//            SELECT*
//FROM Sales.SalesOrderHeader
//ORDER BY OrderDate
//OFFSET(@Skip) ROWS FETCH NEXT(@Take) ROWS ONLY

        }
    }

    public class BulkCopyParallel : IBulkCopy
    {
        public const int BATCH_SIZE = 500;
        public static object LOCKER = new object();
        
        public void Copy(string sourceConnectionString, string sourceTable, string targetConnectionString, string targetTable)
        {

            using (var testConnection = new SqlConnection(targetConnectionString))
            {
                testConnection.Open();
                var command = new SqlCommand($"truncate table {targetTable}", testConnection);
                command.ExecuteNonQuery();
            }

            var studentCount = 0;
            using (var testConnection = new SqlConnection(sourceConnectionString))
            {
                testConnection.Open();
                var command = new SqlCommand($"select count(*) from {sourceTable};", testConnection);
                studentCount = (int)command.ExecuteScalar();
            }

            var watch = new Stopwatch();
            watch.Start();

            int counter = 0;
            Parallel.For(0, studentCount/BATCH_SIZE, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, i =>
            {
                int skip;
                lock (LOCKER)
                {
                    skip = counter * BATCH_SIZE;
                    Interlocked.Increment(ref counter);
                }
                var done = false;
                while (!done)
                {
                    try
                    {
                        using (var testConnection = new SqlConnection(sourceConnectionString))
                        {
                            testConnection.Open();
                            var command = new SqlCommand($"select * from {sourceTable} " +
                                                         $"ORDER BY Id " +
                                                         $"OFFSET({skip}) ROWS FETCH NEXT({BATCH_SIZE}) ROWS ONLY",
                                testConnection);
                            using (var reader = command.ExecuteReader())
                            {
                                using (
                                    var bc = new SqlBulkCopy(targetConnectionString,
                                        SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction))
                                {
                                    bc.BatchSize = 0;
                                    bc.BulkCopyTimeout = 1800;
                                    bc.DestinationTableName = targetTable;
                                    bc.WriteToServer(reader);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        continue;
                    }
                    done = true;
                }
            });

            watch.Stop();
            Console.WriteLine("Elapsed: " + watch.ElapsedMilliseconds);
            Console.ReadLine();
        }
    }
}
