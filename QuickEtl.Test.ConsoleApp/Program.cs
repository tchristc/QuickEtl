using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickEtl.Test.ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //var class1 = new Class1();
            //class1.Start();
            //"data source=SQLDWV02;integrated security=True;MultipleActiveResultSets=True;application name=Ods_Validation"
            var copy = new BulkCopyParallel();
            copy.Copy("data source=SQLSWV06;initial catalog=EdFi_Api_2017;integrated security=True;application name=SqlBulkCopy", 
                "edfi.Student", 
                "data source=(local);initial catalog=Toms_Ods;integrated security=True;application name=SqlBulkCopy", 
                "dbo.Student");

        }
    }
}
