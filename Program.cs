using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace system_of_record_transformation
{
    class Program
    {
        static int Main(string[] args)
        {
            //accepts SSIS Execute Process Task argument
            if (args.Length <= 0)
            {
                Console.WriteLine("No Container entered.");
                Console.ReadKey();
                return 0;
            }

            try
            {
                var paramContainerName = args[0];
                //var paramContainerName = "PA_PurchasePartSupplier";
                var busLogic = new BusinessLogic();
                var writeBack = busLogic.DeDupeMergeDataTable(paramContainerName);

                var dataService = new DataService();
                dataService.UpdateMergeTable(paramContainerName, writeBack);

                return 1;
            }

            catch (Exception e)
            {
                string method = $"Error Method: {e.StackTrace}";
                string message = $"Error Message: {e.Message}";
                Console.WriteLine(method);
                Console.WriteLine(message);
                Console.ReadLine();

                return -1;
            };
        }
    }
}