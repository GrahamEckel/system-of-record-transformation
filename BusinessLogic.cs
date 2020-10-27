using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Collections;

namespace system_of_record_transformation
{
    public class BusinessLogic
    {
        /// <summary>
        /// "Database diagram" dict of Table : Keys 
        /// </summary>
        static Dictionary<string, List<string>> ContainerKeys = new Dictionary<string, List<string>>
        {
            {"TableName1", new List<string> {"Key"}},
            {"TableName2", new List<string> {"Key1", "Key2"}},
            {"TableNameN", new List<string> {"Key1", "Key2", "KeyN"}},
        };

        /// <summary>
        /// Creates list of column headers to loop through while updating Hierarchy column
        /// "primes the engine"
        /// </summary>
        /// <returns></returns>
        public List<string> ColumnIterator(string ContainerName)
        {
            var dataService = new DataService();
            List<string> ColumnIterator = dataService.GetMergeDataWithKeys(ContainerName).Columns.Cast<DataColumn>().Select(x => x.ColumnName).ToList();

            List<string> Column100 = new List<string>();
            var SoRData = dataService.GetSoRData(ContainerName);
            foreach (var l in SoRData)
            {
                if (l.Hierarchy == 100 && !Column100.Contains(l.FieldName))
                {
                    Column100.Add(l.FieldName);
                }
            }

            foreach (var col in Column100)
            {
                if (ColumnIterator.Contains(col))
                {
                    ColumnIterator.Remove(col);
                }
            }

            var Values = ContainerKeys[ContainerName];
            foreach (var col in Values)
            {
                if (ColumnIterator.Contains(col))
                {
                    ColumnIterator.Remove(col);
                }
            }

            if (ColumnIterator.Contains("System"))
            {
                ColumnIterator.Remove("System");
            }
            if (ColumnIterator.Contains("system"))
            {
                ColumnIterator.Remove("system");
            }
            if (ColumnIterator.Contains("Hierarchy"))
            {
                ColumnIterator.Remove("Hierarchy");
            }
            return ColumnIterator;
        }

        /// <summary>
        /// Updates the hierarchy column, builds a view for sorting, applies SoRT logic
        /// "The engine"
        /// </summary>
        /// <returns> MergeDataWithKeys </returns>
        public DataTable CreateMergeDataTableWithKeysAndHierarchy(string ContainerName)
        {
            var dataService = new DataService();
            var SoRData = dataService.GetSoRData(ContainerName);
            var MergeDataWithKeys = dataService.GetMergeDataWithKeys(ContainerName);

            var businessLogic = new BusinessLogic();
            var columnIterator = businessLogic.ColumnIterator(ContainerName);

            foreach (string column in columnIterator)
            {
                foreach (DataRow row in MergeDataWithKeys.Rows)
                {
                    var SoRHierarchyValue = SoRData.FirstOrDefault(x => x.FieldName == column && x.SourceName == row["System"].ToString());

                    row.BeginEdit();
                    if (SoRHierarchyValue != null) row["Hierarchy"] = SoRHierarchyValue.Hierarchy;
                    row.EndEdit();
                }

                var vwMergeDataWithKeys = new DataView(MergeDataWithKeys);
                string sqlScript = ContainerKeys[ContainerName].Aggregate((a, b) => a + ',' + b);
                vwMergeDataWithKeys.Sort = $"{sqlScript} asc";

                foreach (DataRow row in MergeDataWithKeys.Rows)
                {
                    DataRowView[] allDuplicateRows;
                    var searchString = new object[ContainerKeys[ContainerName].Count];

                    if (ContainerKeys[ContainerName].Count > 1)
                    {
                        for (var i = 0; i < ContainerKeys[ContainerName].Count; i++)
                        {
                            searchString[i] = row[i].ToString();
                        }

                        allDuplicateRows = vwMergeDataWithKeys.FindRows(searchString);
                    }
                    else
                    {
                        searchString[0] = row[ContainerKeys[ContainerName].First()].ToString();
                        allDuplicateRows = vwMergeDataWithKeys.FindRows(searchString);
                    }

                    if (allDuplicateRows.Length > 1)
                    {
                        allDuplicateRows = allDuplicateRows.OrderBy(c => c.Row["Hierarchy"]).ToArray();

                        if (string.IsNullOrWhiteSpace(row[column].ToString()))
                        {
                            foreach (var position in allDuplicateRows)
                            {
                                if (!string.IsNullOrWhiteSpace(position[column].ToString()))
                                {
                                    row.BeginEdit();
                                    row[column] = position[column].ToString();
                                    row.EndEdit();
                                }
                            }
                        }
                        if (Convert.ToInt32(row["Hierarchy"].ToString()) != 1)
                        {
                            foreach (var position in allDuplicateRows)
                            {
                                if (!string.IsNullOrWhiteSpace(position[column].ToString()))
                                {
                                    row.BeginEdit();
                                    row[column] = position[column].ToString();
                                    row.EndEdit();
                                }
                            }
                        }
                    }
                }
            }
            return MergeDataWithKeys;
        }

        /// <summary>
        /// Sorts the MergeData datatable, creates the dedupe hashtable and fills based on the sort
        /// Uses the hash table to "dedupe" from the MergeData datatable through lookups instead of table scans
        /// </summary>
        /// <returns> writeBack </returns>
        public DataTable DeDupeMergeDataTable(string ContainerName)
        {
            var businessLogic = new BusinessLogic();
            var mergeData = businessLogic.CreateMergeDataTableWithKeysAndHierarchy(ContainerName);

            var vwMergeData = new DataView(mergeData);
            vwMergeData.Sort = "hierarchy asc";
            DataTable writeBack = vwMergeData.ToTable();

            var DeDupeHTable = new Hashtable();
            var duplicateList = new ArrayList();

            foreach (DataRow row in writeBack.Rows)
            {
                string keyString = "";
                foreach (string str in ContainerKeys[ContainerName])
                {
                    keyString = keyString + row[str].ToString();
                }

                if (DeDupeHTable.Contains(keyString))
                {
                    duplicateList.Add(row);
                }
                else
                {
                    DeDupeHTable.Add(keyString, string.Empty);
                }
            }

            foreach (DataRow row in duplicateList)
                writeBack.Rows.Remove(row);
            return writeBack;
        }
    }
}