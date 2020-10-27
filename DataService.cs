using system_of_record_transformation.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace system_of_record_transformation
{

    /// <summary>
    /// Getting data for the app
    /// </summary>
    public class DataService
    {

        /// <summary>
        /// Gets System of Record data using usp from DB
        /// </summary>
        /// <param ContainerName="ContainerName"> name of the container to apply SoRT logic </param>
        /// <returns></returns>
        public List<SoRData> GetSoRData(string ContainerName)
        {
            var lsSoRData = new List<SoRData>();
            using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["MetaConnectionString"].ToString()))
            using (var sqlCommand = new SqlCommand("[sor].[uspGetSoRDataByContainer]", sqlConnection))
            {
                sqlConnection.Open();
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.Parameters.AddWithValue("@ContainerName", ContainerName);
                SqlDataReader sdr = sqlCommand.ExecuteReader();
                if (sdr.HasRows)
                {
                    while (sdr.Read())
                    {
                        var SoRData = new SoRData
                        {
                            SourceName = sdr.GetStringOrEmpty("SourceName"),
                            SourceNumber = sdr.GetIntOrZero("SourceNumber"),
                            Containername = sdr.GetStringOrEmpty("ContainerName"),
                            FieldID = sdr.GetIntOrZero("FieldID"),
                            FieldName = sdr.GetStringOrEmpty("FieldName"),
                            Hierarchy = sdr.GetIntOrZero("Hierarchy")
                        };
                        lsSoRData.Add(SoRData);
                    }
                }
            }
            return lsSoRData;
        }

        /// <summary>
        /// Gets System of Record IDs using usp from DB
        /// </summary>
        /// <returns></returns>
        public List<SoRID> GetSoRID()
        {
            var lsSoRID = new List<SoRID>();
            using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["MetaConnectionString"].ToString()))
            using (var sqlCommand = new SqlCommand("[sor].[uspGetSoRID]", sqlConnection))
            {
                sqlConnection.Open();
                sqlCommand.CommandType = CommandType.StoredProcedure;
                SqlDataReader sdr = sqlCommand.ExecuteReader();
                if (sdr.HasRows)
                {
                    while (sdr.Read())
                    {
                        var SoRID = new SoRID
                        {
                            DataSourceID = sdr.GetIntOrZero("DataSourceID"),
                            DataSource = sdr.GetStringOrEmpty("DataSource"),
                            Location = sdr.GetStringOrEmpty("Location"),
                            Function = sdr.GetStringOrEmpty("Function"),
                            Type = sdr.GetStringOrEmpty("Type")
                        };
                        lsSoRID.Add(SoRID);
                    }
                }
            }
            return lsSoRID;
        }

        /// <summary>
        /// calls usp's to update a staged merged data with keys
        /// </summary>
        /// <param name="containerName"> name of the table we're returning</param>
        /// <returns> a data table with merged data and updated keys</returns>
        public DataTable GetMergeDataWithKeys(string containerName)
        {
            var dtGetMergeDataWithKeys = new DataTable();
            using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["MetaConnectionString"].ToString()))
            using (var sqlCommand = new SqlCommand($"sor.{containerName}UpdateWithKeys", sqlConnection))
            {
                sqlConnection.Open();
                sqlCommand.CommandType = CommandType.StoredProcedure;
                SqlDataReader sdr = sqlCommand.ExecuteReader();
                {
                    dtGetMergeDataWithKeys.Load(sdr);
                }
            }

            dtGetMergeDataWithKeys.Columns.Add("Hierarchy", typeof(int));

            return dtGetMergeDataWithKeys;
        }

        /// <summary>
        /// writes back to mrg."tableName" table
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="dataTable"></param>
        /// <returns></returns>
        public bool UpdateMergeTable(string tableName, DataTable dataTable)
        {
            try
            {
                TruncateMergeTable(tableName);
                var objTable = dataTable;
                //removing Hierarchy so the table is the same shape as the destination
                objTable.Columns.Remove("Hierarchy");
                objTable.AcceptChanges();
                var sqlConnection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["ErpConnectionString"].ToString());

                var sqlBulkCopy = new SqlBulkCopy(sqlConnection) { DestinationTableName = "mrg." + tableName };
                sqlConnection.Open();

                sqlBulkCopy.WriteToServer(objTable);
                sqlConnection.Close();

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

        }

        /// <summary>
        /// truncates source merge table, enabling write back to an empty table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool TruncateMergeTable(string tableName)
        {
            using (
                var sqlConnection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["ErpConnectionString"].ToString()))
            using (var sqlCommand = new SqlCommand($"truncate table mrg.{tableName}", sqlConnection))
            {
                sqlConnection.Open();
                sqlCommand.ExecuteNonQuery();
            }
            return true;
        }

    }
}