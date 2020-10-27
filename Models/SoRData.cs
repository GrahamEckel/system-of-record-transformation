using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace system_of_record_transformation.Models
{
    /// <summary>
    /// Logic for FieldRankFromExcel
    /// </summary>
    public class SoRData
    {
        public string SourceName { get; set; }
        public int SourceNumber { get; set; }
        public string Containername { get; set; }
        public int FieldID { get; set; }
        public string FieldName { get; set; }
        public int Hierarchy { get; set; }
    }

    /// <summary>
    /// IDs and Descriptions for Data Sources
    /// </summary>
    public class SoRID
    {
        public int DataSourceID { get; set; }
        public string DataSource { get; set; }
        public string Location { get; set; }
        public string Function { get; set; }
        public string Type { get; set; }
    }
}
