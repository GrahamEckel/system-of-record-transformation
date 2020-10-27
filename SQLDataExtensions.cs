using System;
using System.Data.SqlClient;

namespace system_of_record_transformation
{
    public static class SqlDataExtensions
    {
        /// <summary>
        /// Gets the string value of the given field name. Can return null value.
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve string value from</param>
        /// <returns>string</returns>
        public static string GetNullableString(this SqlDataReader reader, string fieldName)
        {
            return reader[fieldName] == DBNull.Value
                ? null
                : reader[fieldName].ToString();
        }

        /// <summary>
        /// Gets the string value of the given field name. Returns empty string if null.
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve string value from</param>
        /// <returns>string, empty string if null</returns>
        public static string GetStringOrEmpty(this SqlDataReader reader, string fieldName)
        {
            return reader[fieldName] == DBNull.Value
                ? string.Empty
                : reader[fieldName].ToString();
        }

        /// <summary>
        /// Gets the int value of the given field name. Throws an ArgumentException if invalid integer
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve int value from</param>
        /// <returns>int</returns>
        public static int GetInt(this SqlDataReader reader, string fieldName)
        {
            int x;
            if (reader[fieldName] == DBNull.Value || !int.TryParse(reader[fieldName].ToString(), out x))
                throw new ArgumentException("Input was not a valid integer");

            return x;
        }

        /// <summary>
        /// Gets the nullable int value of the given field name.
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve nullable int value from</param>
        /// <returns>int?</returns>
        public static int? GetNullableInt(this SqlDataReader reader, string fieldName)
        {
            int x;
            return reader[fieldName] == DBNull.Value || !int.TryParse(reader[fieldName].ToString(), out x)
                ? (int?)null
                : x;
        }

        /// <summary>
        /// Gets the int value of the given field name. Returns 0 if null or invalid
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve int value from</param>
        /// <returns>int, 0 if null or invalid</returns>
        public static int GetIntOrZero(this SqlDataReader reader, string fieldName)
        {
            int x;
            return reader[fieldName] == DBNull.Value || !int.TryParse(reader[fieldName].ToString(), out x)
                ? 0
                : x;
        }

        /// <summary>
        /// Gets the int value of the given field name. Returns -1 if null or invalid
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve int value from</param>
        /// <returns>int, -1 if null or invalid</returns>
        public static int GetIntOrNegativeOne(this SqlDataReader reader, string fieldName)
        {
            int x;
            return reader[fieldName] == DBNull.Value || !int.TryParse(reader[fieldName].ToString(), out x)
                ? -1
                : x;
        }

        /// <summary>
        /// Gets the DateTime value of the given field name. Throws an ArgumentException if invalid DateTime
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve DateTime value from</param>
        /// <returns>DateTime</returns>
        public static DateTime GetDateTime(this SqlDataReader reader, string fieldName)
        {
            DateTime x;
            if (reader[fieldName] == DBNull.Value || !DateTime.TryParse(reader[fieldName].ToString(), out x))
                throw new ArgumentException("Input was not a valid datetime");

            return x;
        }

        /// <summary>
        /// Gets the nullable DateTime value of the given field name.
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve nullable DateTime value from</param>
        /// <returns>DateTime?</returns>
        public static DateTime? GetNullableDateTime(this SqlDataReader reader, string fieldName)
        {
            DateTime x;
            return reader[fieldName] == DBNull.Value || !DateTime.TryParse(reader[fieldName].ToString(), out x)
                ? (DateTime?)null
                : x;
        }

        /// <summary>
        /// Gets the DateTime value of the given field name. Returns DateTime.MinValue if null or invalid
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve DateTime value from</param>
        /// <returns>DateTime, DateTime.MinValue if null or invalid</returns>
        public static DateTime GetDateTimeOrMinValue(this SqlDataReader reader, string fieldName)
        {
            DateTime x;
            return reader[fieldName] == DBNull.Value || !DateTime.TryParse(reader[fieldName].ToString(), out x)
                ? DateTime.MinValue
                : x;
        }

        /// <summary>
        /// Gets the DateTime value of the given field name. Returns DateTime.MaxValue if null or invalid
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve DateTime value from</param>
        /// <returns>DateTime, DateTime.MaxValue if null or invalid</returns>
        public static DateTime GetDateTimeOrMaxValue(this SqlDataReader reader, string fieldName)
        {
            DateTime x;
            return reader[fieldName] == DBNull.Value || !DateTime.TryParse(reader[fieldName].ToString(), out x)
                ? DateTime.MaxValue
                : x;
        }

        /// <summary>
        /// Gets the decimal value of the given field name. Throws an ArgumentException if invalid decimal
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve decimal value from</param>
        /// <returns>decimal</returns>
        public static decimal GetDecimal(this SqlDataReader reader, string fieldName)
        {
            decimal x;
            if (reader[fieldName] == DBNull.Value || !decimal.TryParse(reader[fieldName].ToString(), out x))
                throw new ArgumentException("Input was not a valid decimal");

            return x;
        }

        /// <summary>
        /// Gets the nullable decimal value of the given field name.
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve decimal value from</param>
        /// <returns>decimal?</returns>
        public static decimal? GetNullableDecimal(this SqlDataReader reader, string fieldName)
        {
            decimal x;
            return reader[fieldName] == DBNull.Value || !decimal.TryParse(reader[fieldName].ToString(), out x)
                ? (decimal?)null
                : x;
        }

        /// <summary>
        /// Gets the decimal value of the given field name. Returns 0 if invalid decimal
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve decimal value from</param>
        /// <returns>decimal, 0 if null or invalid</returns>
        public static decimal GetDecimalOrZero(this SqlDataReader reader, string fieldName)
        {
            decimal x;
            return reader[fieldName] == DBNull.Value || !decimal.TryParse(reader[fieldName].ToString(), out x)
                ? 0
                : x;
        }

        /// <summary>
        /// Gets the decimal value of the given field name. Returns -1 if invalid decimal
        /// </summary>
        /// <param name="reader">SqlDataReader</param>
        /// <param name="fieldName">Name of the field to retrieve decimal value from</param>
        /// <returns>decimal, -1 if null or invalid</returns>
        public static decimal GetDecimalOrNegativeOne(this SqlDataReader reader, string fieldName)
        {
            decimal x;
            return reader[fieldName] == DBNull.Value || !decimal.TryParse(reader[fieldName].ToString(), out x)
                ? -1
                : x;
        }

        public static bool GetBool(this SqlDataReader reader, string fieldName)
        {
            bool x;
            if (reader[fieldName] == DBNull.Value || !bool.TryParse(reader[fieldName].ToString(), out x))
                throw new ArgumentException("Input was not a valid bool");

            return x;
        }

        public static bool GetBoolOrFalse(this SqlDataReader reader, string fieldName)
        {
            bool x;
            return reader[fieldName] == DBNull.Value || !bool.TryParse(reader[fieldName].ToString(), out x)
                ? false
                : x;
        }

        public static bool GetBoolOrTrue(this SqlDataReader reader, string fieldName)
        {
            bool x;
            return reader[fieldName] == DBNull.Value || !bool.TryParse(reader[fieldName].ToString(), out x)
                ? true
                : x;
        }
    }
}
