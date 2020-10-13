using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections;
using System.Xml;
using System.Collections.Generic;
using System.Web;
using System.Configuration;
using System.Linq;
using System.Text;

namespace PrismaticAPI.DataAccessLayer
{
    public class DataAccessLayer
    {

        public string GetDefaultConnectionString { get; set; }


        #region Variable Declaration
        private AppActivityTracker Activity { get; set; }

        private SqlConnection objCon;
        private SqlCommand objCom;
        private SqlDataReader objDtReader;
        private SqlParameter objParm;
        static SqlDataAdapter objAdp;
        string Connection = string.Empty;
        private string connString = string.Empty;
        private static string strValue;
        #endregion

        #region Constructor
        /// <summary>
        ///Setup connection string for user management which is defined in app.config
        /// </summary>
        public DataAccessLayer()
        {
            connString = ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString;
                //DAL.Default.DefaultConn.ToString();
            GetDefaultConnectionString = connString;

            //read parameter from web.config file whether to track activity or not
            Activity = new AppActivityTracker(true);
        }


        /// <summary>
        /// Setup connection string for key specified in strDBKey parameter & defined key in applications web.config
        /// </summary>
        /// <param name="strDBKey">database connectionstring key name defined in applications web.config file </param>
        public DataAccessLayer(string strDBKey)
        {
            connString = ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString;
            GetDefaultConnectionString = connString;

           
            Activity = new AppActivityTracker(true);
        }

        #endregion

        #region Public Methods 
        /// <summary>
        /// Executes the store procedure and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <returns>object</returns>
        public object ExecuteScalar(string strProcName)
        {
            object obj;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(connString);
                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                objCon.Open();
                obj = objCom.ExecuteScalar();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(obj != null ? 1 : 0);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return obj;
        }

        /// <summary>
        /// Executes the store procedure against provided DB key connection and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <returns>object</returns>
        public object ExecuteScalar(string strProcName, string strDBKey)
        {
            object obj;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                objCon.Open();
                obj = objCom.ExecuteScalar();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(obj != null ? 1 : 0);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return obj;
        }

        /// <summary>
        /// Executes the store procedure and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <returns></returns>
        public object ExecuteScalar(string strProcName, Hashtable htable)
        {
            object obj;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(connString);
                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objCon.Open();
                obj = objCom.ExecuteScalar();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(obj != null ? 1 : 0);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return obj;
        }

        /// <summary>
        /// Executes the store procedure against provided DB key and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns></returns>
        public object ExecuteScalar(string strProcName, Hashtable htable, string strDBKey)
        {
            object obj;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);
                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objCon.Open();
                obj = objCom.ExecuteScalar();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(obj != null ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return obj;
        }


        /// <summary>
        /// Executes the SQL store procedure against the default connection and returns the number of rows affected.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <returns></returns>
        public long ExecuteNonQuery(string strProcName)
        {
            int retvalue;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(connString);
                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;

                objCon.Open();
                retvalue = objCom.ExecuteNonQuery();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(retvalue);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return retvalue;
        }

        /// <summary>
        /// Executes the SQL store procedure against the provided DB key connection and returns the number of rows affected.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>long</returns>
        public long ExecuteNonQuery(string strProcName, string strDBKey)
        {
            int retvalue;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);
                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;

                objCon.Open();
                retvalue = objCom.ExecuteNonQuery();

                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(retvalue);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return retvalue;
        }

        /// <summary>
        /// Executes the SQL store procedure against the default connection and returns the number of rows affected.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <param name="htable"></param>
        /// <param name="strDBKey"></param>
        /// <returns>long</returns>
        public long ExecuteNonQuery(string strProcName, Hashtable htable)
        {
            int retvalue;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(connString);
                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }

                objCon.Open();
                retvalue = objCom.ExecuteNonQuery();

                objCon.Close();
                objCom.Dispose();


                //update executed records against current activity 
                UpdateDBACtivity(retvalue);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return retvalue;
        }

        /// <summary>
        /// Executes the SQL store procedure against the the provided DB key connection and returns the number of rows affected.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <returns>long</returns>
        public long ExecuteNonQuery(string strProcName, Hashtable htable, string strDBKey)
        {
            int retvalue;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }

                objCon.Open();
                retvalue = objCom.ExecuteNonQuery();

                objCon.Close();
                objCom.Dispose();


                //update executed records against current activity 
                UpdateDBACtivity(retvalue);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return retvalue;
        }

        /// <summary>
        /// Executes the SQL store procedure against the the provided DB key connection and returns the parameter specified in "strOutputParam".
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strOutputParam">Output parameter name</param>
        /// <param name="htable">(Optional)Input parameters</param>
        /// <param name="strDBKey">(Optional)Database key specified in DAL.Setting file</param>
        /// <returns>string</returns>
        public string ExecuteNonQuery(string strProcName, string strOutputParam, Hashtable htable = null, string strDBKey = null)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(string.IsNullOrEmpty(strDBKey) ? connString : ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                //dt));
                objCom = new SqlCommand(strProcName, objCon);
                objCon.Open();
                objCom.CommandType = CommandType.StoredProcedure;

                if (htable != null)
                {
                    foreach (DictionaryEntry dist in htable)
                    {
                        objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                    }
                }

                objParm = objCom.Parameters.AddWithValue(strOutputParam, DbType.String.ToString());
                objParm.Size = 60;
                objParm.Direction = ParameterDirection.Output;
                objCom.ExecuteNonQuery();
                if (objCom.Parameters[strOutputParam].Value != null)
                {
                    strValue = objCom.Parameters[strOutputParam].Value.ToString();
                }
                else
                {
                    strValue = "INVALID";
                }
                    
                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(strOutputParam != null ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return strValue;
        }

        /// <summary>
        /// Executes the SQL store procedure against the the provided DB key connection and returns the object list.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <param name="arrLst">List of return parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>Object List</returns>
        public object[] ExecuteNonQuery(string strProcName, Hashtable htable, ArrayList arrLst, string strDBKey)
        {
            object[] paramValue;

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);
                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);
                objCom = new SqlCommand(strProcName, objCon);

                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dict in htable)
                {
                    objCom.Parameters.AddWithValue((string)dict.Key, dict.Value);
                }
                paramValue = new object[arrLst.Count];
                if (objCon.State != ConnectionState.Open)
                {
                    objCon.Open();
                }
                objCom.ExecuteNonQuery();
                objCon.Close();
                objCom.Dispose();

                //update executed records against current activity 
                UpdateDBACtivity(paramValue.Length);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return paramValue;
        }


        /// <summary>
        /// Sends the SQL statements to the default connection and returns a SqlDataReader.
        /// </summary>
        /// <param name="strProcName">SQL statements</param>
        /// <returns>SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string strProcName)
        {
            try
            {

                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(connString);

                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;

                objCon.Open();
                objDtReader = objCom.ExecuteReader(CommandBehavior.CloseConnection);

                //update executed records against current activity 
                UpdateDBACtivity(objDtReader.HasRows ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return objDtReader;
        }

        /// <summary>
        /// Sends the SQL statements to the provided DB key connection and returns a SqlDataReader.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string strProcName, string strDBKey)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);

                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;

                objCon.Open();
                objDtReader = objCom.ExecuteReader(CommandBehavior.CloseConnection);

                //update executed records against current activity 
                UpdateDBACtivity(objDtReader.HasRows ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return objDtReader;
        }

        /// <summary>
        /// Sends the SQL statements against the default connection and returns a SqlDataReader.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <param name="htable"></param>
        /// <returns>SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string strProcName, Hashtable htable)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(Convert.ToString(connString));

                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }

                objCon.Open();
                objDtReader = objCom.ExecuteReader(CommandBehavior.CloseConnection);

                //update executed records against current activity
                UpdateDBACtivity(objDtReader.HasRows ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return objDtReader;
        }

        /// <summary>
        /// Sends the SQL statements against the provided DB Key connection and returns a SqlDataReader.
        /// </summary>
        /// <param name="strProcName">SQL statements</param>
        /// /// <param name="htable">Input parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string strProcName, Hashtable htable, string strDBKey)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);

                if (objCon.State == ConnectionState.Open)
                {
                    objCon.Close();
                }

                objCom = new SqlCommand();
                objCom.Connection = objCon;
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }

                objCon.Open();
                objDtReader = objCom.ExecuteReader(CommandBehavior.CloseConnection);

                //update executed records against current activity
                UpdateDBACtivity(objDtReader.HasRows ? 1 : 0);
            }
            catch (Exception ex)
            {

                throw ex;
            }
            return objDtReader;
        }



        /// <summary>
        /// Excute the SQL store procedure against the default connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <returns>Dataset</returns>
        public DataSet GetDataSet(string strProcName)
        {
            DataSet dsResult = new DataSet();
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(connString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;
                objAdp = new SqlDataAdapter(cmdObj);
                objAdp.Fill(dsResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dsResult != null && dsResult.Tables.Count > 0)
                    UpdateDBACtivity(dsResult.Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dsResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the provided DB key connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string strProcName,string strDBKey)
        {
            DataSet dsResult = new DataSet();
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                SqlCommand cmdObj = new SqlCommand();
                
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandTimeout = 0;
                cmdObj.CommandType = CommandType.StoredProcedure;
                
                cmdObj.Connection = conn;
                objAdp = new SqlDataAdapter(cmdObj);
                objAdp.Fill(dsResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dsResult != null && dsResult.Tables.Count > 0)
                    UpdateDBACtivity(dsResult.Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dsResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the default connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string strProcName, Hashtable htable)
        {
            DataSet dsResult = new DataSet();

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(connString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;

                objAdp = new SqlDataAdapter(cmdObj);

                foreach (DictionaryEntry dist in htable)
                {
                    objAdp.SelectCommand.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objAdp.Fill(dsResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dsResult != null && dsResult.Tables.Count > 0)
                    UpdateDBACtivity(dsResult.Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dsResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the provided DB key connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string strProcName, Hashtable htable, string strDBKey)
        {
            DataSet dsResult = new DataSet();

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;

                objAdp = new SqlDataAdapter(cmdObj);

                foreach (DictionaryEntry dist in htable)
                {
                    objAdp.SelectCommand.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objAdp.Fill(dsResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dsResult != null && dsResult.Tables.Count > 0)
                    UpdateDBACtivity(dsResult.Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dsResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the provided DB key or default(if not provided) connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strTableName">Name of output table</param>
        /// <param name="htable">Input parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>DataSet</returns>
        public DataSet GetDataSet(string strProcName, string strTableName,Hashtable htable=null, string strDBKey=null)
        {
            DataSet dsResult = new DataSet();

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                var conStr = string.IsNullOrEmpty(strDBKey) ? connString : ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString;
                objAdp = new SqlDataAdapter(strProcName, conStr);

                if (htable != null)
                {
                    foreach (DictionaryEntry dist in htable)
                    {
                        objAdp.SelectCommand.Parameters.AddWithValue((string)dist.Key, dist.Value);
                    }
                }
                
                objAdp.Fill(dsResult, strTableName);

                //update executed records against current activity
                if (dsResult != null && dsResult.Tables.Count > 0)
                    UpdateDBACtivity(dsResult.Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dsResult;
        }




        /// <summary>
        /// Excute the SQL store procedure against the default connection and returns the datatable.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <returns>Dataset</returns>
        public DataTable GetDataTable(string strProcName)
        {
            var dtResult = new DataTable();
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(connString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;
                objAdp = new SqlDataAdapter(cmdObj);
                objAdp.Fill(dtResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dtResult != null && dtResult.Rows.Count > 0)
                    UpdateDBACtivity(dtResult.Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dtResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the provided DB key connection and returns the datatable.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>DataSet</returns>
        public DataTable GetDataTable(string strProcName, string strDBKey)
        {
            var dtResult = new DataTable();
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;
                objAdp = new SqlDataAdapter(cmdObj);
                objAdp.Fill(dtResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dtResult != null && dtResult.Rows.Count > 0)
                    UpdateDBACtivity(dtResult.Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dtResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the default connection and returns the dataset.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <returns>DataSet</returns>
        public DataTable GetDataTable(string strProcName, Hashtable htable)
        {
            var dtResult = new DataTable();

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(connString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;

                objAdp = new SqlDataAdapter(cmdObj);

                foreach (DictionaryEntry dist in htable)
                {
                    objAdp.SelectCommand.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objAdp.Fill(dtResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dtResult != null && dtResult.Rows.Count > 0)
                    UpdateDBACtivity(dtResult.Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dtResult;
        }

        /// <summary>
        /// Excute the SQL store procedure against the provided DB key connection and returns the datatable.
        /// </summary>
        /// <param name="strProcName">SQL store procedure</param>
        /// <param name="htable">Input parameters</param>
        /// <param name="strDBKey">Database key specified in DAL.Setting file</param>
        /// <returns>DataSet</returns>
        public DataTable GetDataTable(string strProcName, Hashtable htable, string strDBKey)
        {
            var dtResult = new DataTable();

            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                SqlCommand cmdObj = new SqlCommand();
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
                cmdObj.CommandText = strProcName;
                cmdObj.CommandType = CommandType.StoredProcedure;
                cmdObj.Connection = conn;

                objAdp = new SqlDataAdapter(cmdObj);

                foreach (DictionaryEntry dist in htable)
                {
                    objAdp.SelectCommand.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                objAdp.Fill(dtResult);

                conn.Close();
                cmdObj.Dispose();

                //update executed records against current activity
                if (dtResult != null && dtResult.Rows.Count > 0)
                    UpdateDBACtivity(dtResult.Rows.Count);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return dtResult;
        }



        /// <summary>
        /// To get output in xml format for default connection.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <param name="strDBKey"></param>
        /// <returns>string</returns>
        public string GetOutputXML(string strProcName, string strDBKey)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, null);

                string strXML = "";
                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);

                if (objCon.State != ConnectionState.Open)
                {
                    objCon.Open();
                }

                objCom = new SqlCommand(strProcName, objCon);
                objCom.CommandType = CommandType.StoredProcedure;
                System.Xml.XmlReader objXMLReader = objCom.ExecuteXmlReader();
                while (objXMLReader.Read())
                {
                    strXML = strXML + objXMLReader.ReadOuterXml();
                }

                //update executed records against current activity 
                UpdateDBACtivity(!string.IsNullOrEmpty(strXML) ? 1 : 0);

                return strXML;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objCon.Close();
            }
        }

        /// <summary>
        /// To get output in xml format for the provided DB key connection.
        /// </summary>
        /// <param name="strProcName"></param>
        /// <param name="htable"></param>
        /// /// <param name="strDBKey"></param>
        /// <returns>string</returns>
        public string GetOutputXML(string strProcName, Hashtable htable, string strDBKey)
        {
            try
            {
                //track database activity
                TrackDBACtivity(strProcName, htable);

                string strXML = "";
                objCon = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString);

                if (objCon.State != ConnectionState.Open)
                {
                    objCon.Open();
                }

                objCom = new SqlCommand();
                objCom.CommandText = strProcName;
                objCom.CommandType = CommandType.StoredProcedure;
                objCom.Connection = objCon;
                foreach (DictionaryEntry dist in htable)
                {
                    objCom.Parameters.AddWithValue((string)dist.Key, dist.Value);
                }
                //objCon.Open();
                // SqlDataReader objDtReader = objCom.ExecuteReader(CommandBehavior.CloseConnection);                

                System.Xml.XmlReader objXMLReader = objCom.ExecuteXmlReader();
                while (objXMLReader.Read())
                {
                    strXML = strXML + objXMLReader.ReadOuterXml();
                }

                //update executed records against current activity 
                UpdateDBACtivity(!string.IsNullOrEmpty(strXML) ? 1 : 0);

                return strXML;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objCon.Close();
            }
        }

        /// <summary>
        /// Created By : KALPAK KS
        /// Created On : 22 JUN 2012
        /// Purppose : function for converting xml data into dataset
        /// </summary>
        /// <returns></returns>
        /// 
        public DataSet FuncConvertToDataset(XmlNode xNode)
        {
            try
            {
                DataSet ds = new DataSet();
                string tmp = string.Empty;
                if ((xNode != null) && xNode.InnerXml.Length > 0)
                {
                    XmlTextReader reader = new XmlTextReader(xNode.OuterXml, XmlNodeType.Element, null);
                    ds.ReadXml(reader);
                }
                return ds;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }


        /// <summary>
        /// Export provided data in CSV format.
        /// </summary>
        /// <param name="data">Input data to export in file</param>
        /// <param name="fileName">Name of exported file</param>
        /// <returns></returns>
        public void ExportCSV(DataTable data, string fileName)
        {
            try
            {
                HttpContext context = HttpContext.Current;
                context.Response.Clear();
                context.Response.ContentType = "text/csv";
                context.Response.AddHeader("Content-Disposition", "attachment; filename=" + fileName + ".csv");

                //rite column header names
                for (int i = 0; i < data.Columns.Count; i++)
                {
                    if (i > 0)
                    {
                        context.Response.Write(",");
                    }
                    context.Response.Write(data.Columns[i].ColumnName);
                }
                context.Response.Write(Environment.NewLine);
                //Write data
                foreach (DataRow row in data.Rows)
                {
                    for (int i = 0; i < data.Columns.Count; i++)
                    {
                        if (i > 0)
                        {
                            //row[i] = row[i].ToString().Replace(",", "");
                            context.Response.Write(",");

                            if (row[i].ToString() == "2252719")
                            {

                                string str = "12042468";
                            }
                        }
                        string strWrite = row[i].ToString();
                        strWrite = strWrite.Replace("<br>", "");
                        strWrite = strWrite.Replace("<br/>", "");
                        strWrite = strWrite.Replace("\n", "");
                        strWrite = strWrite.Replace("\t", "");
                        strWrite = strWrite.Replace("\r", "");
                        strWrite = strWrite.Replace(",", "");
                        strWrite = strWrite.Replace("\"", "");


                        context.Response.Write(strWrite);
                    }
                    context.Response.Write(Environment.NewLine);
                }
                context.Response.Flush();
                context.Response.End();
            }
            catch (Exception )
            {

            }
        }

        ///// <summary>
        ///// Lookup on input table and returns input column from default database based on where condition 
        ///// </summary>
        ///// <param name="strTableName">Name of the table on which query executes</param>
        ///// <param name="strColumnName">Name of the column which will return in query</param>
        ///// <param name="strWhere">where condition executes on table</param>
        ///// <returns>single value in the object</returns>
        //public object Lookup(string strTableName, string strColumnName, string strWhere)
        //{
        //    object obj;

        //    try
        //    {
        //        var strBuilder = new StringBuilder("select ");
        //        SqlConnection objCon = new SqlConnection(connString);
        //        SqlCommand objCom = new SqlCommand();

        //        strBuilder.Append(strColumnName);
        //        strBuilder.Append(" from ");
        //        strBuilder.Append(strTableName);
        //        strBuilder.Append(" " + strWhere);

        //        objCom.Connection = objCon;
        //        objCom.CommandText = "prc_Lookup";
        //        objCom.CommandType = CommandType.StoredProcedure;
        //        objCom.Parameters.AddWithValue("@Query", strBuilder.ToString());

        //        objCon.Open();
        //        obj = objCom.ExecuteScalar();

        //        objCon.Close();
        //        objCom.Dispose();
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }

        //    return obj;
        //}

        ///// <summary>
        ///// Lookup on input table and returns input column from specified DBKey connection database based on where condition 
        ///// </summary>
        ///// <param name="strTableName">Name of the table on which query executes</param>
        ///// <param name="strColumnName">Name of the column which will return in query</param>
        ///// <param name="strWhere">where condition executes on table</param>
        ///// <param name="strDBKey">databse key specified in DAL.settings file</param>
        ///// <returns>single value in the object</returns>
        //public object Lookup(string strTableName, string strColumnName, string strWhere,string strDBKey)
        //{
        //    object obj;

        //    try
        //    {
        //        var strBuilder = new StringBuilder("select ");
        //        SqlConnection objCon = new SqlConnection(ConfigurationManager.ConnectionStrings[strDBKey].ConnectionString);
        //        SqlCommand objCom = new SqlCommand();
        //        strBuilder.Append(strColumnName);
        //        strBuilder.Append(" from ");
        //        strBuilder.Append(strTableName);
        //        strBuilder.Append(" " + strWhere);

        //        objCom.Connection = objCon;
        //        objCom.CommandText = "prc_Lookup";
        //        objCom.CommandType = CommandType.StoredProcedure;
        //        objCom.Parameters.AddWithValue("@Query", strBuilder.ToString());

        //        objCon.Open();
        //        obj = objCom.ExecuteScalar();

        //        objCon.Close();
        //        objCom.Dispose();
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }

        //    return obj;
        //}

        #endregion

        #region Private Methods
        private void TrackDBACtivity(string strProcName,Hashtable htable=null)
        {
            try
            {
                Activity.TrackDBActivity(HttpContext.Current.Request.Params["URL"], strProcName, "", htable);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void UpdateDBACtivity(long numOfRowsAffected)
        {
            try
            {
                Activity.NumOfRowsAffected = numOfRowsAffected;
                Activity.UpdateActivity("");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion
    }
}
