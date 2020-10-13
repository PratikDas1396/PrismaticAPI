using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace PrismaticAPI.DataAccessLayer
{
    public class AppActivityTracker
    {
        private bool IsTrack { get; set; }
        private string ActivityID { get; set; }
        public long NumOfRowsAffected { get; set; }
        private string ConnectionString { get; set; }

        public AppActivityTracker()
        {
            this.IsTrack = false;
            ConnectionString = ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString;
        }

        public AppActivityTracker(bool isTrack)
        {
            this.IsTrack = isTrack;
            ConnectionString = ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString;
        }

        /// <summary>
        /// Tracking Application level Activities performed by user.
        /// </summary>
        /// <param name="strAppID">Application ID</param>
        /// <param name="strAppName">Application Name</param>
        /// <param name="strMstrModuleCode">Code of module used in Application</param>
        /// <param name="strPageName">Activity tracking page name</param>
        /// <param name="strFunName">Activity tracking function name</param>
        /// <param name="strUserID">Activity tracking user Id</param>
        public void TrackAppActivity(string strAppID, string strAppName, string strMstrModuleCode, string strPageName, string strFunName, string strUserID)
        {
            #region Declaration
            string strLogSrchParam = string.Empty;
            #endregion

            try
            {
                var htbl = new Hashtable();
                htbl.Add("@AppID", strAppID);
                htbl.Add("@ModuleId", strMstrModuleCode);
                htbl.Add("@PageName", strPageName);
                htbl.Add("@FunctionName", strFunName);
                htbl.Add("@QueryName", "");
                htbl.Add("@QueryParameters", "");
                htbl.Add("@CreatedBy", strUserID);

                ActivityID = (string)Exec_Scaler("prc_InsActivity", htbl);
                //ActivityID = (string)Exec_Ins_Command("prc_InsActivity", "@ActivityID", htbl);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Tracking Database level Activities performed by user.
        /// </summary>
        /// <param name="strPageName">Activity tracking page name</param>
        /// <param name="strQuerry">Name of the query to track</param>
        /// <param name="htable">List of parameters passed to query</param>
        /// <param name="strUserID">Activity tracking user Id</param>
        public void TrackDBActivity(string strPageName, string strQuerry, string strUserID, Hashtable htable=null)
        {
            #region Declaration
            string strLogSrchParam = string.Empty;
            #endregion

            try
            {
                //check whether Track activity or not
                if (IsTrack)
                {
                    if (htable != null)
                    {
                        IDictionaryEnumerator en = htable.GetEnumerator();
                        while (en.MoveNext())
                        {

                            if (strLogSrchParam == "")
                                strLogSrchParam = en.Key.ToString() + ":" + en.Value.ToString();
                            else
                                strLogSrchParam = strLogSrchParam + "," + en.Key.ToString() + ":" + en.Value.ToString();
                        }
                    }

                    var htbl = new Hashtable();
                    htbl.Add("@AppID", "");
                    htbl.Add("@ModuleId", "");
                    htbl.Add("@PageName", strPageName);
                    htbl.Add("@FunctionName", "");
                    htbl.Add("@QueryName", strQuerry);
                    htbl.Add("@QueryParameters", strLogSrchParam);
                    htbl.Add("@CreatedBy", strUserID);

                    ActivityID = (string)Exec_Scaler("prc_InsActivity", htbl);

                    //ActivityID = (string)Exec_Scaler("Prc_InsertLogSearchParam '" + strPageName + "','" + strQuerry + "','" + strLogSrchParam + "','" + strUserID + "'");
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Update executed number of rows against each activity
        /// </summary>
        public void UpdateActivity(string userID)
        {
            try
            {
                if (!string.IsNullOrEmpty(ActivityID))
                {
                    var htbl = new Hashtable();
                    htbl.Add("@ActivityID", ActivityID);
                    htbl.Add("@NumOfRowsAffected", NumOfRowsAffected);
                    htbl.Add("@UpdatedBy", userID);
                    Exec_Scaler("prc_UpdActivity", htbl);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// Executes the store procedure and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <param name="strProcName">SQL store procedure name</param>
        /// /// <param name="htable"></param>
        /// <returns>object</returns>
        public object Exec_Scaler(string strProcName, Hashtable htable)
        {
            object obj;

            try
            {
                SqlConnection objCon = new SqlConnection(ConnectionString);
                SqlCommand objCom = new SqlCommand();
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
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return obj;
        }
    }
}
