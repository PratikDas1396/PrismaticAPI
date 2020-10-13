using System;
using System.Collections;
using System.Data.SqlClient;

namespace PrismaticAPI.DataAccessLayer
{
   public class ErrorLog
    {
        #region Variable Declaration
        DataAccessLayer objDAL = new DataAccessLayer();
        #endregion
        #region Constructor
        public ErrorLog()
        {
            //
            // TODO: Add constructor logic here
            //
        }
        #endregion
        #region Methods
        /// <summary>
        /// METHOD TO INSERT ERROR LOG ENTRY IN TABLE CBFrmErrLog
        /// </summary>
        /// <param name="AppId">Application Id</param>
        /// <param name="PageName"><Page Name/param>
        /// <param name="FuncName">Function Name</param>
        /// <param name="ErrDesc">Error Description</param>
        /// <param name="UserId">User Id</param>
        /// <param name="strDB">database connection string name</param>
        /// <returns></returns>
        public string LogErr(int AppId, string PageName, string FuncName, string ErrDesc, string UserId,string strDB)
        {
            string strOutput = "";
            try
            {
                objDAL = new DataAccessLayer(); // VAPT Points
                Hashtable htParam = new Hashtable();
                htParam.Add("@AppId", AppId);
                htParam.Add("@PageName", PageName);
                htParam.Add("@FuncName", FuncName);
                htParam.Add("@ErrDesc", ErrDesc);
                htParam.Add("@UserId", UserId);
                htParam.Add("@DatabseName", strDB);
                strOutput = Convert.ToString(objDAL.GetDataTable("Prc_InsErrLog",htParam, "DefaultConn").Rows[0][0]);
                htParam.Clear();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return strOutput;
        }


        /// <summary>
        /// Method to Insert error log entry in table CBFrmErrLog and send error mail 
        /// </summary>
        /// <param name="AppId">Application Id</param>
        /// <param name="PageName"><Page Name/param>
        /// <param name="FuncName">Function Name</param>
        /// <param name="ErrDesc">Error Description</param>
        /// <param name="UserId">User Id</param>
        /// <param name="strDB">database connection string name</param>
        /// <returns></returns>
        public string LogErrAndSendMail(int AppId, string PageName, string FuncName, string ErrDesc, string UserId, string strDB)
        {
            string strOutput = "";
            try
            {
                Hashtable htParam = new Hashtable();
                htParam.Add("@AppId", AppId);
                htParam.Add("@PageName", PageName);
                htParam.Add("@FuncName", FuncName);
                htParam.Add("@ErrDesc", ErrDesc);
                htParam.Add("@UserId", UserId);
                htParam.Add("@DatabseName", strDB);

                strOutput = objDAL.ExecuteNonQuery("Prc_InsErrLogSendMail", "@SeqNo", htParam, "DefaultConn");

                htParam.Clear();
            }
            catch (Exception)
            {
                throw;
            }
            return strOutput;
        }

        #endregion
    }
}
