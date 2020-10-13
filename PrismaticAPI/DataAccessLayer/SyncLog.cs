using System;
using System.Xml;
using System.Data;
using System.Collections;
using System.Configuration;

namespace PrismaticAPI.DataAccessLayer
{
    public class SyncLog
    {
        #region Variable Declaration
        private string connString = string.Empty;
        DataAccessLayer objDAL = new DataAccessLayer();
        #endregion

        #region Constructor
        public SyncLog()
        {
            //commented by kalpak for sending db connection name instead of db connectionstring
            //connString = ConfigurationManager.ConnectionStrings["DefaultConn"].ConnectionString;
            connString = "DefaultConn";
        }
        #endregion

        #region Methods

        /// <summary>
        /// Method to insert log entry in Sync table
        /// </summary>
        /// <param name="AppId"></param>
        /// <param name="strInteractionNo"></param>
        /// <param name="strCreatedBy"></param>
        /// <param name="objXMLDoc"></param>
        /// <param name="MethodName"></param>
        /// <param name="strDB"></param>
        /// <returns></returns>
        public string InsertIntoSyncLog(int AppId, string strInteractionNo, string strCreatedBy, XmlDocument objXMLDoc, string MethodName, string strDB)
        {
            string strSyncID = string.Empty;
            var htParam = new Hashtable();
            try
            {
                htParam.Add("@AppId", AppId);
                htParam.Add("@InteractionNo", strInteractionNo);
                htParam.Add("@CreatedBy", strCreatedBy);
                htParam.Add("@CreatedDt", System.DateTime.Now);
                htParam.Add("@XMLDOC", objXMLDoc.InnerXml);
                htParam.Add("@MethodName", MethodName);
                htParam.Add("@DatabseName", strDB);

                DataAccessLayer objDAL = new DataAccessLayer();
                object obj12 = objDAL.ExecuteNonQuery("Prc_InsSyncLog", htParam, connString);
                try
                {
                    strSyncID = obj12.ToString();
                }
                catch
                {
                    strSyncID = "";
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally {
                htParam = null;
            }
            
            return strSyncID;
        }

        /// <summary>
        /// Update sync log method with output response
        /// </summary>
        /// <param name="objXMLDoc"></param>
        /// <param name="strSyncID"></param>
        /// <param name="strRefNo"></param>
        /// <returns></returns>
        public string UpdateIntoSyncLog(XmlDocument objXMLDoc, string strSyncID, string strRefNo)
        {
            var objDAL = new DataAccessLayer();
            var ds = objDAL.FuncConvertToDataset(objXMLDoc.DocumentElement);

            try
            {
                if (ds.Tables[0].Rows[0]["ErrorCode"].ToString() == "0")
                {
                    var objHT = new Hashtable();
                    objHT.Add("@SeqNo", Convert.ToInt32(strSyncID));
                    objHT.Add("@ResponseXML", objXMLDoc.InnerXml);
                    objHT.Add("@ErrDesc", DBNull.Value);
                    objHT.Add("@RefNo", strRefNo);
                    objDAL.ExecuteNonQuery("prc_UpdSyncLog", objHT, connString);
                    objHT = null;
                }
                else
                {
                    var objHT = new Hashtable();
                    objHT.Add("@SeqNo", Convert.ToInt32(strSyncID));
                    objHT.Add("@ResponseXML", objXMLDoc.InnerXml);
                    objHT.Add("@ErrDesc", ds.Tables[0].Rows[0]["ErrorDescription"].ToString());
                    objHT.Add("@RefNo", strRefNo);
                    objDAL.ExecuteNonQuery("prc_UpdSyncLog", objHT, connString);
                    objHT = null;
                }
            }
            catch
            {
                if (ds.Tables[0].Columns.Contains("Error_Codes"))
                {

                    if (ds.Tables[0].Rows[0]["Error_Codes"].ToString() == string.Empty)
                    {
                        var objHT = new Hashtable();
                        objHT.Add("@SeqNo", Convert.ToInt32(strSyncID));
                        objHT.Add("@ResponseXML", objXMLDoc.InnerXml);
                        objHT.Add("@ErrDesc", DBNull.Value);
                        objHT.Add("@RefNo", strRefNo);
                        objDAL.ExecuteNonQuery("prc_UpdSyncLog", objHT, connString);
                        objHT = null;
                    }
                    else
                    {
                        var objHT = new Hashtable();
                        objHT.Add("@SeqNo", Convert.ToInt32(strSyncID));
                        objHT.Add("@ResponseXML", objXMLDoc.InnerXml);
                        objHT.Add("@ErrDesc", ds.Tables[0].Rows[0]["Error_Codes"].ToString());
                        objHT.Add("@RefNo", strRefNo);
                        objDAL.ExecuteNonQuery("prc_UpdSyncLog", objHT, connString);
                        objHT = null;
                    }
                }
                else
                {
                    var objHT = new Hashtable();
                    objHT.Add("@SeqNo", Convert.ToInt32(strSyncID));
                    objHT.Add("@ResponseXML", objXMLDoc.InnerXml);
                    objHT.Add("@ErrDesc", "");
                    objHT.Add("@RefNo", strRefNo);
                    objDAL.ExecuteNonQuery("prc_UpdSyncLog", objHT, connString);
                    objHT = null;
                }
            }
            finally {
                objDAL = null;
                ds = null;
            }
            return string.Empty;
        }

        #endregion
    }
}
