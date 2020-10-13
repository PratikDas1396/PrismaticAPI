using PrismaticAPI.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Newtonsoft.Json;

namespace PrismaticAPI.Controllers
{
    [RoutePrefix("Lead")]
    public class LeadController : ApiController
    {

        [Route("Apply")]
        public Response Apply([FromBody]Lead lead)
        {
            SqlConnection con = new SqlConnection(ConfigurationManager.ConnectionStrings["AppConn"].ConnectionString);
            Response resp = new Response();
            try
            {
                SqlCommand cmd = new SqlCommand("PRC_InsLeadDtls", con);
                con.Open();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("CourseID", Guid.NewGuid());
                cmd.Parameters.AddWithValue("Name", lead.Name);
                cmd.Parameters.AddWithValue("Email", lead.Email);
                cmd.Parameters.AddWithValue("ContactNo", lead.ContactNo);
                cmd.Parameters.AddWithValue("CreatedBy", lead.Name);
                cmd.ExecuteNonQuery();
                resp.status = 0;
                resp.msg = "Success";
            }
            catch (Exception ex)
            {
                resp.status = 1;
                resp.msg = String.Format("{0} \n {1}", ex.Message , Convert.ToString(ex.InnerException));
            }
            finally
            {
                con.Close();
            }

            return resp;
        }

        [Route("GetLeads")]
        public DataSet GetLeads()
        {
            SqlConnection con = new SqlConnection(ConfigurationManager.ConnectionStrings["AppConn"].ConnectionString);
            DataSet ds = new DataSet();
            try
            {
                
                SqlCommand cmd = new SqlCommand("PRC_GetLeadDtls", con);
                con.Open();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(ds);
                
            }
            catch (Exception ex)
            {

            }
            finally
            {
                con.Close();
            }
            return ds;
        }
    }
}
