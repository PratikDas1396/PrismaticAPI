using PrismaticAPI.Models;
using System;
using System.Web.Http;

namespace PrismaticAPI.Controllers
{
    [RoutePrefix("Authentication")]
    public class AuthenticationController : ApiController
    {
        [Route("authenticate")]
        public Response Authenticate([FromBody] Credentials credentials)
        {
            Response response = new Response();
            response.status = 0;
            response.msg = "success";
            try
            {
                if(credentials.userid == "admin" && credentials.password == "admin")
                {
                    response.data = credentials.userid;
                }
            }
            catch(Exception ex)
            {
                
            }
            return response;
        }
    }

    public class Credentials
    {
        public string userid { get; set; }
        public string password { get; set; }
    }
}
