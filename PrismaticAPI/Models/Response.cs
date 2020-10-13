using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace PrismaticAPI.Models
{
    public class Response
    {
        public int status { get; set; }
        public string msg { get; set; }
        public object data { get; set; }
    }
}