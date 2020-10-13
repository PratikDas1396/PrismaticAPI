using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace PrismaticAPI.Models
{
    public class Lead
    {
        public Guid CourseID { get; set; }
        public string LeadID { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public string ContactNo { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreatedDtim { get; set; }
    }
}