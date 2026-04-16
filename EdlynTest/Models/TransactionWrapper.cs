using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class TransactionWrapper
    {
        public TransactionWrapper()
        {
            ResultSet = new List<Object>();
            Messages = new List<string>();
        }

        public bool IsSuccess { get; set; }
        public List<string> Messages { get; set; }
        public List<Object> ResultSet { get; set; }
    }
}
