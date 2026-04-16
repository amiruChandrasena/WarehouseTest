using System;
using System.Collections.Generic;
using System.Text;

namespace Services
{
    public class CustomException: Exception
    {
        public override string Message
        {
            get
            {
                if (Message.Contains("LOCKMODE"))
                {
                    return "The character you entered is not a valid digit";
                }
                else
                {
                    return Message;
                }
            }
        }
    }
}
