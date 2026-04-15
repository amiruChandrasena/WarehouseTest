using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Business
{
    internal class WriteLogFile
    {
        public static bool WriteLog(string strFileName, string strMessage)
        {
            try
            {
                string strPath = "C:\\Temp\\WH";

                try
                {
                    if (Directory.Exists(strPath))
                    {
                        //The code will execute if the folder exists
                    }
                    //The below code will create a folder if the folder is not exists in C#.Net.            
                    DirectoryInfo folder = Directory.CreateDirectory(strPath);
                }
                catch { }

                FileStream objFilestream = new FileStream(string.Format("{0}\\{1}", strPath, strFileName), FileMode.Append, FileAccess.Write);
                StreamWriter objStreamWriter = new StreamWriter((Stream)objFilestream);
                objStreamWriter.WriteLine(strMessage);
                objStreamWriter.Close();
                objFilestream.Close();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}
