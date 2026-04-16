using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Text;

namespace Services.Ingres
{
    public class IssueService : IIssueService
    {
        private readonly string connectionString;

        public IssueService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper InsertIssueLog(DateTime timeStamp, int palletNo, string newLocation, string movedBy, string remark, DateTime syncTime)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = IssueSQL.ResourceManager.GetString("InsertIssueLog");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@MovedBy", OdbcType.VarChar).Value = movedBy;
                        command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = newLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@Remark", OdbcType.VarChar).Value = remark;
                        command.Parameters.Add("@SyncTime", OdbcType.DateTime).Value = syncTime;
                        command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = timeStamp;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertIssueLog : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
