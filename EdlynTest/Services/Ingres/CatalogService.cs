using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Text;
using Westwind.Utilities.Data;

namespace Services.Ingres
{
    public class CatalogService: ICatalogService
    {
        private readonly string connectionString;

        public CatalogService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetCatalogByCatalogCode(string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            Catalog catalog = new Catalog();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = CatalogSQL.ResourceManager.GetString("GetCatalogByCatalogCode");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    catalog.CatalogCode = dReader.catlog_code;
                                    catalog.CatalogDesc = dReader.description;
                                    catalog.NoScanRepl = dReader.no_scan_repl;
                                    catalog.GLAccount = dReader.gl_account;
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetCatalogByCatalogCode: No catalog item found with code " + catalogCode);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCatalogByCatalogCode: " + e.Message);
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(catalog);
            return wrapper;
        }

        public TransactionWrapper GetCatalogByCatalogCodeForGLAccount(int jobNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            Catalog catalog = new Catalog();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = CatalogSQL.ResourceManager.GetString("GetCatalogByCatalogCodeForGLAccount");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.VarChar).Value = jobNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    catalog.CatalogCode = dReader.catlog_code;
                                    catalog.CatalogDesc = dReader.description;
                                    catalog.GLAccount = dReader.gl_account_no;
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetCatalogByCatalogCodeForGLAccount: No catalog item found with JobNo " + jobNo.ToString());
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCatalogByCatalogCodeForGLAccount: " + e.Message);
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(catalog);
            return wrapper;
        }
    }
}
