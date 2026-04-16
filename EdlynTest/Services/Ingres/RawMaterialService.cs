using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;

namespace Services.Ingres
{
    public class RawMaterialService : IRawMaterialService
    {
        private readonly string connectionString;

        public RawMaterialService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public double GetRateTonneByCatalogCode(string catalogCode)
        {
            double ratetonne = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = RawMaterialSQL.ResourceManager.GetString("GetRateTonneByCatalogCode");
                    
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
                                    ratetonne = dReader.rate_tonne;
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    ratetonne = -1;
                }
            }

            return ratetonne;
        }
        
        public string GetUOMByProductCode(string catalogCode)
        {
            string uom = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = RawMaterialSQL.ResourceManager.GetString("GetUOMByProductCode");
                    
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
                                    uom = dReader.rate_tonne;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    uom = "";
                }
            }

            return uom;
        }

        public float GetUOMConvertionByCatalogCode(string catalogCode)
        {
            float convertion = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = RawMaterialSQL.ResourceManager.GetString("GetUOMConvertionByCatalogCode");

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
                                    convertion = dReader.convertion;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    convertion = 0;
                }
            }

            return convertion;
        }
    }
}
