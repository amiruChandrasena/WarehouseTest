using System;
using System.Collections.Generic;
using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using System.Data.Odbc;
using Services.Ingres.SQLResources;
using System.Data;
using System.Linq;
using Models.Utility;
using System.Reflection;

namespace Services.Ingres
{
    public class LoginService : ILoginService
    {
        private readonly string connectionString;

        public LoginService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetForkliftOperator(ForkliftOperator fOperator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    ForkliftOperator fliftOperator = new ForkliftOperator();

                    string queryString = LoginSQL.ResourceManager.GetString("GetForkliftOperator");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = fOperator.UserId;
                        command.Parameters.Add("@Password", OdbcType.VarChar).Value = fOperator.Password;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();

                            dynamic dReader = new DynamicDataReader(reader);
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    fliftOperator.UserId = dReader.userid;
                                    fliftOperator.Name = dReader.name;
                                    fliftOperator.EmployeeNo = dReader.employee_no;
                                    fliftOperator.Password = "";

                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Invalid username or password");
                                return wrapper;
                            }
                        }
                    }
                    queryString = LoginSQL.ResourceManager.GetString("CheckIfSupervisor");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = "%" + fOperator.UserId + "%";

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                int count = reader.GetInt32(0);
                                if (count > 0)
                                {
                                    fliftOperator.IsSupervisor = true;
                                }
                                else
                                {
                                    fliftOperator.IsSupervisor = false;
                                }
                            }
                        }
                    }

                    queryString = LoginSQL.ResourceManager.GetString("CheckIfScanWholeRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = "%" + fOperator.UserId + "%";

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                int count = reader.GetInt32(0);
                                if (count > 0)
                                {
                                    fliftOperator.CanScanWholeRack = true;
                                }
                                else
                                {
                                    fliftOperator.CanScanWholeRack = false;
                                }
                            }
                        }
                    }

                    wrapper.ResultSet.Add(fliftOperator);

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetForkliftOperator: " + e.Message);
                    wrapper.ResultSet.Clear();
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.Messages.Add("GetForkliftOperator: Returned " + wrapper.ResultSet.Count.ToString() + " rows.");

            return wrapper;
        }

        public TransactionWrapper GetRackingZones()
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = LoginSQL.ResourceManager.GetString("GetRackingZones");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RackingZone rackingZone = new RackingZone
                                {
                                    Code = reader.GetString(0).Trim(),
                                    Description = reader.GetString(1).Trim()
                                };
                                wrapper.ResultSet.Add(rackingZone);
                            }

                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRackingZones: " + e.Message);
                    wrapper.ResultSet.Clear();
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.Messages.Add("GetRacking Zones : Returned " + wrapper.ResultSet.Count.ToString() + " rows.");

            return wrapper;
        }

        public TransactionWrapper GetWarehouseIDName()
        {
            
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = LoginSQL.ResourceManager.GetString("GetWarehouseIdName");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {                                
                                int is3PL = reader.GetInt32(2);
                                bool is3P = false;

                                switch (is3PL)
                                {
                                    case 0:
                                        is3P = false;
                                        break;
                                    case 1:
                                        is3P = true;
                                        break;
                                    default:
                                        is3P = false;
                                        break;
                                }

                                Warehouse warehouse = new Warehouse
                                {
                                    Id = reader.GetString(0).Trim(),
                                    Name = reader.GetString(1).Trim(),
                                    Is3PL = Convert.ToBoolean(reader.GetInt32(2)),
                                    TransitWh = reader["transit_wh"].ToString(),
                                    ProductionWh = reader["production_wh"].ToString(),
                                    QualityWh = reader["quality_wh"].ToString()
                                };

                                wrapper.ResultSet.Add(warehouse);
                            
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseIDName : " + e.Message);
                    wrapper.ResultSet.Clear();
                    return wrapper;
                }

                if (connection.State == ConnectionState.Open)
                    connection.Close();
            }

            wrapper.IsSuccess = true;
            wrapper.Messages.Add("GetWarehouseIDName : Returned " + wrapper.ResultSet.Count.ToString() + " rows.");

            return wrapper;
        }

        public TransactionWrapper GetUserDefaults(string userid)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            ForkliftOperator fliftOperator = new ForkliftOperator();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = LoginSQL.ResourceManager.GetString("GetUserDefaults");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = userid;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    fliftOperator.DefaultWarehouse = reader.GetString(0);
                                    fliftOperator.DefaultRackingZone = reader.GetString(1);
                                }

                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(fliftOperator);
                                return wrapper;
                            }

                        }

                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Username does not exist");
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUserDefaults : " + e.Message);
                    return wrapper;
                }
            }
        }


    }
}
