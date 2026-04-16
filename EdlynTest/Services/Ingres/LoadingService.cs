using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Text;
using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;

namespace Services.Ingres
{
    public class LoadingService : ILoadingService
    {
        private readonly string connectionString;
        
        public LoadingService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetCarrier(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string carrierName = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = LoadingSQL.ResourceManager.GetString("GetCarrier");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNumber", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    carrierName = reader.GetString(0);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetCarrier : No Carrier found");
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(carrierName);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCarrier : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetDeliveryDetails(string customerCode, string catalogCode, int assigneeNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            LoadingAddress loadingAddress = null;
            string queryString = "";
            bool hasAssigneeNumber = false;
            if (assigneeNumber > 0)
            {
                hasAssigneeNumber = true;
            }
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    if (assigneeNumber > 0)
                    {
                        queryString = LoadingSQL.ResourceManager.GetString("GetDeliveryDetailsWithAssigneeNumber");
                    }
                    else
                    {
                        queryString = LoadingSQL.ResourceManager.GetString("GetDeliveryDetailsDefaultAssignee");
                    }

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CustomerCode", OdbcType.VarChar).Value = customerCode;
                        if (hasAssigneeNumber)
                        {
                            command.Parameters.Add("@AssigneeNumber", OdbcType.Int).Value = assigneeNumber;
                        }
                        else
                        {
                            assigneeNumber = 1;
                        }

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    loadingAddress = new LoadingAddress();
                                    loadingAddress.AddressName = dReader.name;
                                    loadingAddress.AssigneeNumber = assigneeNumber;
                                    loadingAddress.City = dReader.city;
                                    loadingAddress.State = dReader.state;
                                    loadingAddress.CustomerCode = customerCode;
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(loadingAddress);
                                return wrapper;
                            }
                        }
                    }

                    if (loadingAddress == null)
                    {
                        queryString = LoadingSQL.ResourceManager.GetString("GetCustomerAddress");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@CustomerCode", OdbcType.VarChar).Value = customerCode;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        loadingAddress = new LoadingAddress();
                                        loadingAddress.AddressName = dReader.name;
                                        loadingAddress.AssigneeNumber = assigneeNumber;
                                        loadingAddress.City = dReader.city;
                                        loadingAddress.State = dReader.state;
                                        loadingAddress.CustomerCode = customerCode;
                                    }
                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(loadingAddress);
                                    return wrapper;
                                }
                                else
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("GetDeliveryAddress : Could not find address");
                                    return wrapper;
                                }
                            }
                        }
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetDeliveryAddress : This error should be unreachable. Evidently not.");
                        return wrapper; 
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetDeliveryAddress : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletDetails(int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletDetail> palletDetails = new List<PalletDetail>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = LoadingSQL.ResourceManager.GetString("GetPalletDetails");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        PalletDetail palletDetail = new PalletDetail();
                                        palletDetail.CatalogCode = reader.GetString(0);
                                        palletDetail.PalletUnits = reader.GetInt32(1);
                                        palletDetails.Add(palletDetail);
                                    }
                                }
                                else
                                {
                                    wrapper.Messages.Add("No pallet details for pallet # " + palletNumber);
                                }
                            }
                        }
                    if (palletDetails.Count > 0)
                    {
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(palletDetails);
                        return wrapper;
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetails : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletsInManifest(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<LoadingPallet> loadingPallets = new List<LoadingPallet>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = LoadingSQL.ResourceManager.GetString("GetPalletsInManifest");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNumber", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    LoadingPallet loadingPallet = new LoadingPallet();
                                    loadingPallet.PalletNumber = dReader.pallet_no;
                                    loadingPallet.InvoiceNumber = dReader.ivce_no;
                                    loadingPallet.CustomerCode = dReader.cust_code;
                                    loadingPallet.AssigneeNumber = dReader.assignee_no;
                                    loadingPallet.PicklistNumber = dReader.plist_no;
                                    loadingPallet.LoadingConfirmed = dReader.loading_confirmed;

                                    loadingPallets.Add(loadingPallet);
                                }
                            }
                            else
                            {
                                LoadingPallet loadingPallet = new LoadingPallet();
                                loadingPallets.Add(loadingPallet);
                            }

                        }

                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(loadingPallets);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletsInManifest : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPicklistNumber(int invoiceNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int picklistNumber = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = LoadingSQL.ResourceManager.GetString("GetPicklistNumber");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@InvoiceNumber", OdbcType.Int).Value = invoiceNumber;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    picklistNumber = reader.GetInt32(0);
                                    wrapper.ResultSet.Add(picklistNumber);
                                    wrapper.IsSuccess = true;
                                }
                            } 
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPicklistNumber : No picklist number found");
                            }
                        }

                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPicklistNumber : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetailDespatched(int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = LoadingSQL.ResourceManager.GetString("UpdatePalletDetailDespatched");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletDetailDespatched : Pallet not found");
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetailDespatched : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
