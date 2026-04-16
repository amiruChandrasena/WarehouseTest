using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using Models;
using System.Data.Odbc;
using Services.Ingres.SQLResources;
using Models.Utility;
using Abstractions.ServiceInterfaces;
using System.Globalization;
using System.Reflection.PortableExecutable;
using System.IO;
using Common;

namespace Services.Ingres
{
    public class PickingService : IPickingService
    {
        private readonly string connectionString;
        private CultureInfo culture;

        public PickingService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
            culture = new CultureInfo("en-AU");
        }

        #region Select Methods

        public int CheckPalletShelfLife(int licensedPalletNo, string reservedCatalogCode, int shelfLife)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("CheckPalletShelfLife");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = reservedCatalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.Now.AddDays(shelfLife);

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                                return count;
                            }
                        }

                        return count;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public float GetAvailablePickingQuantity(int licensedPalletNo, string reservedCatalogCode, int shelfLife)
        {
            float availablePickQuantity = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetAvailablePickQuantity");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = reservedCatalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.Now.AddDays(shelfLife);

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                availablePickQuantity = reader.GetFloat(0);
                                return availablePickQuantity;
                            }

                        }

                        return availablePickQuantity;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public string GetBinLocationOfPallet(int palletNo)
        {
            string binLocation = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetBinLocationOfPallet");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    binLocation = reader.GetString(0);
                                    return binLocation;
                                }
                            }

                        }

                        return binLocation;
                    }
                }
                catch (Exception)
                {
                    return binLocation;
                }
            }
        }

        public TransactionWrapper GetCarrier(string carrierCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            Carrier carrier = new Carrier();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetCarrier");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CarrierCode", OdbcType.VarChar).Value = carrierCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    carrier.AbnNumber = dReader.abn_no;
                                    carrier.CarrierCode = dReader.carrier_code;
                                    carrier.Contact = dReader.contact;
                                    carrier.Email = dReader.email;
                                    carrier.FaxNumber = dReader.fax_no;
                                    carrier.FuelLevy = dReader.fuel_levy;
                                    carrier.Location = dReader.location;
                                    carrier.Mobile = dReader.mobile;
                                    carrier.Name = dReader.name;
                                    carrier.Remarks = dReader.remarks;
                                    carrier.TaxInvoice = dReader.tax_inv;
                                    carrier.Telephone = dReader.telephone;
                                    carrier.Type = dReader.type;
                                    carrier.ConnotePrefix = dReader.connote_prefix;
                                    carrier.Version = dReader.version;
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(carrier);
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No carrier found for carrier code " + carrierCode);
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

        public TransactionWrapper GetCatalogCodeFromPalletDetail(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetCatalogCodeFromPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    wrapper.ResultSet.Add(reader.GetString(0));
                                }
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetCatalogCodeFromPalletDetail : Could not find pallet details");
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCatalogCodeFromPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCatalogItemForPicking(string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetCatalogItemForPicking");
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
                                    CatalogItem catalogItem = new CatalogItem
                                    {
                                        CatalogCode = dReader.catlog_code,
                                        Description = dReader.description,
                                        PalletType = dReader.pallet_type,
                                        ShelfLife = dReader.shelf_life,
                                        UomPallet = dReader.uom_pallet
                                    };

                                    catalogItem.CatalogCode = catalogItem.CatalogCode.Trim();

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(catalogItem);
                                    return wrapper;
                                }
                            }
                        }

                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetCatalogItemForPicking : No details found for code " + catalogCode);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCatalogItemForPicking : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCheckCarrierCode(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string carrierCode = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetCheckCarrierCode");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    carrierCode = reader.GetString(0);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Cannot get carrier code");
                                return wrapper;
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(carrierCode);

                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCheckCarrierCode: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCheckCustomerAndAddress(int picklistNo)
        {
            string customerName = "";
            string address1 = "";
            string address2 = "";
            bool hasRows = true;

            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetCheckCustomerAndAddressOne");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    customerName = reader.GetString(0);
                                    address1 = reader.GetString(1);
                                    address2 = reader.GetString(2) + " " + reader.GetString(3) + " " + reader.GetString(4) + " " + reader.GetString(5) + " (" + reader.GetString(6) + ")";
                                }
                            }
                            else
                            {
                                hasRows = false;
                            }
                        }
                    }

                    if (hasRows)
                    {
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(customerName);
                        wrapper.ResultSet.Add(address1);
                        wrapper.ResultSet.Add(address2);
                        return wrapper;
                    }
                    else
                    {
                        queryString = PickingSQL.ResourceManager.GetString("GetCheckCustomerAndAddressTwo");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        customerName = reader.GetString(0);
                                        address1 = reader.GetString(1);
                                        address2 = reader.GetString(2) + " " + reader.GetString(3) + " " + reader.GetString(4) + " " + reader.GetString(5) + " (" + reader.GetString(6) + ")";
                                    }
                                }
                                else
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("Cannot get address details");
                                    return wrapper;
                                }
                            }
                        }

                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(customerName);
                        wrapper.ResultSet.Add(address1);
                        wrapper.ResultSet.Add(address2);
                        return wrapper;

                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCheckCustomerAndAddress: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCheckManifestAndPicklist(int palletNo)
        {
            int picklistNumber = 0;
            int manifestNumber = 0;

            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetCheckManifestAndPicklistFromPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    if (picklistNumber == 0)
                                    {
                                        picklistNumber = reader.GetInt32(0);
                                        manifestNumber = reader.GetInt32(1);
                                    }
                                    else
                                    {
                                        picklistNumber = 0;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (picklistNumber == 0)
                    {
                        wrapper.Messages.Add("Pallet " + palletNo.ToString() + " does not belong to a finalized picking list.");
                    }

                    queryString = PickingSQL.ResourceManager.GetString("GetCheckManifestAndPicklistFromMLS");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    picklistNumber = reader.GetInt32(0);
                                    manifestNumber = reader.GetInt32(1);
                                }
                            }
                        }
                    }

                    if (picklistNumber == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet " + palletNo.ToString() + " not used in picking");
                        return wrapper;
                    }
                    else
                    {
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(picklistNumber);
                        wrapper.ResultSet.Add(manifestNumber);
                        return wrapper;
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCheckManifestAndPicklist: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCheckPalletCount(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<int> palletNumbers = new List<int>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetCheckManifestPalletNumbers");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletNumbers.Add(reader.GetInt32(0));
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(palletNumbers);
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetCheckPalletCount" + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetManifest(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            Manifest manifest = new Manifest();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetManifest");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    manifest.AdviceFlag = dReader.advice_flag;
                                    manifest.AreaCode = dReader.area_code;
                                    manifest.CarrierCode = dReader.carrier_code;
                                    manifest.ConfirmFlag = dReader.confirm_flag;
                                    manifest.DateCreated = dReader.date_created;
                                    manifest.LoadingStatus = dReader.loading_status;
                                    manifest.ManifestNumber = dReader.manifest_no;
                                    manifest.NumberOfPallets = dReader.no_pallets;
                                    manifest.PalletType = dReader.pallet_type;
                                    manifest.PalletWeight = dReader.pallet_weight;
                                    manifest.PlanFlag = dReader.plan_flag;
                                    manifest.Rego = dReader.rego;
                                    manifest.Remark = dReader.remark;
                                    manifest.ReserveFlag = dReader.reserve_flag;
                                    manifest.RunDate = dReader.run_date;
                                    manifest.RunNumber = dReader.run_no;
                                    manifest.Status = dReader.status;
                                    manifest.TimeIn = dReader.time_in;
                                    manifest.TimeOut = dReader.time_out;
                                    manifest.LoadOption = dReader.load_option;
                                    manifest.TruckFront = dReader.truck_front;
                                    manifest.TruckRear = dReader.truck_rear;
                                    manifest.Version = dReader.version;
                                    manifest.OpenPalletNumber = dReader.open_pallet_no;
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(manifest);
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetManifest : No record found for manifest " + manifestNo.ToString());
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetManifest : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetForkliftPallet(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetForkliftPallet");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PalletDetail palletDetail = new PalletDetail
                                    {
                                        PalletNumber = dReader.pallet_no,
                                        OldPalletNumber = dReader.old_pallet_no,
                                        CatalogCode = dReader.catlog_code,
                                        OriginalPalletUnits = dReader.orig_pallet_units,
                                        PalletUnits = dReader.pallet_units,
                                        CatalogDescription = dReader.description,
                                        BestBefore = dReader.best_before
                                    };

                                    palletDetail.CatalogCode = palletDetail.CatalogCode.Trim();

                                    PalletHeader palletHeader = new PalletHeader
                                    {
                                        PalletNumber = palletNo,
                                        Status = dReader.status,
                                        TransferStatus = dReader.transfer_status,
                                        Quality = dReader.quality,
                                        PlanNumber = dReader.plan_no,
                                        BinLocation = dReader.bin_location,
                                        WarehouseId = dReader.warehouse_id,
                                    };

                                    palletHeader.PalletDetails.Add(palletDetail);

                                    wrapper.ResultSet.Add(palletHeader);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetForkliftPallet : No pallet found");
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetForkliftPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetManifestLoadingStatus(int manifestNo, int picklistNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<ManifestLoadingStatus> manifestLoadingStatuses = new List<ManifestLoadingStatus>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetManifestLoadingStatus");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    ManifestLoadingStatus manLoadStatus = new ManifestLoadingStatus
                                    {
                                        PalletNumber = dReader.pallet_no,
                                        PicklistNumber = dReader.plist_no,
                                        CatalogCode = dReader.catlog_code,
                                        PalletUnits = dReader.pallet_units,
                                        //BestBefore = dReader.best_before,
                                        OldPalletNumber = dReader.old_pallet_no,
                                        ManifestNumber = manifestNo
                                    };
                                    DateTime bestBefore = dReader.best_before;
                                    manLoadStatus.BestBefore = bestBefore.ToString(DateFormats.ddMMyy);
                                    manifestLoadingStatuses.Add(manLoadStatus);
                                }
                                wrapper.ResultSet.Add(manifestLoadingStatuses);
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetManifestLoadingStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetManifestLoadingStatusCount(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetManifestLoadingStatusCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }

                        if (count > 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Pallet #" + palletNo.ToString() + " has already been loaded.");
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetManifestLoadingStatusCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetNegativePickBin()
        {
            int noNegativePickBin = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetNegativePickBin");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    noNegativePickBin = reader.GetInt32(0);
                                    return noNegativePickBin;
                                }
                            }
                        }
                        return noNegativePickBin;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public int GetNewPalletNumber()
        {
            int newPalletNumber = 0;
            string keyType = "PALL";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = UtilitySQL.ResourceManager.GetString("UpdateUniqueKey");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = keyType;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected <= 0)
                        {
                            return newPalletNumber;
                        }
                    }

                    string queryString = UtilitySQL.ResourceManager.GetString("SelectUniqueKey");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = keyType;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    newPalletNumber = reader.GetInt32(0);
                                    return newPalletNumber;
                                }
                            }
                        }
                        return newPalletNumber;
                    }
                }
                catch (Exception)
                {
                    return 0;
                }
            }
        }

        public TransactionWrapper GetOverpickedItems(ManifestLoadingStatus manLoadStatus)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string overpickedCatalogCode = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetOverpickedItems");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PickedQty", OdbcType.Int).Value = manLoadStatus.PickedQuantity;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = manLoadStatus.PicklistNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    overpickedCatalogCode = reader.GetString(0);
                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(overpickedCatalogCode);
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetOverpickedItems : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetPalletCount(string binLocation)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }
                        return count;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetPalletCountInLocation(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int palletNo = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletCountInLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation1", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@BinLocation2", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletNo = reader.GetInt32(0);
                                    wrapper.ResultSet.Add(palletNo);
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletCountInLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletDetailForPickedItems(int licensedPalletNumber, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletDetail> palletDetails = new List<PalletDetail>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletDetailForPickedItems");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNumber", OdbcType.Int).Value = licensedPalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PalletDetail palletDetail = new PalletDetail
                                    {
                                        CatalogCode = dReader.catlog_code,
                                        BestBefore = dReader.best_before,
                                        PalletUnits = dReader.pallet_units
                                    };
                                    palletDetails.Add(palletDetail);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetailForPickedItems : Could not find pallet details");
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(palletDetails);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetailForPickedItems : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletPrintedAt(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string printedAt = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletPrintedAt");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    printedAt = reader.GetString(0);
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(printedAt);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletPrintedAt : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletQuantity(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            float palletQuantity = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletQuantity");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletQuantity = reader.GetFloat(0);
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(palletQuantity);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletQuantity : Could not find pallet " + palletNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletViaPickingLabel(string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int palletNo = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPalletViaPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletNo = reader.GetInt32(0);
                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(palletNo);
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletViaPickingLabel : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetPickerAllocationCount(int manifestNo, int picklistNo)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetPickerAllocationCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }
                        return count;
                    }
                }
                catch (Exception e)
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetPickerManifestPicklistNumber(string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PickerAllocation pickerAllocation = new PickerAllocation();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetPickerManifestPicklistNumber");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pickerAllocation.ManifestNumber = dReader.manifest_no;
                                    pickerAllocation.PicklistNumber = dReader.plist_no;
                                    pickerAllocation.OpenPalletNumber = dReader.open_pallet_no;
                                }

                                wrapper.ResultSet.Add(pickerAllocation);
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No orders allocated to " + originator);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickerManifestPicklistNumber : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper GetPickerNotes(int manifestNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PickerNote> pickerNotes = new List<PickerNote>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPickerNotes");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNumber", OdbcType.Int).Value = manifestNumber;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PickerNote pickerNote = new PickerNote
                                    {
                                        Note = reader.GetString(0),
                                        NoteNumber = reader.GetInt32(1)
                                    };
                                    pickerNotes.Add(pickerNote);
                                }
                            }

                            wrapper.IsSuccess = true;
                            wrapper.ResultSet.Add(pickerNotes);
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickerNotes: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPickerTransferNumber(string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PickerAllocation pickerAllocation = new PickerAllocation();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetPickerTransferNumber");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pickerAllocation.ManifestNumber = dReader.manifest_no;
                                    pickerAllocation.RegistrationNumber = dReader.reg_no;
                                    pickerAllocation.PicklistNumber = dReader.plist_no;
                                    pickerAllocation.OpenPalletNumber = dReader.open_pallet_no;
                                }

                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(pickerAllocation);
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No orders allocated to " + originator);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickerTransferNumber : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetPickingLabelCount(string pickingLabel)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPickingLabelCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;
                        command.Parameters.Add("@Date", OdbcType.DateTime).Value = DateTime.Now.AddYears(-1);

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }
                        return count;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public int GetPickingLabelPallet(string pickingLabel)
        {
            int palletNo = 0;
            int reprintLabel = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    {
                        string queryString = PickingSQL.ResourceManager.GetString("GetPickingLabelPallet");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        palletNo = reader.GetInt32(0);
                                    }
                                }
                            }
                        }

                        if (palletNo > 0)
                        {
                            return palletNo;
                        }
                        else
                        {
                            try
                            {
                                reprintLabel = Convert.ToInt32(pickingLabel);
                            }
                            catch
                            {
                                return 0;
                            }
                        }

                        queryString = PickingSQL.ResourceManager.GetString("GetReprintedPallet");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = reprintLabel;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        palletNo = reader.GetInt32(0);
                                    }
                                }
                            }
                        }

                        return palletNo;
                    }
                }
                catch
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetPickingSequenceDetails(List<PicklistItem> picklistItems, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int sequenceType = 0;
            bool isbb = true;

            string logFilePath = "C:\\Logs\\PickingSequenceDetails.txt";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string catalogCode = string.Empty;
                    int picklistNo = 0;

                    for (int i = 0; i < picklistItems.Count; i++)
                    {
                        picklistItems[i].PickingSequenceP = 0;
                        picklistItems[i].BinLocation = "";

                        catalogCode = picklistItems[i].CatalogCode;
                        picklistNo = picklistItems[i].PicklistNumber;

                        DateTime? bbDate = GetBestbeforeDateByPListNo(picklistNo, catalogCode);
                        if (bbDate.HasValue)
                        {
                            // safe to use bbDate.Value or bbDate directly
                            isbb = true;
                        }
                        else
                        {
                            isbb = false;
                        }

                        isbb = false; // Remove thisn ***************

                        if (isbb)
                        {
                            if (picklistItems[i].LooseQuantity > 0)
                            {
                                picklistItems[i].PickingSequenceP = 999999; // OpenROAD has 6 digits here but 7 digits elsewhere, has been reflected here but may need to investigate
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsPartWithBBDate");

                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                                    command.Parameters.Add("@BestBeforeDate", OdbcType.Date).Value = bbDate.Value;

                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {
                                        dynamic dReader = new DynamicDataReader(reader);

                                        if (reader.HasRows)
                                        {
                                            while (reader.Read())
                                            {
                                                picklistItems[i].PickingSequenceP = dReader.picking_seq;
                                                if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                                {
                                                    picklistItems[i].BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                                    picklistItems[i].LicensedPalletNumber = dReader.licenced_pallet_no;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (picklistItems[i].FullPalletQuantity > 0 && picklistItems[i].BinLocation == "")
                            {
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsFullWithBBDate");
                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                                    command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;
                                    command.Parameters.Add("@BestBeforeDate", OdbcType.Date).Value = bbDate.Value;

                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {

                                        dynamic dReader = new DynamicDataReader(reader);

                                        if (reader.HasRows)
                                        {
                                            while (reader.Read() && String.IsNullOrEmpty(picklistItems[i].BinLocation))
                                            {
                                                if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                                {
                                                    picklistItems[i].BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                                }

                                                picklistItems[i].PickingSequenceB = dReader.picking_seq;
                                                picklistItems[i].PickingSequenceP = 9999999;
                                            }

                                            isbb = true;
                                        }
                                    }
                                }
                            }

                            if (picklistItems[i].BinLocation == "")
                            {
                                picklistItems[i].PickingSequenceB = 9999999;
                                picklistItems[i].PickingSequenceP = 9999999;
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsNoneWithBBDate");
                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                                    command.Parameters.Add("@BestBeforeDate", OdbcType.Date).Value = bbDate.Value;

                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            while (reader.Read() && String.IsNullOrEmpty(picklistItems[i].BinLocation))
                                            {
                                                if (!reader.IsDBNull(0)) { picklistItems[i].BinLocation = reader.GetString(0); }
                                            }

                                            isbb = true;
                                        }
                                    }
                                }
                            }
                        }
                        else 
                        {
                            if (picklistItems[i].LooseQuantity > 0)
                            {
                                picklistItems[i].PickingSequenceP = 999999; // OpenROAD has 6 digits here but 7 digits elsewhere, has been reflected here but may need to investigate
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsPart");

                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {
                                        dynamic dReader = new DynamicDataReader(reader);
                                        if (reader.HasRows)
                                        {
                                            while (reader.Read())
                                            {
                                                picklistItems[i].PickingSequenceP = dReader.picking_seq;
                                                if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                                {
                                                    picklistItems[i].BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;

                                                    picklistItems[i].LicensedPalletNumber = dReader.licenced_pallet_no;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (picklistItems[i].FullPalletQuantity > 0 && picklistItems[i].BinLocation == "")
                            {
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsFull");
                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                                    command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;

                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {

                                        dynamic dReader = new DynamicDataReader(reader);

                                        if (reader.HasRows)
                                        {
                                            while (reader.Read() && String.IsNullOrEmpty(picklistItems[i].BinLocation))
                                            {
                                                if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                                {
                                                    picklistItems[i].BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                                }
                                                picklistItems[i].PickingSequenceB = dReader.picking_seq;
                                                picklistItems[i].PickingSequenceP = 9999999;
                                            }
                                        }

                                    }
                                }
                            }

                            if (picklistItems[i].BinLocation == "")
                            {
                                picklistItems[i].PickingSequenceB = 9999999;
                                picklistItems[i].PickingSequenceP = 9999999;
                                string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsNone");
                                using (OdbcCommand command = new OdbcCommand(queryString, connection))
                                {
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picklistItems[i].CatalogCode;
                                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;

                                    using (OdbcDataReader reader = command.ExecuteReader())
                                    {
                                        if (reader.HasRows)
                                        {
                                            while (reader.Read() && String.IsNullOrEmpty(picklistItems[i].BinLocation))
                                            {
                                                if (!reader.IsDBNull(0)) { picklistItems[i].BinLocation = reader.GetString(0); }
                                            }
                                        }

                                    }
                                }
                            }
                        }

                        // Check if the licenced pallet number is greater than 0 and if so, get the pallet header details.
                        // This is to ensure that the bin location is cleared for pallets in E1 warehouse.
                        if (picklistItems[i].LicensedPalletNumber > 0)
                        {
                            TransactionWrapper wrapperPallet = new TransactionWrapper();
                            wrapperPallet = GetPalletHeaderByPalletNo(picklistItems[i].LicensedPalletNumber);

                            PalletHeader palletHeader = wrapperPallet.ResultSet[0] as PalletHeader;
                            if (palletHeader != null && palletHeader.WarehouseId != null)
                            {
                                if (picklistItems[i].BinLocation.Contains("E1"))
                                {
                                    if (palletHeader.WarehouseId == "QP" || palletHeader.WarehouseId == "QR")
                                    {
                                        picklistItems[i].BinLocation = "";
                                    }
                                }
                            }
                        }

                        // Write to log file
                        try
                        {
                            using (StreamWriter writer = new StreamWriter(logFilePath, true))
                            {
                                writer.WriteLine($"{DateTime.Now}: Pallet Count {picklistItems.Count}, Licenced Pallet Number: {picklistItems[i].LicensedPalletNumber}, CatalogCode: {picklistItems[i].CatalogCode}, BinLocation: {picklistItems[i].BinLocation}");
                            }
                        }
                        catch (Exception logEx)
                        {
                        }

                        
                        

                        //// Write to log file
                        //try
                        //{
                        //    using (StreamWriter writer = new StreamWriter(logFilePath, true))
                        //    {
                        //        writer.WriteLine($"{DateTime.Now}: PalletNumber: {picklistItems[i].LicensedPalletNumber}, WarehouseId: {palletHeader.WarehouseId}");
                        //    }
                        //}
                        //catch (Exception logEx)
                        //{
                        //}
                    }

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(picklistItems);
                    return wrapper;

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickingSequenceDetails : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletHeaderByPalletNo(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletHeader pallet = new PalletHeader();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletHeaderByPalletNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        //command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pallet.Status = reader.GetString(0);
                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = palletNo;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletHeaderByPalletNo: No pallet found for " + palletNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (CustomException e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletHeaderByPalletNo: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(pallet);
            return wrapper;
        }
        public TransactionWrapper GetPickingSequenceDetailsFull(string catalogCode, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PicklistItem picklistItem = new PicklistItem();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsFull");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                    {
                                        picklistItem.BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                    }
                                    picklistItem.PickingSequenceB = dReader.picking_seq;
                                    picklistItem.PickingSequenceP = 9999999;
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(picklistItem);
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickingSequenceDetailFull : " + e.Message);
                    return wrapper;

                }
            }
        }

        public TransactionWrapper GetPickingSequenceDetailsNone(string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PicklistItem picklistItem = new PicklistItem();
            picklistItem.PickingSequenceB = 9999999;
            picklistItem.PickingSequenceP = 9999999;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsNone");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    if (!reader.IsDBNull(0)) { picklistItem.BinLocation = reader.GetString(0); }
                                }

                                wrapper.ResultSet.Add(picklistItem);
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickingSequenceDetailsNone : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPickingSequenceDetailsPart(string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PicklistItem picklistItem = new PicklistItem();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPickingSequenceDetailsPart");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    picklistItem.PickingSequenceP = dReader.picking_seq;
                                    if (dReader.warehouse_code != null && dReader.room_code != null && dReader.rack_code != null)
                                    {
                                        picklistItem.BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                    }
                                    picklistItem.LicensedPalletNumber = dReader.licenced_pallet_no;
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(picklistItem);
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPickingSequenceDetailsPart : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPicklistItems(string warehouseCode, int manifestNo, int picklistNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPicklistItems");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {

                                    PicklistItem plistItem = new PicklistItem
                                    {
                                        PicklistNumber = dReader.plist_no,
                                        Name = dReader.name,
                                        Description = dReader.description,
                                        CatalogGroup = dReader.cat_group,
                                        CatalogCode = dReader.catlog_code,
                                        NewDateRequired = dReader.new_date_required,
                                        ManifestNumber = dReader.manifest_no,
                                        MixPalShedDate = dReader.mix_pal_shed_date,
                                        SourceNumber = dReader.source_no,
                                        SequenceNumber = dReader.sequence_no,
                                        PickedFullQuantity = dReader.picked_full_qty,
                                        FullPalletQuantity = dReader.pallet_qty,
                                        //LooseQuantity = dReader.reqd_qty - (dReader.pallet_qty * dReader.units_per_pallet),
                                        UnitsPerPallet = dReader.units_per_pallet,
                                        MixPalletReady = dReader.mix_pal_ready_flag,
                                        CreditStatus = dReader.credit_status,
                                        RequiredQuantity = dReader.reqd_qty
                                    };

                                    plistItem.LooseQuantity = Convert.ToInt32(Math.Round(plistItem.RequiredQuantity)) - (Convert.ToInt32(Math.Floor(plistItem.FullPalletQuantity)) * plistItem.UnitsPerPallet);

                                    plistItem.CatalogCode = plistItem.CatalogCode.Trim(); // catalog codes have a bunch of whitespace at the end

                                    if (plistItem.LooseQuantity < 0)
                                    {
                                        plistItem.LooseQuantity = 0;
                                    }
                                    wrapper.ResultSet.Add(plistItem);
                                }
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Could not find picklist items for picklist: " + picklistNo);
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPicklistItems : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPicklistPickedQuantity(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<ManifestLoadingStatus> manLoadStats = new List<ManifestLoadingStatus>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPicklistPickedQuantity");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    ManifestLoadingStatus manLoadStatus = new ManifestLoadingStatus();
                                    manLoadStatus.PicklistNumber = dReader.plist_no;
                                    manLoadStatus.CatalogCode = dReader.catlog_code;
                                    long palletUnits = reader.GetInt32(2);
                                    manLoadStatus.PickedQuantity = (int)palletUnits;
                                    manLoadStats.Add(manLoadStatus);
                                }
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(manLoadStats);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(manLoadStats);
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPicklistPickedQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetPlanNumber(int palletNo)
        {
            int planNumber = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetPlanNumber");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                planNumber = reader.GetInt32(0);
                                return planNumber;
                            }
                        }
                        return planNumber;
                    }
                }
                catch (Exception)
                {
                    return planNumber;
                }
            }
        }

        public TransactionWrapper GetTransfer(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string WHTo = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetTransfer");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                WHTo = reader.GetString(0);
                            }
                        }
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(WHTo);
                        return wrapper;

                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetTransfer : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetUnitsLeftOnPallet(int palletNo)
        {
            int unitsLeft = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetUnitsLeftOnPallet");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                unitsLeft = reader.GetInt32(0);
                                return unitsLeft;
                            }
                        }
                        return unitsLeft;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetUom(string uomPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetUom");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@Uom", OdbcType.VarChar).Value = uomPallet;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    Uom uom = new Uom();

                                    decimal conversion = dReader.conversion;
                                    uom.Conversion = decimal.ToDouble(conversion);
                                    uom.UnitOfMeasure = dReader.uom;
                                    uom.Description = dReader.description;
                                    uom.Version = dReader.version;

                                    wrapper.ResultSet.Add(uom);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUom : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper GetWarehouseRack(string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseRackByLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {

                                    WarehouseRack rack = new WarehouseRack
                                    {
                                        WarehouseCode = dReader.warehouse_code,
                                        RoomCode = dReader.room_code,
                                        RackCode = dReader.rack_code,
                                        CellCount = dReader.cell_count,
                                        ShelvesCount = dReader.shelves_count,
                                        AssignedCatalogCode = dReader.assigned_catlog_code.ToString().Trim(),
                                        ReservedCatalogCode = dReader.reserved_catlog_code.ToString().Trim(),
                                        RackDepth = dReader.rack_depth,
                                        NumberOfLevels = dReader.no_of_levels,
                                        CurrentUsedCell = dReader.current_used_cell,
                                        BatchNumber = dReader.batch_no,
                                        ZoneNumber = dReader.zone_no,
                                        BestBefore = dReader.best_before,
                                        FillingSequence = dReader.filling_seq,
                                        AssignedTime = dReader.assigned_time,
                                        LastCycleCountTime = dReader.last_cycle_count_time,
                                        SkipValidation = dReader.skip_validation,
                                        Status = dReader.status,
                                        IsleCode = dReader.isle_code,
                                        BayCode = dReader.bay_code,
                                        LevelCode = dReader.level_code,
                                        PositionCode = dReader.position_code,
                                        IsPick = dReader.ispick,
                                        ReplenishLevel = dReader.replenish_level,
                                        RackLocationCode = dReader.rack_location_code,
                                        UnitsLeft = dReader.units_left,
                                        UnitOfMeasure = dReader.uom,
                                        LicensedPalletNo = dReader.licenced_pallet_no,
                                        PickingSequence = dReader.picking_seq
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
                                    return wrapper;
                                }
                            }
                        }

                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetWarehouseRack : Cannot find details for rack " + warehouseCode + "." + roomCode + "." + rackCode);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetWarehouseRack(DateTime bestbeforeDate, string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseRackByLocationByBBDate");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestbeforeDate;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {

                                    WarehouseRack rack = new WarehouseRack
                                    {
                                        WarehouseCode = dReader.warehouse_code,
                                        RoomCode = dReader.room_code,
                                        RackCode = dReader.rack_code,
                                        CellCount = dReader.cell_count,
                                        ShelvesCount = dReader.shelves_count,
                                        AssignedCatalogCode = dReader.assigned_catlog_code.ToString().Trim(),
                                        ReservedCatalogCode = dReader.reserved_catlog_code.ToString().Trim(),
                                        RackDepth = dReader.rack_depth,
                                        NumberOfLevels = dReader.no_of_levels,
                                        CurrentUsedCell = dReader.current_used_cell,
                                        BatchNumber = dReader.batch_no,
                                        ZoneNumber = dReader.zone_no,
                                        BestBefore = dReader.best_before,
                                        FillingSequence = dReader.filling_seq,
                                        AssignedTime = dReader.assigned_time,
                                        LastCycleCountTime = dReader.last_cycle_count_time,
                                        SkipValidation = dReader.skip_validation,
                                        Status = dReader.status,
                                        IsleCode = dReader.isle_code,
                                        BayCode = dReader.bay_code,
                                        LevelCode = dReader.level_code,
                                        PositionCode = dReader.position_code,
                                        IsPick = dReader.ispick,
                                        ReplenishLevel = dReader.replenish_level,
                                        RackLocationCode = dReader.rack_location_code,
                                        UnitsLeft = dReader.units_left,
                                        UnitOfMeasure = dReader.uom,
                                        LicensedPalletNo = dReader.licenced_pallet_no,
                                        PickingSequence = dReader.picking_seq
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
                                    return wrapper;
                                }
                            }
                        }

                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetWarehouseRack : Cannot find details for rack " + warehouseCode + "." + roomCode + "." + rackCode);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public DateTime? GetBestbeforeDateByPListNo(int plistNo, string catalogCode)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetBestbeforeDateByPListNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PickListNo", OdbcType.Int).Value = plistNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    // Assuming column name is "best_before"
                                    if (!reader.IsDBNull(reader.GetOrdinal("use_by_date")))
                                    {
                                        return reader.GetDateTime(reader.GetOrdinal("use_by_date"));
                                    }
                                }
                            }

                            // Not found
                            return null;
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Log exception or rethrow
                    throw new Exception("Error in GetBestbeforeDateByPListNo: " + ex.Message, ex);
                }
            }
        }


        #endregion

        #region Insert/Update Methods

        public TransactionWrapper DeleteManifestLoadingStatus(ManifestLoadingStatus manLoadStatus)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string deleteString = PickingSQL.ResourceManager.GetString("DeleteManifestLoadingStatus");
                    using (OdbcCommand command = new OdbcCommand(deleteString, connection))
                    {
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = manLoadStatus.PicklistNumber;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = manLoadStatus.PalletNumber;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manLoadStatus.ManifestNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                        DateTime bestBefore = Convert.ToDateTime(manLoadStatus.BestBefore, culture);
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                        //command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = manLoadStatus.BestBefore;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = manLoadStatus.PalletUnits;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("DeleteManifestLoadingStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper FinaliseMixPalletDetail(ManifestLoadingStatus pallet, int manifestNo, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PickingSQL.ResourceManager.GetString("FinaliseMixPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = pallet.PicklistNumber;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@BestBeforeDay", OdbcType.Date).Value = pallet.BestBefore;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("FinaliseMixPalletDetail : Could not find pallet details for " + pallet.PalletNumber);
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("FinaliseMixPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper FinaliseNonMixPalletDetail(ManifestLoadingStatus pallet, int manifestNo, string warehouseCode, float palletQuantity)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PickingSQL.ResourceManager.GetString("FinaliseNonMixPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletQty", OdbcType.Real).Value = palletQuantity;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = pallet.PicklistNumber;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallet.CatalogCode;
                        DateTime bestBefore = Convert.ToDateTime(pallet.BestBefore, culture);
                        command.Parameters.Add("@BestBeforeDay", OdbcType.Int).Value = bestBefore.Day;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("FinaliseNonMixPalletDetail : Could not find pallet " + pallet.PalletNumber);
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("FinaliseNonMixPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertManifestLoadingStatus(ManifestLoadingStatus picked, int palletQty, int picklistNo, int manifestNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PickingSQL.ResourceManager.GetString("InsertManifestLoadingStatus");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletQty", OdbcType.Int).Value = palletQty;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = picked.PalletUnits;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = picked.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = picked.CatalogCode;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.ParseExact(picked.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertManifestLoadingStatus : Error inserting");
                        }
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertManifestLoadingStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public bool InsertPalletLocationLog(PalletLocationLog palletLocationLog)
        {
            bool isSuccess = false;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("InsertPalletLocationLog");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@MovedBy", OdbcType.VarChar).Value = palletLocationLog.MovedBy;
                        command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = palletLocationLog.NewLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletLocationLog.PalletNo;
                        command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = palletLocationLog.SyncTime;
                        command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = palletLocationLog.Timestamp;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = palletLocationLog.ManifestNo;
                        command.Parameters.Add("@Remark", OdbcType.VarChar).Value = palletLocationLog.Remark;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            isSuccess = true;
                        }

                        return isSuccess;
                    }

                }
                catch (Exception)
                {
                    return isSuccess;
                }
            }
        }

        public TransactionWrapper InsertNewPallet(PalletHeader pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int stockUpdate = 1;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PickingSQL.ResourceManager.GetString("InsertNewPallet");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@PrintedAt", OdbcType.VarChar).Value = pallet.PrintedAt;
                        command.Parameters.Add("@PrintDate", OdbcType.DateTime).Value = pallet.PrintDate;
                        command.Parameters.Add("@PlanNo", OdbcType.Int).Value = pallet.PlanNumber;
                        command.Parameters.Add("@TransferStatus", OdbcType.VarChar).Value = pallet.TransferStatus;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = pallet.WarehouseId;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = pallet.Status;
                        command.Parameters.Add("@Quality", OdbcType.VarChar).Value = pallet.Quality;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = pallet.BinLocation;
                        command.Parameters.Add("@StockUpdate", OdbcType.Int).Value = stockUpdate;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected <= 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertNewPallet : Error");
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertNewPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertPalletDetail(int palletNo, string warehouseCode, string catalogCode, int palletUnits, string bestBefore, int licensedPalletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PickingSQL.ResourceManager.GetString("InsertPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@OriginalPalletUnits", OdbcType.Int).Value = palletUnits;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletUnits;
                        if (String.IsNullOrEmpty(bestBefore))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }
                        else if (Convert.ToDateTime(bestBefore, culture) != DateTime.MinValue)
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.ParseExact(bestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = licensedPalletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertRemovedPallets(ManifestLoadingStatus manLoadStatus, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PickingSQL.ResourceManager.GetString("InsertRemovedPallets");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manLoadStatus.ManifestNumber;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = manLoadStatus.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = manLoadStatus.PicklistNumber;
                        command.Parameters.Add("@PalletQty", OdbcType.Int).Value = manLoadStatus.PickedQuantity;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = manLoadStatus.PalletUnits;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.ParseExact(manLoadStatus.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertRemovedPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetOpenPalletNumber(int manifestNo, int picklistNo, int palletNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PickingSQL.ResourceManager.GetString("SetOpenPalletNumber");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected <= 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetOpenPalletNumber : Could not find record");
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetOpenPalletNumber : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetUsedCellCount(string warehouseCode, string roomCode, string rackCode, string catalogCode, string bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string binLocation = warehouseCode + "." + roomCode + "." + rackCode;

            if (binLocation == "")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("SetUsedCellCount : Bin Location empty");
                return wrapper;
            }

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = "";

                    if (catalogCode != "")
                    {
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateAssignedCatalogCodeForRack");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.Messages.Add("UpdateAssignedCatalogCodeForRack: Complete");
                        }
                    }

                    if (!String.IsNullOrEmpty(bestBefore))
                    {
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateBestBeforeForRack");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(bestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture); 
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.Messages.Add("UpdateBestBeforeForRack: Complete");
                        }
                    }

                    int cellCount = 0;
                    int palletCount = 0;

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetCellCountFromRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                cellCount = reader.GetInt32(0);
                            }
                        }
                        wrapper.Messages.Add("GetCellCountFromRack: Complete");
                    }

                    palletCount = GetPalletCount(binLocation);
                    if (palletCount == -1)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetPalletCount : Error");
                        return wrapper;
                    }

                    if (palletCount <= 0)
                    {
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateRackToEmpty");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.Messages.Add("UpdateRackToEmpty: Complete");

                            if (rowsAffected == 0)
                            {
                                //command.Parameters.Clear();
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack");
                                rowsAffected = command.ExecuteNonQuery();
                                wrapper.Messages.Add("UpdateTempHoldRack: Complete");
                            }
                        }
                    }
                    else if (palletCount < cellCount)
                    {
                        string status = "A";
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateRackToStatus");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                            command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.Messages.Add("UpdateRackToStatus: Complete (1)");
                            if (rowsAffected == 0)
                            {
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack2");
                                command.Parameters.Clear();
                                command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                                command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                                command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                                command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;
                                rowsAffected = command.ExecuteNonQuery();
                                wrapper.Messages.Add("UpdateTempHoldRack2: Complete");
                            }
                        }
                    }
                    else if (palletCount == cellCount || palletCount > cellCount)
                    {
                        string status = "F";
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateRackToStatus");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                            command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.Messages.Add("UpdateRackToStatus: Complete (2)");

                            if (rowsAffected == 0)
                            {
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack2");
                                command.Parameters.Clear();
                                command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                                command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                                command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                                command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;
                                rowsAffected = command.ExecuteNonQuery();
                                wrapper.Messages.Add("UpdateTempHoldRack2: Complete (2)");
                            }
                        }
                    }

                    int usedCellCountForRoom = 0;

                    queryString = PutAwaySQL.ResourceManager.GetString("GetUsedCellCountForRoom");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                usedCellCountForRoom = reader.GetInt32(0);
                            }
                        }

                        wrapper.Messages.Add("GetUsedCellCountForRoom: Complete");
                    }

                    updateString = PutAwaySQL.ResourceManager.GetString("UpdateRoomFillLevel");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@CellCount", OdbcType.Int).Value = usedCellCountForRoom;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.Messages.Add("UpdateRoomFillLevel: Complete");
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetUsedCellCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateManifestLoadingStatus(float palletQuantity, int pickedQuantity, int manifestNo, int picklistNo,
                                                              int openPallet, string reservedCatalogCode, string originator,
                                                              string bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdateManifestLoadingStatus");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PalletQty", OdbcType.Real).Value = palletQuantity;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = pickedQuantity;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;
                        command.Parameters.Add("@OpenPalletNo", OdbcType.Int).Value = openPallet;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = reservedCatalogCode;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        if (String.IsNullOrEmpty(bestBefore))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }
                        else if (Convert.ToDateTime(bestBefore, culture) != DateTime.MinValue)
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.ParseExact(bestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateManifestLoadingStatus : Row not found");
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateManifestLoadingStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateManifestStatus(string status, int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdateManifestStatus");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateManifestStatus : Could not find manifest " + manifestNo);
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateManifestStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetail(int pickedQtyTemp, float palletQuantity, int palletNo, string reservedCatalogCode, string bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PalletQty", OdbcType.Int).Value = pickedQtyTemp;
                        command.Parameters.Add("@PalletUnits", OdbcType.Real).Value = palletQuantity;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = reservedCatalogCode;
                        if (String.IsNullOrEmpty(bestBefore))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }
                        else if (Convert.ToDateTime(bestBefore, culture) != DateTime.MinValue)
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = DateTime.ParseExact(bestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
                        }
                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(rowsAffected);
                        return wrapper;

                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetailUnpicked(ManifestLoadingStatus manLoadStatus, string warehouseId)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int oldPalletNumber = 0;
            int updatedRows = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PickingSQL.ResourceManager.GetString("GetOldPalletNumber");
                    using (OdbcCommand selectCommand = new OdbcCommand(queryString, connection))
                    {
                        selectCommand.Parameters.Add("@PalletNumber", OdbcType.Int).Value = manLoadStatus.PalletNumber;
                        selectCommand.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                        selectCommand.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(manLoadStatus.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture); 

                        using (OdbcDataReader reader = selectCommand.ExecuteReader())
                        {

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    oldPalletNumber = reader.GetInt32(0);
                                }
                            }
                        }
                    }
                    if (oldPalletNumber > 0)
                    {
                        string updateString = PickingSQL.ResourceManager.GetString("UpdatePalletDetailUnpicked");
                        using (OdbcCommand updateCommand = new OdbcCommand(updateString, connection))
                        {
                            updateCommand.Parameters.Add("@PalletUnits", OdbcType.Int).Value = manLoadStatus.PalletUnits;
                            updateCommand.Parameters.Add("@PalletQty", OdbcType.Int).Value = manLoadStatus.PickedQuantity;
                            updateCommand.Parameters.Add("@PalletNumber", OdbcType.Int).Value = oldPalletNumber;
                            updateCommand.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                            updateCommand.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(manLoadStatus.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);

                            updatedRows = updateCommand.ExecuteNonQuery();
                        }
                    }

                    if (updatedRows == 0)
                    {
                        string insertString = PickingSQL.ResourceManager.GetString("InsertPalletDetailUnpicked");
                        using (OdbcCommand insertCommand = new OdbcCommand(insertString, connection))
                        {
                            insertCommand.Parameters.Add("@PalletNumber", OdbcType.Int).Value = oldPalletNumber;
                            insertCommand.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseId;
                            insertCommand.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                            insertCommand.Parameters.Add("@PalletUnits1", OdbcType.Int).Value = manLoadStatus.PalletUnits;
                            insertCommand.Parameters.Add("@PalletUnits2", OdbcType.Int).Value = manLoadStatus.PalletUnits;
                            insertCommand.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(manLoadStatus.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);

                            int insertedRows = insertCommand.ExecuteNonQuery();
                        }
                    }

                    string deleteString = PickingSQL.ResourceManager.GetString("DeletePalletDetailUnpicked");
                    using (OdbcCommand deleteCommand = new OdbcCommand(deleteString, connection))
                    {
                        deleteCommand.Parameters.Add("@PalletNumber", OdbcType.Int).Value = manLoadStatus.PalletNumber;
                        deleteCommand.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;
                        deleteCommand.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(manLoadStatus.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture);

                        int deletedRows = deleteCommand.ExecuteNonQuery();
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetailUnpicked : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetailWarehouse(string whTo, int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletDetailWarehouse");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = whTo;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletDetailWarehouse : Could not update pallet " + palletNo);
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetailWarehouse : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetailWithZeroValue(ManifestLoadingStatus pallet, int manifestNo, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletDetailWithZeroValue");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = pallet.PicklistNumber;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallet.CatalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = DateTime.ParseExact(pallet.BestBefore, Common.DateFormats.ddMMyy,
                                       System.Globalization.CultureInfo.InvariantCulture); 

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetailWithZeroValue : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderPickingLabel(string pickingLabel, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected <= 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderPickingLabel : Could not find pallet");
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderPickingLAbel : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderStatus(string status, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderStatus");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@Timestamp", OdbcType.DateTime).Value = DateTime.Now;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderStatus : Could not find pallet " + palletNo);
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderUnpicked(string warehouseCode, string roomCode, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderUnpicked");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = warehouseCode + "." + roomCode + ".UNPICKED";
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int updatedRows = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderUnpicked : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderWarehouse(string warehouseCode, string status, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderWarehouse");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderWarehouse : Cannot find record for pallet " + palletNo);
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderWarehouse : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletLocationToPicked(int palletNo, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = "";
                    if (status == "")
                    {
                        updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletLocationToPicked1");
                    }
                    else
                    {
                        updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletLocationToPicked2");
                    }
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        if (status != "")
                        {
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        }

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletLocationToPicked : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletMovementInfo(string status, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletMovementInfo");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletMovementInfo : No records for pallet " + palletNo);
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletMovementInfo : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePickedQuantity(ManifestLoadingStatus manLoadStatus)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePickedQuantity");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PickingQty", OdbcType.Int).Value = manLoadStatus.PickedQuantity;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = manLoadStatus.PicklistNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = manLoadStatus.CatalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePickedQuantity : Could not find picklist detail " + manLoadStatus.PicklistNumber);
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePickedQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePickerAllocationStatus(string status, int manifestNo, int picklistNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePickerAllocationStatus");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePickerAllocationStatus : Cannot find record");
                            return wrapper;
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePickerAllocationStatus : " + e.Message);
                    return wrapper;
                }
            }
        }



        public TransactionWrapper UpdateWarehouseRoomConfig(float unitsLeft, string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdateWarehouseRoomConfig");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@UnitsLeft", OdbcType.Real).Value = unitsLeft;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateWarehouseRoomConfig : Could not find location " + warehouseCode + "." + roomCode + "." + rackCode);
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateWarehouseRoomConfig : " + e.Message);
                    return wrapper;
                }
            }
        }
        #endregion

        public TransactionWrapper GetUseByDateByPlistNoAndCatCode(int picklistNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<UseByDates> useByDates = new List<UseByDates>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PickingSQL.ResourceManager.GetString("GetUseByDateByPlistNoAndCatCode");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PicklistNo", OdbcType.Int).Value = picklistNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    UseByDates udate = new UseByDates
                                    {
                                        InvoiceNo = dReader.ivce_no,
                                        PlistNo = dReader.plist_no,
                                        CatalogCode = dReader.catlog_code,
                                        BatchNo = dReader.batch_no,
                                        PalletQty = dReader.pallet_qty,
                                        CartonQty = dReader.carton_qty
                                    };

                                    DateTime ubydate = dReader.use_by_date;
                                    udate.UseByDate = ubydate;
                                    useByDates.Add(udate);
                                }
                                wrapper.ResultSet.Add(useByDates);
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUseByDateByPlistNoAndCatCode : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePlistPalletCountAndSpaces(int plistNumber, int palletCount, int palletSpace)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePlistPalletCountAndSpaces");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                        command.Parameters.Add("@PalletSpace", OdbcType.Int).Value = palletSpace;
                        command.Parameters.Add("@plistNumber", OdbcType.Int).Value = plistNumber;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePlistPalletCountAndSpaces : Cannot find record");
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePlistPalletCountAndSpaces : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderPalletCount(int palletNumber, int palletCount)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateQuery = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderPalletCount");
                    using (OdbcCommand command = new OdbcCommand(updateQuery, connection))
                    {
                        command.Parameters.Add("@PalletCount", OdbcType.Int).Value = palletCount;
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderPalletCount : Cannot find record");
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderPalletCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetTransitWarehouses(string fromWh, string toWh)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    // Base query
                    StringBuilder queryBuilder = new StringBuilder(@"
                        SELECT 
                            from_wh, 
                            to_wh, 
                            transit_wh, 
                            production_wh, 
                            quality_wh
                        FROM 
                            transit_warehouse
                        WHERE 1=1");

                    List<OdbcParameter> parameters = new List<OdbcParameter>();
                    
                    if (!string.IsNullOrEmpty(fromWh) && !string.IsNullOrEmpty(toWh))
                    {
                        queryBuilder.Append(" AND from_wh = ?");
                        parameters.Add(new OdbcParameter { Value = fromWh });

                        queryBuilder.Append(" AND to_wh = ?");
                        parameters.Add(new OdbcParameter { Value = toWh });
                    }
                    else if (!string.IsNullOrEmpty(fromWh))
                    {
                        queryBuilder.Append(" AND from_wh = ?");
                        parameters.Add(new OdbcParameter { Value = fromWh });
                    }
                    else if (!string.IsNullOrEmpty(toWh))
                    {
                        queryBuilder.Append(" AND to_wh = ?");
                        parameters.Add(new OdbcParameter { Value = toWh });
                    }

                    using (OdbcCommand command = new OdbcCommand(queryBuilder.ToString(), connection))
                    {
                        // Add only relevant parameters
                        foreach (var param in parameters)
                        {
                            command.Parameters.Add(param);
                        }

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    TransitWarehouse transWh = new TransitWarehouse
                                    {
                                        FromWh = reader["from_wh"].ToString(),
                                        ToWh = reader["to_wh"].ToString(),
                                        TransitWh = reader["transit_wh"].ToString(),
                                        ProductionWh = reader["production_wh"].ToString(),
                                        QualityWh = reader["quality_wh"].ToString()
                                    };

                                    wrapper.ResultSet.Add(transWh);
                                }

                                wrapper.IsSuccess = true;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("No Transit Warehouses.");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error retrieving transit warehouses: " + e.Message);
                }
            }

            return wrapper;
        }
    }
}
