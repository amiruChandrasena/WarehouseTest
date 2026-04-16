using Abstractions.ServiceInterfaces;
using Common;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Globalization;
using System.Text;

namespace Services.Ingres
{
    public class BoneRoasterService : IBoneRoasterService
    {
        private readonly string connectionString;
        private CultureInfo culture;

        public BoneRoasterService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetAllPalletDetails(string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetAllPalletDetailsByWarehouse");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@warehouse_id", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@def_warehouse", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                List<PalletBRModel> palletList = new List<PalletBRModel>();
                                while (reader.Read())
                                {
                                    PalletBRModel pallet = new PalletBRModel();
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.CatalogCode = dReader.catlog_code.Trim();
                                    pallet.OriginalPalletUnits = Convert.ToInt32(dReader.orig_pallet_units);
                                    pallet.PalletUnits = Convert.ToInt32(dReader.pallet_units);
                                    //pallet.BestBefore = DateTime.ParseExact(Convert.ToString(dReader.best_before), Common.DateFormats.ddmmyyyywithouttime, CultureInfo.InvariantCulture);
                                    string bestBeforeStr = Convert.ToString(dReader.best_before);
                                    if (!string.IsNullOrEmpty(bestBeforeStr))
                                    {
                                        pallet.BestBefore = DateTime.ParseExact(bestBeforeStr, Common.DateFormats.ddmmyyyywithouttime, CultureInfo.InvariantCulture);
                                    }
                                    else
                                    {
                                        pallet.BestBefore = DateTime.MinValue; // or DateTime.Now or `null` if field is nullable
                                    }
                                    pallet.OldPalletNumber = Convert.ToInt32(dReader.old_pallet_no);
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.DaysLeft = Convert.ToInt32(dReader.days_left);
                                    pallet.BatchNumber = Convert.ToInt32(dReader.batch_no);
                                    pallet.ManifestNo = Convert.ToInt32(dReader.manifest_no);
                                    pallet.Status = dReader.status;
                                    palletList.Add(pallet);
                                }

                                //while (reader.Read())
                                //{
                                //    PalletBRModel pallet = new PalletBRModel();
                                //    pallet.PalletNumber = dReader.pallet_no?.ToString();
                                //    pallet.CatalogCode = dReader.catlog_code?.ToString();

                                //    pallet.OriginalPalletUnits = dReader.orig_pallet_units != null ? Convert.ToInt32(dReader.orig_pallet_units) : 0;
                                //    pallet.PalletUnits = dReader.pallet_units != null ? Convert.ToInt32(dReader.pallet_units) : 0;

                                //    string bestBeforeStr = Convert.ToString(dReader.best_before);
                                //    if (!string.IsNullOrEmpty(bestBeforeStr))
                                //    {
                                //        pallet.BestBefore = DateTime.ParseExact(bestBeforeStr, Common.DateFormats.ddmmyyyywithouttime, CultureInfo.InvariantCulture);
                                //    }
                                //    else
                                //    {
                                //        pallet.BestBefore = DateTime.MinValue; // or DateTime.Now or `null` if field is nullable
                                //    }

                                //    pallet.OldPalletNumber = dReader.old_pallet_no != null ? Convert.ToInt32(dReader.old_pallet_no) : 0;
                                //    pallet.BinLocation = dReader.bin_location?.ToString();
                                //    pallet.DaysLeft = dReader.days_left != null ? Convert.ToInt32(dReader.days_left) : 0;
                                //    pallet.BatchNumber = dReader.batch_no != null ? Convert.ToInt32(dReader.batch_no) : 0;
                                //    pallet.ManifestNo = dReader.manifest_no != null ? Convert.ToInt32(dReader.manifest_no) : 0;
                                //    pallet.Status = dReader.status?.ToString();

                                //    palletList.Add(pallet);
                                //}

                                wrapper.ResultSet.Add(palletList);
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetAllPalletDetails : Could not find details at " + warehouseCode);
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
                    wrapper.Messages.Add("GetAllPalletDetails : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetAllManifestsByWarehouse(string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetAllManifestsByWarehouse");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@def_warehouse", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                List<ManifestBRModel> manifestList = new List<ManifestBRModel>();
                                while (reader.Read())
                                {
                                    ManifestBRModel manifest = new ManifestBRModel();
                                    manifest.ManifestNumber = dReader.manifest_no;
                                    manifest.ManifestDate = DateTime.ParseExact(Convert.ToString(dReader.run_date), Common.DateFormats.ddmmyyyywithouttime, CultureInfo.InvariantCulture);
                                    manifestList.Add(manifest);
                                }

                                wrapper.ResultSet.Add(manifestList);
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetAllManifestsByWarehouse : Could not find details at " + warehouseCode);
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
                    wrapper.Messages.Add("GetAllManifestsByWarehouse : " + e.Message);
                    return wrapper;
                }
            }
        } 
        
        public TransactionWrapper GetOnhandQtyByCatCodeAndWarehouse(string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetOnhandQtyByCatCodeAndWarehouse");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                StockDetailBRModel stockDetails = new StockDetailBRModel();
                                while (reader.Read())
                                {

                                    stockDetails.OnHandQty = dReader.manifest_no;
                                    stockDetails.Version = dReader.run_date;
                                }

                                wrapper.ResultSet.Add(stockDetails);
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add($"GetOnhandQtyByCatCodeAndWarehouse : Could not find details at catalog {catalogCode} and warehouse {warehouseCode}");
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
                    wrapper.Messages.Add("GetOnhandQtyByCatCodeAndWarehouse : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetManifestDetailsByManifestNo(string manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetManifestDetailsByManifestNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@manifest_no", OdbcType.VarChar).Value = manifestNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            CultureInfo culture = new CultureInfo("en-AU");
                            dynamic dReader = new DynamicDataReader(reader);

                            List<ManifestDetailsBRModel> manifestDetails = new List<ManifestDetailsBRModel>();

                            if (reader.HasRows)
                            {
                                ManifestDetailsBRModel manifest;
                                while (reader.Read())
                                {
                                    manifest = new ManifestDetailsBRModel();
                                    manifest.ManifestNumber = dReader.manifest_no;
                                    manifest.CatalogCode = dReader.catlog_code;
                                    manifest.PalletNumber = Convert.ToString(dReader.pallet_no);
                                    manifest.OrigPalletUnits = Convert.ToInt32(dReader.orig_pallet_units);
                                    manifest.PalletUnits = Convert.ToInt32(dReader.pallet_units);
                                    manifest.BestBefore = DateTime.ParseExact(Convert.ToString(dReader.best_before), Common.DateFormats.ddmmyyyywithouttime, CultureInfo.InvariantCulture);
                                    //manifest.BestBefore = DateTime.ParseExact(Convert.ToString(dReader.best_before), Common.DateFormats.ddMMyy,
                                    //   System.Globalization.CultureInfo.InvariantCulture);

                                    manifest.OldPalletNumber = Convert.ToString(dReader.old_pallet_no);
                                    manifest.BinLocation = dReader.bin_location;
                                    manifest.DaysLeft = Convert.ToInt32(dReader.day_left);
                                    manifest.BatchNumber = Convert.ToString(dReader.batch_no);
                                    manifest.Status = dReader.status;
                                    manifestDetails.Add(manifest);
                                }

                                wrapper.ResultSet.Add(manifestDetails);
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetManifestDetailsByManifestNo : Could not find details at manifest - " + manifestNo);
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
                    wrapper.Messages.Add("GetManifestDetailsByManifestNo : " + e.Message);
                    return wrapper;
                }
            }
        }

        private bool UpdateUniqueKey(string keyType)
        {
            bool isSuccess = false;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdateUniqueKey");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = keyType;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            isSuccess = true;
                        }

                        return isSuccess;
                    }
                }
                catch (Exception ex)
                {
                    return isSuccess;
                }
            }
        }

        public TransactionWrapper GetUniqueKey(string keyType)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (UpdateUniqueKey(keyType))
            {
                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        string queryString = BoneRoasterSQL.ResourceManager.GetString("GetUniqueKey");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@KeyType", OdbcType.VarChar).Value = keyType;
                            
                            // Execute the query and read the updated value
                            ManifestBRModel manifest = new ManifestBRModel();

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    manifest.ManifestNumber = Convert.ToInt32(reader["unq_num"].ToString());
                                }
                            }

                            wrapper.ResultSet.Add(manifest);
                            wrapper.IsSuccess = true;
                        }
                    }
                    catch (Exception e)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetUniqueKey : " + e.Message);
                    }
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("UpdateUniqueKey : cannot update unique number");
            }

            return wrapper;
        }

        public TransactionWrapper InsertManifest(TransferPalletBRModel manifest)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("InsertManifest");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ManifestNo", OdbcType.VarChar).Value = manifest.ManifestNo;
                        command.Parameters.Add("@WarehouseTo", OdbcType.VarChar).Value = manifest.WarehouseTo;
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = manifest.UserId;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertManifest : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper UpdatePalletHeader(string warehouseCode, string status, PalletBRModel pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdatePalletHeader");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.VarChar).Value = pallet.PalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeader : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper UpdatePalletHeaderWarehouseCode(string warehouseCode, PalletBRModel pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdatePalletHeaderWarehouseCode");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@PalletNo", OdbcType.VarChar).Value = pallet.PalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderWarehouseCode : " + e.Message);
                }
            }

            return wrapper;
        }
        
        public TransactionWrapper UpdatePalletHeaderBinLocation(string binLocation, string status, PalletBRModel pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdatePalletHeaderBinLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@RackedTime", OdbcType.DateTime).Value = DateTime.Now;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.VarChar).Value = pallet.PalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderBinLocation : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper UpdatePalletDetail(string warehouseCode, PalletBRModel palletDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdatePalletDetail");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@ManifestNo", OdbcType.VarChar).Value = palletDetail.ManifestNo;
                        command.Parameters.Add("@PalletNo", OdbcType.VarChar).Value = palletDetail.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = palletDetail.CatalogCode;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetail : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper UpdateManifestStatus(int manifestNo, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdateManifestStatus");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateManifestStatus : " + e.Message);
                }
            }
            
            return wrapper;
        }

        public TransactionWrapper GetSellingCodeByCatalogCode(string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetSellingCodeByCatalogCode");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        PalletBRModel palletBRModel = new PalletBRModel();
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                palletBRModel.SellingCode = reader["selling_code"].ToString();
                            }
                        }

                        wrapper.ResultSet.Add(palletBRModel);
                        wrapper.IsSuccess = true;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetSellingCodeByCatalogCode : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper GetOnHandPreByCatalogCode(string WarehouseId, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("GetOnHandPreByCatalogCode");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = WarehouseId;

                        StockDetailBRModel stockdetail = new StockDetailBRModel();
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                stockdetail.OnHandQty = Convert.ToSingle(reader["on_hand_qty"].ToString());
                                stockdetail.Version = Convert.ToInt32(reader["version"].ToString());
                            }
                        }

                        wrapper.ResultSet.Add(stockdetail);
                        wrapper.IsSuccess = true;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetOnHandPreByCatalogCode : " + e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper UpdateStockDetailTransferQty(StockDetailBRModel stockDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdateStockDetailTransferQty");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@TransferQty", OdbcType.Double).Value = stockDetail.TransferQty;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDetail.CatalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDetail.WarehouseCode;
                        command.Parameters.Add("@Version", OdbcType.VarChar).Value = stockDetail.Version;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            wrapper.IsSuccess = true;
                        }
                        else
                        {
                            wrapper = InsertStockDetailTransferQty(stockDetail);
                            if (!wrapper.IsSuccess)
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add($"UpdateStockDetailTransferQty: Cannot insert stock detail for {stockDetail.WarehouseCode}");
                                return wrapper;
                            }
                        }

                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateStockDetailTransferQty : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertStockDetailTransferQty(StockDetailBRModel stockDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("InsertStockDetailTransferQty");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDetail.CatalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDetail.WarehouseCode;
                        command.Parameters.Add("@TransferQty", OdbcType.Double).Value = stockDetail.TransferQty;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            wrapper.IsSuccess = true;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertStockDetailTransferQty : cannot insert stock detail transfer quantity.");
                        }

                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertStockDetailTransferQty : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertStockMovementTransferPallet(StockMovementBRModel stockMove)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("InsertStockMovementTransferPallet");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockMove.CatalogCode;
                        command.Parameters.Add("@Timestamp", OdbcType.DateTime).Value = DateTime.Now.ToString(DateFormats.yyyyMMddWithTime);
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockMove.WarehouseCode;
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = stockMove.MoveQty;
                        command.Parameters.Add("@UserId", OdbcType.VarChar).Value = stockMove.UserId;
                        command.Parameters.Add("@OnHandPre", OdbcType.Double).Value = stockMove.OnHandQty;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            wrapper.IsSuccess = true;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertStockMovementTransferPallet : cannot insert stock movement.");
                        }

                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertStockMovementTransferPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateManifestWarehouseByManifetsNo(int manifestNo, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = BoneRoasterSQL.ResourceManager.GetString("UpdateManifestWarehouseByManifetsNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = manifestNo;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected > 0)
                        {
                            wrapper.IsSuccess = true;
                        }

                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateManifestWarehouseByManifetsNo : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
