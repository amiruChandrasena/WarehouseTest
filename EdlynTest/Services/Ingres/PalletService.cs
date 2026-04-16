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
using System.Threading.Tasks;
using System.Transactions;

namespace Services.Ingres
{
    public class PalletService : IPalletService
    {
        private readonly string connectionString;

        public PalletService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper CheckPalletLabelExist(string rmCatalogCode, int jobNo, string tagId, ref int rowCount, ref int tagExist)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int isJobNoMatched = 0;
            int isTagIdMatched = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("CheckJobAndTagMatch");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                        command.Parameters.Add("@RmNumber", OdbcType.VarChar).Value = rmCatalogCode;
                        command.Parameters.Add("@TagId", OdbcType.VarChar).Value = tagId;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                isJobNoMatched = reader.GetInt32(0);
                                isTagIdMatched = reader.GetInt32(1);
                            }
                        }
                    }

                    if (isJobNoMatched == 0 || isTagIdMatched == 0)
                    {
                        tagExist = 1;
                    }

                    if (!String.IsNullOrEmpty(tagId))
                    {
                        queryString = PalletSQL.ResourceManager.GetString("GetPalletLabelRows");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@TagId", OdbcType.VarChar).Value = tagId;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    rowCount = reader.GetInt32(0);
                                }
                            }
                        }

                        if (rowCount > 0)
                        {
                            string negLabel = "-" + tagId;
                            string updateString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderPickingLabel");

                            using (OdbcCommand command = new OdbcCommand(updateString, connection))
                            {
                                command.Parameters.Add("@TagId", OdbcType.VarChar).Value = tagId;

                                int rowsAffected = command.ExecuteNonQuery();
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CheckPalletLabelExist: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetAllRMPalletsByCriteria(PalletFilterCriteriaModel criteriaModel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletLabelModel> transDetails = new List<PalletLabelModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetAllRMPalletsByCriteria");

                    if (criteriaModel.PlanNo != 0)
                        queryString = queryString + " AND ph.plan_no = " + criteriaModel.PlanNo.ToString();

                    if (!String.IsNullOrEmpty(criteriaModel.CatalogCode))
                        queryString = queryString + " AND pd.catlog_code = '" + criteriaModel.CatalogCode + "'";

                    if (criteriaModel.BestBefore != DateTime.MinValue)
                        queryString = queryString + " AND pd.best_before <= " + criteriaModel.BestBefore.ToString();

                    if (criteriaModel.BestBeforeFrom != DateTime.MinValue)
                        queryString = queryString + " AND pd.best_before >= " + criteriaModel.BestBeforeFrom.ToString();

                    if (criteriaModel.PrintFrom != DateTime.MinValue)
                        queryString = queryString + " AND ph.print_date >= " + criteriaModel.PrintFrom.ToString();

                    if (criteriaModel.PrintTo != DateTime.MinValue)
                        queryString = queryString + " AND ph.print_date <= " + criteriaModel.PrintTo.ToString();

                    if (criteriaModel.Status != "" && criteriaModel.Status != "U")
                    {
                        if (criteriaModel.Status != "W")
                        {
                            queryString = queryString + " AND ph.status = '" + criteriaModel.Status + "'";
                        }
                        else
                        {
                            queryString = queryString + " AND ph.status NOT IN ('D','T','M','E')";
                        }
                    }

                    if (criteriaModel.PalletNo != 0)
                        queryString = queryString + " AND ph.pallet_no = " + criteriaModel.PalletNo.ToString();


                    if (!string.IsNullOrEmpty(criteriaModel.BatchNo))
                        queryString = queryString + " AND pd.rm_batch_no = " + criteriaModel.BatchNo;

                    if (!string.IsNullOrEmpty(criteriaModel.LvPickingLabel))
                        queryString = queryString + " AND ph.picking_label = '" + criteriaModel.LvPickingLabel + "'";

                    if (criteriaModel.QualityWh != "XX")
                        queryString = queryString + " AND ph.quality = " + criteriaModel.QualityWh;

                    if (!string.IsNullOrEmpty(criteriaModel.WarehouseCode))
                        queryString = queryString + " AND ph.warehouse_id = '" + criteriaModel.WarehouseCode + "'";

                    if (criteriaModel.LvBinLocation == "SC" && criteriaModel.WarehouseCode != "")
                    {
                        queryString = queryString + " and ph.bin_location = " + criteriaModel.LvBinLocation;
                        queryString = queryString + " and ph.warehouse_id = " + criteriaModel.WarehouseCode;
                    }
                    else if (!string.IsNullOrEmpty(criteriaModel.LvBinLocation))
                    {
                        queryString = queryString + " and ph.bin_location LIKE '" + criteriaModel.LvBinLocation + "%'";
                    }

                    queryString = queryString + " ORDER BY pd.pallet_no , pd.catlog_code, pd.best_before";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        //command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        //command.Parameters.Add("@FromWarehouse", OdbcType.VarChar).Value = defaultwarehouse;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PalletLabelModel palletLabel = new PalletLabelModel();
                                    palletLabel.CatalogCode = dReader.catlog_code;
                                    palletLabel.PrintDate = dReader.print_date;
                                    palletLabel.Description = dReader.description;
                                    palletLabel.PalletNumber = dReader.pallet_no;
                                    //palletLabel.OldPalletNumber = dReader.old_pallet_no;
                                    palletLabel.PlanNumber = dReader.plan_no;
                                    //palletLabel.LineNumber = dReader.line_no;
                                    palletLabel.Quality = dReader.quality;
                                    DateTime bestBefore = dReader.best_before;
                                    if (bestBefore.Year < 9999)
                                    {
                                        palletLabel.BestBefore = bestBefore.ToString(Common.DateFormats.ddmmyyyywithouttime);
                                    } else
                                    {
                                        palletLabel.BestBefore = "";
                                    }

                                    //palletLabel.BestBefore = dReader.best_before;
                                    palletLabel.Status = dReader.status;
                                    palletLabel.BatchNumber = dReader.batch_no;
                                    palletLabel.WarehouseId = dReader.warehouse_id;
                                    palletLabel.PalletUnitsRm = dReader.pallet_units;
                                    palletLabel.StockQuantity = Math.Round(dReader.stock_qty, Common.Common.decimalPlaces);
                                    //palletLabel.OriginalPalletUnits = dReader.orig_pallet_units;
                                    palletLabel.BinLocation = dReader.bin_location;
                                    palletLabel.DaysOld = (DateTime.Now - palletLabel.PrintDate).Days;
                                    if (!String.IsNullOrEmpty(palletLabel.BestBefore))
                                    {
                                        palletLabel.DaysLeft = (Convert.ToDateTime(bestBefore) - DateTime.Now).Days;
                                    }
                                    palletLabel.Uom = dReader.uom_stock;
                                    if (palletLabel.BinLocation == "SC")
                                    {
                                        palletLabel.StockCount = "Yes";
                                    }

                                    transDetails.Add(palletLabel);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("No pallets found or pallet already issued.");
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
            wrapper.ResultSet.Add(transDetails);
            wrapper.IsSuccess = true;
            return wrapper;
        }

        public TransactionWrapper GetRMPalletNumberbyScanPalletLabel(string scanPalletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletLabelModel palletLabel = new PalletLabelModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetRMPalletNumberbyScanPalletLabel");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@picking_label", OdbcType.VarChar).Value = scanPalletNumber;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletLabel.PalletNumber = dReader.pallet_no;
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMPalletNumberByScanPalletLabel: No pallet found for picking label " + scanPalletNumber);
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMPalletNumbersByScanPalletLabel: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(palletLabel);
            return wrapper;
        }

        public TransactionWrapper GetPalletDetail(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletHeader pallet = new PalletHeader();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailRM");
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

                                    PalletDetail palletDetail = new PalletDetail();
                                    CatalogItem catalogItem = new CatalogItem();

                                    pallet.Status = reader.GetString(0);
                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                    try
                                    {
                                        if (dReader.best_before != null)
                                        {
                                            palletDetail.BestBefore = Common.DateFormats.ParseDateWithoutTime(Convert.ToString(dReader.best_before));
                                        }
                                    }
                                    catch {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("GetPalletDetails: No best before date for pallet " + palletNo.ToString());
                                        return wrapper;
                                    }

                                    palletDetail.PalletUnits = dReader.pallet_units;
                                    palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                    catalogItem.CatalogCode = dReader.catlog_code;
                                    catalogItem.Description = dReader.description;
                                    palletDetail.CatalogItem = catalogItem;

                                    pallet.PalletDetails.Add(palletDetail);
                                }

                                if (pallet.BinLocation != "" && pallet.BinLocation != null)
                                {
                                    string roomType = "";
                                    string[] binLocationParts = pallet.BinLocation.Split('.');

                                    if (binLocationParts.Length == 3)
                                    {
                                        GetRoomType(ref roomType, binLocationParts[0], binLocationParts[1]);
                                        pallet.RoomType = roomType;

                                        if (pallet.RoomType != "")
                                        {
                                            string rackedTime = "";
                                            GetStageOneRackedTime(ref rackedTime, roomType, pallet.PalletNumber);
                                            pallet.StageOneRackedTime = rackedTime;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetails: No pallet found for " + palletNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (CustomException e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetails: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(pallet);
            return wrapper;
        }

        public TransactionWrapper GetPalletDetailForJobIssueSave(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletHeader pallet = new PalletHeader();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailRM");
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

                                    PalletDetail palletDetail = new PalletDetail();
                                    CatalogItem catalogItem = new CatalogItem();

                                    pallet.Status = reader.GetString(0);
                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                    try
                                    {
                                        palletDetail.BestBefore = Common.DateFormats.ParseDateWithoutTime(Convert.ToString(dReader.best_before));
                                    }
                                    catch { }

                                    palletDetail.PalletUnits = dReader.pallet_units;
                                    palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                    catalogItem.CatalogCode = dReader.catlog_code;
                                    catalogItem.Description = dReader.description;
                                    palletDetail.CatalogItem = catalogItem;

                                    pallet.PalletDetails.Add(palletDetail);
                                }

                                if (pallet.BinLocation != "" && pallet.BinLocation != null)
                                {
                                    string roomType = "";
                                    string[] binLocationParts = pallet.BinLocation.Split('.');

                                    if (binLocationParts.Length == 3)
                                    {
                                        GetRoomType(ref roomType, binLocationParts[0], binLocationParts[1]);
                                        pallet.RoomType = roomType;

                                        if (pallet.RoomType != "")
                                        {
                                            string rackedTime = "";
                                            GetStageOneRackedTime(ref rackedTime, roomType, pallet.PalletNumber);
                                            pallet.StageOneRackedTime = rackedTime;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetails: No pallet found for " + palletNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (CustomException e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetails: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(pallet);
            return wrapper;
        }

        public TransactionWrapper GetPalletDetailForIssuePallet(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletHeader pallet = new PalletHeader();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailRM");
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

                                    PalletDetail palletDetail = new PalletDetail();
                                    CatalogItem catalogItem = new CatalogItem();

                                    pallet.Status = reader.GetString(0);
                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                    try
                                    {
                                        palletDetail.BestBefore = Common.DateFormats.ParseDateWithoutTime(Convert.ToString(dReader.best_before));
                                    }
                                    catch { }
                                    palletDetail.PalletUnits = dReader.pallet_units;
                                    palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                    catalogItem.CatalogCode = dReader.catlog_code;
                                    catalogItem.Description = dReader.description;
                                    palletDetail.CatalogItem = catalogItem;

                                    pallet.PalletDetails.Add(palletDetail);
                                }

                                if (pallet.BinLocation != "" && pallet.BinLocation != null)
                                {
                                    string roomType = "";
                                    string[] binLocationParts = pallet.BinLocation.Split('.');

                                    if (binLocationParts.Length == 3)
                                    {
                                        GetRoomType(ref roomType, binLocationParts[0], binLocationParts[1]);
                                        pallet.RoomType = roomType;

                                        if (pallet.RoomType != "")
                                        {
                                            string rackedTime = "";
                                            GetStageOneRackedTime(ref rackedTime, roomType, pallet.PalletNumber);
                                            pallet.StageOneRackedTime = rackedTime;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetails: No pallet found for " + palletNo.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (CustomException e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetails: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(pallet);
            return wrapper;
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

        public TransactionWrapper GetPalletNoRMByPickingLabel(int pickingLable)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PalletHeader pallet = new PalletHeader();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletNoRMByPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        //command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLable;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    pallet.PalletNumber = dReader.pallet_no;
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletNoRMByPalletNoOrPalletLabel: No pallet found for " + pickingLable.ToString());
                                return wrapper;
                            }
                        }
                    }
                }
                catch (CustomException e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletNoRMByPalletNoOrPalletLabel: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(pallet);
            return wrapper;
        }

        public TransactionWrapper GetPalletNosInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<int> palletNumbers = new List<int>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetPalletNosInRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                palletNumbers.Add(reader.GetInt32(0));
                            }
                        }
                    }
                }
                catch
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletNosInRack: Error");
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(palletNumbers);
            return wrapper;
        }

        public string GetRoomType(ref string roomType, string warehouseCode, string roomCode)
        {
            string returnMessage = "GetRoomType : ";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetRoomType");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { roomType = reader.GetString(0); }
                                if (roomType == "")
                                {
                                    returnMessage += "No room type found";
                                }
                                else
                                {
                                    returnMessage += "Successful";
                                }
                            }
                        }
                        return returnMessage;
                    }
                }
                catch (Exception e)
                {
                    returnMessage += "Failed : " + e.Message;
                    return returnMessage;
                }
            }
        }

        public string GetStageOneRackedTime(ref string rackedTime, string roomType, int palletNo)
        {
            string returnMessage = "GetStageOneRackedTime : ";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetStageOneRackedTime");
                    queryString += "'%" + roomType + "%'";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        //command.Parameters.Add("@RoomTypeLike", OdbcType.VarChar).Value = roomTypeLike;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { rackedTime = reader.GetDateTime(0).ToString(); }
                                if (rackedTime == "" || rackedTime is null)
                                {
                                    returnMessage += "No stage one racked time found";
                                }
                                else
                                {
                                    returnMessage += "Successful";
                                }

                            }
                        }
                        return returnMessage;
                    }
                }
                catch (Exception e)
                {
                    returnMessage += "Failed : " + e.Message;
                    return returnMessage;
                }
            }
        }

        public TransactionWrapper InsertPalletHeader(string Originator, PalletHeader palletHeader)
        {
            int PalletNo = 0;

            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = CreateNewPalletNo(ref PalletNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }
            
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    int stk_update = GetPalletStockUpdByPalletNo(PalletNo); 

                    connection.Open();

                    if (stk_update == 0)
                    {
                        string insertString = PalletSQL.ResourceManager.GetString("InsertPalletHeader");
                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;
                            command.Parameters.Add("@PrintedAt", OdbcType.Int).Value = "HH-" + Originator;
                            command.Parameters.Add("@PrintedDate", OdbcType.Int).Value = DateTime.Now.ToString();
                            command.Parameters.Add("@PlanNo", OdbcType.VarChar).Value = -3;
                            command.Parameters.Add("@TransferStatus", OdbcType.Int).Value = "P";
                            command.Parameters.Add("@WarehouseId", OdbcType.Date).Value = palletHeader.WarehouseId;
                            command.Parameters.Add("@Status", OdbcType.Int).Value = "W";
                            command.Parameters.Add("@Quality", OdbcType.Int).Value = "G";
                            command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = "PICKED";
                            command.Parameters.Add("@PickingLabel", OdbcType.Int).Value = "";

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                    else
                    {
                        string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeader");
                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = "PICKED";
                            command.Parameters.Add("@ConfirmInd", OdbcType.Int).Value = palletHeader.ConfirmInd;
                            command.Parameters.Add("@Status", OdbcType.Int).Value = palletHeader.Status;
                            command.Parameters.Add("@RackedTime", OdbcType.Int).Value = palletHeader.RackedTime;
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper CreateNewPalletNo(ref int maxPalletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetMaxPalletNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                maxPalletNo = reader.GetInt32(0);
                            }
                        }
                    }

                    maxPalletNo = maxPalletNo + 1;
                    string insertString = PalletSQL.ResourceManager.GetString("CreateNewPalletNo");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@NewPalletNo", OdbcType.Int).Value = maxPalletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            return wrapper;
        }

        public int GetPalletStockUpdByPalletNo(int PalletNo)
        {
            int stk_update= 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetPalletStockUpdByPalletNo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                stk_update = reader.GetInt32(0);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                }
            }

            return stk_update;
        }

        public TransactionWrapper CloseTranferOpenPallet(string Originator, int TransferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("CloseTranferOpenPallet");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = TransferNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CloseTranferOpenPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderLabelforClosePallet(int palletNo, string tagId)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderLabelforClosePallet");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@TagId", OdbcType.VarChar).Value = tagId;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CloseTranferOpenPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletQuantity(int PalletNo, double StockQty, double IssueQty, string CatalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    if (CatalogCode == "")
                    {
                        string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletQuantityWithoutCatalogCode");

                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@PalletQty", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@StockQty", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                    else
                    {
                        string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletQuantityWithCatalogCode");

                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@PalletQty", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@StockQty", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = IssueQty;
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = CatalogCode;

                            int rowsAffected = command.ExecuteNonQuery();
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderRMforFullIssue(int PalletNo, string WarehouseCode, string Originator, string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderRMforFullIssue");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = WarehouseCode;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = Originator;
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderRMForFullIssue : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetailRMforFullIssue(int PalletNo, string WarehouseCode, string ManifestNo, double IssueQty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletDetailRMForFullIssue");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = WarehouseCode;
                        command.Parameters.Add("@ManifestNo", OdbcType.VarChar).Value = ManifestNo;
                        command.Parameters.Add("@OrigPalletUnits", OdbcType.Double).Value = IssueQty;
                        command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = IssueQty;
                        command.Parameters.Add("@StockQty", OdbcType.Double).Value = IssueQty;
                        command.Parameters.Add("@PalletQty", OdbcType.Double).Value = IssueQty;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletDetailRMForFullIssue : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderStatusByPalletNo(int PalletNo, string Status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderStatusByPalletNo");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = Status;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderStatusByPalletNo : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper DeleteEmptyPallet(int PalletNo)
        {
            double Lf_Qty = 1;
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetPalletStockQuantity");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Lf_Qty = Math.Round(reader.GetDouble(0), 3);
                            }
                        }
                    }

                    if (Lf_Qty == 0)
                    {
                        string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderStatusByPalletNo");

                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = "D";
                            command.Parameters.Add("@PalletNo", OdbcType.Int).Value = PalletNo;

                            int rowsAffected = command.ExecuteNonQuery();
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetAllRMPalletDetailsByOldNewPalletNo(int oldPalletNo, int newPalletNo)

        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletDetail> transDetails = new List<PalletDetail>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetAllRMPalletDetailsByOldNewPalletNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@NewPalletNo", OdbcType.Int).Value = newPalletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PalletDetail palletDetails = new PalletDetail();
                                    palletDetails.PalletNumber = dReader.pallet_no;
                                    palletDetails.OldPalletNumber = dReader.old_pallet_no;
                                    palletDetails.CatalogCode = dReader.catlog_code;
                                    //palletDetails.CatalogDescription = dReader;
                                    //palletDetails.BatchNumber = dReader;
                                    palletDetails.BestBefore = dReader.best_before;
                                    palletDetails.OriginalPalletUnits = dReader.orig_pallet_units;
                                    //palletDetails.PalletQuantity = dReader;
                                    palletDetails.PalletUnits = dReader.pallet_units;
                                    //palletDetails.PicklistNumber = dReader;
                                    transDetails.Add(palletDetails);
                                }
                            } else
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllRMPalletDetailsByOldNewPalletNo: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(transDetails);
            return wrapper;
        }

        public TransactionWrapper InsertPalletDetails(PalletDetail details)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("InsertPalletDetails");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = details.CatalogCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = details.PalletNumber;
                        command.Parameters.Add("@OrigPalletUnits", OdbcType.Double).Value = details.OriginalPalletUnits;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = details.PalletUnits;
                        if ((details.BestBefore.Year > 2000) && (details.BestBefore.Year < 9999))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = details.BestBefore.ToString(DateFormats.MMddyy);
                        } else
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = "";
                        }
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = details.WarehouseId;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = details.OldPalletNumber;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = details.BatchNumber;
                        command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = details.ManifestNo;
                        command.Parameters.Add("@StockQty", OdbcType.Double).Value = details.StockQty;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletDetails : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertPalletDetailsForIssue(PalletDetail palletDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("InsertPalletDetailsForIssue");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = palletDetail.CatalogCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletDetail.PalletNumber;
                        command.Parameters.Add("@OrigPalletUnits", OdbcType.Double).Value = palletDetail.IssueQty;
                        command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = palletDetail.IssueQty;
                        if ((palletDetail.BestBefore.Year > 2000) && (palletDetail.BestBefore.Year < 9999))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = palletDetail.BestBefore.ToString(DateFormats.ddmmyyyywithouttime);//DateFormats.MMddyy
                        }
                        else
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = "";
                        }

                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = palletDetail.WarehouseId;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = palletDetail.OldPalletNumber;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = palletDetail.BatchNumber;
                        command.Parameters.Add("@PalletQty", OdbcType.Double).Value = palletDetail.IssueQty;
                        command.Parameters.Add("@StockQty", OdbcType.Double).Value = palletDetail.IssueQty;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletDetailsForIssue : " + e.Message);
                    return wrapper;
                }
            }
        }
    

        public TransactionWrapper UpdatePalletDetails(PalletDetail details)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletUnitsByOldAndNewPalletNo");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = details.IssueQty;
                        command.Parameters.Add("@OriginalPalletUnits", OdbcType.Double).Value = details.OriginalPalletUnits + details.IssueQty;
                        command.Parameters.Add("@PalletQuantity", OdbcType.Double).Value = details.PalletQuantity;
                        command.Parameters.Add("@OldPalletNumber", OdbcType.Int).Value = details.OldPalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletQuantity : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletHeaderWarehouseAndBinLocation(PalletHeader pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("UpdatePalletHeaderWarehouseAndBinLocation");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = pallet.WarehouseId;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = pallet.BinLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.VarChar).Value = pallet.PalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public float GetActualPalletUnitsByJobNo(int jobNo)
        {
            float units = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetActualPalletUnitsByJobNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                        //command.Parameters.Add("@RoomTypeLike", OdbcType.VarChar).Value = roomTypeLike;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    units = dReader.pallet_no;
                                }
                            }
                        }

                        return units;
                    }
                }
                catch (Exception e)
                {
                    return units;
                }
            }
        }

        public TransactionWrapper CreatePalletHeader(PalletHeader palletHeader, ref int newPalletNumber)
        {
            int rowsAffected = 0;

            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = CreateNewPalletNo(ref newPalletNumber);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PalletSQL.ResourceManager.GetString("CreateNewPallet");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = newPalletNumber;
                        command.Parameters.Add("@PrintedAt", OdbcType.VarChar).Value = palletHeader.PrintedAt;
                        command.Parameters.Add("@PrintedDate", OdbcType.DateTime).Value = palletHeader.PrintDate.ToString();
                        command.Parameters.Add("@PlanNo", OdbcType.Int).Value = palletHeader.PlanNumber;
                        command.Parameters.Add("@TransferStatus", OdbcType.VarChar).Value = palletHeader.TransferStatus;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = palletHeader.WarehouseId;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = palletHeader.Status;
                        command.Parameters.Add("@Quality", OdbcType.VarChar).Value = palletHeader.Quality;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = palletHeader.BinLocation;
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = palletHeader.PickingLabel;

                        rowsAffected = command.ExecuteNonQuery();
                    }

                    if (rowsAffected > 0)
                    {
                        foreach (PalletDetail det in palletHeader.PalletDetails)
                        {
                            det.PalletNumber = newPalletNumber;
                            TransactionWrapper twPalletDetStatus = InsertPalletDetails(det);
                            if (!twPalletDetStatus.IsSuccess)
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add($"CreatePalletDetail : Couldn't insert pallet detail {palletHeader.PalletNumber}");
                                return wrapper;
                            }
                        }
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"CreatePalletHeader : Couldn't insert pallet header {palletHeader.PalletNumber}");
                        return wrapper;
                    }
                    
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CreatePalletHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetOpenPalletNumber(int palletNumber, int transferNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = PalletSQL.ResourceManager.GetString("SetOpenPalletNumber");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;
                        command.Parameters.Add("@TransferNumber", OdbcType.Int).Value = transferNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetOpenPalletNumber: Could not update open pallet number of transfer " + transferNumber.ToString());
                            return wrapper;
                        } else
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetOpenPalletNumber: " + e.Message);
                    return wrapper;
                }
            }
        }

    }
}
