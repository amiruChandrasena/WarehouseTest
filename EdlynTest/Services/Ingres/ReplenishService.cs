using Microsoft.Extensions.Configuration;
using Abstractions.ServiceInterfaces;
using System;
using System.Collections.Generic;
using System.Text;
using Models;
using System.Data.Odbc;
using Services.Ingres.SQLResources;
using Models.Utility;
using System.Linq;
using System.Globalization;

namespace Services.Ingres
{
    public class ReplenishService : IReplenishService
    {
        private readonly string connectionString;
        private CultureInfo culture;

        public ReplenishService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
            culture = new CultureInfo("en-AU");
        }

        public TransactionWrapper CheckIsPick(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int isPick = 0;
            string catalogCode = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("CheckIsPick");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    isPick = reader.GetInt32(0);
                                    catalogCode = reader.GetString(1);
                                }
                            }
                        }
                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(isPick);
                        wrapper.ResultSet.Add(catalogCode);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CheckIsPick : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper CheckProductsInRack(string warehouseCode, string roomCode, string rackCode, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string binLocation = warehouseCode + "." + roomCode + "." + rackCode;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("CheckProductsInRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string rackCatalogCode = reader.GetString(0);
                                if (!rackCatalogCode.Trim().Equals(catalogCode.Trim()))
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add(binLocation + "already has " + rackCatalogCode + ". Cannot move.");
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
                    wrapper.Messages.Add("CheckProductsInRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper DeleteEmptyPalletDetail(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string deleteString = PutAwaySQL.ResourceManager.GetString("DeleteEmptyPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(deleteString, connection))
                    {
                        command.Parameters.Add("PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected != -1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("DeleteEmptyPalletDetail : Error");
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("DeleteEmptyPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetAssignedLocationAndPickingSequence(string warehouseCode, string roomCode, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string assignedLocation = "";
            int pickingSequence = 0;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetAssignedLocationAndPickingSequence");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    assignedLocation = reader.GetString(0);
                                    pickingSequence = reader.GetInt32(1);
                                }

                                wrapper.ResultSet.Add(assignedLocation);
                                wrapper.ResultSet.Add(pickingSequence);
                                
                                wrapper.IsSuccess = true;
                            }
                        }
                        
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAssignedLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetBinLocations(string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<ReplenishLocation> repLocations = new List<ReplenishLocation>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetBinLocations");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);

                                while (reader.Read())
                                {
                                    ReplenishLocation repLocation = new ReplenishLocation();
                                    repLocation.WarehouseCode = dReader.warehouse_code;
                                    repLocation.RoomCode = dReader.room_code;
                                    repLocation.RackCode = dReader.rack_code;
                                    repLocation.BestBefore = dReader.best_before;
                                    repLocation.AssignedCatlogCode = dReader.assigned_catlog_code.ToString().Trim();
                                    repLocation.ReplenishLevel = dReader.replenish_level;
                                    repLocation.UnitsLeft = dReader.units_left;
                                    repLocation.Required = reader.GetInt32(7);
                                    repLocation.BinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                    repLocations.Add(repLocation);
                                }
                                repLocations = repLocations.OrderByDescending(r => r.Required).ToList();
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(repLocations);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetBinLocations : None found");
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetBinLocations : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetBinLocationsToday(string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<ReplenishLocation> repLocations = new List<ReplenishLocation>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetBinLocationsToday");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    ReplenishLocation repLocation = new ReplenishLocation();
                                    repLocation.WarehouseCode = warehouseCode;
                                    repLocation.RoomCode = roomCode;
                                    repLocation.RackCode = dReader.rack_code;
                                    repLocation.BestBefore = dReader.best_before;
                                    repLocation.AssignedCatlogCode = dReader.assigned_catlog_code.ToString().Trim();
                                    repLocation.ReplenishLevel = dReader.replenish_level;
                                    repLocation.UnitsLeft = dReader.units_left;
                                    repLocation.OrderQuantity = reader.GetInt32(7);
                                    repLocation.RequiredFull = reader.GetInt32(8);
                                    repLocation.OrderQuantityLoose = reader.GetInt32(9);
                                    repLocation.BinLocation = warehouseCode + "." + roomCode + "." + dReader.rack_code;

                                    if (repLocation.UnitsLeft < repLocation.OrderQuantityLoose + repLocation.ReplenishLevel)
                                    {
                                        repLocation.Required = repLocation.OrderQuantityLoose + repLocation.ReplenishLevel - Convert.ToInt32(repLocation.UnitsLeft);
                                    }

                                    repLocations.Add(repLocation);
                                }
                                repLocations = repLocations.OrderByDescending(r => r.Required).ToList();
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(repLocations);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetBinLocationsToday : None found");
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetBinLocationsToday : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetNextSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            bestBefore = bestBefore.Replace('-', '/');
            DateTime bestBeforeDate = Convert.ToDateTime(bestBefore, culture);
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetNextSuggestedRackOne");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBeforeDate;
                        command.Parameters.Add("@PickingSequence", OdbcType.Int).Value = pickingSequence;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    ReplenishItem replenishItem = new ReplenishItem();
                                    replenishItem.BestBefore = reader.GetDate(0);
                                    wrapper.Messages.Add("Got best before date");
                                    //replenishItem.PickingSequence = dReader.picking_seq;
                                    replenishItem.PickingSequence = reader.GetInt32(1);
                                    replenishItem.PalletUnits = reader.GetInt32(3);
                                    //replenishItem.PalletUnits = dReader.pallet_units;
                                    replenishItem.BinLocation = dReader.bin_location;
                                    wrapper.ResultSet.Add(replenishItem);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                    }

                    queryString = ReplenishSQL.ResourceManager.GetString("GetNextSuggestedRackTwo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBeforeDate;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    ReplenishItem replenishItem = new ReplenishItem();
                                    replenishItem.BestBefore = reader.GetDate(0);
                                    //replenishItem.PickingSequence = dReader.picking_seq;
                                    replenishItem.PickingSequence = reader.GetInt32(1);
                                    replenishItem.PalletUnits = reader.GetInt32(3);
                                    //replenishItem.PalletUnits = dReader.pallet_units;
                                    replenishItem.BinLocation = dReader.bin_location;
                                    wrapper.ResultSet.Add(replenishItem);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("No next bin location found");
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetNextSuggestedRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public string GetPalletCount(ref int palletCount, string binLocation)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                string returnString = "GetPalletCount : ";

                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                palletCount = reader.GetInt32(0);
                            }
                        }
                    }

                    returnString += "Successful";
                    return returnString;
                }
                catch (Exception e)
                {
                    palletCount = -1;
                    returnString += "GetPalletCount : " + e.Message;
                    return returnString;
                }
            }
        }

        public TransactionWrapper GetPalletDetail(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            PalletHeader pallet = new PalletHeader();
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {

                                    PalletDetail palletDetail = new PalletDetail();
                                    CatalogItem catalogItem = new CatalogItem();

                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                    pallet.BestBefore = dReader.best_before;
                                    pallet.RoomType = dReader.room_type;
                                    pallet.RoomTypeStageTwo = dReader.room_type_stage2;//Add By Irosh 2023/05/11
                                    palletDetail.PalletUnits = dReader.pallet_units;
                                    //palletDetail.BestBefore = dReader.best_before;
                                    palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                    palletDetail.PalletNumber = dReader.pallet_no;
                                    palletDetail.CatalogCode = dReader.catlog_code;
                                    catalogItem.CatalogCode = dReader.catlog_code;
                                    catalogItem.CatalogCode = catalogItem.CatalogCode.Trim();
                                    catalogItem.Description = dReader.description;
                                    catalogItem.RoomType = dReader.room_type;
                                    catalogItem.TimeToMature = dReader.time_to_mature;
                                    catalogItem.MaxWaitingHours = dReader.max_waiting_hrs;
                                    catalogItem.StageOneRoomHours = dReader.stage1_room_hrs;
                                    catalogItem.LabApprovalAfterStageOne = dReader.lab_approval_after_stage1;
                                    catalogItem.RoomTypeStageTwo = dReader.room_type_stage2;
                                    catalogItem.StageTwoRoomHours = dReader.stage2_room_hrs;
                                    catalogItem.ShelfLife = dReader.shelf_life;

                                    palletDetail.CatalogItem = catalogItem;
                                    pallet.PalletDetails.Add(palletDetail);
                                }

                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(pallet);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetail : Could not find details for pallet number " + palletNo);
                                return wrapper;
                            }
                        }
                    }
                } 
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletDetailForUpdate(int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailForUpdate");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
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
                                        BatchNumber = dReader.batch_no,
                                        BestBefore = dReader.best_before,
                                        PalletUnits = dReader.pallet_units
                                    };

                                    wrapper.ResultSet.Add(palletDetail);
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
                    wrapper.Messages.Add("GetPalletDetailForUpdate : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetPalletUnits(int palletNo)
        {
            int units = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletUnits");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                units = reader.GetInt32(0);
                            }
                        }   
                        return units;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetPreviousSuggestedRack(string warehouseCode, string roomCode, string catalogCode, string bestBefore, int pickingSequence)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            bestBefore = bestBefore.Replace('-', '/');
            DateTime bestBeforeDate = Convert.ToDateTime(bestBefore, culture);
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetPreviousSuggestedRackOne");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBeforeDate;
                        command.Parameters.Add("@PickingSequence", OdbcType.Int).Value = pickingSequence;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    ReplenishItem replenishItem = new ReplenishItem();
                                    replenishItem.BestBefore = reader.GetDate(0);
                                    //replenishItem.PickingSequence = dReader.picking_seq;
                                    replenishItem.PickingSequence = reader.GetInt32(1);
                                    replenishItem.PalletUnits = reader.GetInt32(3);
                                    //replenishItem.PalletUnits = dReader.pallet_units;
                                    replenishItem.BinLocation = dReader.bin_location;
                                    wrapper.ResultSet.Add(replenishItem);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                    }

                    queryString = ReplenishSQL.ResourceManager.GetString("GetPreviousSuggestedRackTwo");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBeforeDate;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    ReplenishItem replenishItem = new ReplenishItem();
                                    replenishItem.BestBefore = reader.GetDate(0);
                                    //replenishItem.PickingSequence = dReader.picking_seq;
                                    replenishItem.PickingSequence = reader.GetInt32(1);
                                    replenishItem.PalletUnits = reader.GetInt32(3);
                                    //replenishItem.PalletUnits = dReader.pallet_units;
                                    replenishItem.BinLocation = dReader.bin_location;
                                    wrapper.ResultSet.Add(replenishItem);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("No previous bin location found");
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPreviousSuggestedRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public int GetProductCountOnPallet(string catalogCode, int palletNo)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetProductCountOnPallet");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

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
                catch
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetRackCount(string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetRackCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                count = reader.GetInt32(0);
                            }
                        }
                        wrapper.ResultSet.Add(count);
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRackCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetRackLicensedPalletAndCatalogCode(ref int licensedPalletNo, ref string reservedCatalogCode,
                                                                       string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetRackLicensedPalletAndCatalogCode");
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
                                    if (!reader.IsDBNull(0)) { licensedPalletNo = dReader.licenced_pallet_no; }
                                    if (!reader.IsDBNull(1)) { reservedCatalogCode = dReader.reserved_catlog_code.ToString().Trim(); }
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
                    wrapper.Messages.Add("GetRackLicensedPalletAndCatalogCode : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetReplenishItemDetails(string catalogCode, string warehouseCode, string roomCode, bool isPick)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            ReplenishItem replenishItem = new ReplenishItem();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = ReplenishSQL.ResourceManager.GetString("GetReplenishItemDetails");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode1", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode.Trim();
                        command.Parameters.Add("@WarehouseCode2", OdbcType.VarChar).Value = warehouseCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    replenishItem.CatalogCode = catalogCode;
                                    replenishItem.BestBefore = reader.GetDateTime(0);
                                    replenishItem.PickingSequence = reader.GetInt32(1);
                                    replenishItem.BinLocation = reader.GetString(2);
                                    replenishItem.PalletUnits = reader.GetInt32(3);
                                    replenishItem.RoomType = reader.GetString(4);
                                    replenishItem.IsPick = isPick;

                                    wrapper.ResultSet.Add(replenishItem);
                                    wrapper.IsSuccess = true;
                                    return wrapper;
                                }

                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetReplenishItemDetails : This should be unreachable");
                                return wrapper;
                            }
                            else
                            {
                                replenishItem.CatalogCode = catalogCode;
                                replenishItem.IsPick = isPick;
                                wrapper.ResultSet.Add(replenishItem);
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetReplenishItemDetails : " + e.Message);
                    return wrapper;
                }
            }
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
                    returnMessage += "GetRoomType : " + e.Message;
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
                    returnMessage += "GetStageOneRackedTime : " + e.Message;
                    return returnMessage;
                }
            }
        }

        public TransactionWrapper GetWarehouseConfig(string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseConfig");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    WarehouseConfig whConfig = new WarehouseConfig
                                    {
                                        Description = dReader.description,
                                        FillLevel = dReader.fill_level,
                                        LocationCode = dReader.location_code,
                                        MaxCapacity = dReader.max_capasity, // (sic)
                                        ProductionArea = dReader.production_area,
                                        Type = dReader.type,
                                        WarehouseId = dReader.warehouse_id,
                                        WhRoomCode = dReader.whroom_code,
                                        SkipValidation = dReader.skip_validation,
                                        AssignedPersonOne = dReader.assigned_person1,
                                        AssignedPersonTwo = dReader.assigned_person2
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(whConfig);
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetWarehouseConfig : Could not find warehouse config details for " + warehouseCode + "."
                                                + roomCode);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseConfig : " + e.Message);
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

                                    // get pallet count for rack
                                    string binLocation = warehouseCode + "." + roomCode + "." + rackCode;
                                    int palletCount = 0;
                                    wrapper.Messages.Add(GetPalletCount(ref palletCount, binLocation));
                                    wrapper.Messages.Clear();
                                    rack.PalletCount = palletCount;

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
                                    return wrapper;
                                }
                            }
                        }
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No details found for rack location :" + warehouseCode + "." + roomCode + "." + rackCode);
                        return wrapper;

                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseRack : " + e.Message);
                    wrapper.ResultSet.Clear();
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertPalletDetail(int licensedPalletNo, string catalogCode, int palletUnits, PalletHeader pallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = ReplenishSQL.ResourceManager.GetString("InsertPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@PalletNo1", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@OrigPalletUnits", OdbcType.Int).Value = palletUnits;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletUnits;
                        command.Parameters.Add("@PalletNo2", OdbcType.Int).Value = pallet.PalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = pallet.BestBefore;

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

        public bool ReleaseFromStageOneRoom(int palletNo)
        {
            bool isSuccess = false;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("ReleaseFromStage1Room");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected != -1)
                        {
                            isSuccess = true;
                        }


                    }
                    return isSuccess;
                }
                catch (Exception)
                {
                    return isSuccess;
                }
            }

        }

        public TransactionWrapper SetBinLocation(string binLocation, string status, int licensedPalletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = ReplenishSQL.ResourceManager.GetString("SetBinLocation");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation1", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@LicensedPalletNumber", OdbcType.VarChar).Value = licensedPalletNumber;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@BinLocation2", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected <= 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetBinLocation : More than one pallet header for pallet # " + licensedPalletNumber + " found.");
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetBinLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetBinLocation(string warehouseCode, string roomCode, string rackCode, string status, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string binLocation = warehouseCode + "." + roomCode + "." + rackCode;
            DateTime dateNow = DateTime.Now;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = PutAwaySQL.ResourceManager.GetString("SetBinLocation");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@DateNow", OdbcType.DateTime).Value = dateNow;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetBinLocation : Could not find pallet header " + palletNo.ToString());
                            return wrapper;
                        }
                        else if (rowsAffected > 1)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetBinLocation : More than one pallet header found for " + palletNo.ToString());
                            return wrapper;
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetBinLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetLicensedPallet(int palletNo, string catalogCode, string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = PutAwaySQL.ResourceManager.GetString("SetLicensedPallet");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetLicensedPallet : Zero or more than one row updated");
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetLicensedPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetPlanNumber(int licensedPalletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = ReplenishSQL.ResourceManager.GetString("SetPlanNumber");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNumber", OdbcType.Int).Value = licensedPalletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected <= 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetPlanNumber : Error");
                        }

                        return wrapper; // shouldn't ever get here
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetPlanNumber : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetUsedCellCount(string binLocation, string warehouseCode, string roomCode, string rackCode, string catalogCode, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            //string binLocation = warehouseCode + "." + roomCode + "." + rackCode;

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
                        }
                    }

                    if (bestBefore != DateTime.MinValue || bestBefore != null)
                    {
                        updateString = PutAwaySQL.ResourceManager.GetString("UpdateBestBeforeForRack");
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                            int rowsAffected = command.ExecuteNonQuery();
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
                    }

                    string message = GetPalletCount(ref palletCount, binLocation);
                    if (palletCount == -1)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add(message);
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

                            if (rowsAffected == 0)
                            {
                                //command.Parameters.Clear();
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack");
                                rowsAffected = command.ExecuteNonQuery();
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

                            if (rowsAffected == 0)
                            {
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack2");
                                rowsAffected = command.ExecuteNonQuery();
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

                            if (rowsAffected == 0)
                            {
                                command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateTempHoldRack2");
                                rowsAffected = command.ExecuteNonQuery();
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
                    }

                    updateString = PutAwaySQL.ResourceManager.GetString("UpdateRoomFillLevel");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@CellCount", OdbcType.Int).Value = usedCellCountForRoom;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        int rowsAffected = command.ExecuteNonQuery();
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

        public TransactionWrapper UpdateRackWithLicensedPalletNo(int licensedPalletNo, string catalogCode, string warehouseCode, string roomCode, string rackCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    string updateString = ReplenishSQL.ResourceManager.GetString("UpdateRackWithLicensedPalletNo");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateRackWithLicensedPalletNo : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateOldPalletDetail(PalletDetail palletDetail, int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    string updateString = PutAwaySQL.ResourceManager.GetString("UpdateOldPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateOldPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetail(int palletUnits, int palletNumber, string catalogCode, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = ReplenishSQL.ResourceManager.GetString("UpdatePalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits1", OdbcType.Int).Value = palletUnits;
                        //command.Parameters.Add("@PalletUnits2", OdbcType.Int).Value = palletUnits;
                        //command.Parameters.Add("@LicensedPalletNumber", OdbcType.Int).Value = licensedPalletNumber;
                        command.Parameters.Add("@PalletNumber", OdbcType.VarChar).Value = palletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBefore;

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

        public TransactionWrapper UpdatePalletPlanNo(int licensedPalletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = ReplenishSQL.ResourceManager.GetString("UpdatePalletPlanNo");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;

                        int rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected < 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletPlanNo: Error");
                            return wrapper;
                        }
                        else if (rowsAffected > 1)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletPlanNo: More than one row found!");
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
                    wrapper.Messages.Add("UpdatePalletPlanNo : " + e.Message);
                    return wrapper;
                }
            }
        }

    }
}
