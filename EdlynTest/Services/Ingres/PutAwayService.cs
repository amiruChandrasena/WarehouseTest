using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Reflection;
using System.Text;
using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;

namespace Services.Ingres
{
    public class PutAwayService : IPutAwayService
    {
        private readonly string connectionString;

        string logFileName = String.Format("PutAway_Service_{0}.txt", DateTime.Now.ToString("yyyyMMdd"));


        public PutAwayService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        #region Select Methods

        public TransactionWrapper CheckIfConsolidateRM(string catalogCode, ref int consolidate)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetConsolidate");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                consolidate = reader.GetInt32(0);
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
        }

        public int CheckIfPalletPicked(int palletNo)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("CheckIfPalletPicked");
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
                        return count;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public string CheckLastCycleCount(string warehouseCode, string roomCode, string rackCode)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                string lastCountedAt = "";

                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("CheckLastCycleCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                lastCountedAt = reader.GetDateTime(0).ToString();
                            }
                        }
                        return lastCountedAt;
                    }
                }
                catch (Exception e)
                {

                    return "CheckLastCycleCount : " + e.Message;
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

        public TransactionWrapper CheckRMCatalogCodeExists(string binLocation, int palletNo, string catalogCode, ref bool doesExist)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("CheckRMCatalogCodeExists");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                doesExist = true;
                            } else
                            {
                                doesExist = false;
                            }
                        }
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetActiveRackByProductAndBestBefore(string warehouseCode, string roomCode, string catalogCode,
                                                                        string roomType, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetActiveRackByProductAndBestBefore");
                    queryString += " AND ro.type LIKE '%" + roomType + "%'"; // odbc sucks at using variables with a LIKE
                    queryString += " ORDER BY ra.filling_seq ASC, ra.current_used_cell DESC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    string curObjBinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                    int count = GetLocationCount(curObjBinLocation, catalogCode);
                                    if (count == -1)
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("GetLocationCount: Exception thrown");
                                        return wrapper;
                                    }
                                    else if (count == 0)
                                    {
                                        WarehouseRack rack = new WarehouseRack
                                        {
                                            WarehouseCode = dReader.warehouse_code,
                                            RoomCode = dReader.room_code,
                                            RackCode = dReader.rack_code,
                                            CurrentUsedCell = dReader.current_used_cell,
                                            CellCount = dReader.cell_count,
                                            Status = dReader.status
                                        };

                                        wrapper.IsSuccess = true;
                                        wrapper.ResultSet.Add(rack);
                                        return wrapper;

                                    }
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
                    wrapper.Messages.Add("GetActiveRackByProductAndBestBefore : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetAssignedEmptyRack(string warehouseCode, string roomCode, string catalogCode,
                                                        string roomType, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetAssignedEmptyRack");
                    queryString += " AND ro.type LIKE '%" + roomType + "%'"; // odbc sucks at using variables with a LIKE
                    queryString += " ORDER BY ra.filling_seq ASC, ra.current_used_cell DESC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

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
                                        CurrentUsedCell = dReader.current_used_cell,
                                        CellCount = dReader.cell_count,
                                        Status = dReader.status
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
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
                    wrapper.Messages.Add("GetAssignedEmptyRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetCurrentLocationRoomType(string warehouseCode, string roomCode, ref string currentRoomType)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetCurrentLocationRoomType");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    currentRoomType = reader.GetString(0);
                                }
                            }
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
        }

        public string GetLastCycleCount(ref WarehouseRack rack, string lastCountedAt)
        {
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                string returnString = "GetLastCycleCount : ";

                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetLastCycleCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = rack.WarehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = rack.RoomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rack.RackCode;
                        command.Parameters.Add("@StartTime", OdbcType.DateTime).Value = Convert.ToDateTime(lastCountedAt);

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
                            dynamic dReader = new DynamicDataReader(reader);

                            while (reader.Read())
                            {
                                rack.RackCycle = new WarehouseRackCycle
                                {
                                    SuggestedRack = dReader.suggested_rack,
                                    CountedBy = dReader.counted_by,
                                    SystemPalletCount = dReader.system_pallet_count,
                                    ActualPalletCount = dReader.actual_pallet_count,
                                    StartTime = dReader.start_time,
                                    FinalisedTime = dReader.finalised_time,
                                    Status = dReader.status
                                };
                            }
                        }

                        if (rack.RackCycle != null)
                        {
                            returnString += "Successful";
                        }
                        else
                        {
                            returnString += "No Cycle Count Found";
                        }

                        return returnString;
                    }
                }
                catch (Exception e)
                {
                    returnString += e.Message;
                    return returnString;
                }
            }
        }

        public int GetLocationCount(string binLocation, string catalogCode)
        {
            int count = 1;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetLocationCount");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { count = reader.GetInt32(0); }
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

        public DateTime GetPalletBestBefore(int palletNo)
        {
            DateTime bestBefore = new DateTime();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletBestBefore");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { bestBefore = reader.GetDate(0); }
                            }
                        }
                        return bestBefore;
                    }
                }
                catch (Exception)
                {
                    return bestBefore;
                }
            }
        }

        public int GetPalletByTag(string scanData)
        {
            bool isNumeric = int.TryParse(scanData, out int palletNo);

            if ((isNumeric && palletNo < 5000000) || !isNumeric)
            {
                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletByTag");
                        using (OdbcCommand command = new OdbcCommand(queryString, connection))
                        {
                            command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = scanData;

                            using (OdbcDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        palletNo = reader.GetInt32(0);
                                    }
                                } else
                                {
                                    return 0;
                                }
                            }

                            return palletNo;
                        }
                    } catch (Exception)
                    {
                        return -1;
                    }
                }
            } else
            {
                return 0;
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
                    returnString += e.Message;
                    return returnString;
                }
            }
        }
        /*
        public string GetPalletCountOnRack(ref int palletCount, string binLocation)
        {
            string returnMessage = "GetPalletCountOnRack : ";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {

                    }
                }
            }

        }*/

        public TransactionWrapper GetPalletDetail(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string returnMessage = "GetPalletDetail : ";

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
                                palletDetail.PalletUnits = dReader.pallet_units;
                                //palletDetail.BestBefore = dReader.best_before;
                                palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                palletDetail.PalletNumber = dReader.pallet_no;
                                palletDetail.CatalogCode = dReader.catlog_code.Trim();
                                palletDetail.BestBefore = dReader.pd_best_before;
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

                                if (pallet.PrintDate > DateTime.Now)
                                {
                                    pallet.PrintDate = DateTime.MinValue;
                                }
                                if (pallet.RackedTime > DateTime.Now)
                                {
                                    pallet.RackedTime = DateTime.MinValue;
                                }

                                palletDetail.CatalogItem = catalogItem;
                                pallet.PalletDetails.Add(palletDetail);

                            }

                            if (pallet.BinLocation != "" && pallet.BinLocation != null)
                            {
                                string roomType = "";
                                string[] binLocationParts = pallet.BinLocation.Split('.');
                                if (binLocationParts.Length != 3)
                                {
                                    wrapper.Messages.Add("Failed to split bin location");
                                }
                                else
                                {
                                    wrapper.Messages.Add(GetRoomType(ref roomType, binLocationParts[0], binLocationParts[1]));
                                    pallet.RoomType = roomType;
                                    if (pallet.RoomType != "")
                                    {
                                        string rackedTime = "";
                                        wrapper.Messages.Add(GetStageOneRackedTime(ref rackedTime, roomType, pallet.PalletNumber));
                                        pallet.StageOneRackedTime = rackedTime;
                                    }
                                }

                            }

                            wrapper.ResultSet.Add(pallet);

                            returnMessage += "Returned " + wrapper.ResultSet.Count + " rows.";
                            wrapper.IsSuccess = true;
                            wrapper.Messages.Add(returnMessage);
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    returnMessage += e.Message;
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(returnMessage);
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

        public TransactionWrapper GetPalletDetailForMixPallet(int palletNo, string catalogCode, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailforMixPalletNew");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode.Trim();
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    PalletDetail palletDetail = new PalletDetail
                                    {
                                        PalletNumber = palletNo,
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
                    wrapper.Messages.Add("GetPalletDetailForMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletDetailRawMaterials(int palletNo, string scanData)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletDetailRM");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@ScanData", OdbcType.VarChar).Value = scanData;

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

                                    pallet.Status = reader.GetString(0).ToString();
                                    pallet.TransferStatus = dReader.transfer_status;
                                    pallet.PrintDate = dReader.print_date;
                                    pallet.PalletNumber = dReader.pallet_no;
                                    pallet.BinLocation = dReader.bin_location;
                                    pallet.WarehouseId = dReader.warehouse_id;
                                    pallet.KeepInStageRoom = dReader.keep_in_stage_room;
                                    pallet.RackedTime = dReader.racked_time;
                                    
                                    try
                                    {
                                        pallet.BestBefore = Common.DateFormats.ParseDateWithoutTime(Convert.ToString(dReader.best_before));
                                    }
                                    catch { pallet.BestBefore = DateTime.MinValue; }
                                        
                                    palletDetail.PalletUnits = dReader.pallet_units;
                                    //palletDetail.BestBefore = dReader.best_before;
                                    palletDetail.OriginalPalletUnits = dReader.orig_pallet_units;
                                    palletDetail.PalletNumber = dReader.pallet_no;
                                    palletDetail.CatalogCode = dReader.catlog_code;
                                    palletDetail.PalletQuantity = dReader.pallet_qty;

                                    Common.WriteLogFile.WriteLog(logFileName, String.Format("{0} - {1}", DateTime.Now.ToString(), "GetPalletDetailRM " + palletNo.ToString() + "/" + palletDetail.PalletQuantity.ToString()));

                                    catalogItem.CatalogCode = dReader.catlog_code;
                                    catalogItem.CatalogCode = catalogItem.CatalogCode.Trim();
                                    catalogItem.Description = dReader.description;
                                    /*catalogItem.RoomType = dReader.room_type;
                                    catalogItem.TimeToMature = dReader.time_to_mature;
                                    catalogItem.MaxWaitingHours = dReader.max_waiting_hrs;
                                    catalogItem.StageOneRoomHours = dReader.stage1_room_hrs;
                                    catalogItem.LabApprovalAfterStageOne = dReader.lab_approval_after_stage1;
                                    catalogItem.RoomTypeStageTwo = dReader.room_type_stage2;
                                    catalogItem.StageTwoRoomHours = dReader.stage2_room_hrs;
                                    catalogItem.ShelfLife = dReader.shelf_life;*/

                                    if (pallet.PrintDate > DateTime.Now)
                                    {
                                        pallet.PrintDate = DateTime.MinValue;
                                    }
                                    if (pallet.RackedTime > DateTime.Now)
                                    {
                                        pallet.RackedTime = DateTime.MinValue;
                                    }

                                    if (pallet.BestBefore.Year == 9999)
                                    {
                                        pallet.BestBefore = DateTime.MinValue;
                                    }

                                    palletDetail.CatalogItem = catalogItem;
                                    pallet.PalletDetails.Add(palletDetail);

                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletDetailRawMaterials: No pallet details found for pallet " + palletNo.ToString());
                                return wrapper;
                            }

                            wrapper.ResultSet.Add(pallet);
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletDetailRawMaterials: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletNumbersInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string returnMessage = "GetPalletNumbersInRack : ";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletNumbersInRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                wrapper.ResultSet.Add(reader.GetInt32(0));
                            }
                        }
                        returnMessage += "Returned " + wrapper.ResultSet.Count + "rows.";
                        wrapper.IsSuccess = true;
                        wrapper.Messages.Add(returnMessage);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    returnMessage += e.Message;
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(returnMessage);
                    return wrapper;
                }
            }
        }

        public string GetPalletStatus(int palletNo)
        {
            string status = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletStatus");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { status = reader.GetString(0); }
                            }
                        }
                        return status;
                    }
                }
                catch (Exception e)
                {
                    return e.Message;
                }
            }
        }

        public TransactionWrapper GetPalletStockQuantityRM(int palletNo, string catalogCode, ref float stockQty, ref string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletStockQuantityAndPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    stockQty = reader.GetFloat(0);
                                    pickingLabel = reader.GetString(1);
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletStockQuantityRM: Could not get stock quantity for pallet " + palletNo.ToString());
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletStockQuantityRM: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletStockQuantityTotal(int palletNo, ref double units)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletStockQtyTotal");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    units = reader.GetDouble(0);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletStockQuantityTotal: Could not get total stock quantity for pallet " + palletNo.ToString());
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
                    wrapper.Messages.Add("GetPalletStockQuantityTotal: " + e.Message);
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

        public int GetProductCountOnPallet(int palletNo, string catalogCode)
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
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

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

        public string GetPalletQuality(int palletNo)
        {
            string quality = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletQuality");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!reader.IsDBNull(0)) { quality = reader.GetString(0); }
                            }
                        }
                        return quality;
                    }
                }
                catch (Exception e)
                {
                    return e.Message;
                }
            }
        }

        public int GetPalletShelfLife(int palletNo)
        {
            int shelfLife = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetPalletShelfLife");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                shelfLife = reader.GetInt32(0);
                            }
                        }
                        return shelfLife;
                    }
                }
                catch (Exception)
                {
                    return -1;
                }
            }
        }

        public TransactionWrapper GetProductsInLocation(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<string> catalogCodes = new List<string>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetProductsInLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while(reader.Read())
                                {
                                    string code = reader.GetString(0);
                                    catalogCodes.Add(code);
                                }
                            }

                            wrapper.IsSuccess = true;
                            wrapper.ResultSet.Add(catalogCodes);
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetProductsInLocation: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetRackByRoomType(string warehouseCode, string roomCode, string roomType)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            DateTime assignedTimeCheck = DateTime.Now.AddMinutes(-10);
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetRackByRoomType");
                    queryString += " AND ro.type LIKE '%" + roomType + "%'"; // odbc sucks at using variables with a LIKE
                    queryString += " ORDER BY ra.filling_seq ASC, ra.current_used_cell DESC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@AssignedTimeCheck", OdbcType.DateTime).Value = assignedTimeCheck;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

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
                                        CurrentUsedCell = dReader.current_used_cell,
                                        CellCount = dReader.cell_count,
                                        Status = dReader.status
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
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
                    wrapper.Messages.Add("GetRackByRoomType : " + e.Message);
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

        public TransactionWrapper GetReplenishLocation(string warehouseCode, string roomCode, string catalogCode)
        {
            WarehouseRack rack = new WarehouseRack();
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetReplenishLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    rack.WarehouseCode = dReader.warehouse_code;
                                    rack.RoomCode = dReader.room_code;
                                    rack.RackCode = dReader.rack_code;
                                }

                                wrapper.IsSuccess = true;
                                wrapper.Messages.Add("Found replenish location");
                                wrapper.ResultSet.Add(rack);
                                return wrapper;
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Could not find replenish location");
                                return wrapper;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetReplenishLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetReservedActiveRack(string warehouseCode, string roomCode, string catalogCode, string roomType, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetReservedActiveRack");
                    queryString += " AND ro.type LIKE '%" + roomType + "%'"; // odbc sucks at using variables with a LIKE
                    queryString += " ORDER BY ra.filling_seq ASC, ra.current_used_cell DESC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode1", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                        command.Parameters.Add("@CatalogCode2", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            while (reader.Read())
                            {
                                string curObjBinLocation = dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code;
                                int count = GetLocationCount(curObjBinLocation, catalogCode);
                                if (count == -1)
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("GetLocationCount: Exception thrown");
                                    return wrapper;
                                }
                                else if (count == 0)
                                {
                                    WarehouseRack rack = new WarehouseRack
                                    {
                                        WarehouseCode = dReader.warehouse_code,
                                        RoomCode = dReader.room_code,
                                        RackCode = dReader.rack_code,
                                        CurrentUsedCell = dReader.current_used_cell,
                                        CellCount = dReader.cell_count,
                                        Status = dReader.status
                                    };

                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
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
                    wrapper.Messages.Add("GetReservedActiveRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetReservedEmptyRack(string warehouseCode, string roomCode, string catalogCode, string roomType)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetReservedEmptyRack");
                    queryString += " AND ro.type LIKE '%" + roomType + "%'"; // odbc sucks at using variables with a LIKE
                    queryString += " ORDER BY ra.filling_seq ASC, ra.current_used_cell DESC";

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;

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
                                        CurrentUsedCell = dReader.current_used_cell,
                                        CellCount = dReader.cell_count,
                                        Status = dReader.status
                                    };
                                    wrapper.IsSuccess = true;
                                    wrapper.ResultSet.Add(rack);
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
                    wrapper.Messages.Add("GetReservedEmptyRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public string GetRoomType (ref string roomType, string warehouseCode, string roomCode)
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

        public TransactionWrapper GetScannedRMRackPalletNoAndPickingLabel(string binLocation, string catalogCode, ref int palletNo, ref string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetScannedRMRackPalletNoAndPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                palletNo = reader.GetInt32(0);
                                pickingLabel = reader.GetString(1);
                            }
                        }
                    }
                    wrapper.IsSuccess = true;
                    return wrapper;
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
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

        public int GetUniqueProductCount(int palletNo)
        {
            int count = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetUniqueProductCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            count++;
                        }

                        return count;
                    }
                } catch (Exception)
                {
                    return -1;
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

        public TransactionWrapper GetWarehouseRack(string warehouseCode, string roomCode, string rackCode, bool checkLastCycle)
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

                                    if (checkLastCycle)
                                    {
                                        // check if last cycle for this rack
                                        string lastCountedAt = CheckLastCycleCount(warehouseCode, roomCode, rackCode);
                                        if (lastCountedAt != "")
                                        {
                                            DateTime dateCheck = new DateTime();
                                            if (DateTime.TryParse(lastCountedAt, out dateCheck))
                                            {
                                                // get last cycle count
                                                wrapper.Messages.Add(GetLastCycleCount(ref rack, lastCountedAt));
                                            }
                                            else
                                            {
                                                wrapper.Messages.Add("CheckLastCycleCount : " + lastCountedAt);
                                            }
                                        }
                                        else
                                        {
                                            wrapper.Messages.Add("CheckLastCycleCount : No Last Cycle Count found");
                                        }
                                    }
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

        public TransactionWrapper GetWarehouseLocationCount(string warehouseCode, string roomCode, string rackCode, ref int count)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseLocationCount");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = roomCode;
                        command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rackCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    count = reader.GetInt32(0);
                                }
                            }

                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseLocationCount: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetWarehouseRack(string catalogCode, string roomType, string warehouseCode, string rackingZone, DateTime bestBefore)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string returnMessage = "GetWarehouseRack : ";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseRackByProduct");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@RoomType", OdbcType.VarChar).Value = roomType;
                        command.Parameters.Add("@AssignedCatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = bestBefore;
                        command.Parameters.Add("@ReservedCatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = warehouseCode;
                        command.Parameters.Add("@RackingZone", OdbcType.VarChar).Value = rackingZone;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            while (reader.Read())
                            {
                                WarehouseRack rack = new WarehouseRack
                                {
                                    WarehouseCode = dReader.warehouse_code,
                                    RoomCode = dReader.room_code,
                                    RackCode = dReader.rack_code,
                                    CurrentUsedCell = dReader.current_used_cell,
                                    CellCount = dReader.cell_count,
                                    Status = dReader.status
                                };

                                wrapper.ResultSet.Add(rack);
                            }
                        }
                    }

                    returnMessage += "Returned " + wrapper.ResultSet.Count().ToString() + " rows";
                    wrapper.Messages.Add(returnMessage);
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    returnMessage += "Failed : " + e.Message;
                    wrapper.Messages.Add(returnMessage);
                    wrapper.IsSuccess = false;
                    return wrapper;
                }
            }
        }

        

        #endregion

        #region Insert/Update Methods
        public TransactionWrapper ConsolidatePalletDetailPalletQty(int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PutAwaySQL.ResourceManager.GetString("ConsolidatePalletDetailPalletQty");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 1)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("ConsolidatePalletDetailPalletQty: More than one row of details found for pallet " + palletNo.ToString());
                            return wrapper;
                        }
                        else if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("ConsolidatePalletDetailPalletQty: No details found to consolidate pallet " + palletNo.ToString());
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
                    wrapper.Messages.Add("ConsolidatePalletDetailPalletQty: " + e.Message);
                    return wrapper;
                }
            }
        }
    

        public TransactionWrapper ConsolidatePalletDetailStockQty(float stockQty, int palletQty, int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PutAwaySQL.ResourceManager.GetString("ConsolidatePalletDetailStockQty");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@StockQty", OdbcType.Double).Value = stockQty;
                        command.Parameters.Add("@PalletQty", OdbcType.Int).Value = palletQty;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 1)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("ConsolidatePalletDetailStockQty: More than one row of details found for pallet " + palletNo.ToString());
                            return wrapper;
                        } else if (rowsAffected == 0)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("ConsolidatePalletDetailStockQty: No details found to consolidate pallet " + palletNo.ToString());
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
                    wrapper.Messages.Add("ConsolidatePalletDetailStockQty: " + e.Message);
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
                        if (rowsAffected > 0)
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

        public TransactionWrapper CreatePalletHeaderForMixPallet(PalletHeader palletHeader, ref int newPalletNumber)
        {
            int rowsAffected = 0;

            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = CreateNewPalletNoForMixPallet(ref newPalletNumber);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = PutAwaySQL.ResourceManager.GetString("CreatePalletHeaderForMixPalletNew");
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

                    if (rowsAffected == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add($"CreatePalletHeaderForMixPallet : Couldn't insert pallet header {palletHeader.PalletNumber}");
                        return wrapper;
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("CreatePalletHeaderForMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper CreateNewPalletNoForMixPallet(ref int maxPalletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PalletSQL.ResourceManager.GetString("GetMaxPalletNoForMixPallet");
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
                    string insertString = PalletSQL.ResourceManager.GetString("CreateNewPalletNoForMixPallet");
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

        public TransactionWrapper InsertPalletDetail(int licensedPalletNo, int palletNo, string catalogCode, PalletDetail palletDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PutAwaySQL.ResourceManager.GetString("InsertPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@LicensedPalletNo", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@PalletNo1", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@PalletNo2", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = palletDetail.BatchNumber;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 0)
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

        public bool InsertPalletMovementLogRM(PalletLocationLog palletLocationLog)
        {
            bool isSuccess = false;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = PutAwaySQL.ResourceManager.GetString("InsertPalletMovementLogRM");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@MovedBy", OdbcType.VarChar).Value = palletLocationLog.MovedBy;
                        command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = palletLocationLog.NewLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletLocationLog.PalletNo;
                        command.Parameters.Add("@Remark", OdbcType.VarChar).Value = palletLocationLog.Remark;
                        command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = palletLocationLog.SyncTime;
                        command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = palletLocationLog.Timestamp;

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

        public TransactionWrapper FinaliseRackCycleCount(WarehouseRack rack)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string returnMessage = "FinaliseRackCycleCount : ";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                OdbcCommand command = new OdbcCommand();
                OdbcTransaction transaction = null;

                command.Connection = connection;

                try
                {
                    connection.Open();

                    transaction = connection.BeginTransaction();

                    command.Connection = connection;
                    command.Transaction = transaction;

                    // update last cycle count
                    command.CommandText = PutAwaySQL.ResourceManager.GetString("FinaliseRackCycleCount");
                    command.Parameters.Add("@ActualPalletCount", OdbcType.Int).Value = rack.RackCycle.ActualPalletCount;
                    command.Parameters.Add("@CountedBy", OdbcType.VarChar).Value = rack.RackCycle.CountedBy;
                    command.Parameters.Add("@FinalisedTime", OdbcType.DateTime).Value = rack.RackCycle.FinalisedTime;
                    command.Parameters.Add("@Status", OdbcType.VarChar).Value = rack.RackCycle.Status;
                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = rack.WarehouseCode;
                    command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = rack.RoomCode;
                    command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rack.RackCode;
                    command.Parameters.Add("@StartTime", OdbcType.DateTime).Value = rack.RackCycle.StartTime;
                    command.ExecuteNonQuery();

                    command.Parameters.Clear();

                    // update warehouse room config

                    command.CommandText = PutAwaySQL.ResourceManager.GetString("UpdateLastCycleCount");
                    command.Parameters.Add("@LastCycleCountTime", OdbcType.DateTime).Value = rack.LastCycleCountTime;
                    command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = rack.WarehouseCode;
                    command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = rack.RoomCode;
                    command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rack.RackCode;
                    command.ExecuteNonQuery();

                    // commit
                    transaction.Commit();
                    returnMessage += "Successful";
                    wrapper.Messages.Add(returnMessage);
                    wrapper.IsSuccess = true;

                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    returnMessage += "Failed: " + e.Message;

                    try
                    {
                        transaction.Rollback();
                        returnMessage += " Rollback successful";
                    }
                    catch
                    {
                        returnMessage += " Rollback failed!";
                        wrapper.Messages.Add(returnMessage);
                        return wrapper;
                    }

                    wrapper.Messages.Add(returnMessage);
                    return wrapper;
                }
            }
        }

        public bool ReleasePalletFromStageOneRoom(int palletNo)
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

        public TransactionWrapper SetPalletLocation(string warehouseCode, string roomCode, string rackCode, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string binLocation = warehouseCode + "." + roomCode + "." + rackCode;

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = PutAwaySQL.ResourceManager.GetString("SetPalletLocation");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation1", OdbcType.VarChar).Value = binLocation;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@BinLocation2", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("SetPalletLocation : Pallet No - " + palletNo.ToString() + " not found.");
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SetPalletLocation : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SetUsedCellCount(string warehouseCode, string roomCode, string rackCode, string catalogCode, DateTime bestBefore)
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

        public TransactionWrapper UpdatePalletPickingLabel(string pickingLabel, int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = PickingSQL.ResourceManager.GetString("UpdatePalletHeaderPickingLabel");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PickingLabel", OdbcType.VarChar).Value = pickingLabel;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else if (rowsAffected > 1) // pallet number in pallet header should be unique
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletPickingLabel: More than one row found in pallet header for pallet: " + palletNo.ToString());
                            return wrapper;
                        } else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletPickingLabel: Could not find pallet: " + palletNo.ToString());
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletPickingLabel: " + e.Message);
                    return wrapper;
                }
            }
        }

        public bool UpdateRackAssignedCatalogCode(WarehouseRack rack, string catalogCode, DateTime bestBefore)
        {
            bool isSuccess = false;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    OdbcTransaction transaction = connection.BeginTransaction();
                    try
                    {
                        string updateString = PutAwaySQL.ResourceManager.GetString("UpdateRackAssignedCatalogCode");
                        DateTime dateNow = DateTime.Now;
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                            if (bestBefore != DateTime.MinValue)
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.Date).Value = bestBefore;
                            }
                            else
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = "";
                            }
                            command.Parameters.Add("@DateNow", OdbcType.DateTime).Value = dateNow;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = rack.WarehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = rack.RoomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rack.RackCode;

                            command.Transaction = transaction;
                            int rowsAffected = command.ExecuteNonQuery();
                            if (rowsAffected == 1)
                            {
                                isSuccess = true;
                                transaction.Commit();
                            }
                            else
                            {
                                transaction.Rollback();
                                isSuccess = false;
                            }
                        }
                        return isSuccess;
                    }
                    catch (Exception e)
                    {
                        transaction.Rollback();
                        isSuccess = false;
                        return isSuccess;
                        
                    }
                }
                catch (Exception e)
                {
                    isSuccess = false;
                    return isSuccess;
                }
            }
        }
        
        public bool UpdateRoomConfigLicencedPalletNo(WarehouseRack rack)
        {
            bool isSuccess = false;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    OdbcTransaction transaction = connection.BeginTransaction();
                    try
                    {
                        string updateString = PutAwaySQL.ResourceManager.GetString("UpdateRoomConfigLicencedPalletNo");
                        DateTime dateNow = DateTime.Now;
                        using (OdbcCommand command = new OdbcCommand(updateString, connection))
                        {
                            command.Parameters.Add("@LicencedPalletNo", OdbcType.Int).Value = rack.LicensedPalletNo;
                            command.Parameters.Add("@WarehouseCode", OdbcType.VarChar).Value = rack.WarehouseCode;
                            command.Parameters.Add("@RoomCode", OdbcType.VarChar).Value = rack.RoomCode;
                            command.Parameters.Add("@RackCode", OdbcType.VarChar).Value = rack.RackCode;

                            command.Transaction = transaction;
                            int rowsAffected = command.ExecuteNonQuery();
                            if (rowsAffected == 1)
                            {
                                isSuccess = true;
                                transaction.Commit();
                            }
                            else
                            {
                                transaction.Rollback();
                                isSuccess = false;
                            }
                        }
                        return isSuccess;
                    }
                    catch (Exception e)
                    {
                        transaction.Rollback();
                        isSuccess = false;
                        return isSuccess;
                        
                    }
                }
                catch (Exception e)
                {
                    isSuccess = false;
                    return isSuccess;
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
                        if (rowsAffected > 0)
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

        public TransactionWrapper UpdatePalletDetail(ref int rowsAffected, PalletDetail palletDetail, int licensedPalletNo, int palletNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    string updateString = PutAwaySQL.ResourceManager.GetString("UpdatePalletDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits1", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletUnits2", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNo;
                        command.Parameters.Add("@LicensedPalletNumber", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
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
        
        

        public TransactionWrapper UpdatePalletHeaderStatus(int palletNo, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = PutAwaySQL.ResourceManager.GetString("UpdatePalletHeaderStatus");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = palletNo;
                        
                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                        else if (rowsAffected > 1) // pallet number in pallet header should be unique
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderStatus: More than one row found in pallet header for pallet: " + palletNo.ToString());
                            return wrapper;
                        }
                        else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderStatus: Could not find pallet: " + palletNo.ToString());
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderStatus: " + e.Message);
                    return wrapper;
                }
            }
        }
        #endregion

        public TransactionWrapper GetWarehouseRackByBinLocation(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = PutAwaySQL.ResourceManager.GetString("GetWarehouseRackByBinLocation");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

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
                        wrapper.Messages.Add("GetWarehouseRackByBinLocation: No details found for bin location :" + binLocation);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetWarehouseRackByBinLocation : " + e.Message);
                    wrapper.ResultSet.Clear();
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateNewPalletDetailForMixPallet(PalletDetail palletDetail, int licensedPalletNo, ref int rowsAffected)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    string updateString = PutAwaySQL.ResourceManager.GetString("UpdateNewPalletDetailForMixPallet");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits1", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletUnits2", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@LicensedPalletNumber", OdbcType.Int).Value = licensedPalletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = palletDetail.CatalogCode;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 0)
                            wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateNewPalletDetailForMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateOldPalletDetailForMixPallet(PalletDetail palletDetail, int oldPalletNo, ref int rowsAffected)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    string updateString = PutAwaySQL.ResourceManager.GetString("UpdateOldPalletDetailForMixPallet");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = palletDetail.CatalogCode.Trim();
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = palletDetail.BatchNumber;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 0)
                            wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateOldPalletDetailForMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertNewPalletDetailForMixPallet(int newPalletNo, int oldPalletNo, PalletDetail palletDetail)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PutAwaySQL.ResourceManager.GetString("InsertNewPalletDetailForMixPallet");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@NewPalletNo", OdbcType.Int).Value = newPalletNo;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@OrigPalletUnits", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletDetail.PalletUnits;
                        command.Parameters.Add("@PalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = palletDetail.CatalogCode.Trim();
                        command.Parameters.Add("@BatchNo", OdbcType.Int).Value = palletDetail.BatchNumber;
                        command.Parameters.Add("@BestBefore", OdbcType.Date).Value = palletDetail.BestBefore;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 0)
                            wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertNewPalletDetailForMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper ValidateMixPallet(PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PutAwaySQL.ResourceManager.GetString("ValidateMixPallet");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@AssignCatCode", OdbcType.VarChar).Value = palletMix.palletDetail.CatalogCode.Trim();
                        command.Parameters.Add("@ReserveCatCode", OdbcType.VarChar).Value = palletMix.palletDetail.CatalogCode.Trim();
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = palletMix.BinLocationTo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = false;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("ValidateMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper ValidateBulkMixPallet(PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = PutAwaySQL.ResourceManager.GetString("ValidateBulkMixPallet");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = palletMix.BinLocationTo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                wrapper.IsSuccess = true;
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = false;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("ValidateBulkMixPallet : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
