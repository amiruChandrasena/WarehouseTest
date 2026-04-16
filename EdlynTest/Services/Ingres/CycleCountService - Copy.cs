using Abstractions.ServiceInterfaces;
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
    public class CycleCountService : ICycleCountService
    {
        private readonly string connectionString;

        public CycleCountService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetConversion(string uom, ref decimal conversion)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CycleCountSQL.ResourceManager.GetString("GetConversion");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@Uom", OdbcType.VarChar).Value = uom;
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                conversion = reader.GetDecimal(0);
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetConversion : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper GetPalletsInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<CycleCountPallet> ccPallets = new List<CycleCountPallet>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CycleCountSQL.ResourceManager.GetString("GetPalletsInRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    CycleCountPallet ccPallet = new CycleCountPallet();
                                    ccPallet.PalletNumber = dReader.pallet_no;
                                    ccPallet.CatalogCode = dReader.catlog_code;
                                    DateTime bestBeforeDate = dReader.best_before;
                                    ccPallet.BestBefore = bestBeforeDate.ToString("dd/MM/yy");
                                    ccPallet.OldBestBefore = bestBeforeDate.ToString("dd/MM/yy");
                                    ccPallet.PalletUnits = reader.GetInt32(3);
                                    ccPallet.OldPalletUnits = reader.GetInt32(3);

                                    ccPallets.Add(ccPallet);
                                }

                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(ccPallets);
                                return wrapper;
                            }

                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletsInRack : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletNumbers(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<int> palletNumbers = new List<int>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CycleCountSQL.ResourceManager.GetString("GetPalletNumbers");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    palletNumbers.Add(reader.GetInt32(0));
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletNumbers : No pallet numbers found at " + binLocation);
                            }

                            wrapper.IsSuccess = true;
                            wrapper.ResultSet.Add(palletNumbers);
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletNumbers : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetRMPalletsInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<CycleCountRMPallet> ccPallets = new List<CycleCountRMPallet>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CycleCountSQL.ResourceManager.GetString("GetRMPalletsInRack");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                dynamic dReader = new DynamicDataReader(reader);
                                while (reader.Read())
                                {
                                    CycleCountRMPallet ccPallet = new CycleCountRMPallet();
                                    ccPallet.PalletNumber = dReader.pallet_no;
                                    ccPallet.CatalogCode = dReader.catlog_code;
                                    ccPallet.Description = dReader.description;
                                    ccPallet.Uom = dReader.uom_stock;
                                    ccPallet.UomOrder = dReader.uom_order;
                                    DateTime bestBeforeDate = dReader.best_before;
                                    if (bestBeforeDate.Year == DateTime.MaxValue.Year)
                                    {
                                        ccPallet.BestBefore = "";
                                        ccPallet.OldBestBefore = "";
                                    } else
                                    {
                                        ccPallet.BestBefore = bestBeforeDate.ToString("dd/MM/yy");
                                        ccPallet.OldBestBefore = bestBeforeDate.ToString("dd/MM/yy");
                                    }
                                    //ccPallet.Description = bestBeforeDate.ToString();
                                    ccPallet.PalletUnits = reader.GetDouble(6);
                                    ccPallet.OldPalletUnits = reader.GetDouble(6);
                                    ccPallet.StockQuantity = reader.GetDouble(7);
                                    ccPallet.OldStockQuantity = reader.GetDouble(7);

                                    ccPallets.Add(ccPallet);
                                }

                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add(ccPallets);
                                return wrapper;
                            }

                            wrapper.IsSuccess = true;
                            return wrapper;
                        }
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMPalletsInRack : " + e.Message);
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
                    string queryString = CycleCountSQL.ResourceManager.GetString("GetWarehouseRack");
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
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetWarehouseRack : No rack found for " + warehouseCode + "." + roomCode + "." + rackCode);
                                return wrapper;
                            }

                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetWarehouseRack : You really broke it now");
                            return wrapper;
                        }
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

        public TransactionWrapper InsertStockCount(string binLocation, string originator, List<CycleCountPallet> pallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = CycleCountSQL.ResourceManager.GetString("InsertStockCount");
                    for (int i = 0; i < pallets.Count; i++)
                    {
                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            if (!String.IsNullOrEmpty(pallets[i].BestBefore))
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = pallets[i].BestBefore;
                            }
                            else
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = "";
                            }
                            command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallets[i].CatalogCode;
                            command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                            command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                            command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = pallets[i].PalletUnits;
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = "A";
                            command.Parameters.Add("@StockTakeDate", OdbcType.DateTime).Value = DateTime.Now;

                            int rowsAffected = command.ExecuteNonQuery();
                        }
                    }
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertStockCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertRMStockCount(string binLocation, string originator, List<CycleCountRMPallet> pallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = CycleCountSQL.ResourceManager.GetString("InsertRMStockCount");
                    for (int i = 0; i < pallets.Count; i++)
                    {
                        using (OdbcCommand command = new OdbcCommand(insertString, connection))
                        {
                            if (!String.IsNullOrEmpty(pallets[i].BestBefore))
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = pallets[i].BestBefore;
                            }
                            else
                            {
                                command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = "";
                            }
                            command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;
                            command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallets[i].CatalogCode;
                            command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                            command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                            command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = pallets[i].PalletUnits;
                            command.Parameters.Add("@Status", OdbcType.VarChar).Value = "A";
                            command.Parameters.Add("@StockTakeDate", OdbcType.DateTime).Value = DateTime.Now;
                            command.Parameters.Add("@StockQuantity", OdbcType.Double).Value = pallets[i].StockQuantity;

                            int rowsAffected = command.ExecuteNonQuery();
                        }
                    }
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertRMStockCount : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdatePalletDetail(List<CycleCountPallet> pallets, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = CycleCountSQL.ResourceManager.GetString("InsertPalletLocationLog");

                    for (int i = 0; i < pallets.Count; i++)
                    {
                        string remark = "";
                        if ((pallets[i].OldPalletUnits != pallets[i].PalletUnits) || (pallets[i].OldBestBefore != pallets[i].BestBefore))
                        {
                            if (pallets[i].OldPalletUnits != pallets[i].PalletUnits)
                            {
                                remark = "Adj units from " + pallets[i].OldPalletUnits.ToString() + " to " + pallets[i].PalletUnits.ToString();
                                using (OdbcCommand command = new OdbcCommand(insertString, connection))
                                {
                                    command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                                    command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = DateTime.Now;
                                    command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = 0;
                                    command.Parameters.Add("@Remark", OdbcType.VarChar).Value = remark;

                                    int rowsAffected = command.ExecuteNonQuery();
                                }
                            }

                            if (pallets[i].OldBestBefore != pallets[i].BestBefore)
                            {
                                remark = "Adj best before from " + pallets[i].OldBestBefore.ToString() + " to "  + pallets[i].BestBefore.ToString();
                                using (OdbcCommand command = new OdbcCommand(insertString, connection))
                                {
                                    command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                                    command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = DateTime.Now;
                                    command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = 0;
                                    command.Parameters.Add("@Remark", OdbcType.VarChar).Value = remark;

                                    int rowsAffected = command.ExecuteNonQuery();
                                }
                            }

                            string updateString = CycleCountSQL.ResourceManager.GetString("UpdatePalletDetail");
                            using (OdbcCommand command = new OdbcCommand(updateString, connection))
                            {
                                command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = pallets[i].PalletUnits;
                                command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = pallets[i].BestBefore;
                                command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallets[i].CatalogCode;
                                command.Parameters.Add("@OldBestBefore", OdbcType.DateTime).Value = pallets[i].OldBestBefore;

                                int rowsAffected = command.ExecuteNonQuery();
                                if (rowsAffected == 0)
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("UpdatePalletDetail: Could not update pallet detail for pallet #" + pallets[i].PalletNumber.ToString());
                                    return wrapper;
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletLocationLog : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateRMPalletDetail(List<CycleCountRMPallet> pallets, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = CycleCountSQL.ResourceManager.GetString("InsertPalletLocationLog");

                    for (int i = 0; i < pallets.Count; i++)
                    {
                        string remark = "";
                        if ((pallets[i].OldBestBefore != pallets[i].BestBefore) || (pallets[i].OldStockQuantity != pallets[i].StockQuantity))
                        {
                            if (pallets[i].OldStockQuantity != pallets[i].StockQuantity)
                            {
                                remark = "Adj units from " + pallets[i].OldStockQuantity.ToString() + " to " + pallets[i].StockQuantity.ToString();
                                using (OdbcCommand command = new OdbcCommand(insertString, connection))
                                {
                                    command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                                    command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = DateTime.Parse(DateTime.Now.ToShortDateString(), new CultureInfo("en-AU"));
                                    command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = 0;
                                    command.Parameters.Add("@Remark", OdbcType.VarChar).Value = remark;

                                    int rowsAffected = command.ExecuteNonQuery();
                                }
                            }

                            if (pallets[i].OldBestBefore != pallets[i].BestBefore)
                            {
                                remark = "Adj best before from " + pallets[i].OldBestBefore.ToString() + " to " + pallets[i].BestBefore.ToString();
                                using (OdbcCommand command = new OdbcCommand(insertString, connection))
                                {
                                    command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                                    command.Parameters.Add("@NewLocation", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@SyncTime", OdbcType.VarChar).Value = "";
                                    command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = DateTime.Parse(DateTime.Now.ToShortDateString(), new CultureInfo("en-AU"));
                                    command.Parameters.Add("@ManifestNo", OdbcType.Int).Value = 0;
                                    command.Parameters.Add("@Remark", OdbcType.VarChar).Value = remark;

                                    int rowsAffected = command.ExecuteNonQuery();
                                }
                            }

                            // Check if the catalog code is "packaging" and update accordingly
                            // If it is, do not update the best before date
                            // If it is not, update the best before date
                            // Get the catalog code from the database
                            Catalog catalog = new Catalog();
                            catalog = GetCatalogByCatalogCode(pallets[i].CatalogCode);

                            // Check if the catalog code is "packaging"
                            if (catalog.CatalogCode.ToLower() == "packaging")
                            {
                                // Update the pallet detail without the best before date
                                string updateString = CycleCountSQL.ResourceManager.GetString("UpdateRMPalletDetailWithoutBestbefore");
                                using (OdbcCommand command = new OdbcCommand(updateString, connection))
                                {
                                    command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = pallets[i].PalletUnits;
                                    command.Parameters.Add("@StockQuantity", OdbcType.Double).Value = pallets[i].StockQuantity;
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallets[i].CatalogCode;

                                    int rowsAffected = command.ExecuteNonQuery();
                                    if (rowsAffected == 0)
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("UpdateRMPalletDetailWithoutBestbefore: Could not update pallet detail for pallet #" + pallets[i].PalletNumber.ToString());
                                        return wrapper;
                                    }
                                }
                            }
                            else
                            {
                                // Update the best before date
                                string updateString = CycleCountSQL.ResourceManager.GetString("UpdateRMPalletDetail");
                                using (OdbcCommand command = new OdbcCommand(updateString, connection))
                                {
                                    command.Parameters.Add("@PalletUnits", OdbcType.Double).Value = pallets[i].PalletUnits;
                                    command.Parameters.Add("@BestBefore", OdbcType.VarChar).Value = DateTime.Parse(pallets[i].BestBefore, new CultureInfo("en-AU")).ToString("MM/dd/yy");
                                    command.Parameters.Add("@StockQuantity", OdbcType.Double).Value = pallets[i].StockQuantity;
                                    command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = pallets[i].PalletNumber;
                                    command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallets[i].CatalogCode;
                                    if (!String.IsNullOrEmpty(pallets[i].OldBestBefore))
                                    {
                                        command.Parameters.Add("@OldBestBefore", OdbcType.VarChar).Value = DateTime.Parse(pallets[i].OldBestBefore, new CultureInfo("en-AU")).ToString("MM/dd/yy");
                                    }
                                    else
                                    {
                                        command.Parameters.Add("@OldBestBefore", OdbcType.VarChar).Value = "";
                                    }

                                    int rowsAffected = command.ExecuteNonQuery();
                                    if (rowsAffected == 0)
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("UpdateRMPalletDetail: Could not update pallet detail for pallet #" + pallets[i].PalletNumber.ToString());
                                        return wrapper;
                                    }
                                }
                            }
                        }
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateRMPalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public Catalog GetCatalogByCatalogCode(string catalogCode)
        {
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
                                    catalog.CatGroup = dReader.cat_group;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    
                }
            }

            return catalog;
        }

        public TransactionWrapper UpdatePalletHeader(string binLocation, string newBinLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = CycleCountSQL.ResourceManager.GetString("UpdatePalletHeader");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@NewBinLocation", OdbcType.VarChar).Value = newBinLocation;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

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

        public TransactionWrapper UpdateStockCountStatus(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = CycleCountSQL.ResourceManager.GetString("UpdateStockCountStatus");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateStockCountStatus : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateRMStockCountStatus(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = CycleCountSQL.ResourceManager.GetString("UpdateRMStockCountStatus");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateRMStockCountStatus : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
