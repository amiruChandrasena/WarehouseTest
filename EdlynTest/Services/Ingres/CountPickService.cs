using Abstractions.ServiceInterfaces;
using Common;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Dto;
using Models.Utility;
using Services.Ingres.SQLResources;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Globalization;
using System.Text;

namespace Services.Ingres
{
    public class CountPickService : ICountPickService
    {
        private readonly string connectionString; 

        public CountPickService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper DeletePalletDetail(int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string deleteString = CountPickSQL.ResourceManager.GetString("DeletePalletDetail");
                    using (OdbcCommand command = new OdbcCommand(deleteString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("DeletePalletDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPalletLabelModels(int palletNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletLabelModel> palletLabels = new List<PalletLabelModel>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CountPickSQL.ResourceManager.GetString("GetPalletLabelModels");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;

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
                                    palletLabel.OldPalletNumber = dReader.old_pallet_no;
                                    palletLabel.PlanNumber = dReader.plan_no;
                                    palletLabel.LineNumber = dReader.line_no;
                                    palletLabel.Quality = dReader.quality;
                                    DateTime bestBeforeDate = dReader.best_before;
                                    palletLabel.BestBefore = bestBeforeDate.ToString(DateFormats.ddmmyyyywithouttime);
                                    palletLabel.Status = dReader.status;
                                    palletLabel.BatchNumber = dReader.batch_no;
                                    palletLabel.WarehouseId = dReader.warehouse_id;
                                    palletLabel.PalletUnits = dReader.pallet_units;
                                    palletLabel.OriginalPalletUnits = dReader.orig_pallet_units;
                                    palletLabel.BinLocation = dReader.bin_location;
                                    palletLabel.DaysOld = (DateTime.Now - palletLabel.PrintDate).Days;
                                    palletLabel.DaysLeft = (Convert.ToDateTime(palletLabel.BestBefore) - DateTime.Now).Days;
                                    if (palletLabel.BinLocation == "SC")
                                    {
                                        palletLabel.StockCount = "Yes";
                                    }

                                    palletLabels.Add(palletLabel);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPalletLabelModels : No pallets found for # " + palletNumber.ToString());
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = true;
                        wrapper.ResultSet.Add(palletLabels);
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetPalletLabelModels : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetPickLocationDetail(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = CountPickSQL.ResourceManager.GetString("GetPickLocationDetail");
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
                                    if (binLocation != dReader.warehouse_code + "." + dReader.room_code + "." + dReader.rack_code)
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("GetPickLocationDetail : Invalid pick location " + binLocation);
                                        return wrapper;
                                    }
                                    CountPickDto countPickDto = new CountPickDto();
                                    countPickDto.PalletNumber = dReader.licenced_pallet_no;
                                    countPickDto.CatalogCode = dReader.reserved_catlog_code.ToString().Trim();
                                    countPickDto.PalletUnits = Convert.ToInt32(dReader.units_left);
                                    wrapper.ResultSet.Add(countPickDto);
                                }
                            }
                            else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPickLocationDetail : Could not find details at " + binLocation);
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
                    wrapper.Messages.Add("GetPickLocationDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertPalletDetail(PalletLabelModel pallet, int palletNumber, string warehouseId)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = CountPickSQL.ResourceManager.GetString("InsertPalletDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;
                        command.Parameters.Add("@OldPalletNumber", OdbcType.Int).Value = pallet.OldPalletNumber;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = pallet.CatalogCode;
                        command.Parameters.Add("@OriginalPalletUnits", OdbcType.Int).Value = pallet.PalletUnits;
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = pallet.PalletUnits;
                        if (!String.IsNullOrEmpty(pallet.BestBefore))
                        {
                            command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = pallet.BestBefore;
                        }
                        else
                        {
                            wrapper.IsSuccess = true; // original OpenROAD skips any that have no best before date
                            return wrapper;
                        }
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseId;
                        command.Parameters.Add("@BatchNumber", OdbcType.Int).Value = pallet.BatchNumber;

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

        public TransactionWrapper InsertPalletMovement(int palletNumber, string reason, string originator, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = CountPickSQL.ResourceManager.GetString("InsertPalletMovement");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;
                        command.Parameters.Add("@TimeStamp", OdbcType.DateTime).Value = DateTime.Now;
                        command.Parameters.Add("@Reason", OdbcType.VarChar).Value = reason;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletMovement : " + e.Message);
                    return wrapper;
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

                    string insertString = CountPickSQL.ResourceManager.GetString("InsertStockCount");
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
                                command.Parameters.Add("@BestBefore", OdbcType.DateTime).Value = null;
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

        public TransactionWrapper UpdatePalletHeaderNotInLocation(int palletNumber, string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = CountPickSQL.ResourceManager.GetString("UpdatePalletHeaderNotInLocation");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletNumber", OdbcType.Int).Value = palletNumber;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdatePalletHeaderNotInLocation : " + e.Message);
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
                    string updateString = CountPickSQL.ResourceManager.GetString("UpdateStockCountStatus");
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

        public TransactionWrapper UpdateWarehouseConfigUnits(int palletUnits, string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string updateString = CountPickSQL.ResourceManager.GetString("UpdateWarehouseConfigUnits");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@PalletUnits", OdbcType.Int).Value = palletUnits;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = binLocation;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = true;
                    wrapper.Messages.Add("UpdateWarehouseConfigUnits : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
