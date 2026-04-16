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
    public class StockService : IStockService
    {
        private readonly string connectionString;

        public StockService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper SaveStockDocket(StockDocketModel stockDocket)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = StockSQL.ResourceManager.GetString("GetOnHandQty");
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDocket.CatalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDocket.WarehouseId;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while(reader.Read())
                                {
                                    stockDocket.OnHandPre = reader.GetDouble(0);
                                }
                            } else
                            {
                                stockDocket.OnHandPre = 0;
                            }
                        }
                    }

                    int multiplier = 0;
                    if (stockDocket.MoveType.Equals("BATC") || stockDocket.MoveType.Equals("FEED"))
                    {
                        multiplier = -1;
                    } else
                    {
                        multiplier = 1;
                    }

                    string insertString = StockSQL.ResourceManager.GetString("InsertStockMovement");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDocket.CatalogCode;
                        command.Parameters.Add("@Timestamp", OdbcType.DateTime).Value = DateTime.Parse(DateTime.Now.ToString(), new CultureInfo("en-AU")).ToString(DateFormats.ddMMyyWithTime);
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDocket.WarehouseId;
                        command.Parameters.Add("@MoveDate", OdbcType.Date).Value = DateTime.Parse(stockDocket.MoveDate.ToString(), new CultureInfo("en-AU")).ToString(DateFormats.ddMMyy);
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = stockDocket.MoveQty * multiplier;
                        command.Parameters.Add("@MoveType", OdbcType.VarChar).Value = stockDocket.MoveType;
                        command.Parameters.Add("@OldCost", OdbcType.Double).Value = stockDocket.RateTonne;
                        command.Parameters.Add("@Narration", OdbcType.VarChar).Value = stockDocket.Narration;
                        command.Parameters.Add("@DocNoPeer", OdbcType.Int).Value = stockDocket.JobNo;
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = stockDocket.Originator;
                        command.Parameters.Add("@OnHandPre", OdbcType.Double).Value = stockDocket.OnHandPre;

                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    wrapper = UpdateOnHandQuantityHeader(stockDocket.MoveQty * multiplier, stockDocket.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    } else
                    {
                        int rowsUpdated = Convert.ToInt32(wrapper.ResultSet[0]);
                        if (rowsUpdated <= 0)
                        {
                            wrapper = InsertOnHandQuantityHeader(stockDocket.CatalogCode, "N", 1, stockDocket.MoveQty * multiplier);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                        }
                    }

                    wrapper = UpdateOnHandQtyDetail(stockDocket.CatalogCode, stockDocket.WarehouseId, stockDocket.MoveQty * multiplier);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    } else
                    {
                        int rowsUpdated = Convert.ToInt32(wrapper.ResultSet[0]);
                        if (rowsUpdated <= 0)
                        {
                            wrapper = InsertOnHandQtyDetail(stockDocket.CatalogCode, stockDocket.WarehouseId, stockDocket.MoveQty * multiplier);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                        }
                    }

                    wrapper = UpdateMixDetail(stockDocket.JobNo, 0, stockDocket.CatalogCode, "P");

                    return wrapper;

                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("SaveStockDocket : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper SaveAdjustments(StockTransferModel stockObj)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            //IF no record in the header TABLE INSERT it 
            foreach (StockTransferDetailModel det in stockObj.StockDetails)
            {
                if (det.RowStatus == "RsNew")
                {
                    PuStockDetailModel puStockDetail = new PuStockDetailModel();

                    //get current on hand qty 
                    puStockDetail = GetCurrentOnhandQtyByCatalogCodeAndWarehouse(det.CatalogCode, stockObj.WarehouseFrom);

                    if (puStockDetail == null || puStockDetail.CatalogCode == "")
                    {
                        puStockDetail.CatalogCode = det.CatalogCode;
                        puStockDetail.OnHandQty = 0;
                        puStockDetail.Status = "A";
                        puStockDetail.WarehouseId = stockObj.WarehouseFrom;

                        //Save StockDetail
                        wrapper = InsertStockDetail(puStockDetail);
                    }

                    det.OnHandFromPre = puStockDetail.OnHandQty;

                    //Deduct move quantity from warehouse
                    wrapper = UpdateOnHandQtyDetail(det.CatalogCode, stockObj.WarehouseFrom, -1 * det.MoveQty);
                    if (!wrapper.IsSuccess)
                    {
                        return wrapper;
                    } else
                    {
                        int rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                        wrapper = InsertOnHandQtyDetail(det.CatalogCode, stockObj.WarehouseFrom, -1 * det.MoveQty);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                    }

                    PuStockMovementModel stMoveObj = new PuStockMovementModel();
                    stMoveObj.CatalogCode = det.CatalogCode;
                    stMoveObj.WarehouseId = stockObj.WarehouseFrom;
                    stMoveObj.BinLocation = det.BinLocationFrom;
                    stMoveObj.MoveQty = -1 * det.MoveQty;
                    stMoveObj.OnhandPre = det.OnHandFromPre;
                    stMoveObj.Timestamp = DateTime.Now;
                    stMoveObj.MoveDate = stockObj.MoveDate;
                    stMoveObj.MoveType = stockObj.MoveType;
                    stMoveObj.OldCost = det.UnitPrice;
                    stMoveObj.NewCost = stockObj.Cost;
                    stMoveObj.AuthCode = stockObj.Originator;
                    stMoveObj.SourceCode = "U";
                    stMoveObj.SourceNo = 170;
                    stMoveObj.RefCode = "";
                    stMoveObj.Narration = stockObj.Narration;
                    stMoveObj.AnalysisCode = "";
                    wrapper = InsertStockMovement(stMoveObj);
                    if (!wrapper.IsSuccess)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveAdjustments : Error from Insert StockMovement");
                        return wrapper;
                    }

                    //get current on hand qty - to warehouse
                    puStockDetail = GetCurrentOnhandQtyByCatalogCodeAndWarehouse(det.CatalogCode, stockObj.WarehouseTo);

                    if (puStockDetail == null || puStockDetail.CatalogCode == "")
                    {
                        puStockDetail.CatalogCode = det.CatalogCode;
                        puStockDetail.OnHandQty = 0;
                        puStockDetail.Status = "A";
                        puStockDetail.WarehouseId = stockObj.WarehouseTo;

                        //Save StockDetail
                        wrapper = InsertStockDetail(puStockDetail);
                    }

                    det.OnHandToPre = puStockDetail.OnHandQty;

                    //Deduct move quantity from warehouse
                    wrapper = UpdateOnHandQtyDetail(det.CatalogCode, stockObj.WarehouseTo, det.MoveQty);
                    if (!wrapper.IsSuccess)
                    {
                        return wrapper;
                    } else
                    {
                        int rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                        wrapper = InsertOnHandQtyDetail(det.CatalogCode, stockObj.WarehouseFrom, det.MoveQty);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                    }

                    stMoveObj = new PuStockMovementModel();
                    stMoveObj.CatalogCode = det.CatalogCode;
                    stMoveObj.WarehouseId = stockObj.WarehouseTo;
                    stMoveObj.BinLocation = det.BinLocationTo;
                    stMoveObj.MoveQty = det.MoveQty;
                    stMoveObj.OnhandPre = det.OnHandToPre;
                    stMoveObj.Timestamp = DateTime.Now;
                    stMoveObj.MoveDate = stockObj.MoveDate;
                    stMoveObj.MoveType = stockObj.MoveType;
                    stMoveObj.OldCost = det.UnitPrice;
                    stMoveObj.NewCost = stockObj.Cost;
                    stMoveObj.AuthCode = stockObj.Originator;
                    stMoveObj.SourceCode = "U";
                    stMoveObj.SourceNo = 170;
                    stMoveObj.RefCode = "";
                    stMoveObj.Narration = stockObj.Narration;
                    stMoveObj.AnalysisCode = "";
                    wrapper = InsertStockMovement(stMoveObj);
                    if (!wrapper.IsSuccess)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("SaveAdjustments : Error from Insert StockMovement");
                        return wrapper;
                    }

                    wrapper.IsSuccess = true;
                }
            }

            return wrapper;
        }

        public PuStockDetailModel GetCurrentOnhandQtyByCatalogCodeAndWarehouse(string catalogCode, string warehouseFrom)
        {
            PuStockDetailModel detail = new PuStockDetailModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = StockSQL.ResourceManager.GetString("GetCurrentOnhandQtyByCatalogCodeAndWarehouse");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseFrom", OdbcType.VarChar).Value = warehouseFrom;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    detail = new PuStockDetailModel();
                                    detail.BinLoc1 = dReader.bin_loc_1;
                                    detail.BinLoc2 = dReader.bin_loc_2;
                                    detail.BinLoc3 = dReader.bin_loc_3;
                                    detail.CatalogCode = dReader.catlog_code;
                                    detail.OnHandQty = double.Parse(dReader.on_hand_qty);
                                    detail.Status = dReader.status;
                                    detail.WarehouseId = dReader.warehouse_id;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    detail = new PuStockDetailModel();
                }
            }

            return detail;
        }

        public TransactionWrapper InsertStockDetail(PuStockDetailModel stockDet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string sqlString = StockSQL.ResourceManager.GetString("InsertStockDetail");
                    using (OdbcCommand command = new OdbcCommand(sqlString, connection))
                    {
                        command.Parameters.Add("BinLoc1", OdbcType.VarChar).Value = stockDet.BinLoc1;
                        command.Parameters.Add("BinLoc2", OdbcType.VarChar).Value = stockDet.BinLoc2;
                        command.Parameters.Add("BinLoc3", OdbcType.VarChar).Value = stockDet.BinLoc3;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDet.CatalogCode;
                        command.Parameters.Add("@OnHandQty", OdbcType.VarChar).Value = stockDet.OnHandQty;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = stockDet.Status;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDet.WarehouseId;

                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertStockDetail : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper InsertStockMovement(PuStockMovementModel stockDet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string sqlString = StockSQL.ResourceManager.GetString("InsertStockMovement");
                    using (OdbcCommand command = new OdbcCommand(sqlString, connection))
                    {
                        command.Parameters.Add("@AnalysisCode", OdbcType.VarChar).Value = stockDet.AnalysisCode;
                        command.Parameters.Add("@AuthCode", OdbcType.VarChar).Value = stockDet.AuthCode;
                        command.Parameters.Add("@BatchNo", OdbcType.VarChar).Value = stockDet.BatchNo;
                        command.Parameters.Add("@BinLocation", OdbcType.VarChar).Value = stockDet.BinLocation;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = stockDet.CatalogCode;
                        command.Parameters.Add("@CatalogCodeT", OdbcType.VarChar).Value = stockDet.CatalogCode;
                        command.Parameters.Add("@CustCode", OdbcType.VarChar).Value = stockDet.CustCode;
                        command.Parameters.Add("@DocNoPeer", OdbcType.VarChar).Value = stockDet.DocNoPeer;
                        command.Parameters.Add("@DocNoSupp", OdbcType.VarChar).Value = stockDet.DocNoSupp;
                        command.Parameters.Add("@MoveDate", OdbcType.DateTime).Value = stockDet.MoveDate;
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = stockDet.MoveQty;
                        command.Parameters.Add("@MoveQtyS", OdbcType.VarChar).Value = stockDet.MoveQtyS;
                        command.Parameters.Add("@MoveType", OdbcType.VarChar).Value = stockDet.MoveType;
                        command.Parameters.Add("@Narration", OdbcType.VarChar).Value = stockDet.Narration;
                        command.Parameters.Add("@NewCost", OdbcType.VarChar).Value = stockDet.NewCost;
                        command.Parameters.Add("@OldCost", OdbcType.VarChar).Value = stockDet.OldCost;
                        command.Parameters.Add("@OnHandPre", OdbcType.VarChar).Value = stockDet.OnhandPre;
                        command.Parameters.Add("@RefCode", OdbcType.VarChar).Value = stockDet.RefCode;
                        command.Parameters.Add("@SalesNo", OdbcType.VarChar).Value = stockDet.SalesNo;
                        command.Parameters.Add("@SourceCode", OdbcType.VarChar).Value = stockDet.SourceCode;
                        command.Parameters.Add("@SourceNo", OdbcType.Int).Value = stockDet.SourceNo;
                        command.Parameters.Add("@SuppCode", OdbcType.VarChar).Value = stockDet.SuppCode;
                        command.Parameters.Add("@Timestamp", OdbcType.VarChar).Value = stockDet.Timestamp;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = stockDet.WarehouseId;
                        command.Parameters.Add("@WarehouseIdT", OdbcType.VarChar).Value = stockDet.WarehouseIdT;

                        int rowsAffected = command.ExecuteNonQuery();
                    }

                    wrapper.IsSuccess = true;
                    return wrapper;
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertStockMovement : " + e.Message);
                    return wrapper;
                }
            }

        }

        public TransactionWrapper UpdateOnHandQtyDetail(string catalogCode, string warehouseFrom, double moveQty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string sqlString = StockSQL.ResourceManager.GetString("UpdateOnHandQtyDetail");
                    using (OdbcCommand command = new OdbcCommand(sqlString, connection))
                    {
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = moveQty;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseFrom", OdbcType.VarChar).Value = warehouseFrom;

                        int rowsAffected = command.ExecuteNonQuery();

                        wrapper.ResultSet.Add(rowsAffected);
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                    
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateOnHandQtyDetail : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertOnHandQtyDetail(string catalogCode, string warehouseId, double moveQty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = StockSQL.ResourceManager.GetString("InsertOnHandQtyDetail");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@WarehouseId", OdbcType.VarChar).Value = warehouseId;
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = moveQty;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertOnHandQtyDetail: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateOnHandQuantityHeader(double moveQty, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = StockSQL.ResourceManager.GetString("UpdateOnHandQtyHeader");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = moveQty;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.ResultSet.Add(rowsAffected);
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateOnHandQuantityHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertOnHandQuantityHeader(string catalogCode, string status, int version, double moveQty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = StockSQL.ResourceManager.GetString("InsertOnHandQtyHeader");
                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@Version", OdbcType.Int).Value = version;
                        command.Parameters.Add("@MoveQty", OdbcType.Double).Value = moveQty;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertOnHandQuantityHeader : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateMixDetail(int jobNo, int splitNo, string catalogCode, string status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = StockSQL.ResourceManager.GetString("UpdateMixDetail");
                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = status;
                        command.Parameters.Add("@JobNo", OdbcType.Int).Value = jobNo;
                        command.Parameters.Add("@SplitNo", OdbcType.Int).Value = splitNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateMixDetail : " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
