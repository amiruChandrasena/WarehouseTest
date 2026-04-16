using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using Models.Utility;
using Services.Ingres.SQLResources;

namespace Services.Ingres
{
    public class StockTransferService : IStockTransferService
    {
        private readonly string connectionString;

        public StockTransferService(IConfiguration configuration)
        {
            connectionString = configuration.GetConnectionString("IngresDatabase");
        }

        public TransactionWrapper GetAllRMStockTransferHeaderList(string originator, string defaultwarehouse)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<StockTransferRMHeaderModel> transDetails = new List<StockTransferRMHeaderModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = StockTransferSQL.ResourceManager.GetString("GetAllRMStockTransferHeaderList");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        //command.Parameters.Add("@Originator", OdbcType.VarChar).Value = originator;
                        command.Parameters.Add("@FromWarehouse", OdbcType.VarChar).Value = defaultwarehouse;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    StockTransferRMHeaderModel header = new StockTransferRMHeaderModel
                                    {
                                        TransferNo = dReader.tran_no,
                                        AuthCode = dReader.auth_code.Trim(),
                                        MoveType = dReader.movt_type.Trim(),
                                        MoveDate = dReader.movt_date,
                                        RefCode = dReader.ref_code.Trim(),
                                        WarehouseFrom = dReader.warehouse_from.Trim(),
                                        WarehouseTo = dReader.warehouse_to.Trim(),
                                        CarrierCode = dReader.carrier_code.Trim(),
                                        Narration = dReader.narration.Trim(),
                                        Status = dReader.status.Trim(),
                                        ManifestNo = dReader.manifest_no,
                                        Picker = dReader.picker.Trim(),
                                        OpenPalletNo = dReader.open_pallet_no
                                    };

                                    transDetails.Add(header);
                                }
                            } 
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllRMStockTransferHeaderList: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(transDetails);
            return wrapper;
        }

        public TransactionWrapper GetCatalogCodeCountInTransfer(int transferNo, string catalogCode, ref int count)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = StockTransferSQL.ResourceManager.GetString("GetCatalogCodeCountInTransfer");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = transferNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    count = reader.GetInt32(0);
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetCatalogCodeCountInTransfer: Reached seemingly impossible error condition");
                            }
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = true;
                    wrapper.Messages.Add("GetCatalogCodeCountInTransfer: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetRMStockTransferHeaderByTransNo(string transferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            StockTransferRMHeaderModel header = new StockTransferRMHeaderModel();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = StockTransferSQL.ResourceManager.GetString("GetRMStockTransferHeaderByTransferNo");
                    
                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@TransferNo", OdbcType.VarChar).Value = transferNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    header = new StockTransferRMHeaderModel
                                    {
                                        TransferNo = dReader.tran_no,
                                        AuthCode = dReader.auth_code.Trim(),
                                        MoveType = dReader.movt_type.Trim(),
                                        MoveDate = dReader.movt_date,
                                        RefCode = dReader.ref_code.Trim(),
                                        WarehouseFrom = dReader.warehouse_from.Trim(),
                                        WarehouseTo = dReader.warehouse_to.Trim(),
                                        CarrierCode = dReader.carrier_code.Trim(),
                                        Narration = dReader.narration.Trim(),
                                        Status = dReader.status.Trim(),
                                        ManifestNo = dReader.manifest_no,
                                        Picker = dReader.picker.Trim(),
                                        OpenPalletNo = dReader.open_pallet_no
                                    };
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMStockTransferHeaderByTransNo: No header found for Transfer No " + transferNo.ToString());
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMStockTransferHeaderByTransNo: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);
            return wrapper;
        }

        public TransactionWrapper GetRMStockTransferDetailsByTransNo(string transferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<StockTransferRMDetailModel> transDetails = new List<StockTransferRMDetailModel>();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string queryString = StockTransferSQL.ResourceManager.GetString("GetRMStockTransferDetailByTransferNo");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@TransferNo", OdbcType.VarChar).Value = transferNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            dynamic dReader = new DynamicDataReader(reader);

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    StockTransferRMDetailModel detObj = new StockTransferRMDetailModel
                                    {
                                        TransferNo = dReader.tran_no,
                                        CatalogCode = dReader.catlog_code.Trim(),
                                        Description = dReader.description.Trim(),
                                        PartNo = dReader.part_number,
                                        MoveQty = Math.Round(dReader.movt_qty, Common.Common.decimalPlaces),
                                        UnitPrice = Math.Round(dReader.unit_price, Common.Common.decimalPlaces),
                                        MoveQtyValue = Math.Round(dReader.move_qty_value, Common.Common.decimalPlaces),
                                        UomStock = dReader.uom_stock,
                                        IssueQty = Math.Round(dReader.issue_qty, Common.Common.decimalPlaces),
                                        OnHandPreQty = Math.Round(dReader.onhand_pre_qty, Common.Common.decimalPlaces),
                                        OnHandQtyWh2 = Math.Round(dReader.on_hand_qty_wh2, Common.Common.decimalPlaces)
                                    };

                                    transDetails.Add(detObj);
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetRMStockTransferDetailByTransferNo: No transfer details found for transfer no " + transferNo.ToString());
                                return wrapper;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetRMStockTransferDetailByTransferNo: " + e.Message);
                    return wrapper;
                }
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(transDetails);
            return wrapper;
        }

        public TransactionWrapper GetTransferPalletCount(int oldPalletNo, int newPalletNo, int transferNo, ref int count)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = StockTransferSQL.ResourceManager.GetString("GetTransferPalletCount");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@OldPalletNumber", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@NewPalletNumber", OdbcType.Int).Value = newPalletNo;
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = transferNo;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    count = reader.GetInt32(0);
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetTransferPalletCount: Reached seemingly impossible error condition");
                                return wrapper;
                            }
                        }

                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetTransferPalletCount: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper GetTransitWarehouse()
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string parameterName = "TRANSIT_WAREHOUSE_RM";
            string parameterValue = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string queryString = UtilitySQL.ResourceManager.GetString("GetSetting");

                    using (OdbcCommand command = new OdbcCommand(queryString, connection))
                    {
                        command.Parameters.Add("@ParameterName", OdbcType.VarChar).Value = parameterName;

                        using (OdbcDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    parameterValue = reader.GetString(0);
                                }
                            } else
                            {
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add(parameterName + " not in settings.");
                                return wrapper;
                            }

                            wrapper.IsSuccess = true;
                            wrapper.ResultSet.Add(parameterValue);
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetTransitWarehouse: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper InsertTransferPallet(int transferNo, int oldPalletNo, int newPalletNo, double issueQuantity)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = StockTransferSQL.ResourceManager.GetString("InsertTransferPallet");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = transferNo;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@NewPalletNo", OdbcType.Int).Value = newPalletNo;
                        command.Parameters.Add("@IssueQuantity", OdbcType.Double).Value = issueQuantity;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertTransferPallet: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateIssueTrasnferStatus(int TransferNo, string Status)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    string insertString = StockTransferSQL.ResourceManager.GetString("UpdateIssueTrasnferStatus");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@Status", OdbcType.VarChar).Value = Status;
                        command.Parameters.Add("@TransferNo", OdbcType.VarChar).Value = TransferNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateIssueTrasnferStatus : " + e.Message);
                    return wrapper;
                }
            }
        }
        
        public TransactionWrapper UpdateStockTrasnferPicker(int TransferNo, string Originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string insertString = StockTransferSQL.ResourceManager.GetString("UpdateStockTrasnferPicker");

                    using (OdbcCommand command = new OdbcCommand(insertString, connection))
                    {
                        command.Parameters.Add("@Originator", OdbcType.VarChar).Value = Originator;
                        command.Parameters.Add("@TransferNo", OdbcType.VarChar).Value = TransferNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        wrapper.IsSuccess = true;
                        return wrapper;
                    }
                }
                catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateStockTrasnferPicker : " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateTransferDetail(double issueQuantity, int transferNo, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = StockTransferSQL.ResourceManager.GetString("UpdateTransferDetail");

                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@IssueQty", OdbcType.Double).Value = issueQuantity;
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = transferNo;
                        command.Parameters.Add("@CatalogCode", OdbcType.VarChar).Value = catalogCode;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected == 1)
                        {
                            wrapper.IsSuccess = true;
                            return wrapper;
                        } else
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateTransferDetail: No details or more than one detail row found for transfer " + transferNo.ToString() + " and product " + catalogCode);
                            return wrapper;
                        }
                    }
                } catch (Exception e)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("UpdateTransferDetail: " + e.Message);
                    return wrapper;
                }
            }
        }

        public TransactionWrapper UpdateTransferPallet(double issueQuantity, int oldPalletNo, int newPalletNo, int transferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string updateString = StockTransferSQL.ResourceManager.GetString("UpdateTransferPallet");

                    using (OdbcCommand command = new OdbcCommand(updateString, connection))
                    {
                        command.Parameters.Add("@IssueQuantity", OdbcType.Double).Value = issueQuantity;
                        command.Parameters.Add("@OldPalletNo", OdbcType.Int).Value = oldPalletNo;
                        command.Parameters.Add("@NewPalletNo", OdbcType.Int).Value = newPalletNo;
                        command.Parameters.Add("@TransferNo", OdbcType.Int).Value = transferNo;

                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected < 1)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateTransferPallet: No record found");
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
                    wrapper.Messages.Add("UpdateTransferPallet: " + e.Message);
                    return wrapper;
                }
            }
        }
    }
}
