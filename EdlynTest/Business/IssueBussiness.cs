using Abstractions.ServiceInterfaces;
using Microsoft.Extensions.Configuration;
using Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Transactions;

namespace Business
{
    public class IssueBussiness
    {
        private readonly string _connectionString;

        private readonly IIssueService _issueService;
        private readonly IRawMaterialService _rawMaterialsService;
        private readonly IPalletService _palletService;
        private readonly IJobService _jobService;
        private readonly IStockService _stockService;

        public IssueBussiness(IConfiguration configuration,
            IIssueService issueService, 
            IRawMaterialService rawMaterialsService, 
            IPalletService palletService, 
            IJobService jobService, 
            IStockService stockService)
        {
            _connectionString = configuration.GetConnectionString("IngresDatabase");

            _issueService = issueService;
            _rawMaterialsService = rawMaterialsService;
            _palletService = palletService;
            _jobService = jobService;
            _stockService = stockService;
        }

        public TransactionWrapper IssuePallet(IssuePalletRMModel issuePallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = IssuePalletSave(issuePallet.Originator,
                    issuePallet.PalletNo,
                    issuePallet.CatalogCode,
                    issuePallet.IssueQty,
                    issuePallet.BinLocation,
                    issuePallet.LocationIssue,
                    issuePallet.WarehouseFrom,
                    issuePallet.WarehouseTo);

            return wrapper;
        }

        private TransactionWrapper IssuePalletSave(string originator, int palletNo,
            string catalogCode, double issueQty,
            string binLocation, string locationIssue,
            string warehouseFrom, string warehouseTo)
        {
            //string locationIssue = string.Empty;
            //string warehouseFrom = string.Empty;
            //string warehouseTo = string.Empty;

            //if (warehouseId.StartsWith('M'))
            //{
            //    locationIssue = "M1.W1.BULK";
            //    warehouseFrom = "MR";
            //    warehouseTo = "MR";
            //}
            //else
            //{
            //    locationIssue = "E1.W1.BULK";
            //    warehouseFrom = "R1";
            //    warehouseTo = "R1";
            //}

            using (TransactionScope scope = new TransactionScope())
            {
                //--which warehouse is the pallet in ? 
                TransactionWrapper wrapper = _palletService.GetPalletDetailForIssuePallet(palletNo);
                PalletHeader palletHeader;
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    return wrapper;
                }
                else
                {
                    palletHeader = wrapper.ResultSet[0] as PalletHeader;
                    palletHeader.WarehouseId = warehouseTo;
                    palletHeader.BinLocation = binLocation;
                    palletHeader.PalletNumber = palletNo;

                    wrapper = _palletService.UpdatePalletHeaderWarehouseAndBinLocation(palletHeader);
                    if (!wrapper.IsSuccess)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (warehouseFrom != warehouseTo)
                    {
                        StockTransferModel transModel = new StockTransferModel();
                        transModel.MoveDate = DateTime.Now;
                        transModel.MoveType = "TRN";
                        transModel.Cost = 0;
                        transModel.Narration = "Pallet Transfer";
                        transModel.WarehouseFrom = warehouseFrom;
                        transModel.WarehouseTo = warehouseTo;

                        List<StockTransferDetailModel> stockTransDetailList = new List<StockTransferDetailModel>();
                        StockTransferDetailModel stockTransDetail = new StockTransferDetailModel();
                        stockTransDetail.CatalogCode = catalogCode;
                        stockTransDetail.BinLocationFrom = binLocation;
                        stockTransDetail.BinLocationTo = binLocation;
                        stockTransDetail.NegTag = "Y";
                        stockTransDetail.OnHandQty = 0;
                        stockTransDetail.OnHandFrom = 0;
                        stockTransDetail.OnHandTo = 0;
                        stockTransDetail.OnHandFromPre = 0;
                        stockTransDetail.OnHandToPre = 0;
                        stockTransDetail.Cost = 0;
                        stockTransDetail.StockVersion = 0;
                        stockTransDetail.MoveQty = issueQty;
                        stockTransDetail.SvMoveQty = 0;
                        stockTransDetail.RowState = "RsNew";
                        stockTransDetailList.Add(stockTransDetail);

                        transModel.StockDetails = stockTransDetailList;

                        wrapper = _stockService.SaveAdjustments(transModel);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    ////Insert into Log
                    string remark = "HH transfer of RM from " + warehouseFrom + "-" + binLocation + " to " + warehouseTo;
                    wrapper = _issueService.InsertIssueLog(DateTime.Now, palletNo, "STARTED", originator, remark, DateTime.Now);
                    if (!wrapper.IsSuccess)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                }

                scope.Complete();
                wrapper.ResultSet.Clear();
                wrapper.IsSuccess = true;
                return wrapper;
            }
        }

        public TransactionWrapper Issue(IssueRMModel issue)
        {
            string str = _connectionString;

            int palletNo = 0;
            TransactionWrapper wrapper = new TransactionWrapper();



            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    if (issue.TagId == "")
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Issue : Tag ID empty");
                        return wrapper;
                    }

                    PalletLabelModel palletLabel;
                    wrapper = _palletService.GetRMPalletNumberbyScanPalletLabel(issue.TagId);
                    if (wrapper.IsSuccess == false)
                    {
                        palletLabel = new PalletLabelModel();
                    }
                    else
                    {
                        palletLabel = wrapper.ResultSet[0] as PalletLabelModel;
                    }

                    if (palletLabel.PalletNumber > 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Issue : Tag ID already exist in system.");
                        return wrapper;
                    }

                    //CREATE a new pallet        
                    PalletHeader palletHeader = new PalletHeader();
                    palletHeader.PalletNumber = 0;
                    palletHeader.PrintDate = DateTime.Now;
                    palletHeader.PlanNumber = -3;
                    palletHeader.TransferStatus = "T";
                    palletHeader.WarehouseId = issue.WarehouseTo;
                    palletHeader.Status = "W";
                    palletHeader.Quality = "G";
                    palletHeader.PrintedAt = "HH-" + issue.Originator;
                    palletHeader.BinLocation = issue.BinLocation;
                    palletHeader.PickingLabel = issue.TagId;

                    wrapper = _palletService.CreatePalletHeader(palletHeader, ref palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        return wrapper;
                    }

                    // Create New Pallet Details
                    PalletDetail palletDetail = new PalletDetail();
                    palletDetail.PalletNumber = palletNo;
                    palletDetail.CatalogCode = issue.CatalogCode;
                    palletDetail.OldPalletNumber = issue.PalletNo;
                    palletDetail.IssueQty = issue.IssueQty;
                    palletDetail.BatchNumber = issue.BatchNo;

                    DateTime parsedDate;
                    string format = Common.DateFormats.ddmmyyyywithouttime;
                    if (DateTime.TryParseExact(issue.BestBefore, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate))
                    {
                        // Parsing successful, now you can use parsedDate
                        palletDetail.BestBefore = parsedDate;
                    }
                    else
                    {
                        palletDetail.BestBefore = DateTime.MaxValue;
                    }

                    palletDetail.WarehouseId = issue.WarehouseTo;

                    wrapper = _palletService.InsertPalletDetailsForIssue(palletDetail);

                    //Update quantity with Old Pallet details
                    _palletService.UpdatePalletQuantity(issue.PalletNo, issue.AvailableQty, issue.IssueQty, issue.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Issue : UpdatePalletQuantity Error");
                        return wrapper;
                    }

                    //check and DELETE IF pallet is 0 
                    _palletService.DeleteEmptyPallet(issue.PalletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Issue : DeleteEmptyPallet Error");
                        return wrapper;
                    }

                    if (issue.WarehouseFrom != issue.WarehouseTo)
                    {
                        StockTransferModel transModel = new StockTransferModel();
                        transModel.MoveDate = DateTime.Now;
                        transModel.MoveType = "TRN";
                        transModel.Cost = 0;
                        transModel.Narration = "Pallet Transfer";
                        transModel.WarehouseFrom = issue.WarehouseFrom;
                        transModel.WarehouseTo = issue.WarehouseTo;

                        List<StockTransferDetailModel> stockTransDetailList = new List<StockTransferDetailModel>();
                        StockTransferDetailModel stockTransDetail = new StockTransferDetailModel();
                        stockTransDetail.CatalogCode = issue.CatalogCode;
                        stockTransDetail.BinLocationFrom = issue.BinLocation;
                        stockTransDetail.BinLocationTo = issue.BinLocation;
                        stockTransDetail.NegTag = "Y";
                        stockTransDetail.OnHandQty = 0;
                        stockTransDetail.OnHandFrom = 0;
                        stockTransDetail.OnHandTo = 0;
                        stockTransDetail.OnHandFromPre = 0;
                        stockTransDetail.OnHandToPre = 0;
                        stockTransDetail.Cost = 0;
                        stockTransDetail.StockVersion = 0;
                        stockTransDetail.MoveQty = issue.IssueQty;
                        stockTransDetail.SvMoveQty = 0;
                        stockTransDetail.RowState = "RsNew";
                        stockTransDetailList.Add(stockTransDetail);

                        transModel.StockDetails = stockTransDetailList;

                        wrapper = _stockService.SaveAdjustments(transModel);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    string LogRemark = "HH transfer of RM from " + issue.WarehouseFrom + "-" + issue.BinLocation
                                       + ":" + palletNo.ToString()
                                       + " to " + issue.WarehouseTo + ":" + palletNo.ToString();
                    _issueService.InsertIssueLog(DateTime.Now, palletNo, "STARTED", issue.Originator, LogRemark, DateTime.MinValue);

                    scope.Complete();
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Issue: " + e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

    }
}
