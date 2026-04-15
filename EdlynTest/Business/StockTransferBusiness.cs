using Abstractions.ServiceInterfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Business
{
    public class StockTransferBusiness
    {
        private readonly IStockTransferService _stockTransferService;
        private readonly IRawMaterialService _rawMaterialsService;
        private readonly IPalletService _palletService;

        public StockTransferBusiness(IStockTransferService stockTransferService, IRawMaterialService rawMaterialsService, IPalletService palletService)
        {
            _stockTransferService = stockTransferService;
            _rawMaterialsService = rawMaterialsService;
            _palletService = palletService;
        }

        public TransactionWrapper GetRMStockTransferByTransferNo(string transferNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _stockTransferService.GetRMStockTransferHeaderByTransNo(transferNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            StockTransferRMHeaderModel header = wrapper.ResultSet[0] as StockTransferRMHeaderModel;

            if (header.Status == "P")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("This transfer number has been Picked");
                return wrapper;
            }
            else if (header.Status == "C")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("This transfer number has been Completed");
                return wrapper;
            }
            else if (header.Status == "T")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("This transfer number has been Transfered");
                return wrapper;
            }

            if (header.ManifestNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add($"This transfer {transferNo} has not been manifested");
                return wrapper;
            }

            wrapper = _stockTransferService.GetRMStockTransferDetailsByTransNo(transferNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            List<StockTransferRMDetailModel> details = wrapper.ResultSet[0] as List<StockTransferRMDetailModel>;
            if (details.Count > 0)
                header.StockTransferDetails = details;

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);

            return wrapper;
        }

        public TransactionWrapper GetAllRMStockTransferHeaderList(string originator, string defaultwarehouse)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _stockTransferService.GetAllRMStockTransferHeaderList(originator, defaultwarehouse);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            List<StockTransferRMHeaderModel> header = wrapper.ResultSet[0] as List<StockTransferRMHeaderModel>;
            if (header.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add($"There are no transfers for picker {originator}");
                return wrapper;
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);

            return wrapper;
        }

        public TransactionWrapper GetTransitWarehouse()
        {
            return _stockTransferService.GetTransitWarehouse();
        }

        public TransactionWrapper IssueTransferRM(IssueTransferRMModel issueTrans)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    //Get rate tonne from rm_master by catalog number
                    double Lf_rateTone = _rawMaterialsService.GetRateTonneByCatalogCode(issueTrans.CatalogCode);
                    if (Lf_rateTone == 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess=false;
                        wrapper.Messages.Add($"GetRateTonneByCatalogCode: Rate Tonne is 0 for Catalog code {issueTrans.CatalogCode}");
                        return wrapper;
                    }

                    //Load old selected pallet number
                    wrapper = _palletService.GetPalletDetail(issueTrans.OldPalletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    PalletHeader pallet = wrapper.ResultSet[0] as PalletHeader;
                    PalletDetail palletDetail = pallet.PalletDetails[0];

                    //Update OLD Pallet Quantity with Issue Quantity
                    wrapper = _palletService.UpdatePalletQuantity(issueTrans.OldPalletNo, palletDetail.StockQty, issueTrans.IssueQty, issueTrans.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    //Check pallet is fully issued
                    //if ((issueTrans.AvailableQty - issueTrans.IssueQty) < 0.01)
                    if (issueTrans.OpenPalletNo == 0) // android side is checking if full pallet, above check isn't working 100%
                    {
                        wrapper = _palletService.UpdatePalletHeaderRMforFullIssue(issueTrans.OldPalletNo,
                                                                                        issueTrans.WarehouseCode,
                                                                                        issueTrans.Originator, "");
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        wrapper = _palletService.UpdatePalletDetailRMforFullIssue(issueTrans.OldPalletNo,
                                                                                        issueTrans.WarehouseCode,
                                                                                        issueTrans.ManifestNo,
                                                                                        issueTrans.IssueQty);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    //Check if detail line needs to be added
                    if (issueTrans.OpenPalletNo != 0)
                    {
                        wrapper = _palletService.GetAllRMPalletDetailsByOldNewPalletNo(issueTrans.OpenPalletNo, issueTrans.OldPalletNo);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }

                        //List<PalletDetail> palletDetails = wrapper.ResultSet[0] as List<PalletDetail>;
                        PalletDetail issueTransPalletDetails = new PalletDetail();
                        issueTransPalletDetails.BatchNumber = pallet.BatchNo;
                        issueTransPalletDetails.BestBefore = palletDetail.BestBefore;
                        issueTransPalletDetails.CatalogCode = issueTrans.CatalogCode;
                        issueTransPalletDetails.IssueQty = issueTrans.IssueQty;
                        issueTransPalletDetails.ManifestNo = issueTrans.ManifestNo;
                        issueTransPalletDetails.OldPalletNumber = issueTrans.OldPalletNo;
                        issueTransPalletDetails.OriginalPalletUnits = Convert.ToInt32(issueTrans.IssueQty);
                        issueTransPalletDetails.PalletNumber = issueTrans.OpenPalletNo;
                        issueTransPalletDetails.PalletUnits = Convert.ToInt32(issueTrans.IssueQty);
                        issueTransPalletDetails.WarehouseId = issueTrans.WarehouseCode;
                        issueTransPalletDetails.StockQty = issueTrans.IssueQty;

                        if (wrapper.ResultSet.Count == 0) // check if any pallets were returned from last query
                        {
                            wrapper = _palletService.InsertPalletDetails(issueTransPalletDetails);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                        else
                        {
                            wrapper = _palletService.UpdatePalletDetails(issueTransPalletDetails);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                    }

                    int count = 0;
                    wrapper = _stockTransferService.GetCatalogCodeCountInTransfer(issueTrans.TransferNo, issueTrans.CatalogCode, ref count);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (count > 0)
                    {
                        wrapper = _stockTransferService.UpdateTransferDetail(issueTrans.IssueQty, issueTrans.TransferNo, issueTrans.CatalogCode);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }
                    else
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Item code " + issueTrans.CatalogCode.Trim() + " is not in transfer number " + issueTrans.TransferNo.ToString());
                    }

                    int newPalletNo = 0;
                    if (issueTrans.OpenPalletNo == 0)
                    {
                        newPalletNo = issueTrans.OldPalletNo;
                    }
                    else
                    {
                        newPalletNo = issueTrans.OpenPalletNo;
                    }

                    wrapper = _stockTransferService.GetTransferPalletCount(issueTrans.OldPalletNo, newPalletNo, issueTrans.TransferNo, ref count);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (count == 0)
                    {
                        wrapper = _stockTransferService.InsertTransferPallet(issueTrans.TransferNo, issueTrans.OldPalletNo, newPalletNo, issueTrans.IssueQty);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }
                    else
                    {
                        wrapper = _stockTransferService.UpdateTransferPallet(issueTrans.IssueQty, issueTrans.OldPalletNo, newPalletNo, issueTrans.TransferNo);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    scope.Complete();
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper FinalizeTransfer(StockTransferRMHeaderModel issueTrans)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _stockTransferService.UpdateIssueTrasnferStatus(issueTrans.TransferNo, "P");
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _stockTransferService.UpdateStockTrasnferPicker(issueTrans.TransferNo, issueTrans.Originator);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }
    }
}
