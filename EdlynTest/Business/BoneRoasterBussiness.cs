using Abstractions.ServiceInterfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Transactions;

namespace Business
{
    public class BoneRoasterBussiness
    {
        private readonly IBoneRoasterService _boneRoasterService;

        public BoneRoasterBussiness(IBoneRoasterService boneRoasterService)
        {
            _boneRoasterService = boneRoasterService;
        }
        
        public TransactionWrapper GetAllPalletDetails(string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            try
            {
                wrapper = _boneRoasterService.GetAllPalletDetails(warehouseCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                wrapper.IsSuccess = true;
                return wrapper;
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }
        
        public TransactionWrapper GetTodayManifestNumber()
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            try
            {
                //Get Manifet Number in Unique Key Table
                wrapper = _boneRoasterService.GetUniqueKey("MANF");
                if (!wrapper.IsSuccess)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUniqueKey: Cannot read manifest.");
                }
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
            }
            
            return wrapper;
        }

        public TransactionWrapper TransferPallet(TransferPalletBRModel transferPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    if (transferPallet.PalletDetails == null || transferPallet.PalletDetails.Count == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("TransferPallet: There are no pallets to transfer.");
                        return wrapper;
                    }

                    //Insert Manifest
                    wrapper = _boneRoasterService.InsertManifest(transferPallet);
                    if (!wrapper.IsSuccess)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("InsertManifest: Cannot update manifest unique key.");
                        return wrapper;
                    }

                    //Update Pallet Header and Pallet Details [OR-Line-191: UpdateTransferPallet For Loop]
                    foreach (var item in transferPallet.PalletDetails)
                    {
                        item.ManifestNo = transferPallet.ManifestNo;

                        wrapper = _boneRoasterService.UpdatePalletDetail(transferPallet.WarehouseTo, item);
                        if (!wrapper.IsSuccess) //B7 - Warehouse Code
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertPalletHeader: Cannot update pallet detail.");
                            return wrapper;
                        }

                        //Update pallet header Line 551
                        wrapper = _boneRoasterService.UpdatePalletHeader("B7", "P", item); //Change C7 - B7 Request from Shahan 2023/12/04
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertPalletHeader: Cannot update pallet header.");
                            return wrapper;
                        }
                    }

                    //Update manifest status - 'D' Line 574
                    wrapper = _boneRoasterService.UpdateManifestStatus(transferPallet.ManifestNo, "D");
                    if (!wrapper.IsSuccess)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("UpdateManifestStatus: Cannot update manifest status as 'D'.");
                        return wrapper;
                    }


                    // Transfer Stock
                    // Li_RtnStatus = Lu_StockMovts.TransferStock
                    string strSellingCode = "";
                    float fltOnHandPre = 0;
                    int intVersion = 0;

                    foreach (var item in transferPallet.PalletDetails)
                    {
                        #region Lu_StockMovts.TransferStock
                        //Get Lv_SellingCode
                        wrapper = _boneRoasterService.GetSellingCodeByCatalogCode(item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetSellingCodeByCatalogCode: Cannot read selling code.");
                        }

                        strSellingCode = (wrapper.ResultSet[0] as PalletBRModel).SellingCode;
                        if (string.IsNullOrEmpty(strSellingCode))
                        {
                            strSellingCode = item.CatalogCode;
                        }

                        #region From Warehouse

                        //Get Lf_OnHandPre for From Warehouse
                        wrapper = _boneRoasterService.GetOnHandPreByCatalogCode(transferPallet.WarehouseFrom, item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"GetSellingCodeByCatalogCode: Error fetching stock details for {item.CatalogCode} In warehouse {transferPallet.WarehouseFrom}");
                        }

                        fltOnHandPre = (wrapper.ResultSet[0] as StockDetailBRModel).OnHandQty;
                        intVersion = (wrapper.ResultSet[0] as StockDetailBRModel).Version;

                        //Update the stock details for FromWarehouse 
                        StockDetailBRModel stockDetailBRModel = new StockDetailBRModel();
                        stockDetailBRModel.TransferQty = -1 * item.PalletUnits;
                        stockDetailBRModel.CatalogCode = item.CatalogCode;
                        stockDetailBRModel.WarehouseCode = transferPallet.WarehouseFrom;
                        stockDetailBRModel.Version = intVersion;
                        wrapper = _boneRoasterService.UpdateStockDetailTransferQty(stockDetailBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"UpdateStockDetailTransferQty: Cannot update stock detail for {transferPallet.WarehouseFrom}");
                            return wrapper;
                        }

                        //Update the stock movement for FromWarehouse 
                        StockMovementBRModel stockMovementBRModel = new StockMovementBRModel();
                        stockMovementBRModel.CatalogCode = item.CatalogCode;
                        stockMovementBRModel.WarehouseCode = transferPallet.WarehouseFrom;
                        stockMovementBRModel.MoveQty = -1 * item.PalletUnits;
                        stockMovementBRModel.UserId = transferPallet.UserId;
                        stockMovementBRModel.OnHandQty = fltOnHandPre;
                        wrapper = _boneRoasterService.InsertStockMovementTransferPallet(stockMovementBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"InsertStockMovementTransferPallet: Cannot insert stock movement for {transferPallet.WarehouseFrom}");
                            return wrapper;
                        }

                        #endregion

                        #region To Warehouse

                        //Get Lf_OnHandPre for To Warehouse
                        wrapper = _boneRoasterService.GetOnHandPreByCatalogCode(transferPallet.WarehouseTo, item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"GetSellingCodeByCatalogCode: Error fetching stock details for {item.CatalogCode} In warehouse {transferPallet.WarehouseFrom}");
                        }

                        fltOnHandPre = (wrapper.ResultSet[0] as StockDetailBRModel).OnHandQty;
                        intVersion = (wrapper.ResultSet[0] as StockDetailBRModel).Version;

                        //Update /Insert the stock details for ToWarehouse 
                        stockDetailBRModel = new StockDetailBRModel();
                        stockDetailBRModel.TransferQty = item.PalletUnits;
                        stockDetailBRModel.CatalogCode = item.CatalogCode;
                        stockDetailBRModel.WarehouseCode = transferPallet.WarehouseTo;
                        stockDetailBRModel.Version = intVersion;
                        wrapper = _boneRoasterService.UpdateStockDetailTransferQty(stockDetailBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"UpdateStockDetailTransferQty: Cannot update stock detail for {transferPallet.WarehouseTo}");
                            return wrapper;
                        }

                        //Update the stock movement for FromWarehouse 
                        stockMovementBRModel = new StockMovementBRModel();
                        stockMovementBRModel.CatalogCode = item.CatalogCode;
                        stockMovementBRModel.WarehouseCode = transferPallet.WarehouseTo;
                        stockMovementBRModel.MoveQty = item.PalletUnits;
                        stockMovementBRModel.UserId = transferPallet.UserId;
                        stockMovementBRModel.OnHandQty = fltOnHandPre;
                        wrapper = _boneRoasterService.InsertStockMovementTransferPallet(stockMovementBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"InsertStockMovementTransferPallet: Cannot insert stock movement for {transferPallet.WarehouseTo}");
                            return wrapper;
                        }

                        #endregion

                        #endregion

                        #region Lu_pallet_header.UpdateWarehouse

                        wrapper = _boneRoasterService.UpdatePalletHeaderWarehouseCode(transferPallet.WarehouseTo, item);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderWarehouseCode: Cannot update pallet header.");
                            return wrapper;
                        }

                        #endregion

                        //#region Lu_pallet_header.setBinLocation

                        ////Li_RtnStatus = Lu_pallet_header.setBinLocation(bin_location = 'B1.F1.PROD', status = 'P');
                        ////Update manifest status - 'D' Line 574
                        //wrapper = _boneRoasterService.UpdatePalletHeaderBinLocation("B1.F1.PROD", "P", item);
                        //if (!wrapper.IsSuccess)
                        //{
                        //    scope.Dispose();
                        //    wrapper.IsSuccess = false;
                        //    wrapper.Messages.Add("UpdatePalletHeaderBinLocation: Cannot update bin location status as 'P'.");
                        //    return wrapper;
                        //}

                        //#endregion
                    }

                    //Update manifest warehouse by manifest 
                    wrapper = _boneRoasterService.UpdateManifestWarehouseByManifetsNo(transferPallet.ManifestNo, transferPallet.WarehouseTo);
                    if (!wrapper.IsSuccess)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("UpdateManifestWarehouseByManifetsNo: Cannot update manifest info.");
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                }
            }
            
            return wrapper;
        }

        public TransactionWrapper ReceivePallet(ReceivePalletBRModel receivePallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (receivePallet.ManifestNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("ReceivePallet: Please enter a Manifest first");
                return wrapper;
            }

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    //Update Pallet Header and Pallet Details [UpdateTransferPallet For Loop]
                    foreach (var item in receivePallet.PalletDetails)
                    {
                        string whTo = receivePallet.WarehouseTo;

                        //Comment this lines for Request by Shahan 2023/12/04
                        //if (receivePallet.WarehouseTo == "BR")
                        //    whTo = "B1"; 

                        whTo = "BR";

                        PalletBRModel palletBR = item;
                        palletBR.ManifestNo = receivePallet.ManifestNo;

                        //Update pallet detail Line 540
                        wrapper = _boneRoasterService.UpdatePalletDetail(whTo, palletBR);
                        if (!wrapper.IsSuccess)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletDetail: Cannot update pallet detail.");
                            return wrapper;
                        }

                        //Update pallet header Line 551
                        wrapper = _boneRoasterService.UpdatePalletHeader(whTo, "P", palletBR); //Change C7 to BR Request by Shahan 2023/12/04
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeader: Cannot update pallet header.");
                            return wrapper;
                        }

                        //Update manifest status - 'D' Line 574
                        wrapper = _boneRoasterService.UpdateManifestStatus(palletBR.ManifestNo, "D");
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdateManifestStatus: Cannot update manifest status as 'D'.");
                            return wrapper;
                        }
                    }

                    // Transfer Stock
                    // Li_RtnStatus = Lu_StockMovts.TransferStock
                    string strSellingCode = "";
                    float fltOnHandPre = 0;
                    int intVersion = 0;

                    foreach (var item in receivePallet.PalletDetails)
                    {
                        #region Lu_StockMovts.TransferStock
                        //Get Lv_SellingCode
                        wrapper = _boneRoasterService.GetSellingCodeByCatalogCode(item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetSellingCodeByCatalogCode: Cannot read selling code.");
                        }

                        strSellingCode = (wrapper.ResultSet[0] as PalletBRModel).SellingCode;
                        if (string.IsNullOrEmpty(strSellingCode))
                        {
                            strSellingCode = item.CatalogCode;
                        }

                        #region From Warehouse

                        //Get Lf_OnHandPre for From Warehouse
                        wrapper = _boneRoasterService.GetOnHandPreByCatalogCode(receivePallet.WarehouseFrom, item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"GetSellingCodeByCatalogCode: Error fetching stock details for {item.CatalogCode} In warehouse {receivePallet.WarehouseFrom}");
                        }

                        fltOnHandPre = (wrapper.ResultSet[0] as StockDetailBRModel).OnHandQty;
                        intVersion = (wrapper.ResultSet[0] as StockDetailBRModel).Version;

                        //Update the stock details for FromWarehouse 
                        StockDetailBRModel stockDetailBRModel = new StockDetailBRModel();
                        stockDetailBRModel.TransferQty = -1 * item.PalletUnits;
                        stockDetailBRModel.CatalogCode = item.CatalogCode;
                        stockDetailBRModel.WarehouseCode = receivePallet.WarehouseFrom;
                        stockDetailBRModel.Version = intVersion;
                        wrapper = _boneRoasterService.UpdateStockDetailTransferQty(stockDetailBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"UpdateStockDetailTransferQty: Cannot update stock detail for {receivePallet.WarehouseFrom}");
                            return wrapper;
                        }

                        //Update the stock movement for FromWarehouse 
                        StockMovementBRModel stockMovementBRModel = new StockMovementBRModel();
                        stockMovementBRModel.CatalogCode = item.CatalogCode;
                        stockMovementBRModel.WarehouseCode = receivePallet.WarehouseFrom;
                        stockMovementBRModel.MoveQty = -1 * item.PalletUnits;
                        stockMovementBRModel.UserId = receivePallet.UserId;
                        stockMovementBRModel.OnHandQty = fltOnHandPre;
                        wrapper = _boneRoasterService.InsertStockMovementTransferPallet(stockMovementBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"InsertStockMovementTransferPallet: Cannot insert stock movement for {receivePallet.WarehouseFrom}");
                            return wrapper;
                        }

                        #endregion

                        #region To Warehouse

                        //Get Lf_OnHandPre for To Warehouse
                        wrapper = _boneRoasterService.GetOnHandPreByCatalogCode(receivePallet.WarehouseTo, item.CatalogCode);
                        if (!wrapper.IsSuccess)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"GetSellingCodeByCatalogCode: Error fetching stock details for {item.CatalogCode} In warehouse {receivePallet.WarehouseFrom}");
                        }

                        fltOnHandPre = (wrapper.ResultSet[0] as StockDetailBRModel).OnHandQty;
                        intVersion = (wrapper.ResultSet[0] as StockDetailBRModel).Version;

                        //Update /Insert the stock details for ToWarehouse 
                        stockDetailBRModel = new StockDetailBRModel();
                        stockDetailBRModel.TransferQty = item.PalletUnits;
                        stockDetailBRModel.CatalogCode = item.CatalogCode;
                        stockDetailBRModel.WarehouseCode = receivePallet.WarehouseTo;
                        stockDetailBRModel.Version = intVersion;
                        wrapper = _boneRoasterService.UpdateStockDetailTransferQty(stockDetailBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"UpdateStockDetailTransferQty: Cannot update stock detail for {receivePallet.WarehouseTo}");
                            return wrapper;
                        }

                        //Update the stock movement for FromWarehouse 
                        stockMovementBRModel = new StockMovementBRModel();
                        stockMovementBRModel.CatalogCode = item.CatalogCode;
                        stockMovementBRModel.WarehouseCode = receivePallet.WarehouseTo;
                        stockMovementBRModel.MoveQty = item.PalletUnits;
                        stockMovementBRModel.UserId = receivePallet.UserId;
                        stockMovementBRModel.OnHandQty = fltOnHandPre;
                        wrapper = _boneRoasterService.InsertStockMovementTransferPallet(stockMovementBRModel);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add($"InsertStockMovementTransferPallet: Cannot insert stock movement for {receivePallet.WarehouseTo}");
                            return wrapper;
                        }

                        #endregion

                        #endregion

                        #region Lu_pallet_header.UpdateWarehouse

                        wrapper = _boneRoasterService.UpdatePalletHeaderWarehouseCode(receivePallet.WarehouseTo, item);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderWarehouseCode: Cannot update pallet header.");
                            return wrapper;
                        }

                        #endregion

                        #region Lu_pallet_header.setBinLocation

                        //Li_RtnStatus = Lu_pallet_header.setBinLocation(bin_location = 'B1.F1.PROD', status = 'P');
                        //Update manifest status - 'D' Line 574
                        wrapper = _boneRoasterService.UpdatePalletHeaderBinLocation("B1.F1.PROD", "P", item);
                        if (!wrapper.IsSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("UpdatePalletHeaderBinLocation: Cannot update bin location status as 'P'.");
                            return wrapper;
                        }

                        #endregion
                    }

                    //Update manifest warehouse by manifest 
                    wrapper = _boneRoasterService.UpdateManifestWarehouseByManifetsNo(receivePallet.ManifestNo, receivePallet.WarehouseTo);
                    if (!wrapper.IsSuccess)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("UpdateManifestWarehouseByManifetsNo: Cannot update manifest info.");
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.Messages.Add("Pallet Transfer Saved.");
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                }
            }

            return wrapper;
        }

        public TransactionWrapper GetAllManifestsByWarehouse(string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            try
            {
                wrapper = _boneRoasterService.GetAllManifestsByWarehouse(warehouseCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                wrapper.IsSuccess = true;
                return wrapper;
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllManifestsByWarehouse : " + e.Message);
                return wrapper;
            }
        }

        public TransactionWrapper GetManifestDetailsByManifestNo(string manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            try
            {
                wrapper = _boneRoasterService.GetManifestDetailsByManifestNo(manifestNo);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                wrapper.IsSuccess = true;
                return wrapper;
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetManifestDetailsByManifestNo : " + e.Message);
                return wrapper;
            }
        }
    }
}
