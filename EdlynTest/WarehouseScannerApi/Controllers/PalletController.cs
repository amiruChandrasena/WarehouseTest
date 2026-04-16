using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Abstractions.ServiceInterfaces;
using Business;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Models;

namespace WarehouseScannerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PalletController : ControllerBase
    {
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IJobService _jobService;
        private readonly PalletBusiness _palletBussiness;

        public PalletController(IPalletService palletService, ICatalogService catalogService, IJobService jobService)
        {
            _palletService = palletService;
            _catalogService = catalogService;
            _jobService = jobService;
            _palletBussiness = new PalletBusiness(_palletService, _catalogService, _jobService);
        }

        [Route("getallrmpalletsbycriteria/{catalogCode}/{warehouseCode}/{rmType?}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllRMPalletsByCriteria(string catalogCode, string warehouseCode, string rmType = "")
        { 
            TransactionWrapper wrapper = new TransactionWrapper();
            
                if (String.IsNullOrEmpty(catalogCode))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllRMPalletsByCriteria: Fail due to missing catalogCode");
                    return wrapper;
                }
                else if (String.IsNullOrEmpty(warehouseCode))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetAllRMPalletsByCriteria: Fail due to missing from warehouse");
                    return wrapper;
                }
                else
                {
                    if (String.IsNullOrEmpty(rmType))
                    {
                        wrapper = _palletBussiness.GetAllRMPalletsByCriteria(catalogCode, warehouseCode);
                    }
                    else
                    {
                        wrapper = _palletBussiness.GetAllRMPalletsByCriteria(rmType, catalogCode, warehouseCode);
                    }

                    return wrapper;
                }
        }

        [Route("getallrmpalletsbycriteriaforissue/{loggedInWarehouse}/{scanData}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetAllRMPalletsByCriteriaforIssue(string loggedInWarehouse, string scanData)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (String.IsNullOrEmpty(scanData))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllRMPalletsByCriteria: Fail due to missing scan data");
                return wrapper;
            }
            else
            {
                bool isNumeric = int.TryParse(scanData, out int scannedNumber);

                if (isNumeric)
                {
                    wrapper = _palletBussiness.GetPalletHeaderByPalletNo(scannedNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    PalletHeader palletHeader = wrapper.ResultSet[0] as PalletHeader;
                    //if (palletHeader.WarehouseId == "QR")
                    //{
                    //    wrapper.IsSuccess = false;
                    //    wrapper.Messages.Add("GetAllRMPalletsByCriteria: Cannot execute warehouse code 'QR'");
                    //    return wrapper;
                    //}

                    if (palletHeader.WarehouseId != loggedInWarehouse)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetPalletHeaderByPalletNo: Scanned warehouse code must be same with logged warehouse code");
                        return wrapper;
                    }

                    wrapper = _palletBussiness.GetAllRMPalletsByCriteria(scannedNumber, "");
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    List<PalletLabelModel> palletLabelModels = wrapper.ResultSet[0] as List<PalletLabelModel>;

                    if (palletLabelModels.Count == 0)
                    {
                        wrapper = _palletBussiness.GetAllRMPalletsByPickingLabel(scanData);
                    }
                }
                else
                {
                    if (scanData.Contains("."))
                    {
                        string[] scanDataSplit = scanData.Split('.');
                        //if (scanDataSplit[1] == "QR")
                        //{
                        //    wrapper.IsSuccess = false;
                        //    wrapper.Messages.Add("GetAllRMPalletsByCriteria: Cannot execute warehouse code 'QR'");
                        //    return wrapper;
                        //}
                        if (scanDataSplit[1] != loggedInWarehouse)
                        {
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetAllRMPalletsByCriteria: Scanned warehouse code must be same with logged warehouse code");
                            return wrapper;
                        }
                        else
                        {
                            wrapper = _palletBussiness.GetAllRMPalletsByCriteria(0, scanData);
                        }
                    }
                    else
                    {
                        wrapper = _palletBussiness.GetAllRMPalletsByPickingLabel(scanData);
                    }
                }
            }

            return wrapper;
        }

        [Route("getrmscanpallet/{palletNo}/{catalogCode}/{warehouseCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRMScanPallet(string palletNo, string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(palletNo))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllRMScanPallet: Fail due to missing pallet number");
                return wrapper;
            }
            else
            {
                wrapper = _palletBussiness.GetRMScanPallet(palletNo, catalogCode, warehouseCode);
                return wrapper;
            }
        }

        [Route("getrmscanpallet/{palletNo}/{warehouseCode}")]
        [HttpGet]
        public ActionResult<TransactionWrapper> GetRMScanPallet(string palletNo, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(palletNo))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetAllRMScanPallet: Fail due to missing planNumber");
                return wrapper;
            }
            else
            {
                wrapper = _palletBussiness.GetRMScanPallet(palletNo, warehouseCode);
                return wrapper;
            }
        }

        [Route("creatermnewpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> CreateRMNewPallet([FromBody] StockTransferRMNewPalletModel newPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (String.IsNullOrEmpty(newPallet.ScannedPalletText))
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CreateRMNewPallet: Please scan a pallet first");
                return wrapper;
            }
            else if (newPallet.OpenPalletNo != 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CreateRMNewPallet: Close current mix pallet " + newPallet.OpenPalletNo.ToString() + " first.");
                return wrapper;
            }
            else
            {
                wrapper = _palletBussiness.CreateRMNewPallet(newPallet.originator, newPallet);
                return wrapper;
            }
        }

        [Route("closermpallet")]
        [HttpPost]
        public ActionResult<TransactionWrapper> CloseRMPallet([FromBody] StockTransferRMClosePalletModel closePallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            if (closePallet.PalletNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CloseRMPallet: Pallet number cannot be empty.");
                return wrapper;
            }
            else if (closePallet.TransferNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("CloseRMPallet: Transfer number cannot be empty.");
                return wrapper;
            }
            else
            {
                wrapper = _palletBussiness.CloseRMPallet(closePallet.originator, closePallet);
                return wrapper;
            }
        }
    }

}